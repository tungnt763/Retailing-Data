from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from lib.utils import read_metadata, create_minio_client
from minio.commonconfig import CopySource
from minio import Minio
import os

HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
SPARK_HOME = os.getenv("SPARK_HOME", "/opt/spark")
SQL_TEMPLATE = "sql_template/audit_load_history.sql"  # <-- CHỈ TÊN TƯƠNG ĐỐI
BUCKET_NAME = os.getenv('MINIO_BUCKET_CLEANED', "cleaned")
ARCHIVED_FOLDER = "archived/"

default_args = {
    'owner': 'tungnt763',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='A DAG to load data from MinIO to PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def dag_load_data_minio_postgres():

    create_table_task = PostgresOperator(
        task_id='create_audit_load_history_table',
        postgres_conn_id='postgres',
        sql=SQL_TEMPLATE,  # Không để os.path.join(HOME, ...)
    )
    
    process_load_data = BashOperator(
        task_id="load_cleaned_bucket_to_postgres",
        bash_command=(
            f'export RUN_ID="{{{{ ti.run_id }}}}"; '
            f'export TASK_ID="{{{{ ti.dag_id }}}}.{{{{ ti.task_id }}}}"; '
            f'{SPARK_HOME}/bin/spark-submit --master spark://spark-master:7077 '
            f'--jars {SPARK_HOME}/jars/postgresql-42.2.23.jar '
            f'{HOME}/dags/load_data/load_data_minio_postgres.py'
        ),
    )

    process_load_weather = BashOperator(
        task_id="load_weather_data_to_postgres",
        bash_command=(
            f'export RUN_ID="{{{{ ti.run_id }}}}"; '
            f'export TASK_ID="{{{{ ti.dag_id }}}}.{{{{ ti.task_id }}}}"; '
            f'{SPARK_HOME}/bin/spark-submit --master spark://spark-master:7077 '
            f'--jars {SPARK_HOME}/jars/postgresql-42.2.23.jar '
            f'{HOME}/dags/load_data/load_data_weather_postgres.py'
        ),
    )

    @task
    def move_files_to_archived():
        minio_client = create_minio_client()

        # List all objects not already in ARCHIVED_FOLDER
        objects = minio_client.list_objects(BUCKET_NAME, prefix="", recursive=True)
        files_to_move = [obj.object_name for obj in objects if not obj.object_name.startswith(ARCHIVED_FOLDER)]
        print(f"Found {len(files_to_move)} files to move.")

        for file in files_to_move:
            destination = f"{ARCHIVED_FOLDER}{file}"
            # Actually copy (MinIO requires dest, source as below)
            minio_client.copy_object(
                BUCKET_NAME,
                destination,
                CopySource(BUCKET_NAME, file),
            )
            # Remove original
            minio_client.remove_object(BUCKET_NAME, file)
            print(f"Moved file {file} to {destination}")

        print("All files moved to archived.")
    move_files_to_archived_task = move_files_to_archived()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_create_serving_table_dag",
        trigger_dag_id="dag_build_serving_table_expand",  # name of your next dag
    )

    create_table_task >> process_load_data >> process_load_weather >> move_files_to_archived_task >> trigger_next

dag = dag_load_data_minio_postgres()
