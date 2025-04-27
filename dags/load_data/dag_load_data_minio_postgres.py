from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from lib.utils import read_metadata
import os

HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
SPARK_HOME = os.getenv("SPARK_HOME", "/opt/spark")
SQL_TEMPLATE = "sql_template/audit_load_history.sql"  # <-- CHỈ TÊN TƯƠNG ĐỐI

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

    create_table_task >> process_load_data

dag = dag_load_data_minio_postgres()
