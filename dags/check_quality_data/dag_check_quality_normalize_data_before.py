from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
from minio import Minio
from minio.commonconfig import CopySource


default_args = {
    'owner': 'tungnt763',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
SPARK_HOME = os.getenv("SPARK_HOME", "/opt/spark")
BUCKET_NAME = os.getenv('MINIO_BUCKET_RAW', "raw")
ARCHIVED_FOLDER = "archived/"
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', "minio")
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', "minio123")

minio_client = Minio(
    MINIO_ENDPOINT,  # Use 'localhost:9000' or correct Docker hostname
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

@dag(
    default_args=default_args,
    description='A DAG to check data quality and normalize data',
    schedule_interval=None,  # Triggered by the external task sensor
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def dag_check_quality_normalize_data_before():
    # Wait for the successful completion of dag_extract_data.py
    # wait_for_extract_data = ExternalTaskSensor(
    #     task_id='wait_for_extract_data',
    #     external_dag_id='dag_extract_data',
    #     external_task_id=None,  # Wait for the entire DAG to complete
    #     mode='poke',
    #     timeout=600,
    #     poke_interval=30,
    # )

    @task
    def check_new_files():
        objects = minio_client.list_objects(BUCKET_NAME, recursive=True)
        files = [obj.object_name for obj in objects]
        if not files:
            raise ValueError("No new files found in the bucket.")
        return files

    @task
    def ensure_cleaned_bucket_exists():
        cleaned_bucket = os.getenv('MINIO_BUCKET_CLEANED', 'cleaned')
        if not minio_client.bucket_exists(cleaned_bucket):
            minio_client.make_bucket(cleaned_bucket)
            print(f"Bucket '{cleaned_bucket}' created.")
        else:
            print(f"Bucket '{cleaned_bucket}' already exists.")

    @task
    def move_files_to_archived(files):
        for file in files:
            destination = f"{ARCHIVED_FOLDER}{file}"
            minio_client.copy_object(
                BUCKET_NAME,
                destination,
                CopySource(BUCKET_NAME, file),
            )
            minio_client.remove_object(BUCKET_NAME, file)
            print(f"Moved file {file} to {destination}")

    # BashOperator to call the Spark application
    process_data = BashOperator(
        task_id='process_data',
        bash_command=f'{os.path.join(SPARK_HOME, "bin", "spark-submit")} --master spark://spark-master:7077 {os.path.join(HOME, "dags", "check_quality_data", "check_quality_normalize_data_before.py")}',
    )

    files = check_new_files()
    ensure_bucket = ensure_cleaned_bucket_exists()
    files >> ensure_bucket >> process_data
    process_data >> move_files_to_archived(files)

dag = dag_check_quality_normalize_data_before()
