from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from lib.utils import upload_to_minio
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import shutil
import os

DATA_FOLDER = os.getenv('AIRFLOW_HOME') + '/dags/data'
MINIO_BUCKET = 'raw'

default_args = {
    'owner': 'tung763',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
@dag(
    schedule_interval='*/5 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['extract_data'],
)
def dag_extract_data():
    # Task to wait for the file
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='fs_default',
        filepath='*.csv',
        poke_interval=60,
        timeout=300,
        mode='poke'
    )

    # Task to process the file
    @task
    def process_file(**context):
        files = os.listdir(DATA_FOLDER)
        for file in files:
            if not file.endswith('.csv'):
                continue
            # Check if the file is a CSV file
            file_path = os.path.join(DATA_FOLDER, file)
            key = file.replace('.csv', f'_{context["ts_nodash"]}.csv')
            if os.path.isfile(file_path):
                upload_to_minio(file_path, MINIO_BUCKET, key)
                print(f"Uploaded {file} to MinIO bucket 'raw'")

            archive_dir = os.path.join(DATA_FOLDER, 'archive')
            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)
            shutil.copy(file_path, os.path.join(archive_dir, file))
            os.remove(file_path)
            print(f"Moved {file} to archive folder")

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_quality_normalize_dag",
        trigger_dag_id="dag_check_quality_normalize_data_before",  # name of your next dag
    )
        
    # Set task dependencies
    wait_for_file >> process_file() >> trigger_next

extract_data_dag = dag_extract_data()