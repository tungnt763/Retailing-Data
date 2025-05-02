from airflow.decorators import dag, task, task_group
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from lib.utils import get_table_names, upload_to_minio
import shutil
import os

DATA_FOLDER = os.getenv('AIRFLOW_HOME') + '/dags/data'
MINIO_BUCKET_RAW = os.getenv('MINIO_BUCKET_RAW')
table_names = get_table_names()

default_args = {
    'owner': 'tung763',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
@dag(
    schedule_interval='0 0 * * *',  # Cron expression for daily at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['extract', 'minio'],
)
def dag_extract_to_minio():
    
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        fs_conn_id='fs_default',
        filepath='*.csv',
        poke_interval=60,
        timeout=3600,
        mode='reschedule',
        soft_fail=True
    )

    @task
    def upload_file(table_name, **context):
        files = os.listdir(DATA_FOLDER)
        file_count = 0

        for file in files:
            if not file.endswith('.csv') or table_name not in file:
                continue
            
            file_path = os.path.join(DATA_FOLDER, file)
            key = f'{table_name}/{table_name}_{context["ts_nodash"]}.csv'

            if os.path.isfile(file_path):
                upload_to_minio(file_path, MINIO_BUCKET_RAW, key)
                print(f"--- Uploaded {file} to MinIO bucket 'raw' ---")

            archive_dir = os.path.join(DATA_FOLDER, 'archive')

            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)
            
            shutil.copy(file_path, os.path.join(archive_dir, file))
            os.remove(file_path)
            
            print(f"--- Moved {file} to archive folder ---")

            file_count += 1
        
        print(f"--- {file_count} files uploaded and archived for table {table_name} ---")

    @task_group
    def upload_file_group():
        task_group = []

        for table_name in table_names:
            task_group.append(
                upload_file.override(task_id=f'upload_file_{table_name}')(table_name=table_name)
            )

        return task_group
    
    upload_file_group = upload_file_group()

    wait_for_file >> upload_file_group

extract_to_minio_dag = dag_extract_to_minio()