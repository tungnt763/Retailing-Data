from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule_interval=None, start_date=datetime(2023, 10, 1), catchup=False)
def test_minio():
    """
    Test DAG to check if the MinIO connection is working.
    """
    # This is where you would define your tasks
    @task
    def check_minio_connection():
        """
        Check if the MinIO connection is working.
        """
        # Replace 'minio' with your actual MinIO connection ID
        hook = S3Hook(aws_conn_id='minio')
        if hook.check_for_bucket('raw'):
            print("MinIO connection is working!")
        else:
            if hook.create_bucket(bucket_name='raw'):
                print("Bucket created successfully!")
            else:
                print("Failed to create bucket!")
            print("MinIO connection failed!")
    check_minio_connection()
    
test_minio_dag = test_minio()

