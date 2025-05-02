def extract_file_names_from_metadata(metadata_path):
    """
    Extracts all file names from the new-style metadata JSON file.

    Args:
        metadata_path (str): Path to the metadata JSON file.

    Returns:
        list: A list of file names extracted from the metadata.
    """
    import json

    with open(metadata_path, 'r') as f:
        meta_json = json.load(f)

    file_names = []
    # The key 'tables' is a list of table metadata dicts
    for table in meta_json.get('tables', []):
        if 'file_name' in table:
            file_names.append(table['file_name'])

    return file_names

def upload_to_minio(file_path, bucket_name, key=None):
    """    
    Uploads a file to a MinIO bucket.
    Args:
        file_path (str): The path to the file to be uploaded.
        bucket_name (str): The name of the MinIO bucket.
        key (str, optional): The key under which to store the file in the bucket. 
                             If not provided, the file name will be used.
    """

    from minio import Minio
    import os

    if key is None:
        key = os.path.basename(file_path)

    minio_client = create_minio_client()

    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)

    minio_client.fput_object(bucket_name, key, file_path)
    
    print(f"--- Uploaded {file_path} to bucket {bucket_name} as key {key} ---")

def create_minio_client():
    """
    Creates a MinIO client instance.

    Returns:
        Minio: A MinIO client instance.
    """
    from minio import Minio
    import os

    return Minio(
        os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
        secure=False
    )

def read_metadata():
    """
    Reads metadata from a JSON file.

    Args:
        metadata_path (str): Path to the metadata JSON file.

    Returns:
        dict: Parsed metadata.
    """
    import json
    import os
    HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    metadata_path = os.path.join(HOME, "config", "metadata.json")
    
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found at {metadata_path}")

    with open(metadata_path, 'r') as f:
        return json.load(f)

def get_table_names():
    import os
    import json

    HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    table_metadata_path = os.path.join(HOME, "config", "table_metadata.json")

    if not os.path.exists(table_metadata_path):
        raise FileNotFoundError(f"Table metadata file not found at {table_metadata_path}")

    with open(table_metadata_path, 'r') as f:
        table_metadata = json.load(f)

    table_names = []
    for table_name in table_metadata:
        table_names.append(table_name)
    
    return table_names