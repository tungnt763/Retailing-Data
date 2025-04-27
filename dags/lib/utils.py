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

    minio_client = Minio(
        os.getenv('MINIO_ENDPOINT', 'minio:9000'),   # Use 'localhost:9000' or correct Docker hostname
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
        secure=False
    )
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
    minio_client.fput_object(bucket_name, key, file_path)
    print(f"Uploaded {file_path} to bucket {bucket_name} as key {key}")
