import os
from typing import List
from google.cloud import storage

def serial_upload(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
    """
    Uploads multiple CSV files to the specified bucket.

    Args:
        storage_client: The Google Cloud Storage client.
        bucket_name: The name of the bucket to upload to.
        data_dir: The directory containing the CSV files.
        csv_files: A list of CSV filenames to upload.
    """
    for csv_file in csv_files:
        source_file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(source_file_path):
            upload_blob(storage_client, bucket_name, source_file_path)
        else:
            print(f"File not found: {source_file_path}")


def upload_blob(storage_client: storage.Client, bucket_name: str, source_file_path: str) -> None:
    """
    Uploads a single file to the specified bucket.

    Args:
        storage_client: The Google Cloud Storage client.
        bucket_name: The name of the bucket to upload to.
        source_file_path: The full path of the file to upload.
    """
    bucket = storage_client.bucket(bucket_name)
    source_filename = os.path.basename(source_file_path)
    blob = bucket.blob(source_filename)

    blob.upload_from_filename(source_file_path)
    # print(f"File {source_file_path} uploaded to {source_filename} in bucket {bucket_name}.")
