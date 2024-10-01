import os
import asyncio
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions as google_exceptions

def create_storage_client(key_path: str) -> storage.Client:
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return storage.Client(credentials=credentials)

def upload_file(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_file: str) -> str:
    source_file_path = os.path.join(data_dir, csv_file)
    if not os.path.exists(source_file_path):
        return f"File not found: {source_file_path}"
    
    try:
        upload_blob(storage_client, bucket_name, source_file_path)
    except Exception as e:
        return f"Error uploading {csv_file}: {str(e)}"

def upload_blob(storage_client: storage.Client, bucket_name: str, source_file_path: str) -> None:
    """
    Uploads a single file to the specified Google Cloud Storage bucket.

    Args:
        storage_client (storage.Client): The Google Cloud Storage client.
        bucket_name (str): The name of the bucket to upload to.
        source_file_path (str): The full path of the file to upload.

    Returns:
        None
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        source_filename = os.path.basename(source_file_path)
        blob = bucket.blob(source_filename)
        blob.upload_from_filename(source_file_path)
    except google_exceptions.NotFound:
        print(f"Error: Bucket '{bucket_name}' not found.")
    except FileNotFoundError:
        print(f"Error: Source file '{source_file_path}' not found.")
    except google_exceptions.GoogleAPIError as e:
        print(f"Error uploading file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

async def upload_blob_async(storage_client: storage.Client, bucket_name: str, source_file_path: str) -> None:
    """
    Asynchronous wrapper for the upload_blob function.

    Args:
        storage_client (storage.Client): The Google Cloud Storage client.
        bucket_name (str): The name of the bucket to upload to.
        source_file_path (str): The full path of the file to upload.

    Returns:
        None
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, upload_blob, storage_client, bucket_name, source_file_path)

async def upload_file_async(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_file: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, upload_file, storage_client, bucket_name, data_dir, csv_file)

def delete_all_blobs(storage_client: storage.Client, bucket_name: str) -> None:
    """
    Deletes all blobs (files) in the specified Google Cloud Storage bucket.

    Args:
        storage_client (storage.Client): The Google Cloud Storage client.
        bucket_name (str): The name of the bucket to clear.

    Returns:
        None
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            blob.delete()
        print(f"All files in bucket '{bucket_name}' have been deleted.")
    except google_exceptions.NotFound:
        print(f"Error: Bucket '{bucket_name}' not found.")
    except google_exceptions.GoogleAPIError as e:
        print(f"Error deleting files: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
