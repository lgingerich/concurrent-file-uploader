import os
import asyncio
import concurrent.futures
import multiprocessing
from typing import List
from google.cloud import storage
from google.oauth2 import service_account

def create_storage_client(key_path: str) -> storage.Client:
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return storage.Client(credentials=credentials)


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
    except storage.exceptions.NotFound:
        print(f"Error: Bucket '{bucket_name}' not found.")
    except FileNotFoundError:
        print(f"Error: Source file '{source_file_path}' not found.")
    except storage.exceptions.GoogleCloudError as e:
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

def serial_upload(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
    for csv_file in csv_files:
        source_file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(source_file_path):
            upload_blob(storage_client, bucket_name, source_file_path)
        else:
            print(f"File not found: {source_file_path}")

async def async_upload(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
    for csv_file in csv_files:
        source_file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(source_file_path):
            await upload_blob_async(storage_client, bucket_name, source_file_path)
        else:
            print(f"File not found: {source_file_path}")

def multithreading_upload(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
    def upload_file(csv_file: str) -> None:
        source_file_path = os.path.join(data_dir, csv_file)
        if os.path.exists(source_file_path):
            upload_blob(storage_client, bucket_name, source_file_path)
        else:
            print(f"File not found: {source_file_path}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(upload_file, csv_files)

# def multiprocessing_upload(key_path: str, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
#     def upload_file(args):
#         storage_client, csv_file = args
#         source_file_path = os.path.join(data_dir, csv_file)
#         if os.path.exists(source_file_path):
#             try:
#                 upload_blob(storage_client, bucket_name, source_file_path)
#                 return f"Uploaded {csv_file}"
#             except Exception as e:
#                 return f"Failed to upload {csv_file}: {str(e)}"
#         else:
#             return f"File not found: {source_file_path}"

#     storage_client = create_storage_client(key_path)
    
#     with multiprocessing.Pool() as pool:
#         results = pool.map(upload_file, [(storage_client, csv_file) for csv_file in csv_files])
    
#     for result in results:
#         print(result)
