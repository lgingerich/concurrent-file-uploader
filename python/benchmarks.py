import os
from typing import List, Tuple
from google.cloud import storage
from utils import create_storage_client, upload_file, upload_file_async
import concurrent.futures
import asyncio

def serial_upload(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
    for csv_file in csv_files:
        upload_file(storage_client, bucket_name, data_dir, csv_file)

async def async_upload(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_files: List[str]) -> List[Tuple[str, str]]:
    tasks = [upload_file_async(storage_client, bucket_name, data_dir, csv_file) for csv_file in csv_files]
    return await asyncio.gather(*tasks)

def multithreading_upload(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(upload_file, storage_client, bucket_name, data_dir, csv_file) for csv_file in csv_files]
        concurrent.futures.wait(futures)

def upload_file_wrapper(key_path, bucket_name, data_dir, csv_file):
    storage_client = create_storage_client(key_path)
    result = upload_file(storage_client, bucket_name, data_dir, csv_file)
    return result

def multiprocessing_upload(key_path: str, bucket_name: str, data_dir: str, csv_files: List[str]) -> None:
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(upload_file_wrapper, key_path, bucket_name, data_dir, csv_file) for csv_file in csv_files]
        concurrent.futures.wait(futures)