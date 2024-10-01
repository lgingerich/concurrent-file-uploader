import os
import time
import asyncio
from dotenv import load_dotenv
from benchmarks import (
    serial_upload,
    async_upload,
    multithreading_upload,
    multiprocessing_upload
)
from utils import (
    create_storage_client,
    delete_all_blobs,
)

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
KEY_PATH = os.getenv("cred_file")

def run_single_benchmark(name, upload_func, storage_client, bucket_name, num_runs=1):
    total_time = 0
    for run in range(1, num_runs + 1):
        print("\n----------------------------------------")
        print(f"Starting {name} - Run {run}/{num_runs}")
        delete_all_blobs(storage_client, bucket_name)
        start_time = time.time()
        # upload_func()
        results = upload_func()
        end_time = time.time()
        run_time = end_time - start_time
        total_time += run_time
        print(f"Finished {name} - Run {run}/{num_runs} in {run_time:.2f} seconds")
        if results:
            print(f"Upload results: {results}")
    
    average_time = total_time / num_runs
    return average_time

def run_benchmarks(upload_functions, storage_client, bucket_name):
    for name, func in upload_functions:
        result = run_single_benchmark(name, func, storage_client, bucket_name)
        print(f"\nResult for {name}:")
        print(f"  Average run time: {result:.2f} seconds")

def run_async_upload(storage_client, bucket_name, data_dir, csv_files):
    asyncio.run(async_upload(storage_client, bucket_name, data_dir, csv_files))

def main():
    # Setup client
    storage_client = create_storage_client(KEY_PATH)

    bucket_name = "concurrency-test"
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    # Define upload functions with common parameters
    upload_functions = [
        # ("Serial (Python)", lambda: serial_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
        # ("Async (Python)", lambda: run_async_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
        # ("Multithreading (Python)", lambda: multithreading_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
        # ("Multiprocessing (Python)", lambda: multiprocessing_upload(storage_client, bucket_name, DATA_DIR, csv_files))
        ("Multiprocessing (Python)", lambda: multiprocessing_upload(KEY_PATH, bucket_name, DATA_DIR, csv_files))
    ]

    # Run benchmarks
    run_benchmarks(upload_functions, storage_client, bucket_name)

if __name__ == "__main__":
    main()