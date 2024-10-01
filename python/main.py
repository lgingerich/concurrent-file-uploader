import os
import time
from google.cloud import storage
from dotenv import load_dotenv
from google.oauth2 import service_account
from sequential_upload import sequential_upload
# from threaded_upload import threaded_upload
# from multiprocessing_upload import multiprocessing_upload

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
KEY_PATH = os.path.join(BASE_DIR, os.getenv("cred_file", ""))

def create_storage_client(key_path):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return storage.Client(credentials=credentials)

def run_single_benchmark(upload_func, num_runs=1):
    total_time = 0
    for _ in range(num_runs):
        start_time = time.time()
        upload_func()
        end_time = time.time()
        total_time += end_time - start_time
    
    return total_time / num_runs

def print_results(name, result):
    print(f"\nResult for {name}:")
    print(f"  Average run time: {result:.2f} seconds")

def run_benchmarks(upload_functions):
    for name, func in upload_functions:
        result = run_single_benchmark(func)
        print_results(name, result)

def main():
    # Setup client
    storage_client = create_storage_client(KEY_PATH)

    bucket_name = "concurrency-test"
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    # Define upload functions with common parameters
    upload_functions = [
        ("Sequential (Python)", lambda: sequential_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
    #     # ("Threaded (Python)", lambda: threaded_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
    #     # ("Multiprocessing (Python)", lambda: multiprocessing_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
    ]

    # Run benchmarks
    run_benchmarks(upload_functions)

if __name__ == "__main__":
    main()