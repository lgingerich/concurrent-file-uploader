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
    logger,
    create_storage_client,
    delete_all_blobs,
)

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
KEY_PATH = os.getenv("cred_file")

def run_single_benchmark(name, upload_func, storage_client, bucket_name, num_runs=1):
    total_time = 0
    results = []
    for run in range(1, num_runs + 1):
        logger.info(f"Starting {name} - Run {run}/{num_runs}")
        delete_all_blobs(storage_client, bucket_name)
        start_time = time.time()
        upload_func()
        end_time = time.time()
        run_time = end_time - start_time
        total_time += run_time
        results.append(run_time)
        logger.info(f"Finished {name} - Run {run}/{num_runs}")
    
    average_time = total_time / num_runs
    return average_time, results

def run_benchmarks(upload_functions, storage_client, bucket_name, num_runs=1):
    all_results = {}
    for name, func in upload_functions:
        average_time, run_times = run_single_benchmark(name, func, storage_client, bucket_name, num_runs)
        all_results[name] = {
            "average_time": average_time,
            "run_times": run_times
        }

    print("\n----------------------------------------")
    print("Benchmark Results Summary:")
    print("----------------------------------------")
    for name, data in all_results.items():
        print(f"\n{name}:")
        print(f"  Average run time: {data['average_time']:.2f} seconds")
        if num_runs > 1:
            print(f"  Individual run times: {', '.join([f'{t:.2f}' for t in data['run_times']])} seconds")

def run_async_upload(storage_client, bucket_name, data_dir, csv_files):
    asyncio.run(async_upload(storage_client, bucket_name, data_dir, csv_files))

def main():
    # Setup client
    storage_client = create_storage_client(KEY_PATH)

    bucket_name = "concurrency-test"
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    
    # Define upload functions with common parameters
    upload_functions = [
        ("Serial (Python)", lambda: serial_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
        ("Async (Python)", lambda: run_async_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
        ("Multithreading (Python)", lambda: multithreading_upload(storage_client, bucket_name, DATA_DIR, csv_files)),
        ("Multiprocessing (Python)", lambda: multiprocessing_upload(KEY_PATH, bucket_name, DATA_DIR, csv_files))
    ]

    # Run benchmarks
    run_benchmarks(upload_functions, storage_client, bucket_name, num_runs=3)

if __name__ == "__main__":
    main()