# Bulk File Upload Concurrency Test

This project is a playground for testing concurrent programming applied to bulk file 
upload to Google Cloud Storage (GCS). It compares different upload methods: serial, 
async, multithreading, and multiprocessing.

## Benchmark Results

| Method           | Run 1 (s) | Run 2 (s) | Run 3 (s) | Average (s) |
|------------------|-----------|-----------|-----------|-------------|
| Serial           | 78.92     | 79.20     | 79.05     | 79.06       |
| Async            | 45.41     | 51.37     | 49.30     | 48.69       |
| Multithreading   | 50.34     | 51.92     | 40.83     | 47.70       |
| Multiprocessing  | 38.36     | 43.84     | 41.36     | 41.18       |

System: Apple MacBook Pro M3 Max 14-Core CPU, 32 GB RAM

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/lgingerich/concurrent-file-uploader.git
cd concurrent-file-uploader
```

### 2. Set up virtual environment

#### For macOS/Linux:

```bash
python3 -m venv venv
source python/venv/bin/activate
```

#### For Windows:

```bash
python -m venv venv
python\venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up Google Cloud credentials

1. Create a new project in Google Cloud Console
2. Enable the Google Cloud Storage API
3. Create a service account and download the JSON key file
4. Rename the JSON key file to `gcp-credentials.json` and place it in the project root directory
5. Create a `.env` file in the project root and add the following line:

```
cred_file=gcp-credentials.json
```

## Usage

### 1. Generate test data

To generate test CSV files, run:

```bash
python python/data_generator.py
```

This will create 100 CSV files, each approximately 10MB in size, in the `data` directory.

### 2. Run the upload tests

To run the upload tests, execute:

```bash
python python/main.py
```

This will run the benchmarks for different upload methods and display the results.

## Code Structure

- `python/data_generator.py`: Generates test CSV files
- `python/main.py`: Main script to run benchmarks
- `python/utils.py`: Utility functions for GCS operations
- `python/benchmarks.py`: Implementation of different upload methods

## Customization

You can modify the following parameters in `python/data_generator.py` to adjust the test data:

```python
NUM_FILES = 100
FILE_SIZE_MB = 10
```

To change the number of runs for each benchmark, modify the `num_runs` parameter in the `run_single_benchmark` function call in `python/main.py`.
