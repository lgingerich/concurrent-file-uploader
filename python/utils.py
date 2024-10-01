import os
import asyncio
import logging
from colorlog import ColoredFormatter
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions as google_exceptions

# Function to set up a logger with console handler
def setup_logger(log_level=logging.INFO):
    """
    Set up a logger with a console handler.

    :param log_level: int, logging level
    :return: logging.Logger, configured logger instance
    """
    logger = logging.getLogger("main_logger")
    if not logger.handlers:
        logger.setLevel(log_level)

        # Console handler with color-coded output
        color_scheme = {
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        }
        console_formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(message)s",
            datefmt=None,
            reset=True,
            log_colors=color_scheme,
            secondary_log_colors={},
            style="%",
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger

# Create a single instance of the logger to be used throughout the application
logger = setup_logger()

# Create a Google Cloud Storage client
def create_storage_client(key_path: str) -> storage.Client:
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return storage.Client(credentials=credentials)

# Upload a single file to the specified Google Cloud Storage bucket
def upload_file(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_file: str) -> None:
    source_file_path = os.path.join(data_dir, csv_file)
    if not os.path.exists(source_file_path):
        logger.error(f"File not found: {source_file_path}")
        raise FileNotFoundError(f"File not found: {source_file_path}")
    
    try:
        upload_blob(storage_client, bucket_name, source_file_path)
    except Exception as e:
        logger.error(f"Error uploading {csv_file}: {str(e)}")
        raise

# Upload a single file to the specified Google Cloud Storage bucket
def upload_blob(storage_client: storage.Client, bucket_name: str, source_file_path: str) -> None:
    try:
        bucket = storage_client.bucket(bucket_name)
        source_filename = os.path.basename(source_file_path)
        blob = bucket.blob(source_filename)
        blob.upload_from_filename(source_file_path)
    except google_exceptions.NotFound:
        logger.error(f"Error: Bucket '{bucket_name}' not found.")
    except FileNotFoundError:
        logger.error(f"Error: Source file '{source_file_path}' not found.")
    except google_exceptions.GoogleAPIError as e:
        logger.error(f"Error uploading file: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

# Asynchronous wrapper for the upload_blob function
async def upload_blob_async(storage_client: storage.Client, bucket_name: str, source_file_path: str) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, upload_blob, storage_client, bucket_name, source_file_path)

# Asynchronous wrapper for the upload_file function
async def upload_file_async(storage_client: storage.Client, bucket_name: str, data_dir: str, csv_file: str) -> str:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, upload_file, storage_client, bucket_name, data_dir, csv_file)

# Delete all blobs (files) in the specified Google Cloud Storage bucket
def delete_all_blobs(storage_client: storage.Client, bucket_name: str) -> None:
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            blob.delete()
        logger.info(f"All files in bucket '{bucket_name}' have been deleted.")
    except google_exceptions.NotFound:
        logger.error(f"Error: Bucket '{bucket_name}' not found.")
    except google_exceptions.GoogleAPIError as e:
        logger.error(f"Error deleting files: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
