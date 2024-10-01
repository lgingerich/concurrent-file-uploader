import os
import random
import string
from faker import Faker
import csv

# Initialize Faker
fake = Faker()

# Set up constants
NUM_FILES = 100
TARGET_SIZE_MB = 10
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, 'data')

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_file(file_number):
    file_name = f'data_file_{file_number:04d}.csv'
    file_path = os.path.join(OUTPUT_DIR, file_name)
    
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Name', 'Email', 'Age', 'City', 'Random Data'])
        
        current_size = 0
        while current_size < TARGET_SIZE_MB * 1024 * 1024:  # Convert MB to bytes
            row = [
                fake.name(),
                fake.email(),
                random.randint(18, 80),
                fake.city(),
                generate_random_string(1000)  # Generate 1000 random characters
            ]
            writer.writerow(row)
            current_size = csvfile.tell()  # Get current file size
    
    print(f"Generated {file_name} ({current_size / (1024 * 1024):.2f} MB)")

# Generate files
for i in range(1, NUM_FILES + 1):
    generate_file(i)

print(f"Generated {NUM_FILES} files in {OUTPUT_DIR}")
