import os
import json
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import faker

# Configuration
CAR_DATA_FOLDER = "/opt/airflow/data/reference/car_data"  # Adjust to the directory being monitored
KAFKA_TOPIC = "car_data"
BOOTSTRAP_SERVERS = "kafka:29092"
CHUNK_SIZE = 10  # Number of records per chunk

# Ensure the output directory exists
os.makedirs(CAR_DATA_FOLDER, exist_ok=True)

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_car_data(num_records=500):
    makes = ['Toyota', 'Honda', 'Nissan', 'BMW', 'Mercedes', 'Audi']
    models = ['Sedan', 'SUV', 'Compact', 'Luxury', 'Sports']
    
    cars = []
    for _ in range(num_records):
        reg_date = datetime.now() - timedelta(days=random.randint(0, 730))
        cars.append({
            'CarID': f'CAR{random.randint(100, 999)}',
            'CarMake': random.choice(makes),
            'CarModel': random.choice(models),
            'PlateNo': f'{random.choice("ABCDEF")}-{random.randint(1000, 9999)}',
            'RegistrationDate': reg_date.strftime('%Y-%m-%d'),
            'RegistrationExpiryDate': (reg_date + timedelta(days=365)).strftime('%Y-%m-%d')
        })
    
    return pd.DataFrame(cars)

def save_generated_data_to_csv(data_folder):
    """Generates car data and saves it to a new CSV file in the specified folder."""
    car_df = generate_car_data()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(data_folder, f"car_data_{timestamp}.csv")
    car_df.to_csv(file_path, index=False)
    print(f"Generated data saved to {file_path}")
    return file_path

def send_data_to_kafka(file_path):
    """Reads a CSV file in chunks and sends each chunk to the Kafka topic."""
    try:
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
            records = chunk.to_dict(orient="records")
            for record in records:
                producer.send(KAFKA_TOPIC, record)
                print(f"Sent record to Kafka topic '{KAFKA_TOPIC}': {record}")
            producer.flush()
            time.sleep(1)  # Optional delay between chunks
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

class NewFileHandler(FileSystemEventHandler):
    """Handler for new files added to the directory."""
    def on_created(self, event):
        if not event.is_directory:
            print(f"New file detected: {event.src_path}")
            send_data_to_kafka(event.src_path)

if __name__ == "__main__":
    # Start monitoring the folder for new files
    event_handler = NewFileHandler()
    observer = Observer()
    observer.schedule(event_handler, CAR_DATA_FOLDER, recursive=False)
    observer.start()
    print(f"Monitoring {CAR_DATA_FOLDER} for new files...")

    # Generate initial data for testing
    save_generated_data_to_csv(CAR_DATA_FOLDER)

    try:
        while True:
            time.sleep(1)  # Keep the observer running
    except KeyboardInterrupt:
        print("Stopping the observer...")
        observer.stop()

    observer.join()
    producer.close()
