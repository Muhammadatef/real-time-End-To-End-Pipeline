import os
import json
import random
import pandas as pd
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

# Configuration
OFFICE_DATA_FOLDER = "/opt/airflow/data/reference/office_data"
KAFKA_TOPIC = "office_data"
BOOTSTRAP_SERVERS = "kafka:29092"
NUM_RECORDS = 1000  # Number of records to generate for the batch

# Ensure the output directory exists
os.makedirs(OFFICE_DATA_FOLDER, exist_ok=True)

# Initialize Faker and Kafka Producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_office_data(num_records=NUM_RECORDS):
    """Generate a batch of office branch data based on the schema provided."""
    office_data = []
    for _ in range(num_records):
        office_data.append({
            'OfficeID': f'OFF{random.randint(10, 99)}',
            'MobileNo': fake.phone_number(),
            'Area': fake.city(),
            'OfficeNo': random.randint(1, 100),
            'WorkingHours': f"{random.randint(8, 12)}:00 - {random.randint(17, 20)}:00"
        })
    return pd.DataFrame(office_data)

def save_office_data_to_csv(data_folder):
    """Generate and save office data to a CSV file in the specified folder."""
    office_df = generate_office_data()
    file_path = os.path.join(data_folder, "office_data.csv")
    office_df.to_csv(file_path, index=False)
    print(f"Office data saved to {file_path}")
    return file_path

def send_office_data_to_kafka(file_path):
    """Read office data from CSV and send it as a batch to the Kafka topic."""
    with open(file_path, mode='r') as file:
        csv_reader = pd.read_csv(file).to_dict(orient="records")
        for record in csv_reader:
            producer.send(KAFKA_TOPIC, record)
            print(f"Sent data to Kafka topic '{KAFKA_TOPIC}': {record}")
    producer.flush()
    print("Finished sending office data to Kafka.")

if __name__ == '__main__':
    # Generate and save office data to CSV
    office_csv_path = save_office_data_to_csv(OFFICE_DATA_FOLDER)

    # Produce the data to the Kafka topic as a batch
    send_office_data_to_kafka(office_csv_path)

    # Close the Kafka producer
    producer.close()
