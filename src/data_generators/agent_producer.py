import os
import csv
import json
from kafka import KafkaProducer
import faker
import random
import pandas as pd

def generate_agent_data(num_records=200):
    fake = faker.Faker()
    
    agents = []
    for _ in range(num_records):
        agents.append({
            'AgentID': f'AGT{random.randint(100, 999)}',
            'MobileNo': fake.phone_number(),
            'Name': fake.name(),
            'Gender': random.choice(['M', 'F']),
            'Age': random.randint(25, 65),
            'Nationality': fake.country(),
            'OfficeID': f'OFF{random.randint(10, 99)}'
        })
    
    return pd.DataFrame(agents)

def produce_agent_data(bootstrap_servers, topic, file_path):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            producer.send(topic, row)
            print(f"Sent data to topic {topic}: {row}")
    producer.flush()

if __name__ == '__main__':
    # Define the output directory and CSV path
    output_dir = '/opt/airflow/data/reference'  # Adjust path based on container structure
    os.makedirs(output_dir, exist_ok=True)
    agent_csv_path = os.path.join(output_dir, 'agent_data.csv')
    
    # Generate and save the agent data to CSV
    agent_df = generate_agent_data()
    agent_df.to_csv(agent_csv_path, index=False)

    # Produce data to Kafka
    produce_agent_data(
        bootstrap_servers='kafka:29092', 
        topic='agent_data', 
        file_path=agent_csv_path
    )
