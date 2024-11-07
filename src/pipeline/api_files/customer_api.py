from flask import Flask, jsonify
import os
import json
import psycopg2
from kafka import KafkaProducer, KafkaAdminClient, NewTopic
import pandas as pd
import random
from datetime import datetime, timedelta
import logging
import faker

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths and configurations
data_file_path = '/app/customer_data.csv'
KAFKA_TOPIC = 'customer_data'
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')

# Database connection details
postgres_config = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5439"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "dbname": os.getenv("POSTGRES_DB", "customer_data")
}

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
logger.info("Kafka producer initialized.")

# Generate Customer Data and save to PostgreSQL
def generate_and_insert_customer_data(total_records=3000):
    logger.info("Starting to generate and insert customer data.")
    fake = faker.Faker()
    customers = [
        {
            'CustomerID': f'CUST{random.randint(1000, 9999)}',
            'MobileNo': fake.phone_number(),
            'Name': fake.name(),
            'Gender': random.choice(['M', 'F']),
            'Age': random.randint(18, 70),
            'Nationality': fake.country(),
            'PassportNo': fake.bothify(text='?####????'),
            'IDNo': fake.bothify(text='###-????-####'),
            'HomeAddress': fake.address(),
            'LeaseStartDate': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            'LeasePeriod': random.choice([6, 12, 24, 36])
        }
        for _ in range(total_records)
    ]
    customer_df = pd.DataFrame(customers)

    try:
        with psycopg2.connect(**postgres_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customer_data (
                        CustomerID VARCHAR(50),
                        MobileNo VARCHAR(50),
                        Name VARCHAR(100),
                        Gender VARCHAR(10),
                        Age INTEGER,
                        Nationality VARCHAR(50),
                        PassportNo VARCHAR(50),
                        IDNo VARCHAR(50),
                        HomeAddress VARCHAR(200),
                        LeaseStartDate DATE,
                        LeasePeriod INTEGER
                    )
                """)
                for _, row in customer_df.iterrows():
                    cursor.execute("""
                        INSERT INTO customer_data (
                            CustomerID, MobileNo, Name, Gender, Age, Nationality, PassportNo, IDNo, HomeAddress, LeaseStartDate, LeasePeriod
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['CustomerID'], row['MobileNo'], row['Name'], row['Gender'], row['Age'], row['Nationality'],
                        row['PassportNo'], row['IDNo'], row['HomeAddress'], row['LeaseStartDate'], row['LeasePeriod']
                    ))
                conn.commit()
                logger.info("Data inserted into PostgreSQL successfully.")
    except Exception as e:
        logger.error("Failed to insert data into PostgreSQL: %s", e)

# Fetch data from PostgreSQL and send to Kafka
def fetch_and_produce_customer_data():
    logger.info("Fetching data from PostgreSQL and sending to Kafka.")
    with psycopg2.connect(**postgres_config) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM customer_data")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        for row in rows:
            data = dict(zip(columns, row))
            producer.send(KAFKA_TOPIC, data)
            logger.info(f"Sent record to Kafka topic '{KAFKA_TOPIC}': {data}")
        producer.flush()

# Flask endpoint to initiate data generation
@app.route("/api/generate_data", methods=["GET"])
def generate_data_endpoint():
    generate_and_insert_customer_data()
    return jsonify({"status": "Data generated and inserted into PostgreSQL."})

# Flask endpoint to send data from PostgreSQL to Kafka
@app.route("/api/send_data_to_kafka", methods=["GET"])
def send_data_to_kafka_endpoint():
    fetch_and_produce_customer_data()
    return jsonify({"status": "Data sent from PostgreSQL to Kafka."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
