import logging
import signal
import sys
from flask import Flask, Response, jsonify
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import random
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Kafka configuration
bootstrap_servers = 'kafka:29092'  # Docker inter-container communication
topic = 'sales_data'

# Initialize Kafka producer with retries
producer = None

def initialize_kafka_producer(retries=5, delay=5):
    global producer
    attempt = 0
    while attempt < retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                acks='all',
                max_in_flight_requests_per_connection=1
            )
            logger.info("Kafka producer initialized successfully.")
            return
        except NoBrokersAvailable as e:
            logger.error(f"Failed to initialize Kafka producer (attempt {attempt + 1}/{retries}): {e}")
            attempt += 1
            time.sleep(delay)
    logger.error("Kafka producer initialization failed after multiple attempts.")

initialize_kafka_producer()

def check_topic_availability(topic, timeout=5):
    """Check if the specified Kafka topic is available."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            consumer_timeout_ms=timeout * 1000  # Timeout in milliseconds
        )
        logger.info(f"Successfully subscribed to topic '{topic}'.")
        consumer.close()
        return True
    except Exception as e:
        logger.error(f"Could not subscribe to topic '{topic}': {e}")
        return False

def generate_sales_data():
    """Generates a single sales data record."""
    return {
        'AgentID': f'AGT{random.randint(100, 999)}',
        'OfficeID': f'OFF{random.randint(10, 99)}',
        'CarID': f'CAR{random.randint(100, 999)}',
        'CustomerID': f'CUST{random.randint(1000, 9999)}',
        'Amount': round(random.uniform(10.0, 500.0), 2), 
        'TrxnTimestamp': datetime.utcnow().isoformat()
    }

def send_sales_data_batch(batch_size=5):
    """Generates a batch of sales data and sends each record to Kafka."""
    batch = [generate_sales_data() for _ in range(batch_size)]
    logger.info(f"Generated batch of sales data: {batch}")

    if producer is None:
        logger.error("Kafka producer is not initialized. Cannot send data.")
        return []

    for data in batch:
        try:
            future = producer.send(topic, data)
            future.add_callback(lambda record_metadata: logger.info(
                f"Message sent to {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}: {data}"
            ))
            future.add_errback(lambda exc: logger.error(f"Failed to send data to Kafka: {exc}"))
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")

    # Ensure all messages are flushed to Kafka
    producer.flush()
    return batch

@app.route('/start-stream', methods=['GET'])
def start_stream():
    """Generates and sends a limited number of batches of sales data to Kafka."""
    max_batches = 100  # Define the maximum number of batches to send
    current_batch = 0

    # Check topic availability first
    if not check_topic_availability(topic):
        return jsonify({"error": f"Topic '{topic}' is not available"}), 500

    def generate():
        nonlocal current_batch
        while current_batch < max_batches:
            logger.info("Generating batch of data for streaming...")
            batch_data = send_sales_data_batch(batch_size=5)
            logger.info(f"Streaming batch data to Kafka: {batch_data}")
            yield f"data:{json.dumps(batch_data)}\n\n"
            current_batch += 1
            time.sleep(1)  # Stream data every 1 second
        logger.info("Reached maximum batch limit. Stopping stream.")

    return Response(generate(), mimetype='text/event-stream')


@app.route('/test-produce', methods=['GET'])
def test_produce():
    """Endpoint to manually test a single batch of data production."""
    if producer is None:
        return jsonify({"error": "Kafka producer not initialized"}), 500

    # Check topic availability before sending
    if not check_topic_availability(topic):
        return jsonify({"error": f"Topic '{topic}' is not available"}), 500

    batch_data = send_sales_data_batch(batch_size=5)
    return jsonify(batch_data)

# Graceful shutdown handler
def shutdown_handler(signum, frame):
    logger.info("Shutdown signal received. Closing Kafka producer.")
    if producer:
        producer.close()
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
