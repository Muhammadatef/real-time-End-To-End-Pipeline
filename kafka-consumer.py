from kafka import KafkaConsumer
import json
import logging
import signal
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up the consumer to connect to Kafka
consumer = KafkaConsumer(
    'sales_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sales_data_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
logger.info("Connected to Kafka and subscribed to topic 'sales_data'.")

# Define a handler to gracefully exit on Ctrl+C
def signal_handler(sig, frame):
    logger.info("Gracefully closing Kafka consumer...")
    consumer.close()
    sys.exit(0)

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

# Poll and consume messages from the topic
logger.info("Starting to consume messages from 'sales_data' topic...")
try:
    for message in consumer:
        logger.info(f"Received message: {message.value}")
except Exception as e:
    logger.error(f"Error consuming messages: {e}")
finally:
    consumer.close()
