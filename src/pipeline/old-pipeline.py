from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator 
from datetime import datetime, timedelta
import requests
import subprocess
import logging
import json
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'maf',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'realtime_pipeline',
    default_args=default_args,
    description='Automate real-time pipeline from data ingestion to dashboard refresh',
    schedule_interval='@once',
    start_date=datetime(2024, 11, 6),
    catchup=False,
)

# Kafka Topics and Script Paths
KAFKA_TOPICS = {
    "car": "car_data",
    "agent": "agent_data",
    "gps": "gps_data",
    "office": "office_data",
    "customer": "customer_data",
    "sales": "sales_data"
}

SCRIPT_PATHS = {
    "car": "/opt/airflow/data_generators/car_producer.py",
    "agent": "/opt/airflow/data_generators/agent_producer.py",
    "gps": "/opt/airflow/data_generators/gps_producer.py",
    "office": "/opt/airflow/data_generators/office_producer.py",
    "customer": "/opt/airflow/api/customer_api.py",
    "sales": "/opt/airflow/api/sales_api.py"
}

# Helper function to get Kafka connection details
def get_kafka_conn_details(conn_id="kafka_default"):
    kafka_conn = BaseHook.get_connection(conn_id)
    kafka_host = kafka_conn.host
    kafka_port = kafka_conn.port
    return f"{kafka_host}:{kafka_port}"

# Start Task
start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag,
)

# Task 1: Ingest customer data into Oracle database
def ingest_customer_data_to_oracle():
    subprocess.run(["python3", SCRIPT_PATHS["customer"]], check=True)

ingest_customer_data_task = PythonOperator(
    task_id='ingest_customer_data_task',
    python_callable=ingest_customer_data_to_oracle,
    dag=dag,
)

# Task 2: Run customer_api.py to fetch customer data and push it to Kafka topic `customer_data`
def customer_data_to_kafka():
    subprocess.run(["python3", SCRIPT_PATHS["customer"]], check=True)

customer_data_kafka_task = PythonOperator(
    task_id='customer_data_kafka_task',
    python_callable=customer_data_to_kafka,
    dag=dag,
)

# Task to run `sales_api.py` script
run_sales_data_task = PythonOperator(
    task_id='run_sales_data_task',
    python_callable=lambda: subprocess.run(["python3", SCRIPT_PATHS["sales"]], check=True),
    dag=dag,
)

# Task to trigger `sales_api.py` /start-stream endpoint to begin data production to Kafka `sales_data` topic
trigger_sales_stream_task = SimpleHttpOperator(
    task_id='trigger_sales_stream_task',
    method='GET',
    http_conn_id='sales_api',  # Define this connection in the Airflow UI with the correct URL
    endpoint='start-stream',
    headers={"Content-Type": "application/json"},
    dag=dag,
)

# Function to run data generators and send output to Kafka topics
def run_data_generator(script_path, topic_name):
    kafka_conn = get_kafka_conn_details()
    producer = KafkaProducer(
        bootstrap_servers=kafka_conn,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Execute the script to generate data
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running script {script_path}: {result.stderr}")
        return
    
    # Assume the script outputs JSON data on stdout
    try:
        data = json.loads(result.stdout)  # Parse the JSON output from the script
        producer.send(topic_name, data)
        producer.flush()
        logger.info(f"Sent data to topic '{topic_name}': {data}")
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON output from {script_path}")
    except Exception as e:
        logger.error(f"Failed to send data to Kafka topic '{topic_name}': {e}")

# Define the tasks for each data generator
car_data_task = PythonOperator(
    task_id='car_data_task',
    python_callable=lambda: run_data_generator(SCRIPT_PATHS["car"], KAFKA_TOPICS["car"]),
    dag=dag,
)

agent_data_task = PythonOperator(
    task_id='agent_data_task',
    python_callable=lambda: run_data_generator(SCRIPT_PATHS["agent"], KAFKA_TOPICS["agent"]),
    dag=dag,
)

gps_data_task = PythonOperator(
    task_id='gps_data_task',
    python_callable=lambda: run_data_generator(SCRIPT_PATHS["gps"], KAFKA_TOPICS["gps"]),
    dag=dag,
)

office_data_task = PythonOperator(
    task_id='office_data_task',
    python_callable=lambda: run_data_generator(SCRIPT_PATHS["office"], KAFKA_TOPICS["office"]),
    dag=dag,
)

# Spark streaming pipeline task to process and produce to `unified_output_data` topic
spark_submit_task = SparkSubmitOperator(
    task_id='run_spark_streaming_pipeline',
    application='/opt/airflow/pipeline/streaming_pipeline.py',
    conf={'spark.streaming.stopGracefullyOnShutdown': 'true'},
    executor_memory='4G',
    name='pyspark_streaming_pipeline',
    dag=dag,
)

# InfluxDB update task
def update_influxdb():
    hook = InfluxDBHook(conn_id='influxdb_default')
    hook.write(api='write', bucket='sales_aggregates', org='my_org', record='my_aggregated_data')

influxdb_task = PythonOperator(
    task_id='update_influxdb',
    python_callable=update_influxdb,
    dag=dag,
)

# Grafana dashboard refresh task with timeout for error handling
def refresh_grafana_dashboard():
    url = "http://localhost:3000/api/annotations"
    headers = {
        "Authorization": "Bearer <your-grafana-api-key>",
        "Content-Type": "application/json"
    }
    payload = {"dashboardId": 1, "time": datetime.now().timestamp()}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error(f"Failed to refresh Grafana dashboard: {response.text}")
    except requests.Timeout:
        logger.error("Grafana refresh request timed out.")

grafana_refresh_task = PythonOperator(
    task_id='refresh_grafana_dashboard',
    python_callable=refresh_grafana_dashboard,
    dag=dag,
)

# Define task dependencies
start_dag >> [trigger_sales_stream_task, ingest_customer_data_task]
ingest_customer_data_task >> customer_data_kafka_task
[customer_data_kafka_task, car_data_task, agent_data_task, gps_data_task, office_data_task] >> spark_submit_task
trigger_sales_stream_task >> spark_submit_task
spark_submit_task >> influxdb_task >> grafana_refresh_task


