from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import logging
import subprocess
import os
import pandas as pd
import random
import psycopg2
import faker
import requests
from psycopg2 import sql

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure DAG default arguments
default_args = {
    'owner': 'maf',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'realtime_pipeline',
    default_args=default_args,
    description='Automate real-time pipeline from data ingestion to dashboard refresh',
    schedule_interval='@once',
    start_date=datetime(2024, 11, 6),
    catchup=False,
)

# Start Task
start_dag = DummyOperator(task_id='start_dag', dag=dag)

# Task to trigger data generation and insertion into PostgreSQL
generate_data_task = SimpleHttpOperator(
    task_id='generate_data_task',
    method='GET',
    http_conn_id='customer_api',  # Ensure this connection is set up in Airflow connections
    endpoint='api/generate_data',
    headers={"Content-Type": "application/json"},
    dag=dag,
)

# Task to trigger data sending from PostgreSQL to Kafka
send_data_to_kafka_task = SimpleHttpOperator(
    task_id='send_data_to_kafka_task',
    method='GET',
    http_conn_id='customer_api',
    endpoint='api/send_data_to_kafka',
    headers={"Content-Type": "application/json"},
    dag=dag,
)


# Task to trigger sales API to start data production to Kafka
trigger_sales_stream_task = SimpleHttpOperator(
    task_id='trigger_sales_stream_task',
    method='GET',
    http_conn_id='sales_api',
    endpoint='start-stream',
    headers={"Content-Type": "application/json"},
    dag=dag,
)

# Function to run data generator scripts and raise an error if they fail
def run_data_generator(script_path):
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running script {script_path}: {result.stderr}")
        raise AirflowFailException(f"Script {script_path} failed with error: {result.stderr}")
    else:
        logger.info(f"Successfully ran script {script_path}")


# Define data generator tasks for each script
car_data_task = PythonOperator(
    task_id='car_data_task',
    python_callable=lambda: run_data_generator("/opt/airflow/data_generators/car_producer.py"),
    dag=dag,
)

agent_data_task = PythonOperator(
    task_id='agent_data_task',
    python_callable=lambda: run_data_generator("/opt/airflow/data_generators/agent_producer.py"),
    dag=dag,
)

gps_data_task = PythonOperator(
    task_id='gps_data_task',
    python_callable=lambda: run_data_generator("/opt/airflow/data_generators/gps_producer.py"),
    dag=dag,
)

office_data_task = PythonOperator(
    task_id='office_data_task',
    python_callable=lambda: run_data_generator("/opt/airflow/data_generators/office_producer.py"),
    dag=dag,
)

# Spark streaming pipeline task
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

# Task dependencies
start_dag >> [trigger_sales_stream_task, generate_data_task]
generate_data_task >> send_data_to_kafka_task
[car_data_task, agent_data_task, gps_data_task, office_data_task] >> spark_submit_task
trigger_sales_stream_task >> spark_submit_task
send_data_to_kafka_task >> spark_submit_task
spark_submit_task >> influxdb_task >> grafana_refresh_task
