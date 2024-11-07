from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Configure DAG default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'streaming_pipeline_trigger',
    default_args=default_args,
    description='DAG to trigger the streaming API for Kafka pipeline',
    schedule_interval=timedelta(minutes=1),  # Adjust based on your needs
    start_date=datetime(2024, 11, 6),
    catchup=False,
)

# Function to log successful API trigger
def log_success():
    logging.info("Successfully triggered the API to start the streaming process.")

# Task to trigger the start-stream endpoint
trigger_start_stream = SimpleHttpOperator(
    task_id='trigger_start_stream',
    method='GET',
    http_conn_id='api_connection',  # Configure this connection in Airflow
    endpoint='/start-stream',
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.status_code == 200,
    log_response=True,
    dag=dag,
)

# Task to log success message after triggering API
log_task = PythonOperator(
    task_id='log_success_message',
    python_callable=log_success,
    dag=dag,
)

# Set task dependencies
trigger_start_stream >> log_task
