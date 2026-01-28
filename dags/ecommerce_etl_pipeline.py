from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from src.batch.utils.slack_alert import task_fail_slack_alert

AIRFLOW_HOME = "/opt/airflow"
SRC_PATH = f"{AIRFLOW_HOME}/src"

SPARK_BIN = "/home/airflow/.local/lib/python3.11/site-packages/pyspark/bin/spark-submit"
GCS_JAR = "/opt/airflow/plugins/gcs-connector-hadoop3.jar"

ENV_VARS = {
    "PYTHONPATH": AIRFLOW_HOME,
    "GOOGLE_APPLICATION_CREDENTIALS" : "/opt/airflow/secrets/ecommerce-sa.json",
    "GCS_BUCKET": os.environ.get("GCS_BUCKET")
    
}

default_args = {
    'owner': 'tanh',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': task_fail_slack_alert
}

with DAG(
    dag_id='ecommerce_etl_pipeline',
    default_args=default_args,
    description='Optimized E-commerce Pipeline: Single Spark Job -> BigQuery',
    schedule_interval= '@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'bigquery', 'optimized']
) as dag:
    
    task_spark_etl_driver = BashOperator(
        task_id='spark_full_etl_raw_to_gold',
        bash_command=f'python {SRC_PATH}/etl/main_etl_driver.py',
        env=ENV_VARS
    )
    
    task_load_bq = BashOperator(
        task_id='load_to_bigquery',
        bash_command=f'python {SRC_PATH}/warehouse/load_to_bq.py',
        env=ENV_VARS
    )

task_spark_etl_driver >> task_load_bq