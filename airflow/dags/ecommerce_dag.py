from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# The DAG orchestrates the Bronze -> Silver -> Gold pipeline
with DAG(
    'ecommerce_lakehouse_pipeline',
    default_args=default_args,
    description='E-commerce Data Lakehouse ETL Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ecommerce', 'etl'],
) as dag:

    # Path to the python executable in your virtual environment
    # Note: When running in WSL2, ensure paths are updated to the Linux paths
    PYTHON_BIN = "/path/to/venv/bin/python"
    PROJECT_DIR = "/path/to/ecommerce-analytics"
    
    # Task 1: Batch Ingestion (Raw to Bronze)
    ingest_bronze = BashOperator(
        task_id='ingest_raw_to_bronze',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} scripts/run_phase2.py"
    )

    # Task 2: Silver Transformations (Bronze to Silver)
    transform_silver = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} src/etl/silver_transformations.py"
    )

    # Task 3: Gold Aggregations (Silver to Gold)
    aggregate_gold = BashOperator(
        task_id='aggregate_silver_to_gold',
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} src/etl/gold_aggregations.py"
    )

    # Define Dependencies
    ingest_bronze >> transform_silver >> aggregate_gold
