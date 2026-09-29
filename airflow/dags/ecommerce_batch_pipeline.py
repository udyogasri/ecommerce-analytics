import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

# The Windows path to the project (converted for powershell use)
WINDOWS_PROJECT_DIR = "C:\\Users\\allub\\ecommerce-analytics"
ACTIVATE_VENV = f"cd {WINDOWS_PROJECT_DIR}; .\\venv\\Scripts\\activate"

def create_windows_spark_task(task_id, python_script, args=""):
    """
    Helper to execute a Python script in the Windows environment from WSL2.
    Uses WSL2's native powershell.exe interop.
    """
    command = f"powershell.exe -ExecutionPolicy Bypass -Command \"{ACTIVATE_VENV}; python {python_script} {args}\""
    return BashOperator(
        task_id=task_id,
        bash_command=command
    )

with DAG(
    'ecommerce_batch_pipeline',
    default_args=default_args,
    description='End-to-end batch pipeline for E-commerce Data Lakehouse',
    schedule_interval='0 2 * * *', # Run daily at 2 AM UTC
    start_date=datetime(2023, 1, 1),
    catchup=False, # Do not backfill historically by default to save resources
    tags=['lakehouse', 'ecommerce', 'batch'],
    max_active_runs=1, # Prevent concurrent runs from overlapping and corrupting Delta logs
) as dag:

    # 1. Validation
    validate_env = BashOperator(
        task_id='validate_runtime_config',
        bash_command=f"powershell.exe -Command \"Test-Path {WINDOWS_PROJECT_DIR}\\.env.example\""
    )

    # 2. Bronze Ingestion
    with TaskGroup('bronze_ingestion') as bronze_ingestion:
        # Currently, run_phase2.py processes all configured CSVs. 
        # In a fully modular setup, we'd split these out.
        ingest_all = create_windows_spark_task('ingest_csv_to_bronze', 'scripts/run_phase2.py')

    # 3. Bronze-to-Silver ETL
    with TaskGroup('silver_transformations') as silver_transformations:
        transform_customers = create_windows_spark_task('transform_customers', 'scripts/run_bronze_to_silver.py', '--table customers --storage-mode local')
        transform_orders = create_windows_spark_task('transform_orders', 'scripts/run_bronze_to_silver.py', '--table orders --storage-mode local')
        
        # Parallel execution for silver tables
        [transform_customers, transform_orders]

    # 4. Silver-to-Gold ETL
    with TaskGroup('gold_analytics') as gold_analytics:
        # Dimensions must precede facts
        build_dim_date = create_windows_spark_task('build_dim_date', 'scripts/run_gold_pipeline.py', '--table dim_date --storage-mode local')
        build_dim_customers = create_windows_spark_task('build_dim_customers', 'scripts/run_gold_pipeline.py', '--table dim_customers --storage-mode local')
        
        build_fact_sales = create_windows_spark_task('build_fact_sales', 'scripts/run_gold_pipeline.py', '--table fact_sales --storage-mode local')
        
        build_daily_sales = create_windows_spark_task('build_daily_sales_summary', 'scripts/run_gold_pipeline.py', '--table daily_sales_summary --storage-mode local')

        [build_dim_date, build_dim_customers] >> build_fact_sales >> build_daily_sales

    # Define the execution graph
    validate_env >> bronze_ingestion >> silver_transformations >> gold_analytics
