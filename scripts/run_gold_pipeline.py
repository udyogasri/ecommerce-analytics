import os
import sys
import argparse
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config
from src.etl.spark_session import create_spark_session
from src.gold.dimensions.dim_date import DimDateBuilder
from src.gold.dimensions.dim_customers import DimCustomersBuilder
from src.gold.facts.fact_sales import FactSalesBuilder

from src.gold.marts.daily_sales_summary import DailySalesSummaryBuilder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline(args):
    target_base = os.path.join(config.DATA_DIR, 'local_test', 'lakehouse') if args.storage_mode == 'local' else f"s3://{config.S3_BUCKET_NAME}/{config.S3_PREFIX}"
    
    spark = create_spark_session("GoldPipeline_Phase5")
    
    builders = {
        "dim_date": DimDateBuilder(spark, target_base),
        "dim_customers": DimCustomersBuilder(spark, target_base),
        "fact_sales": FactSalesBuilder(spark, target_base),
        "daily_sales_summary": DailySalesSummaryBuilder(spark, target_base)
    }
    
    tables_to_run = list(builders.keys()) if args.all else [args.table]
    
    # Enforce safe ordering for foreign keys and dependencies
    run_order = ["dim_date", "dim_customers", "fact_sales", "daily_sales_summary"]
    tables_to_run = [t for t in run_order if t in tables_to_run]
    
    logger.info("Starting Phase 5 Gold Pipeline")
    
    for table in tables_to_run:
        if args.dry_run or args.validate_only:
            logger.info(f"[DRY-RUN] Would process table {table}")
            continue
            
        try:
            stats = builders[table].process()
            logger.info(f"Successfully processed {table}. Metrics: {stats}")
        except Exception as e:
            logger.error(f"Failed to process {table}: {e}")
            sys.exit(1)
            
    spark.stop()
    logger.info("Gold Pipeline complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Gold Layer ETL Phase 5")
    parser.add_argument("--all", action="store_true", help="Run all tables")
    parser.add_argument("--table", type=str, help="Run specific table")
    parser.add_argument("--storage-mode", choices=["local", "s3"], default="local")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    
    args = parser.parse_args()
    
    if not args.all and not args.table:
        parser.error("Must specify either --all or --table <table_name>")
        
    run_pipeline(args)
