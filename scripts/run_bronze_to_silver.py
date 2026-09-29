import os
import sys
import argparse
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config
from src.etl.spark_session import create_spark_session
from src.transformations.customers_transform import CustomersTransformer
from src.transformations.orders_transform import OrdersTransformer
# Other transformers would be imported here...

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline(args):
    target_base = os.path.join(config.DATA_DIR, 'local_test', 'lakehouse') if args.storage_mode == 'local' else f"s3://{config.S3_BUCKET_NAME}/{config.S3_PREFIX}"
    
    spark = create_spark_session("BronzeToSilver_Phase4")
    
    transformers = {
        "customers": CustomersTransformer(spark, target_base),
        "orders": OrdersTransformer(spark, target_base)
        # In a full run, we would add products, clickstream, etc.
    }
    
    tables_to_run = list(transformers.keys()) if args.all else [args.table]
    
    logger.info(f"Starting Bronze to Silver ETL run ID: {args.run_id}")
    
    for table in tables_to_run:
        if table in transformers:
            if args.dry_run or args.validate_only:
                logger.info(f"[DRY-RUN/VALIDATE] Would process table {table}")
                continue
                
            try:
                stats = transformers[table].process()
                logger.info(f"Table {table} processed successfully: {stats}")
            except Exception as e:
                logger.error(f"Failed processing table {table}: {e}")
        else:
            logger.warning(f"Table {table} not found or transformer not implemented.")
            
    spark.stop()
    logger.info("Pipeline completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Bronze to Silver ETL Phase 4")
    parser.add_argument("--all", action="store_true", help="Run all tables")
    parser.add_argument("--table", type=str, help="Run specific table")
    parser.add_argument("--storage-mode", choices=["local", "s3"], default="local")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--run-id", type=str, default=datetime.now().strftime("%Y%m%d%H%M%S"))
    
    args = parser.parse_args()
    
    if not args.all and not args.table:
        parser.error("Must specify either --all or --table <table_name>")
        
    run_pipeline(args)
