import sys
import os
import logging
import shutil

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config
from src.etl.spark_session import create_spark_session
from src.ingestion.batch_ingestion import BatchIngestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_phase2_batch():
    logger.info("--- Starting Phase 2: Batch Ingestion (Local Test) ---")
    
    # We will use data/local_test as the target base path instead of S3
    target_base = os.path.join(config.DATA_DIR, 'local_test', 'lakehouse')
    raw_dir = config.RAW_DIR
    
    # Clean up previous local test if exists to ensure a fresh test
    if os.path.exists(target_base):
        shutil.rmtree(target_base)
        
    os.makedirs(target_base, exist_ok=True)
    
    # Init Spark
    try:
        spark = create_spark_session("Phase2_Batch_Test")
    except Exception as e:
        logger.error("Failed to start Spark. Ensure HADOOP_HOME is set.")
        return
        
    ingestion = BatchIngestion(spark, target_base)
    
    # Process all historical files
    success = ingestion.process_all_historical(raw_dir)
    
    if success:
        logger.info("All historical files processed successfully.")
    else:
        logger.error("Some files failed to process.")
        
    # Verify by reading back one of the bronze tables
    try:
        customers_bronze_path = os.path.join(target_base, "bronze", "customers")
        df_cust = spark.read.format("delta").load(customers_bronze_path)
        logger.info(f"Verified Bronze Customers Table. Count: {df_cust.count()}")
        df_cust.select("customer_id", "_ingestion_timestamp", "_batch_id").show(5, truncate=False)
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        
    # Test idempotency (run again)
    logger.info("Testing idempotency (should skip already processed files)...")
    ingestion.process_all_historical(raw_dir)
    
    df_cust_after = spark.read.format("delta").load(customers_bronze_path)
    count_after = df_cust_after.count()
    logger.info(f"Count after second run (should be unchanged): {count_after}")
    
    spark.stop()
    logger.info("Phase 2 test complete.")

if __name__ == "__main__":
    test_phase2_batch()
