import sys
import os
import shutil
import logging
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config
from src.etl.spark_session import create_spark_session
from src.storage.s3_storage import S3Storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_python_java():
    logger.info(f"Python Version: {sys.version.split(' ')[0]}")
    java_home = os.environ.get('JAVA_HOME', 'Not Set')
    logger.info(f"JAVA_HOME: {java_home}")
    
def test_spark_and_delta():
    try:
        spark = create_spark_session("ValidationTest")
        logger.info(f"Spark Session created successfully. Version: {spark.version}")
        
        # Test Delta
        test_dir = os.path.join(config.DATA_DIR, 'local_test', 'delta_test')
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
            
        data = [("A", 1), ("B", 2)]
        df = spark.createDataFrame(data, ["letter", "number"])
        df.write.format("delta").save(test_dir)
        
        # Read back
        df_read = spark.read.format("delta").load(test_dir)
        count = df_read.count()
        logger.info(f"Delta table write and read successful. Row count: {count}")
        
        spark.stop()
        return True
    except Exception as e:
        logger.error(f"Spark/Delta validation failed: {e}")
        return False

def test_s3_connection():
    s3 = S3Storage()
    if not config.S3_BUCKET_NAME or config.S3_BUCKET_NAME == 'my-ecommerce-lakehouse-bucket':
        logger.warning("S3_BUCKET_NAME is not configured or is default. AWS operations will require configuration.")
        return False
    
    if s3.check_bucket_exists():
        logger.info("Successfully connected to S3 bucket.")
        return True
    else:
        logger.error("Failed to connect to S3 bucket.")
        return False

def test_kafka():
    try:
        from kafka import KafkaConsumer
        # Try a simple connection with timeout
        consumer = KafkaConsumer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=2000)
        topics = consumer.topics()
        logger.info(f"Kafka connected. Available topics: {topics}")
        return True
    except Exception as e:
        logger.error(f"Kafka connection failed (Expected if not running): {e}")
        return False

def main():
    logger.info("--- Phase 1: Environment Validation ---")
    check_python_java()
    
    logger.info("\n--- Validating Spark and Delta ---")
    test_spark_and_delta()
    
    logger.info("\n--- Validating S3 Connection ---")
    test_s3_connection()
    
    logger.info("\n--- Validating Kafka ---")
    test_kafka()

if __name__ == "__main__":
    main()
