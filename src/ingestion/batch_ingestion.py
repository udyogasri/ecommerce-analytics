import os
import sys
import logging
from datetime import datetime
import uuid

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config
from src.etl.spark_session import create_spark_session

logger = logging.getLogger(__name__)

class BatchIngestion:
    def __init__(self, spark_session, target_base_path):
        self.spark = spark_session
        self.target_base_path = target_base_path
        # Use a manifest directory to track processed files
        self.manifest_dir = os.path.join(self.target_base_path, "manifests")
        os.makedirs(self.manifest_dir, exist_ok=True)
        
    def _is_file_processed(self, source_file, entity_name):
        manifest_file = os.path.join(self.manifest_dir, f"{entity_name}_processed.txt")
        if not os.path.exists(manifest_file):
            return False
        with open(manifest_file, 'r') as f:
            processed = f.read().splitlines()
        return source_file in processed

    def _mark_file_processed(self, source_file, entity_name):
        manifest_file = os.path.join(self.manifest_dir, f"{entity_name}_processed.txt")
        with open(manifest_file, 'a') as f:
            f.write(f"{source_file}\n")

    def ingest_csv_to_bronze(self, source_file_path, entity_name, source_system="ecommerce_db"):
        source_file_name = os.path.basename(source_file_path)
        
        # Idempotency check
        if self._is_file_processed(source_file_name, entity_name):
            logger.info(f"File {source_file_name} already processed for {entity_name}. Skipping.")
            return True

        batch_id = str(uuid.uuid4())
        logger.info(f"Starting batch ingestion {batch_id} for {entity_name} from {source_file_name}")
        
        try:
            # Read CSV
            df = self.spark.read.csv(source_file_path, header=True, inferSchema=True)
            
            # Add metadata columns
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("source_file", lit(source_file_name)) \
                   .withColumn("batch_id", lit(batch_id)) \
                   .withColumn("source_system", lit(source_system))
            
            # Write to Bronze Delta Table
            target_path = os.path.join(self.target_base_path, "bronze", entity_name)
            
            # Use append mode for incremental loads
            df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(target_path)
                
            self._mark_file_processed(source_file_name, entity_name)
            logger.info(f"Successfully ingested {source_file_name} to Bronze {entity_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest {source_file_name}: {e}")
            return False

    def process_all_historical(self, raw_dir):
        files = {
            "customers": "customers.csv",
            "products": "products.csv",
            "orders": "orders.csv",
            "order_items": "order_items.csv",
            "support_tickets": "support_tickets.csv"
            # clickstream is meant for streaming, but can be batch loaded if needed. We skip it here.
        }
        
        success = True
        for entity, filename in files.items():
            filepath = os.path.join(raw_dir, filename)
            if os.path.exists(filepath):
                if not self.ingest_csv_to_bronze(filepath, entity):
                    success = False
            else:
                logger.warning(f"Source file {filepath} not found.")
                
        return success
