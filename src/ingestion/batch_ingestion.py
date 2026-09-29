import os
import sys
import logging
import uuid
import hashlib
from datetime import datetime, timezone

from pyspark.sql.functions import current_timestamp, lit, md5, concat_ws, col

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config
from config.schemas import SCHEMAS
from src.ingestion.csv_loader import CSVLoader

logger = logging.getLogger(__name__)

class BatchIngestion:
    def __init__(self, spark_session, target_base_path, s3_uploader=None):
        self.spark = spark_session
        self.target_base_path = target_base_path
        self.s3_uploader = s3_uploader
        self.csv_loader = CSVLoader(spark_session)
        self.manifest_path = os.path.join(self.target_base_path, "manifests", "batch_runs")
        
    def _get_file_metadata(self, file_path):
        """Extracts file metadata for the manifest."""
        stat = os.stat(file_path)
        return {
            "file_size": stat.st_size,
            "mod_time": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        }
        
    def _is_file_processed(self, source_file, file_size, mod_time):
        """Check the manifest Delta table to see if this exact file state was already processed."""
        if not os.path.exists(self.manifest_path):
            return False
            
        try:
            manifest_df = self.spark.read.format("delta").load(self.manifest_path)
            # Check if there is a SUCCESS record for this exact file, size, and mod_time
            count = manifest_df.filter(
                (col("source_file") == source_file) &
                (col("file_size") == file_size) &
                (col("mod_time") == mod_time) &
                (col("status") == "SUCCESS")
            ).count()
            return count > 0
        except Exception as e:
            logger.warning(f"Could not read manifest table (might be empty/corrupt): {e}")
            return False

    def _record_manifest(self, source_file, file_size, mod_time, batch_id, status, row_count=0, error_details=""):
        """Record the batch run outcome in the manifest table."""
        # Create a single-row DataFrame
        data = [{
            "source_file": source_file,
            "file_size": file_size,
            "mod_time": mod_time,
            "batch_id": batch_id,
            "status": status,
            "row_count": row_count,
            "ingestion_timestamp": datetime.now(timezone.utc),
            "error_details": error_details
        }]
        df = self.spark.createDataFrame(data)
        
        # Write to Delta table
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(self.manifest_path)

    def ingest_csv_to_bronze(self, source_file_path, entity_name, source_system="ecommerce_db"):
        source_file_name = os.path.basename(source_file_path)
        file_meta = self._get_file_metadata(source_file_path)
        
        # Idempotency check using the manifest
        if self._is_file_processed(source_file_name, file_meta["file_size"], file_meta["mod_time"]):
            logger.info(f"File {source_file_name} with identical size and mod_time already processed. Skipping.")
            return True

        batch_id = str(uuid.uuid4())
        logger.info(f"Starting batch ingestion {batch_id} for {entity_name} from {source_file_name}")
        
        try:
            # Optionally upload to S3 Raw tier first
            if self.s3_uploader:
                s3_raw_prefix = f"raw/{entity_name}"
                self.s3_uploader.upload_file(source_file_path, s3_raw_prefix)
            
            # Load CSV using explicit schema
            schema = SCHEMAS.get(entity_name)
            if not schema:
                raise ValueError(f"No schema defined for entity: {entity_name}")
                
            df = self.csv_loader.load(source_file_path, schema)
            
            # Add required strict metadata columns
            # Calculate a record hash over all source columns for deduplication/tracking
            source_cols = [c for c in df.columns if c != "_corrupt_record"]
            
            df_enriched = df \
                   .withColumn("_ingestion_timestamp", current_timestamp()) \
                   .withColumn("_source_file", lit(source_file_name)) \
                   .withColumn("_batch_id", lit(batch_id)) \
                   .withColumn("_source_system", lit(source_system)) \
                   .withColumn("_record_hash", md5(concat_ws("||", *[col(c).cast("string") for c in source_cols])))
            
            row_count = df_enriched.count()
            
            # Write to Bronze Delta Table
            target_path = os.path.join(self.target_base_path, "bronze", entity_name)
            
            # Use append mode for incremental loads
            df_enriched.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(target_path)
                
            # Record Success in Manifest
            self._record_manifest(source_file_name, file_meta["file_size"], file_meta["mod_time"], batch_id, "SUCCESS", row_count)
            logger.info(f"Successfully ingested {row_count} records from {source_file_name} to Bronze {entity_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest {source_file_name}: {e}")
            self._record_manifest(source_file_name, file_meta["file_size"], file_meta["mod_time"], batch_id, "FAILED", 0, str(e))
            return False

    def process_all_historical(self, raw_dir):
        files = {
            "customers": "customers.csv",
            "products": "products.csv",
            "orders": "orders.csv",
            "order_items": "order_items.csv",
            "support_tickets": "support_tickets.csv"
            # clickstream is handled by Kafka streaming
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
