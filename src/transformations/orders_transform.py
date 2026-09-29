import logging
from pyspark.sql.functions import col, to_timestamp, current_timestamp, trim
from src.quality.data_quality import DataQualityFramework
from src.storage.delta_storage import DeltaStorage
import os

logger = logging.getLogger(__name__)

class OrdersTransformer:
    def __init__(self, spark, target_base_path):
        self.spark = spark
        self.target_base_path = target_base_path
        self.dq = DataQualityFramework(spark)
        self.storage = DeltaStorage(spark)

    def process(self):
        # We merge both batch orders and streaming orders if they exist, but for phase 4 
        # let's assume we process the primary bronze/orders
        bronze_path = os.path.join(self.target_base_path, "bronze", "orders")
        silver_path = os.path.join(self.target_base_path, "silver", "orders")
        quarantine_path = os.path.join(self.target_base_path, "quarantine", "orders")
        
        logger.info(f"Reading Bronze Orders from {bronze_path}")
        df = self.spark.read.format("delta").load(bronze_path)
        
        # Transformation Rules
        df_clean = df.withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                     .withColumn("order_status", trim(col("order_status"))) \
                     .withColumn("order_total", col("order_total").cast("double")) \
                     .withColumn("_silver_timestamp", current_timestamp())

        # Quality Rules
        rules = [
            {"name": "order_id_not_null", "expr": "order_id IS NOT NULL"},
            {"name": "customer_id_not_null", "expr": "customer_id IS NOT NULL"},
            {"name": "positive_total", "expr": "order_total >= 0"}
        ]
        
        valid_df, invalid_df = self.dq.enforce_rules(df_clean, rules)
        
        if valid_df.count() > 0:
            self.storage.merge_or_insert(valid_df, silver_path, ["order_id"])
            
        if invalid_df.count() > 0:
            invalid_df.write.format("delta").mode("append").save(quarantine_path)
            
        return {"valid": valid_df.count(), "quarantined": invalid_df.count()}
