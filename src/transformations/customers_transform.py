import logging
from pyspark.sql.functions import col, trim, lower, to_timestamp, current_timestamp
from src.quality.data_quality import DataQualityFramework
from src.storage.delta_storage import DeltaStorage
import os

logger = logging.getLogger(__name__)

class CustomersTransformer:
    def __init__(self, spark, target_base_path):
        self.spark = spark
        self.target_base_path = target_base_path
        self.dq = DataQualityFramework(spark)
        self.storage = DeltaStorage(spark)

    def process(self):
        bronze_path = os.path.join(self.target_base_path, "bronze", "customers")
        silver_path = os.path.join(self.target_base_path, "silver", "customers")
        quarantine_path = os.path.join(self.target_base_path, "quarantine", "customers")
        
        logger.info(f"Reading Bronze Customers from {bronze_path}")
        df = self.spark.read.format("delta").load(bronze_path)
        
        # Transformation Rules
        df_clean = df.withColumn("customer_name", trim(col("customer_name"))) \
                     .withColumn("email", lower(trim(col("email")))) \
                     .withColumn("city", trim(col("city"))) \
                     .withColumn("signup_date", to_timestamp(col("signup_date"))) \
                     .withColumn("_silver_timestamp", current_timestamp())

        # Quality Rules
        rules = [
            {"name": "customer_id_not_null", "expr": "customer_id IS NOT NULL"},
            {"name": "email_valid", "expr": "email LIKE '%@%.%'"}
        ]
        
        valid_df, invalid_df = self.dq.enforce_rules(df_clean, rules)
        
        # Deduplication happens inside DeltaStorage by customer_id
        if valid_df.count() > 0:
            self.storage.merge_or_insert(valid_df, silver_path, ["customer_id"])
            
        if invalid_df.count() > 0:
            invalid_df.write.format("delta").mode("append").save(quarantine_path)
            
        return {"valid": valid_df.count(), "quarantined": invalid_df.count()}
