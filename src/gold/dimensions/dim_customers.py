import logging
from pyspark.sql.functions import col, date_format
from src.storage.delta_storage import DeltaStorage
import os

logger = logging.getLogger(__name__)

class DimCustomersBuilder:
    def __init__(self, spark, target_base_path):
        self.spark = spark
        self.silver_path = os.path.join(target_base_path, "silver", "customers")
        self.target_path = os.path.join(target_base_path, "gold", "dimensions", "dim_customers")
        self.storage = DeltaStorage(spark)

    def process(self):
        logger.info(f"Building dim_customers from {self.silver_path}")
        
        try:
            df = self.spark.read.format("delta").load(self.silver_path)
        except Exception as e:
            logger.warning(f"Could not read silver customers: {e}")
            return {"rows": 0}
            
        # Select reporting columns, dropping PII like email if not explicitly required, 
        # or keeping it if approved (we drop it here to be safe as per prompt guidelines).
        dim_df = df.select(
            col("customer_id").alias("customer_key"), # Natural key as surrogate for now
            "customer_id",
            "customer_name",
            "customer_segment",
            "city",
            "signup_date"
        ).withColumn("signup_date_key", date_format(col("signup_date"), "yyyyMMdd").cast("int"))

        self.storage.merge_or_insert(dim_df, self.target_path, ["customer_key"])
        return {"rows": dim_df.count()}
