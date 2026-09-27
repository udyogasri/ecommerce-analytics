import os
import sys
import logging
from pyspark.sql.functions import col, current_timestamp, to_timestamp

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config
from src.etl.spark_session import create_spark_session

logger = logging.getLogger(__name__)

class SilverTransformer:
    def __init__(self, spark_session, target_base_path):
        self.spark = spark_session
        self.bronze_base = os.path.join(target_base_path, "bronze")
        self.silver_base = os.path.join(target_base_path, "silver")

    def transform_customers(self):
        logger.info("Transforming customers to Silver...")
        df = self.spark.read.format("delta").load(os.path.join(self.bronze_base, "customers"))
        
        # Clean: Deduplicate by customer_id (keep latest ingestion if multiple)
        df_clean = df.dropDuplicates(["customer_id"]) \
                     .withColumn("signup_date", to_timestamp(col("signup_date"))) \
                     .withColumn("silver_timestamp", current_timestamp())
                     
        # Write to Silver
        df_clean.write.format("delta").mode("overwrite").save(os.path.join(self.silver_base, "customers"))
        return True

    def transform_orders(self):
        logger.info("Transforming orders to Silver...")
        df = self.spark.read.format("delta").load(os.path.join(self.bronze_base, "orders"))
        
        df_clean = df.dropDuplicates(["order_id"]) \
                     .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                     .withColumn("order_total", col("order_total").cast("double")) \
                     .filter(col("order_total") > 0) \
                     .withColumn("silver_timestamp", current_timestamp())
                     
        df_clean.write.format("delta").mode("overwrite").save(os.path.join(self.silver_base, "orders"))
        return True
        
    def transform_order_items(self):
        logger.info("Transforming order_items to Silver...")
        df = self.spark.read.format("delta").load(os.path.join(self.bronze_base, "order_items"))
        
        df_clean = df.dropDuplicates(["order_item_id"]) \
                     .withColumn("silver_timestamp", current_timestamp())
                     
        df_clean.write.format("delta").mode("overwrite").save(os.path.join(self.silver_base, "order_items"))
        return True
        
    def transform_products(self):
        logger.info("Transforming products to Silver...")
        df = self.spark.read.format("delta").load(os.path.join(self.bronze_base, "products"))
        
        df_clean = df.dropDuplicates(["product_id"]) \
                     .withColumn("silver_timestamp", current_timestamp())
                     
        df_clean.write.format("delta").mode("overwrite").save(os.path.join(self.silver_base, "products"))
        return True
        
    def run_all(self):
        self.transform_customers()
        self.transform_products()
        self.transform_orders()
        self.transform_order_items()
        logger.info("Silver transformations completed successfully.")

if __name__ == "__main__":
    spark = create_spark_session("SilverTransformations")
    target_base = os.path.join(config.DATA_DIR, 'local_test', 'lakehouse')
    transformer = SilverTransformer(spark, target_base)
    transformer.run_all()
    spark.stop()
