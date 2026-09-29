import logging
from pyspark.sql.functions import col, sum, countDistinct, count
from src.storage.delta_storage import DeltaStorage
import os

logger = logging.getLogger(__name__)

class DailySalesSummaryBuilder:
    def __init__(self, spark, target_base_path):
        self.spark = spark
        self.target_base_path = target_base_path
        self.target_path = os.path.join(target_base_path, "gold", "marts", "daily_sales_summary")
        self.storage = DeltaStorage(spark)

    def process(self):
        fact_sales_path = os.path.join(self.target_base_path, "gold", "facts", "fact_sales")
        
        logger.info(f"Building daily_sales_summary")
        try:
            df = self.spark.read.format("delta").load(fact_sales_path)
        except Exception as e:
            logger.warning(f"Could not read fact_sales: {e}")
            return {"rows": 0}

        # Aggregate metrics at the daily grain
        mart_df = df.groupBy("order_date_key").agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_key").alias("distinct_customers"),
            sum("quantity").alias("units_sold"),
            sum("gross_sales").alias("gross_sales"),
            sum("discount_amount").alias("discounts"),
            sum("net_sales").alias("net_sales"),
            sum("recognized_sales").alias("recognized_sales")
        ).withColumn("average_order_value", col("net_sales") / col("total_orders"))

        # Merge on date key
        self.storage.merge_or_insert(mart_df, self.target_path, ["order_date_key"])
        
        return {"rows": mart_df.count()}
