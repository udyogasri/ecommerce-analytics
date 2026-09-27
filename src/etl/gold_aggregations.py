import os
import sys
import logging
from pyspark.sql.functions import col, sum, count, current_timestamp, date_format

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config
from src.etl.spark_session import create_spark_session

logger = logging.getLogger(__name__)

class GoldAggregator:
    def __init__(self, spark_session, target_base_path):
        self.spark = spark_session
        self.silver_base = os.path.join(target_base_path, "silver")
        self.gold_base = os.path.join(target_base_path, "gold")

    def aggregate_daily_sales(self):
        logger.info("Aggregating Daily Sales...")
        orders_df = self.spark.read.format("delta").load(os.path.join(self.silver_base, "orders"))
        
        daily_sales = orders_df \
            .withColumn("order_date", date_format(col("order_timestamp"), "yyyy-MM-dd")) \
            .groupBy("order_date") \
            .agg(
                sum("order_total").alias("total_sales"),
                count("order_id").alias("order_count")
            ) \
            .withColumn("gold_timestamp", current_timestamp())
            
        target_path = os.path.join(self.gold_base, "daily_sales")
        daily_sales.write.format("delta").mode("overwrite").save(target_path)
        logger.info("Daily Sales aggregation complete.")
        return True

    def run_all(self):
        self.aggregate_daily_sales()
        logger.info("Gold aggregations completed.")

if __name__ == "__main__":
    spark = create_spark_session("GoldAggregations")
    target_base = os.path.join(config.DATA_DIR, 'local_test', 'lakehouse')
    agg = GoldAggregator(spark, target_base)
    agg.run_all()
    spark.stop()
