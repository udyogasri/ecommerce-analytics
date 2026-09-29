import logging
from pyspark.sql.functions import col, explode, sequence, to_date, date_format, dayofweek, dayofmonth, month, year, quarter, when
from src.storage.delta_storage import DeltaStorage
import os

logger = logging.getLogger(__name__)

class DimDateBuilder:
    def __init__(self, spark, target_base_path):
        self.spark = spark
        self.target_path = os.path.join(target_base_path, "gold", "dimensions", "dim_date")
        self.storage = DeltaStorage(spark)

    def process(self, start_date="2020-01-01", end_date="2030-12-31"):
        logger.info(f"Building dim_date from {start_date} to {end_date}")
        
        # Generate date sequence
        df = self.spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date")
        
        df = df.withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int")) \
               .withColumn("day", dayofmonth(col("full_date"))) \
               .withColumn("day_of_week", dayofweek(col("full_date"))) \
               .withColumn("day_name", date_format(col("full_date"), "EEEE")) \
               .withColumn("month", month(col("full_date"))) \
               .withColumn("month_name", date_format(col("full_date"), "MMMM")) \
               .withColumn("quarter", quarter(col("full_date"))) \
               .withColumn("year", year(col("full_date"))) \
               .withColumn("year_month", date_format(col("full_date"), "yyyyMM").cast("int")) \
               .withColumn("is_weekend", when(dayofweek(col("full_date")).isin([1, 7]), True).otherwise(False))

        # Idempotent overwrite/merge
        # For a date dimension, we can safely overwrite or merge on date_key
        self.storage.merge_or_insert(df, self.target_path, ["date_key"])
        
        return {"rows": df.count()}
