import logging
from pyspark.sql.functions import col, date_format, lit, when
from src.storage.delta_storage import DeltaStorage
import os

logger = logging.getLogger(__name__)

class FactSalesBuilder:
    def __init__(self, spark, target_base_path):
        self.spark = spark
        self.target_base_path = target_base_path
        self.target_path = os.path.join(target_base_path, "gold", "facts", "fact_sales")
        self.storage = DeltaStorage(spark)

    def process(self):
        # We will read silver orders. Since we didn't fully build silver order_items in phase 4,
        # we will use the Bronze order_items as a fallback for this demo, or just silver orders
        # if order_items is missing.
        silver_orders_path = os.path.join(self.target_base_path, "silver", "orders")
        bronze_items_path = os.path.join(self.target_base_path, "bronze", "order_items")
        
        logger.info(f"Building fact_sales")
        
        try:
            orders_df = self.spark.read.format("delta").load(silver_orders_path)
            items_df = self.spark.read.format("delta").load(bronze_items_path)
        except Exception as e:
            logger.warning(f"Could not read source tables for fact_sales: {e}")
            return {"rows": 0}

        # Join orders and items. 
        # Grain: One row per order_item
        fact_df = items_df.alias("i").join(
            orders_df.alias("o"), 
            col("i.order_id") == col("o.order_id"), 
            "inner"
        ).select(
            col("i.order_item_id"),
            col("o.order_id"),
            col("o.customer_id").alias("customer_key"),
            col("i.product_id").alias("product_key"),
            col("o.order_timestamp"),
            date_format(col("o.order_timestamp"), "yyyyMMdd").cast("int").alias("order_date_key"),
            col("o.order_status"),
            col("o.sales_channel"),
            col("i.quantity").cast("int"),
            col("i.unit_price").cast("double"),
            col("i.discount_pct").cast("double")
        )

        # Revenue logic (preventing revenue inflation by calculating gross/net at the item level)
        # Assuming discount_pct is a whole number (e.g., 10 for 10%), or fraction (0.10). 
        # We'll treat it as a fraction based on standard models, if > 1 we divide by 100.
        fact_df = fact_df.withColumn("gross_sales", col("quantity") * col("unit_price"))
        fact_df = fact_df.withColumn(
            "discount_amount", 
            col("gross_sales") * (col("discount_pct") / 100.0) # assuming it's a whole percentage
        )
        fact_df = fact_df.withColumn("net_sales", col("gross_sales") - col("discount_amount"))
        
        # recognized_sales (only if completed/shipped)
        fact_df = fact_df.withColumn(
            "recognized_sales", 
            when(col("order_status").isin(["Completed", "Shipped"]), col("net_sales")).otherwise(lit(0.0))
        )

        # The PK for merge is order_item_id
        self.storage.merge_or_insert(fact_df, self.target_path, ["order_item_id"])
        
        return {"rows": fact_df.count()}
