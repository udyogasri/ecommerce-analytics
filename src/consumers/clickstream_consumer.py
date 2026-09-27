import os
import sys
import logging
from pyspark.sql.functions import from_json, col, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config
from src.etl.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickstreamConsumer:
    def __init__(self, spark_session, target_base_path):
        self.spark = spark_session
        self.target_base_path = target_base_path
        self.checkpoint_dir = os.path.join(target_base_path, "checkpoints", "clickstream")
        self.bronze_dir = os.path.join(target_base_path, "bronze", "clickstream")
        
        # Explicit schema for clickstream JSON
        self.schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("source", StringType(), True)
        ])

    def consume(self):
        logger.info(f"Starting Spark Structured Streaming from topic {config.KAFKA_CLICKSTREAM_TOPIC}...")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", config.KAFKA_CLICKSTREAM_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()
            
        # Parse JSON
        parsed_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp") \
            .withColumn("data", from_json(col("json_value"), self.schema)) \
            .select("data.*", "kafka_timestamp")
            
        # Add metadata and deduplicate (watermarking and dropDuplicates)
        # Assuming event_timestamp can be cast to timestamp for watermarking
        processed_df = parsed_df \
            .withColumn("parsed_event_timestamp", expr("CAST(event_timestamp AS TIMESTAMP)")) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withWatermark("parsed_event_timestamp", "10 minutes") \
            .dropDuplicates(["event_id"])
            
        # Write to Bronze Delta table
        query = processed_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", self.checkpoint_dir) \
            .start(self.bronze_dir)
            
        logger.info("Streaming query started. Awaiting termination...")
        return query

if __name__ == "__main__":
    try:
        spark = create_spark_session("ClickstreamConsumer")
        # For local testing, we use local_test/lakehouse
        target_base = os.path.join(config.DATA_DIR, 'local_test', 'lakehouse')
        consumer = ClickstreamConsumer(spark, target_base)
        query = consumer.consume()
        query.awaitTermination()
        spark.stop()
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
