import os
import sys
import logging
from pyspark.sql.functions import from_json, col, current_timestamp, expr, to_json, struct

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config
from config.schemas import SCHEMAS
from src.etl.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrdersStreamConsumer:
    def __init__(self, spark_session, target_base_path):
        self.spark = spark_session
        self.bronze_dir = os.path.join(target_base_path, "bronze", "orders_stream")
        self.quarantine_dir = os.path.join(target_base_path, "bronze", "quarantine", "orders_stream")
        self.checkpoint_dir = os.path.join(target_base_path, "checkpoints", "orders")
        
        self.kafka_bootstrap = config.KAFKA_BOOTSTRAP_SERVERS
        self.topic = "orders"
        self.schema = SCHEMAS["orders_event"]

    def consume(self):
        logger.info(f"Starting Orders streaming consumer from {self.kafka_bootstrap}, topic {self.topic}")
        
        # Read stream from Kafka
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()
            
        # Extract Kafka metadata and payload
        parsed_df = df.select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_value")
        ).withColumn("parsed_value", from_json(col("raw_value"), self.schema)) \
         .withColumn("ingested_at", current_timestamp())

        # Split into Valid and Invalid (Quarantine)
        # A record is valid if from_json succeeded (not null) and required fields are present
        valid_df = parsed_df.filter(
            col("parsed_value").isNotNull() & 
            col("parsed_value.event_id").isNotNull() & 
            col("parsed_value.order_id").isNotNull()
        ).select(
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "ingested_at",
            "parsed_value.*"
        )
        
        invalid_df = parsed_df.filter(
            col("parsed_value").isNull() | 
            col("parsed_value.event_id").isNull() | 
            col("parsed_value.order_id").isNull()
        ).select(
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "ingested_at",
            "raw_value"
        ).withColumn("rejection_reason", expr("'Malformed JSON or missing required fields'"))

        # Write Valid to Bronze
        valid_query = valid_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", os.path.join(self.checkpoint_dir, "valid")) \
            .trigger(processingTime="5 seconds") \
            .start(self.bronze_dir)
            
        # Write Invalid to Quarantine
        invalid_query = invalid_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", os.path.join(self.checkpoint_dir, "quarantine")) \
            .trigger(processingTime="5 seconds") \
            .start(self.quarantine_dir)

        logger.info("Orders streaming queries started. Awaiting termination...")
        return valid_query, invalid_query

if __name__ == "__main__":
    try:
        spark = create_spark_session("OrdersStreamingConsumer")
        target_base = os.path.join(config.DATA_DIR, 'local_test', 'lakehouse')
        
        consumer = OrdersStreamConsumer(spark, target_base)
        valid_query, invalid_query = consumer.consume()
        
        spark.streams.awaitAnyTermination()
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
