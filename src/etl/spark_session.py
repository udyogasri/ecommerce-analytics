import os
from pyspark.sql import SparkSession
from config.settings import config
import logging

logger = logging.getLogger(__name__)

def create_spark_session(app_name="EcommerceLakehouse"):
    try:
        # We need Delta Lake configured.
        # Ensure compatible versions: PySpark 3.5.x and Delta 3.3.2
        builder = SparkSession.builder.appName(app_name) \
            .master(config.SPARK_MASTER) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1")
        
        # In a real environment, we would also configure AWS credentials for S3 here
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise
