import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    AWS_PROFILE = os.getenv('AWS_PROFILE', 'default')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    S3_PREFIX = os.getenv('S3_PREFIX', 'ecommerce-lakehouse')
    
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_CLICKSTREAM_TOPIC = os.getenv('KAFKA_CLICKSTREAM_TOPIC', 'clickstream')
    KAFKA_ORDERS_TOPIC = os.getenv('KAFKA_ORDERS_TOPIC', 'orders')
    
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
    RAW_DIR = os.path.join(PROJECT_ROOT, 'ecommerce_lakehouse_datasets')

config = Config()
