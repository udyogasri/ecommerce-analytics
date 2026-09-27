import boto3
import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_glue_database(database_name):
    client = boto3.client('glue', region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
    try:
        client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database for E-commerce Data Lakehouse'
            }
        )
        logger.info(f"Successfully created Glue database: {database_name}")
    except client.exceptions.AlreadyExistsException:
        logger.info(f"Glue database {database_name} already exists.")
    except Exception as e:
        logger.error(f"Failed to create database: {e}")

if __name__ == "__main__":
    create_glue_database("ecommerce_lakehouse")
