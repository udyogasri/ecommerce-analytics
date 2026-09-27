import os
import sys
import logging
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config
from src.storage.s3_storage import S3Storage

logging.basicConfig(level=getattr(logging, config.LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)

def upload_raw_data(dry_run=False):
    s3_storage = S3Storage()
    
    if not dry_run and not s3_storage.check_bucket_exists():
        logger.error(f"Cannot upload files. Check your bucket configuration and credentials.")
        return
        
    raw_dir = config.RAW_DIR
    files = [
        "customers.csv",
        "products.csv",
        "orders.csv",
        "order_items.csv",
        "clickstream.csv",
        "support_tickets.csv"
    ]
    
    for f in files:
        local_path = os.path.join(raw_dir, f)
        if not os.path.exists(local_path):
            logger.warning(f"File {local_path} not found. Skipping.")
            continue
            
        # Structure: s3://<bucket>/<prefix>/raw/customers/customers.csv
        # Wait, the prompt says:
        # s3://<bucket>/ecommerce-lakehouse/raw/customers/
        # so prefix = ecommerce-lakehouse
        entity_name = f.replace('.csv', '')
        s3_key = f"{config.S3_PREFIX}/raw/{entity_name}/{f}"
        
        s3_storage.upload_file(local_path, s3_key, dry_run=dry_run)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw datasets to S3")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading")
    args = parser.parse_args()
    
    upload_raw_data(dry_run=args.dry_run)
