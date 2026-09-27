import boto3
from botocore.exceptions import ClientError
import logging
from config.settings import config
import os

logger = logging.getLogger(__name__)

class S3Storage:
    def __init__(self):
        self.bucket = config.S3_BUCKET_NAME
        self.prefix = config.S3_PREFIX
        try:
            self.session = boto3.Session(
                profile_name=config.AWS_PROFILE,
                region_name=config.AWS_REGION
            )
            self.s3_client = self.session.client('s3')
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            self.s3_client = None

    def check_bucket_exists(self):
        if not self.s3_client:
            return False
        if not self.bucket:
            logger.error("S3_BUCKET_NAME is not set in configuration.")
            return False
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '403':
                logger.error(f"Access denied to bucket: {self.bucket}")
            elif error_code == '404':
                logger.error(f"Bucket {self.bucket} does not exist.")
            else:
                logger.error(f"Unexpected error when accessing bucket: {e}")
            return False

    def upload_file(self, local_path, s3_key, dry_run=False):
        if dry_run:
            logger.info(f"[DRY RUN] Would upload {local_path} to s3://{self.bucket}/{s3_key}")
            return True

        if not self.check_bucket_exists():
            return False

        try:
            self.s3_client.upload_file(local_path, self.bucket, s3_key)
            logger.info(f"Successfully uploaded {local_path} to s3://{self.bucket}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False
