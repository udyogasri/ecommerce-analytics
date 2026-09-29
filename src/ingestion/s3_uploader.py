import os
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class S3Uploader:
    """Utility class to upload raw files to Amazon S3."""
    
    def __init__(self, bucket_name, aws_profile=None):
        self.bucket_name = bucket_name
        # Using boto3 session with the AWS credential provider chain
        session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
        self.s3_client = session.client('s3')
        
    def upload_file(self, local_path, s3_prefix):
        """Upload a file to an S3 bucket and return the S3 URI."""
        file_name = os.path.basename(local_path)
        s3_key = os.path.join(s3_prefix, file_name).replace('\\', '/')
        
        logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{s3_key}")
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Upload successful: {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to S3: {e}")
            raise e
