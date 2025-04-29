import boto3
from dotenv import load_dotenv
import os
from loguru import logger

load_dotenv()


aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
aws_bucket_name = os.getenv("AWS_BUCKET_NAME")

def check_s3_bucket():
    """
    Check if the S3 bucket exists and is accessible.
    """
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        s3.head_bucket(Bucket=aws_bucket_name)
        logger.info(f"S3 bucket {aws_bucket_name} exists and is accessible.")
        return True
    except Exception as e:
        logger.error(f"Error accessing S3 bucket {aws_bucket_name}: {e}")
        return False
def main():
    """
    Main function to check the S3 bucket.
    """
    if check_s3_bucket():
        logger.info("S3 bucket check passed.")
    else:
        logger.error("S3 bucket check failed.")
if __name__ == "__main__":
    main()
# This script checks if the S3 bucket exists and is accessible.
