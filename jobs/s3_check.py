import os
import uuid
import boto3
from dotenv import load_dotenv
from loguru import logger
from botocore.exceptions import NoCredentialsError, ClientError

# Load environment variables
load_dotenv()
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")
aws_bucket_name = os.getenv("AWS_BUCKET_NAME")

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

def check_s3_bucket():
    """Check if the S3 bucket exists and is accessible."""
    try:
        s3.head_bucket(Bucket=aws_bucket_name)
        logger.info(f"S3 bucket {aws_bucket_name} exists and is accessible.")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.error(f"Bucket {aws_bucket_name} does not exist.")
        elif error_code == '403':
            logger.error(f"Access denied to bucket {aws_bucket_name}.")
        else:
            logger.error(f"Error accessing bucket {aws_bucket_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def generate_random_file(file_path, size_kb=1):
    """Generate a temporary file with random binary content."""
    try:
        random_content = os.urandom(1024 * size_kb)
        with open(file_path, 'wb') as f:
            f.write(random_content)
        logger.info(f"Generated file: {file_path} ({size_kb}KB)")
        return True
    except Exception as e:
        logger.error(f"Failed to generate file: {e}")
        return False

def upload_to_s3(file_path, s3_key=None):
    """Upload a file to the S3 bucket."""
    if not s3_key:
        s3_key = os.path.basename(file_path)
    
    try:
        s3.upload_file(file_path, aws_bucket_name, s3_key)
        logger.success(f"Uploaded to s3://{aws_bucket_name}/{s3_key}")
        return True
    except NoCredentialsError:
        logger.error("AWS credentials not found!")
    except Exception as e:
        logger.error(f"Upload failed: {e}")
    return False

def main():
    """Main workflow: Check bucket, generate file, upload to S3."""
    if not check_s3_bucket():
        logger.error("Aborting: Bucket check failed.")
        return
    
    # Generate a random file
    temp_file = f"temp_file_{uuid.uuid4().hex}.bin"
    file_size_kb = 100  # 100KB file
    
    if not generate_random_file(temp_file, file_size_kb):
        logger.error("Aborting: File generation failed.")
        return
    
    # Upload to S3
    if upload_to_s3(temp_file):
        logger.info("Cleaning up local file...")
        os.remove(temp_file)
        logger.info("Done!")

if __name__ == "__main__":
    main()