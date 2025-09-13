import boto3
import os
import logging
from botocore.exceptions import NoCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_s3_client(aws_access_key_id, aws_secret_access_key, region_name):
    """
    Initializes and returns an AWS S3 client.
    """
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        return s3_client
    except NoCredentialsError:
        logging.error("AWS credentials not found. Please check your .env file.")
        return None
    except Exception as e:
        logging.error(f"Failed to create S3 client: {e}")
        return None

def download_file_to_memory(s3_client, bucket_name, object_key):
    """
    Downloads a file from S3 to an in-memory byte stream.
    
    Args:
        s3_client: An initialized boto3 S3 client.
        bucket_name (str): The S3 bucket name.
        object_key (str): The key (path) of the file in S3.
        
    Returns:
        BytesIO: A BytesIO object containing the file's content, or None on failure.
    """
    if not s3_client:
        return None

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read()
        logging.info(f"Successfully downloaded file: s3://{bucket_name}/{object_key}")
        return file_content
    except ClientError as e:
        logging.error(f"AWS S3 error for {object_key}: {e.response['Error']['Message']}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while downloading {object_key}: {e}")
        return None
