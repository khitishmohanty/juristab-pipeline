import os
import boto3
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_s3_client(config_path='config/config.yaml'):
    """
    Initializes and returns a boto3 S3 client.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    aws_config = config['aws']

    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=aws_config.get('default_region', os.getenv('AWS_DEFAULT_REGION'))
    )

def get_s3_bucket_name(config_path='config/config.yaml'):
    """
    Gets the S3 bucket name from the config file.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['aws']['s3']['bucket_name']

def get_file_from_s3(s3_client, bucket_name, file_path):
    """
    Downloads a file from S3 and returns its content.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error getting file {file_path} from S3: {e}")
        return None
