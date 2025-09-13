import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

class S3Manager:
    """
    Handles all interactions with AWS S3.
    """
    def __init__(self, region_name: str):
        """
        Initializes the S3 client.
        
        For Fargate deployment, authentication is best handled via an IAM Task Role.
        Boto3 will automatically use the credentials provided by the Task Role.

        Args:
            region_name (str): The AWS region for the S3 bucket.
        """
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=region_name,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            print("S3Manager initialized successfully.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error: AWS credentials not found. Ensure they are set as environment variables or via an IAM role. Details: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred during S3 client initialization: {e}")
            raise

    def get_file_content(self, bucket_name: str, file_key: str) -> str:
        """
        Retrieves the content of a file from an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            file_key (str): The full path (key) to the file within the bucket.

        Returns:
            str: The content of the file, decoded as UTF-8.
        """
        try:
            print(f"Attempting to retrieve file: s3://{bucket_name}/{file_key}")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            print(f"Successfully retrieved file: s3://{bucket_name}/{file_key}")
            return content
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Error: The file was not found at s3://{bucket_name}/{file_key}")
            else:
                print(f"An S3 client error occurred while getting file: {e}")
            raise

    def save_text_file(self, bucket_name: str, file_key: str, data: str):
        """
        Saves a string of data to a text file in an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            file_key (str): The full path (key) where the file will be saved.
            data (str): The string data to save.
        """
        try:
            print(f"Attempting to save file: s3://{bucket_name}/{file_key}")
            self.s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=data, ContentType='text/plain')
            print(f"Successfully saved file: s3://{bucket_name}/{file_key}")
        except ClientError as e:
            print(f"An S3 client error occurred while saving text file: {e}")
            raise

    def save_json_file(self, bucket_name: str, file_key: str, data: str):
        """
        Saves a string of JSON data to a file in an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            file_key (str): The full path (key) where the file will be saved.
            data (str): The JSON string data to save.
        """
        try:
            print(f"Attempting to save JSON file: s3://{bucket_name}/{file_key}")
            self.s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=data, ContentType='application/json')
            print(f"Successfully saved JSON file: s3://{bucket_name}/{file_key}")
        except ClientError as e:
            print(f"An S3 client error occurred while saving JSON file: {e}")
            raise
