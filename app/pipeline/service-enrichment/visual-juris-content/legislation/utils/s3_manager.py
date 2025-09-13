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
        Ensure the Task Role has the necessary S3 permissions (GetObject, PutObject, HeadObject).

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
        
        Raises:
            ClientError: If the file is not found or another S3 error occurs.
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

    def save_text_file(self, bucket_name: str, file_key: str, data: str, content_type: str = 'text/plain'):
        """
        Saves a string of data to a text file in an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            file_key (str): The full path (key) where the file will be saved.
            data (str): The string data to save.
            content_type (str): The MIME type of the file (e.g., 'text/plain', 'text/html').
        """
        try:
            print(f"Attempting to save file: s3://{bucket_name}/{file_key} with ContentType: {content_type}")
            self.s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=data.encode('utf-8'), ContentType=content_type)
            print(f"Successfully saved file: s3://{bucket_name}/{file_key}")
        except ClientError as e:
            print(f"An S3 client error occurred while saving file: {e}")
            raise
            
    def get_file_size(self, bucket_name: str, file_key: str) -> int:
        """
        Retrieves the size of a file in an S3 bucket in bytes.

        Args:
            bucket_name (str): The name of the S3 bucket.
            file_key (str): The full path (key) to the file within the bucket.

        Returns:
            int: The size of the file in bytes. Returns 0 if the file is not found.
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=file_key)
            size = response['ContentLength']
            print(f"Retrieved size for s3://{bucket_name}/{file_key}: {size} bytes")
            return size
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Warning: File not found when getting size for s3://{bucket_name}/{file_key}")
                return 0
            else:
                print(f"An S3 client error occurred while getting file size: {e}")
                raise