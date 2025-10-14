import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from botocore.config import Config
import logging

logger = logging.getLogger(__name__)

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
            # Configure retry strategy
            retry_config = Config(
                retries={
                    'max_attempts': 3,
                    'mode': 'standard'
                },
                region_name=region_name
            )
            
            self.s3_client = boto3.client(
                's3',
                config=retry_config,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            logger.info("S3Manager initialized successfully.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"AWS credentials not found. Ensure they are set as environment variables or via an IAM role. Details: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during S3 client initialization: {e}")
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
            logger.debug(f"Attempting to retrieve file: s3://{bucket_name}/{file_key}")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            logger.info(f"Successfully retrieved file: s3://{bucket_name}/{file_key}")
            return content
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.error(f"The file was not found at s3://{bucket_name}/{file_key}")
            else:
                logger.error(f"An S3 client error occurred while getting file: {e}")
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
            logger.debug(f"Attempting to save file: s3://{bucket_name}/{file_key} with ContentType: {content_type}")
            self.s3_client.put_object(
                Bucket=bucket_name, 
                Key=file_key, 
                Body=data.encode('utf-8'), 
                ContentType=content_type
            )
            logger.info(f"Successfully saved file: s3://{bucket_name}/{file_key}")
        except ClientError as e:
            logger.error(f"An S3 client error occurred while saving file: {e}")
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
            logger.debug(f"Retrieved size for s3://{bucket_name}/{file_key}: {size} bytes")
            return size
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"File not found when getting size for s3://{bucket_name}/{file_key}")
                return 0
            else:
                logger.error(f"An S3 client error occurred while getting file size: {e}")
                raise
    
    def check_file_exists(self, bucket_name: str, file_key: str) -> bool:
        """
        Checks if a file exists in an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            file_key (str): The full path (key) to the file within the bucket.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=file_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                return False
            else:
                logger.error(f"An S3 client error occurred while checking file existence: {e}")
                raise
    
    def folder_exists(self, bucket_name: str, folder_prefix: str) -> bool:
        """
        Checks if a folder (prefix) exists in S3 by checking if any objects exist with that prefix.

        Args:
            bucket_name (str): The name of the S3 bucket.
            folder_prefix (str): The folder prefix to check (should end with '/')

        Returns:
            bool: True if folder exists (has objects), False otherwise.
        """
        try:
            # Ensure folder_prefix ends with '/'
            if not folder_prefix.endswith('/'):
                folder_prefix += '/'
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder_prefix,
                MaxKeys=1  # We only need to know if at least one object exists
            )
            
            exists = 'Contents' in response and len(response['Contents']) > 0
            if exists:
                logger.debug(f"Folder exists: s3://{bucket_name}/{folder_prefix}")
            else:
                logger.debug(f"Folder does not exist: s3://{bucket_name}/{folder_prefix}")
            return exists
            
        except ClientError as e:
            logger.error(f"Error checking if folder exists: {e}")
            raise
    
    def delete_folder(self, bucket_name: str, folder_prefix: str) -> int:
        """
        Deletes all objects in a folder (prefix) in S3.
        
        Note: In S3, folders don't actually exist - they're just common prefixes.
        This method deletes all objects with the given prefix.

        Args:
            bucket_name (str): The name of the S3 bucket.
            folder_prefix (str): The folder prefix to delete (should end with '/')

        Returns:
            int: Number of objects deleted
        """
        try:
            # Ensure folder_prefix ends with '/'
            if not folder_prefix.endswith('/'):
                folder_prefix += '/'
            
            logger.info(f"Deleting folder: s3://{bucket_name}/{folder_prefix}")
            
            # List all objects in the folder
            objects_to_delete = []
            continuation_token = None
            
            while True:
                list_kwargs = {
                    'Bucket': bucket_name,
                    'Prefix': folder_prefix
                }
                
                if continuation_token:
                    list_kwargs['ContinuationToken'] = continuation_token
                
                response = self.s3_client.list_objects_v2(**list_kwargs)
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        objects_to_delete.append({'Key': obj['Key']})
                
                # Check if there are more objects to list
                if response.get('IsTruncated'):
                    continuation_token = response.get('NextContinuationToken')
                else:
                    break
            
            # Delete objects in batches of 1000 (S3 limit)
            total_deleted = 0
            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    delete_response = self.s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': batch}
                    )
                    deleted = len(delete_response.get('Deleted', []))
                    total_deleted += deleted
                    logger.debug(f"Deleted {deleted} objects from batch")
                
                logger.info(f"Successfully deleted {total_deleted} objects from s3://{bucket_name}/{folder_prefix}")
            else:
                logger.info(f"No objects found to delete in s3://{bucket_name}/{folder_prefix}")
            
            return total_deleted
            
        except ClientError as e:
            logger.error(f"Error deleting folder s3://{bucket_name}/{folder_prefix}: {e}")
            raise
    
    def clear_and_recreate_folder(self, bucket_name: str, folder_prefix: str) -> None:
        """
        Deletes all contents of a folder if it exists, preparing it for fresh content.
        
        Args:
            bucket_name (str): The name of the S3 bucket.
            folder_prefix (str): The folder prefix to clear and recreate
        """
        # Ensure folder_prefix ends with '/'
        if not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        if self.folder_exists(bucket_name, folder_prefix):
            logger.info(f"Folder exists. Clearing contents: s3://{bucket_name}/{folder_prefix}")
            deleted_count = self.delete_folder(bucket_name, folder_prefix)
            logger.info(f"Cleared {deleted_count} files from folder")
        else:
            logger.info(f"Folder does not exist yet: s3://{bucket_name}/{folder_prefix}")
        
        # Note: In S3, we don't need to explicitly "create" a folder.
        # It will be created automatically when we save the first file with that prefix.
        logger.info(f"Folder ready for fresh content: s3://{bucket_name}/{folder_prefix}")