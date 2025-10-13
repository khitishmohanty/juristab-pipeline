import boto3
from typing import Optional
from utils import get_logger

class S3Service:
    """Service for S3 operations."""
    
    def __init__(self, config: dict):
        """
        Initialize S3 service.
        
        Args:
            config: AWS configuration
        """
        self.logger = get_logger(__name__)
        self.bucket_name = config['aws']['s3']['bucket_name']
        
        # Create S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config['aws'].get('access_key_id'),
            aws_secret_access_key=config['aws'].get('secret_access_key'),
            region_name=config['aws']['default_region']
        )
    
    def read_file(self, file_path: str) -> str:
        """
        Read a file from S3.
        
        Args:
            file_path: Path to the file in S3
        
        Returns:
            File content as string
        """
        try:
            # Remove leading slash if present
            if file_path.startswith('/'):
                file_path = file_path[1:]
            
            self.logger.info(f"Reading file from S3: {self.bucket_name}/{file_path}")
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            
            content = response['Body'].read().decode('utf-8')
            self.logger.info(f"Successfully read file: {file_path}")
            
            return content
            
        except Exception as e:
            self.logger.error(f"Error reading file {file_path} from S3: {str(e)}")
            return ""
    
    def file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            file_path: Path to the file in S3
        
        Returns:
            True if file exists, False otherwise
        """
        try:
            if file_path.startswith('/'):
                file_path = file_path[1:]
            
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            return True
            
        except:
            return False