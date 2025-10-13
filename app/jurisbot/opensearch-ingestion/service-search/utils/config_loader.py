import os
import yaml
import boto3
from typing import Dict, Any
from dotenv import load_dotenv
from pathlib import Path

class ConfigLoader:
    """Configuration loader for the application."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the configuration loader.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = Path(config_path)
        
        # Load .env file from the same directory as this script
        # This ensures we load from the right location
        env_path = Path(__file__).parent.parent / '.env'
        if env_path.exists():
            load_dotenv(env_path, override=True)  # override=True ensures .env takes precedence
            print(f"Loaded .env from: {env_path}")
        else:
            # Try loading from current directory
            load_dotenv(override=True)
            print("Loading .env from current directory or parent directories")
        
        # Load the YAML configuration
        self.config = self._load_config()
        
        # Inject environment variables into config
        self._inject_env_vars()
        
        # Configure AWS profile/credentials
        self._configure_aws_profile()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _configure_aws_profile(self) -> None:
        """Configure AWS profile and verify credentials."""
        
        # Check if AWS credentials are in environment (from .env or export)
        has_env_creds = all([
            os.environ.get('AWS_ACCESS_KEY_ID'),
            os.environ.get('AWS_SECRET_ACCESS_KEY')
        ])
        
        # If we have environment credentials, verify they're for the right user
        if has_env_creds:
            print(f"AWS credentials found in environment")
            print(f"AWS_ACCESS_KEY_ID: {os.environ.get('AWS_ACCESS_KEY_ID', '')[:4]}...")
        else:
            # Check for AWS profile
            aws_profile = os.environ.get('AWS_PROFILE')
            if aws_profile:
                print(f"Using AWS profile: {aws_profile}")
            else:
                print("Warning: No AWS credentials found in environment")
                print("Boto3 will try to use ~/.aws/credentials or instance role")
        
        # Verify which credentials boto3 is actually using
        try:
            session = boto3.Session()
            credentials = session.get_credentials()
            
            if credentials:
                # Show partial access key for debugging
                access_key = credentials.access_key
                if access_key:
                    key_preview = f"{access_key[:4]}...{access_key[-4:]}"
                    print(f"Boto3 using AWS Access Key: {key_preview}")
                
                # Get the actual identity
                sts = boto3.client('sts')
                identity = sts.get_caller_identity()
                print(f"AWS Identity: {identity['Arn']}")
                
                # Check if it's the correct user
                if 'legal-store-service' in identity['Arn']:
                    print("✓ Correct: Using legal-store-service user")
                else:
                    print("✗ ERROR: Not using legal-store-service user!")
                    print(f"  Current: {identity['Arn']}")
                    print(f"  Expected: arn:aws:iam::808403558610:user/legal-store-service")
                    print("\nTo fix this:")
                    print("1. Create a .env file in your project directory with:")
                    print("   AWS_ACCESS_KEY_ID=<legal-store-service-key>")
                    print("   AWS_SECRET_ACCESS_KEY=<legal-store-service-secret>")
                    print("   AWS_DEFAULT_REGION=ap-southeast-2")
                    print("\n2. Or export these as environment variables before running the script")
            else:
                print("Warning: Could not retrieve AWS credentials from boto3")
                print("Boto3 may not have valid credentials configured")
                
        except Exception as e:
            print(f"Warning: Could not verify AWS identity: {e}")
            print("This may cause issues with AWS services")
    
    def _inject_env_vars(self) -> None:
        """Inject environment variables into configuration."""
        # Database credentials
        if 'DB_USER' in os.environ:
            self.config['database']['source']['username'] = os.environ['DB_USER']
            self.config['database']['destination']['username'] = os.environ['DB_USER']
        
        if 'DB_PASSWORD' in os.environ:
            self.config['database']['source']['password'] = os.environ['DB_PASSWORD']
            self.config['database']['destination']['password'] = os.environ['DB_PASSWORD']
        
        # AWS credentials - these will be used by boto3 automatically
        # We don't need to inject them into config, but we can store them for reference
        if 'AWS_ACCESS_KEY_ID' in os.environ:
            self.config['aws']['access_key_id'] = os.environ['AWS_ACCESS_KEY_ID']
        
        if 'AWS_SECRET_ACCESS_KEY' in os.environ:
            self.config['aws']['secret_access_key'] = os.environ['AWS_SECRET_ACCESS_KEY']
        
        if 'AWS_DEFAULT_REGION' in os.environ:
            self.config['aws']['default_region'] = os.environ['AWS_DEFAULT_REGION']
        elif 'AWS_REGION' in os.environ:
            self.config['aws']['default_region'] = os.environ['AWS_REGION']
        
        # OpenSearch credentials (for non-AWS OpenSearch)
        if 'OPENSEARCH_USER' in os.environ:
            self.config['opensearch']['username'] = os.environ.get('OPENSEARCH_USER', 'admin')
        
        if 'OPENSEARCH_PASSWORD' in os.environ:
            self.config['opensearch']['password'] = os.environ.get('OPENSEARCH_PASSWORD', '')
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.
        
        Args:
            key: Configuration key (dot notation supported)
            default: Default value if key not found
        
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        
        return value