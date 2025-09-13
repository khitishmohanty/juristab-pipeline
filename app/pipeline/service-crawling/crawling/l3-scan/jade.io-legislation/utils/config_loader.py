import os
import yaml
from dotenv import load_dotenv

def load_config():
    """
    Loads configuration from .env and config.yaml.
    Environment variables will override yaml settings if names conflict.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Load base configuration from YAML file
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
        
    # Override or supplement with environment variables
    config['database']['user'] = os.getenv('DB_USER')
    config['database']['password'] = os.getenv('DB_PASSWORD')
    
    config['aws'] = {
        'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'region': os.getenv('AWS_REGION', 'ap-southeast-2') # Default region
    }
    
    return config

