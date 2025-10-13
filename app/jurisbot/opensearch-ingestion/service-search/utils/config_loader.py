import os
import yaml
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
        load_dotenv()
        self.config = self._load_config()
        self._inject_env_vars()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _inject_env_vars(self) -> None:
        """Inject environment variables into configuration."""
        # Database credentials
        if 'DB_USER' in os.environ:
            self.config['database']['source']['username'] = os.environ['DB_USER']
            self.config['database']['destination']['username'] = os.environ['DB_USER']
        
        if 'DB_PASSWORD' in os.environ:
            self.config['database']['source']['password'] = os.environ['DB_PASSWORD']
            self.config['database']['destination']['password'] = os.environ['DB_PASSWORD']
        
        # AWS credentials
        if 'AWS_ACCESS_KEY_ID' in os.environ:
            self.config['aws']['access_key_id'] = os.environ['AWS_ACCESS_KEY_ID']
        
        if 'AWS_SECRET_ACCESS_KEY' in os.environ:
            self.config['aws']['secret_access_key'] = os.environ['AWS_SECRET_ACCESS_KEY']
        
        if 'AWS_DEFAULT_REGION' in os.environ:
            self.config['aws']['default_region'] = os.environ['AWS_DEFAULT_REGION']
        
        # OpenSearch credentials
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