import yaml
from dotenv import load_dotenv
import os
from typing import Any, Dict, List
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    
    """
    A utility class to manage loading configurations from YAML and .env files.
    
    This manager ensures a clean separation between non-sensitive configuration
    (like file paths, model names) and sensitive data (like API keys, passwords),
    which should always be stored in a .env file and never committed to version control.
    """
    
    # Required environment variables
    REQUIRED_ENV_VARS = ['DB_USER', 'DB_PASSWORD', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    
    def __init__(self, config_path: str = 'config/config.yaml', env_path: str = '.env'):
        """
        Initializes the ConfigManager and loads all configurations.

        Args:
            config_path (str): The file path to the main YAML configuration file.
            env_path (str): The file path to the environment variables file.
        """
        self.config_path = config_path
        self.env_path = env_path
        self._config = None
        
        # Load configurations upon initialization
        self._load_environment_variables()
        self._validate_required_env_vars()
        self._load_yaml_config()
        
    def _load_environment_variables(self):
        """
        Loads environment variables from the .env file.
        
        It checks if the file exists and loads it. This makes secrets available
        throughout the application via `os.getenv()`.
        """
        if not os.path.exists(self.env_path):
            logger.warning(f".env file not found at '{self.env_path}'. Skipping.")
            return
            
        logger.info(f"Loading environment variables from: {self.env_path}")
        load_dotenv(dotenv_path=self.env_path)
    
    def _validate_required_env_vars(self):
        """
        Validates that all required environment variables are set.
        
        Raises:
            EnvironmentError: If any required environment variables are missing.
        """
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise EnvironmentError(error_msg)
        
        logger.info("All required environment variables are set.")
        
    def _load_yaml_config(self):
        """
        Loads the main configuration from the specified YAML file.
        
        Raises:
            FileNotFoundError: If the config.yaml file cannot be found.
        """
        try:
            with open(self.config_path, 'r') as f:
                self._config = yaml.safe_load(f)
                logger.info(f"Configuration loaded successfully from: {self.config_path}")
                self._validate_config_structure()
        except FileNotFoundError:
            logger.error(f"Configuration file not found at '{self.config_path}'")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file '{self.config_path}': {e}")
            raise
    
    def _validate_config_structure(self):
        """
        Validates the basic structure of the configuration.
        
        Raises:
            ValueError: If required configuration sections are missing.
        """
        required_sections = ['database', 'aws', 'tables']
        missing_sections = [section for section in required_sections if section not in self._config]
        
        if missing_sections:
            error_msg = f"Missing required configuration sections: {', '.join(missing_sections)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("Configuration structure validation passed.")
        
    def get_config(self) -> Dict[str, Any]:
        """
        Provides access to the loaded configuration dictionary.

        Returns:
            A dictionary containing the configuration from the YAML file.
        """
        if self._config is None:
            raise ValueError("Configuration has not been loaded.")
        return self._config