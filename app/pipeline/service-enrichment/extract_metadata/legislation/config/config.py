import os
import yaml
from dotenv import load_dotenv

class Config:
    """
    A class to load and manage configuration from a YAML file and environment variables.
    """

    def __init__(self, config_path="config.yaml"):
        """
        Initializes the Config class by loading environment variables and the YAML file.

        Args:
            config_path (str): The path to the configuration file.
        """
        load_dotenv()  # Load environment variables from .env file
        self.config = self._load_config(config_path)

    def _load_config(self, config_path):
        """
        Loads the YAML file and substitutes environment variable placeholders.

        Args:
            config_path (str): The path to the configuration file.

        Returns:
            dict: The loaded and processed configuration dictionary.
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        with open(config_path, 'r') as f:
            raw_config = f.read()

        # Substitute environment variables
        processed_config = self._substitute_env_vars(raw_config)
        return yaml.safe_load(processed_config)

    def _substitute_env_vars(self, config_string):
        """
        Finds and replaces environment variable placeholders in the config string.

        Args:
            config_string (str): The string content of the config file.

        Returns:
            str: The config string with environment variables substituted.
        """
        import re
        # Find all placeholders like "${VAR_NAME}"
        placeholders = re.findall(r'\${([A-Z_]+)}', config_string)
        for placeholder in placeholders:
            env_value = os.getenv(placeholder)
            if env_value is not None:
                config_string = config_string.replace(f"${{{placeholder}}}", env_value)
            else:
                print(f"Warning: Environment variable '{placeholder}' not found.")
        return config_string

    def get(self, *keys):
        """
        Retrieves a value from the configuration using a sequence of keys.

        Args:
            *keys: A variable number of keys to navigate the nested dictionary.

        Returns:
            The value associated with the keys, or None if not found.
        """
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value

