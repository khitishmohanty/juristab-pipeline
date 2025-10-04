import os
import yaml
from dotenv import load_dotenv
from pathlib import Path

def load_config():
    """
    Loads configuration from config.yaml and .env files using absolute paths
    relative to the project root.
    """
    # This makes the path finding robust. It assumes the project root is
    # two levels up from this file (utils/ -> root/).
    project_root = Path(__file__).parent.parent
    
    # Load .env file from the project root
    dotenv_path = project_root / '.env'
    load_dotenv(dotenv_path=dotenv_path)

    # UPDATED: Load base configuration from YAML file in the root/config/ directory
    config_path = project_root / 'config' / 'config.yaml'
    
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # --- Override/Add secrets from environment variables ---
    config['database']['destination']['user'] = os.getenv('DB_USER')
    config['database']['destination']['password'] = os.getenv('DB_PASSWORD')
    config['opensearch'] = {
        'host': os.getenv('OPENSEARCH_HOST'),
        'index_name': os.getenv('OPENSEARCH_INDEX_NAME')
    }
    
    print("Configuration loaded successfully.")
    return config

# Load the configuration once and make it available for import
config = load_config()

