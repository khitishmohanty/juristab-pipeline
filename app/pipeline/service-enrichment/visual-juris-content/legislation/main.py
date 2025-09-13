import os
import sys
# Adjust the path to allow imports from the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from utils.config_manager import ConfigManager
from utils.audit_logger import AuditLogger
from src.text_processor import TextProcessor

def main():
    """
    Main entry point for the Text Extraction service.
    """
    print("Starting Text Extraction Service...")
    
    # Assuming the script is run from the service's directory
    config_path = './config/config.yaml'
    env_path = '.env'
    
    try:
        config_manager = ConfigManager(config_path=config_path, env_path=env_path)
        config = config_manager.get_config()
    except Exception as e:
        print(f"FATAL: Failed to load configuration. Aborting. Error: {e}")
        sys.exit(1)

    log_id = None
    try:
        audit_config = config['audit_log']['text_extraction_job']
        
        # FIX: Find the database configuration by its 'name' instead of using it as a key
        audit_db_name = audit_config['database']
        audit_db_config = None
        for db_key, db_properties in config['database'].items():
            if db_properties.get('name') == audit_db_name:
                audit_db_config = db_properties
                break
        
        if audit_db_config is None:
            raise KeyError(f"Database configuration for '{audit_db_name}' not found.")

        logger = AuditLogger(db_config=audit_db_config, table_name=audit_config['table'])
        log_id = logger.log_start(job_name=audit_config['job_name'])

    except Exception as e:
        print(f"FATAL: Could not start audit logger. Aborting. Error: {e}")
        sys.exit(1)

    try:
        processor = TextProcessor(config=config)
        processor.process_cases()
        logger.log_end(log_id, status='completed', message='Text extraction job finished successfully.')
    except Exception as e:
        error_message = f"Job failed due to an unhandled exception: {str(e)}"
        print(error_message)
        logger.log_end(log_id, status='failed', message=error_message)
        sys.exit(1)

if __name__ == "__main__":
    main()
