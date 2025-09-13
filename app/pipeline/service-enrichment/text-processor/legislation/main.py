import sys
import os

# Import utility classes from the 'src' package
from utils.config_manager import ConfigManager
from utils.audit_logger import AuditLogger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
# Import the main processing class from the 'src' package
from src.text_processor import TextProcessor

def main():
    """
    Main function to initialize and run the text processing pipeline.
    """
    print("Starting Text Processing Service...")
    
    # Assuming the script is run from the service's directory
    config_path = './config/config.yaml'
    env_path = '.env'
    
    try:
        # 1. Initialize configuration manager to load settings
        config_manager = ConfigManager(config_path=config_path)
        config = config_manager.get_config()
    except FileNotFoundError:
        print("FATAL: config.yaml not found. Please ensure the configuration file exists.")
        sys.exit(1) # Exit the script with an error code
    except Exception as e:
        print(f"FATAL: An error occurred during configuration loading: {e}")
        sys.exit(1)

    # 2. Initialize the audit logger using the loaded configuration
    audit_config = config.get('audit_log', {}).get('text_extraction_job', {})
    if not audit_config:
        print("FATAL: Audit log configuration 'audit_log.text_extraction_job' not found in config.yaml.")
        sys.exit(1)

    audit_logger = AuditLogger(
        db_config=config['database']['destination'],
        table_name=audit_config['table']
    )
    job_name = audit_config['job_name']
    log_id = None # Initialize log_id to handle potential errors

    try:
        # 3. Log the start of the job
        log_id = audit_logger.log_start(job_name)

        # 4. Initialize the main processor and run the job
        print("Initializing TextProcessor...")
        processor = TextProcessor(config) # This now uses the imported class
        processor.process_cases()

        # 5. Log the successful completion of the job
        print("Job completed successfully.")
        audit_logger.log_end(log_id, 'completed', 'TextProcessor job finished successfully.')

    except Exception as e:
        print(f"FATAL: An unhandled exception occurred during the text processing job: {e}")
        if log_id:
            audit_logger.log_end(log_id, 'failed', f"Job failed with error: {str(e)}")
        sys.exit(1) # Exit with an error code

if __name__ == "__main__":
    main()