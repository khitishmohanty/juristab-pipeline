import sys
import os

# Add the parent directory to the path to allow imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Import utility classes from the 'utils' package
from utils.config_manager import ConfigManager
from utils.audit_logger import AuditLogger

# Import the main processing class
from src.version_processor import VersionProcessor

def main():
    """
    Main function to initialize and run the versioning pipeline.
    """
    print("=" * 60)
    print("Starting Book Versioning Service")
    print("=" * 60)
    
    # Assuming the script is run from the service's directory
    config_path = './config/config.yaml'
    env_path = '.env'
    
    try:
        # 1. Initialize configuration manager to load settings
        print("\n[Step 1/5] Loading configuration...")
        config_manager = ConfigManager(config_path=config_path, env_path=env_path)
        config = config_manager.get_config()
        print("Configuration loaded successfully.")
    except FileNotFoundError as e:
        print(f"FATAL: Configuration file not found: {e}")
        print("Please ensure config.yaml exists in the config directory.")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: An error occurred during configuration loading: {e}")
        sys.exit(1)

    # 2. Initialize the audit logger using the loaded configuration
    print("\n[Step 2/5] Initializing audit logger...")
    audit_config = config.get('audit_log', {}).get('versioning_job', {})
    if not audit_config:
        print("FATAL: Audit log configuration 'audit_log.versioning_job' not found in config.yaml.")
        sys.exit(1)

    try:
        audit_logger = AuditLogger(
            db_config=config['database']['destination'],
            table_name=audit_config['table']
        )
        job_name = audit_config['job_name']
        print("Audit logger initialized successfully.")
    except Exception as e:
        print(f"FATAL: Failed to initialize audit logger: {e}")
        sys.exit(1)
    
    log_id = None

    try:
        # 3. Log the start of the job
        print("\n[Step 3/5] Starting job and logging to audit table...")
        log_id = audit_logger.log_start(job_name)
        print(f"Job started with log ID: {log_id}")

        # 4. Initialize the main processor and run the job
        print("\n[Step 4/5] Initializing VersionProcessor and processing books...")
        processor = VersionProcessor(config)
        processor.process_versions()

        # 5. Log the successful completion of the job
        print("\n[Step 5/5] Finalizing and logging completion...")
        print("=" * 60)
        print("Job completed successfully!")
        print("=" * 60)
        audit_logger.log_end(log_id, 'completed', 'VersionProcessor job finished successfully.')

    except KeyboardInterrupt:
        print("\n\nJob interrupted by user.")
        if log_id:
            audit_logger.log_end(log_id, 'failed', 'Job interrupted by user (KeyboardInterrupt).')
        sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"FATAL ERROR: An unhandled exception occurred")
        print("=" * 60)
        print(f"Error details: {e}")
        print("=" * 60)
        if log_id:
            audit_logger.log_end(log_id, 'failed', f"Job failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()