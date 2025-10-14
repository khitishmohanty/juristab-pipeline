import os
import sys
import argparse
import logging
from logging.handlers import RotatingFileHandler

# Adjust the path to allow imports from the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from utils.config_manager import ConfigManager
from utils.audit_logger import AuditLogger
from src.text_processor import TextProcessor
from src.section_processor import SectionProcessor

def setup_logging(log_level: str = 'INFO'):
    """
    Sets up logging configuration for the application.
    
    Args:
        log_level (str): The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, 'text_extraction.log'),
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Add handlers to root logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def main():
    """
    Main entry point for the Text Extraction service.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Text Extraction and Section Processing Service')
    parser.add_argument('--mode', choices=['juriscontent', 'sections', 'both'], 
                       default='both',
                       help='Processing mode: juriscontent (HTML generation only), sections (section extraction only), or both (default)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       default='INFO',
                       help='Set the logging level')
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level)
    logger.info("="*70)
    logger.info(f"TEXT EXTRACTION SERVICE STARTED")
    logger.info(f"Mode: {args.mode}")
    logger.info("="*70)
    
    # Assuming the script is run from the service's directory
    config_path = './config/config.yaml'
    env_path = '.env'
    
    try:
        config_manager = ConfigManager(config_path=config_path, env_path=env_path)
        config = config_manager.get_config()
    except Exception as e:
        logger.critical(f"Failed to load configuration. Aborting. Error: {e}")
        sys.exit(1)

    # Determine what to process
    process_juriscontent = args.mode in ['juriscontent', 'both']
    process_sections_inline = args.mode == 'both'  # Process sections inline with juriscontent
    process_sections_separately = args.mode == 'sections'  # Process sections separately

    # Process based on mode
    if process_juriscontent:
        log_id = None
        try:
            audit_config = config['audit_log']['text_extraction_job']
            
            # Find the database configuration by its 'name'
            audit_db_name = audit_config['database']
            audit_db_config = None
            for db_key, db_properties in config['database'].items():
                if db_properties.get('name') == audit_db_name:
                    audit_db_config = db_properties
                    break
            
            if audit_db_config is None:
                raise KeyError(f"Database configuration for '{audit_db_name}' not found.")

            audit_logger = AuditLogger(db_config=audit_db_config, table_name=audit_config['table'])
            
            if args.mode == 'both':
                log_id = audit_logger.log_start(job_name='legislation juriscontent and section extraction')
            else:
                log_id = audit_logger.log_start(job_name=audit_config['job_name'])

        except Exception as e:
            logger.critical(f"Could not start audit logger. Aborting. Error: {e}")
            sys.exit(1)

        try:
            processor = TextProcessor(config=config)
            # Pass flag to indicate whether to process sections inline
            processor.process_cases(process_sections=process_sections_inline)
            
            if args.mode == 'both':
                audit_logger.log_end(log_id, status='completed', 
                                   message='Juriscontent generation and section extraction completed successfully.')
            else:
                audit_logger.log_end(log_id, status='completed', 
                                   message='Juriscontent generation completed successfully.')
        except Exception as e:
            error_message = f"Job failed due to an unhandled exception: {str(e)}"
            logger.error(error_message, exc_info=True)
            audit_logger.log_end(log_id, status='failed', message=error_message)
            sys.exit(1)
    
    # Process sections separately (only when mode is 'sections')
    if process_sections_separately:
        log_id = None
        try:
            # Create a new audit log entry for section extraction
            audit_config = config['audit_log']['text_extraction_job']
            
            audit_db_name = audit_config['database']
            audit_db_config = None
            for db_key, db_properties in config['database'].items():
                if db_properties.get('name') == audit_db_name:
                    audit_db_config = db_properties
                    break
            
            if audit_db_config is None:
                raise KeyError(f"Database configuration for '{audit_db_name}' not found.")

            audit_logger = AuditLogger(db_config=audit_db_config, table_name=audit_config['table'])
            log_id = audit_logger.log_start(job_name='legislation section extraction')

        except Exception as e:
            logger.critical(f"Could not start audit logger for section extraction. Aborting. Error: {e}")
            sys.exit(1)

        try:
            section_processor = SectionProcessor(config=config)
            section_processor.process_sections()
            audit_logger.log_end(log_id, status='completed', message='Section extraction job finished successfully.')
        except Exception as e:
            error_message = f"Section extraction job failed: {str(e)}"
            logger.error(error_message, exc_info=True)
            audit_logger.log_end(log_id, status='failed', message=error_message)
            sys.exit(1)
    
    logger.info("="*70)
    logger.info("ALL PROCESSING COMPLETE")
    logger.info("="*70)

if __name__ == "__main__":
    main()