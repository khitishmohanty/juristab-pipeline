import logging
from src.processing import process_legislation_data
from dotenv import load_dotenv

# --- Configuration ---
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

if __name__ == '__main__':
    """
    Main entry point for the ETL application.
    This script initiates the legislation data processing job.
    """
    # Load environment variables from .env file
    load_dotenv()
    logging.info("Environment variables from .env file loaded.")
    
    logging.info("Application started.")
    try:
        # Call the updated function for processing legislation data
        process_legislation_data()
    except Exception as e:
        logging.critical(f"An unhandled error occurred in the main application: {e}", exc_info=True)
    finally:
        logging.info("Application finished.")
