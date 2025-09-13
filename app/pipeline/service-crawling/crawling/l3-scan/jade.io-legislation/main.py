import logging
from src.crawler import run_crawler

if __name__ == "__main__":
    # Configure the logging system
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logging.info("Starting the web crawler...") # Use logging instead of print
    run_crawler()
    logging.info("Web crawler finished.") # Use logging instead of print