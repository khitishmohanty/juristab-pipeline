import logging
import sys

def setup_logger():
    """
    Sets up a basic console logger for the application.
    """
    # Get the root logger
    logger = logging.getLogger()
    
    # Clear existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Set the logging level (e.g., INFO, DEBUG, ERROR)
    logger.setLevel(logging.INFO)

    # Create a handler to stream logs to the console (stdout)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    # Create a formatter to define the log message format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Add the formatter to the handler
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger
