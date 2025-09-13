import logging
from sqlalchemy import create_engine

def create_db_engine(db_config, username, password):
    """
    Creates a SQLAlchemy engine from database configuration.

    Args:
        db_config (dict): A dictionary containing database configuration
                          (dialect, driver, host, port, name).
        username (str): The database username.
        password (str): The database password.

    Returns:
        sqlalchemy.engine.Engine: The created SQLAlchemy engine, or None if connection fails.
    """
    try:
        # Construct the connection string
        connection_str = (
            f"{db_config['dialect']}+{db_config['driver']}://"
            f"{username}:{password}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
        engine = create_engine(connection_str)
        
        # Test the connection to ensure it's valid
        with engine.connect() as connection:
            logging.info(f"Successfully connected to database: {db_config['name']}")
        
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine for '{db_config['name']}'. Error: {e}")
        return None
