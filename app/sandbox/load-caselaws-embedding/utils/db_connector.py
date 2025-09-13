from sqlalchemy import create_engine, text
from .config_loader import config

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine using details from the config.
    """
    db_config = config['database']['destination']
    try:
        engine_url = (
            f"{db_config['dialect']}+{db_config['driver']}://"
            f"{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
        engine = create_engine(engine_url)
        print("Database engine created successfully.")
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        raise

def get_pending_caselaws(engine):
    """
    Fetches the list of caselaws that have passed embedding and need ingestion.

    Args:
        engine: The SQLAlchemy engine instance.

    Returns:
        list: A list of dictionaries, each containing 'source_id' and 'file_path'.
    """
    query = text("""
        SELECT
            ces.source_id,
            cr.file_path
        FROM
            legal_store.caselaw_enrichment_status AS ces
        JOIN
            legal_store.caselaw_registry AS cr ON ces.source_id = cr.source_id
        WHERE
            ces.status_text_embedding = 'pass';
    """)
    
    pending_files = []
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            for row in result:
                pending_files.append(dict(row._mapping))
        print(f"Found {len(pending_files)} caselaws pending ingestion.")
        return pending_files
    except Exception as e:
        print(f"Error fetching pending caselaws: {e}")
        return []
