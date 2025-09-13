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

def get_pending_documents(engine, doc_type: str):
    """
    Fetches the list of documents that have passed embedding and need ingestion.

    Args:
        engine: The SQLAlchemy engine instance.
        doc_type: The type of document to process ('caselaw' or 'legislation').
    """
    # Dynamically set table names based on doc_type
    registry_table = f"{doc_type}_registry"
    status_table = f"{doc_type}_enrichment_status"

    query = text(f"""
        SELECT
            ces.source_id,
            cr.file_path
        FROM
            legal_store.{status_table} AS ces
        JOIN
            legal_store.{registry_table} AS cr ON ces.source_id = cr.source_id
        WHERE
            ces.status_text_embedding = 'pass';
    """)
    
    pending_files = []
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            for row in result:
                pending_files.append(dict(row._mapping))
        print(f"Found {len(pending_files)} {doc_type} documents pending ingestion.")
        return pending_files
    except Exception as e:
        print(f"Error fetching pending {doc_type} documents: {e}")
        return []
