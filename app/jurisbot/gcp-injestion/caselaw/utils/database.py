import mysql.connector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def connect_db(db_config, db_user, db_password):
    """
    Establishes a connection to the MySQL database.
    
    Args:
        db_config (dict): Database configuration from the YAML file.
        db_user (str): Database username.
        db_password (str): Database password.
        
    Returns:
        mysql.connector.connection.MySQLConnection: The database connection object, or None on failure.
    """
    try:
        conn = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_user,
            password=db_password,
            database=db_config['name']
        )
        if conn.is_connected():
            logging.info("Successfully connected to the database.")
            return conn
        else:
            logging.error("Failed to connect to the database.")
            return None
    except mysql.connector.Error as e:
        logging.error(f"Database connection error: {e}")
        return None

def fetch_caselaws_for_gcp_ingestion(conn, columns_to_select, ingestion_criteria):
    """
    Fetches records that are ready for ingestion into GCP based on dynamic criteria.
    
    Args:
        conn: The database connection object.
        columns_to_select (list): A list of column names for the SELECT statement.
        ingestion_criteria (dict): Configuration for the WHERE clause.
        
    Returns:
        list: A list of dictionaries, where each dictionary is a record.
    """
    if not conn:
        return []
    
    cursor = conn.cursor(dictionary=True)
    
    # Build the SELECT part of the query
    select_columns_str = ", ".join(columns_to_select)

    # Build the WHERE clause dynamically from the config
    where_conditions = ingestion_criteria['conditions']
    table_alias = ingestion_criteria['alias']
    
    where_clause_parts = []
    for condition in where_conditions:
        # Note: The 'value' in config.yaml must include quotes for SQL strings
        part = f"{table_alias}.`{condition['column']}` {condition['operator']} {condition['value']}"
        where_clause_parts.append(part)
    
    where_clause_str = " AND ".join(where_clause_parts)
    
    # Construct the final query
    query = f"""
    SELECT 
        {select_columns_str}
    FROM legal_store.caselaw_metadata cm
    JOIN legal_store.caselaw_enrichment_status ces 
        ON cm.source_id = ces.source_id
    JOIN legal_store.caselaw_registry cr 
        ON cm.source_id = cr.source_id
    WHERE 
        {where_clause_str};
    """

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        logging.info(f"Fetched {len(results)} records for GCP ingestion.")
        return results
    except mysql.connector.Error as e:
        logging.error(f"Failed to fetch data for GCP ingestion: {e}")
        return []
    finally:
        cursor.close()


def update_enrichment_status(conn, update_config, source_id, status, duration, start_time, end_time):
    """
    Updates the caselaw_enrichment_status table.
    """
    if not conn:
        return False
        
    cursor = conn.cursor()
    
    table_name = update_config['table']
    columns = update_config['columns']
    
    try:
        query = f"""
        UPDATE `{table_name}`
        SET
            `{columns['processing_status']}` = %s,
            `{columns['processing_duration']}` = %s,
            `{columns['start_time']}` = %s,
            `{columns['end_time']}` = %s
        WHERE source_id = %s
        """
        
        cursor.execute(query, (status, duration, start_time, end_time, source_id))
        conn.commit()
        logging.info(f"Successfully updated record with source_id {source_id} in {table_name}.")
        return True
    except mysql.connector.Error as e:
        logging.error(f"Failed to update record with source_id {source_id} in {table_name}: {e}")
        return False
    finally:
        cursor.close()

