import mysql.connector
import logging
from utils.config_loader import load_config

def get_db_connection():
    """Establishes a connection to the database using loaded credentials."""
    config = load_config()
    db_config = config.get('database', {})
    
    try:
        connection = mysql.connector.connect(
            host=db_config.get('host'),
            port=db_config.get('port'),
            database=db_config.get('name'),
            user=db_config.get('user'),
            password=db_config.get('password')
        )
        if connection.is_connected():
            # This log can be noisy, so it's commented out. Uncomment for debugging.
            # logging.info("Successfully connected to the database")
            return connection
    except mysql.connector.Error as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        return None

def get_urls_to_crawl(table_name):
    """Fetches URLs and book names from a table where l3_scan_status is not 'pass'."""
    connection = get_db_connection()
    if connection is None:
        return []

    cursor = connection.cursor(dictionary=True)
    # Modified to explicitly exclude 'in_progress' records from the initial fetch
    query = f"SELECT id, book_url, book_name FROM {table_name} WHERE l3_scan_status IS NULL OR l3_scan_status NOT IN ('pass', 'in_progress')"
    
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except mysql.connector.Error as e:
        logging.error(f"Error fetching records from {table_name}: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def lock_record(table_name, record_id):
    """
    Attempts to lock a record by setting its status to 'in_progress'.
    This is an atomic operation to prevent race conditions.
    Returns True if the lock was successful, False otherwise.
    """
    connection = get_db_connection()
    if connection is None:
        return False

    cursor = connection.cursor()
    try:
        query = f"""
            UPDATE {table_name}
            SET l3_scan_status = 'in_progress'
            WHERE id = %s AND (l3_scan_status IS NULL OR l3_scan_status NOT IN ('pass', 'in_progress'))
        """
        params = (record_id,)
        cursor.execute(query, params)
        connection.commit()

        # If rowcount is 1, we successfully locked the record.
        # If it's 0, another process locked it in the moments since we fetched the list.
        if cursor.rowcount == 1:
            logging.info(f"Successfully locked record ID: {record_id}")
            return True
        else:
            logging.info(f"Record ID: {record_id} was already locked by another process. Skipping.")
            return False
            
    except mysql.connector.Error as e:
        logging.error(f"Error locking record {record_id} in {table_name}: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def update_scan_result(table_name, record_id, status, error_message=None, size_miniviewer=None, size_full_content=None):
    """
    Updates the final scan status, error message, and file sizes for a given record.
    """
    connection = get_db_connection()
    if connection is None:
        return

    cursor = connection.cursor()
    
    if error_message:
        error_message = (error_message[:1024] + '...') if len(error_message) > 1024 else error_message

    try:
        query = f"""
            UPDATE {table_name} 
            SET 
                l3_scan_status = %s, 
                l3_scan_error = %s,
                size_miniviewer = %s,
                size_full_content = %s
            WHERE id = %s
        """
        params = (status, error_message, size_miniviewer, size_full_content, record_id)
        
        cursor.execute(query, params)
        connection.commit()
        
    except mysql.connector.Error as e:
        logging.error(f"Error updating result for record {record_id} in {table_name}: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
