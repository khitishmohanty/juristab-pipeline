import mysql.connector
from uuid import uuid4
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseManager:
    """
    Manages database connections and operations for legal legislation data.
    """
    def __init__(self, db_config):
        """
        Initializes the DatabaseManager with the database connection configuration.

        Args:
            db_config (dict): A dictionary containing database connection details.
        """
        self.db_config = db_config
        self.conn = None

    def _get_connection(self):
        """
        Establishes a connection to the MySQL database.
        
        Returns:
            mysql.connector.connection.MySQLConnection: The database connection object.
        """
        try:
            self.conn = mysql.connector.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['name']
            )
            logging.info("Successfully connected to the database.")
            return self.conn
        except mysql.connector.Error as err:
            logging.error(f"Database connection failed: {err}")
            return None

    def close_connection(self):
        """
        Closes the database connection if it is open.
        """
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logging.info("Database connection closed.")
    
    def upsert_legislation_metadata(self, metadata, source_id, expected_columns):
        """
        Inserts or updates a record in the legislation_metadata table.
        Uses ON DUPLICATE KEY UPDATE for an atomic "upsert" operation,
        assuming 'source_id' is a unique key.

        Args:
            metadata (dict): The dictionary of extracted metadata.
            source_id (str): The unique identifier for the legislation.
            expected_columns (list): A canonical list of expected database column names.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if not self._get_connection():
            return False

        cursor = self.conn.cursor()

        try:
            # Filter metadata to only include keys that match expected columns and have a value
            filtered_metadata = {key: metadata[key] for key in expected_columns if key in metadata and metadata[key] is not None}
            
            if not filtered_metadata:
                logging.warning(f"No valid metadata to insert for source_id: {source_id}")
                return True # Not an error, just nothing to do.

            # Prepare data for insertion, including a new UUID for 'id' and the 'source_id'
            filtered_metadata['source_id'] = source_id
            filtered_metadata['id'] = str(uuid4())
            
            # The 'second_reading_speech_dates' might be a dict; convert to JSON string for DB
            if 'second_reading_speech_dates' in filtered_metadata and isinstance(filtered_metadata['second_reading_speech_dates'], dict):
                filtered_metadata['second_reading_speech_dates'] = json.dumps(filtered_metadata['second_reading_speech_dates'])

            columns = filtered_metadata.keys()
            values = list(filtered_metadata.values())
            
            # Prepare the ON DUPLICATE KEY UPDATE clause, excluding 'id' and 'source_id'
            update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in columns if col not in ['id', 'source_id']])

            # Escape all column names with backticks
            column_str = ", ".join([f"`{col}`" for col in columns])
            
            query = f"""
                INSERT INTO legislation_metadata ({column_str})
                VALUES ({', '.join(['%s'] * len(values))})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            
            cursor.execute(query, values)
            logging.info(f"Upserted legislation_metadata record for source_id: {source_id}")
            self.conn.commit()
            return True

        except mysql.connector.Error as err:
            logging.error(f"Database operation failed for legislation_metadata: {err}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()

    def update_enrichment_status(self, source_id, updates):
        """
        Inserts or updates the legislation_enrichment_status table using an atomic "upsert".
        This method will create a new record if one doesn't exist, or update the
        existing one if it does. Assumes 'source_id' is a unique key.

        Args:
            source_id (str): The unique ID for the case law.
            updates (dict): A dictionary where keys are column names and values for updating/inserting.
        """
        if not self._get_connection():
            return False
        
        if not updates:
            logging.info("No updates to perform for enrichment status.")
            return True

        cursor = self.conn.cursor()
        
        try:
            insert_data = updates.copy()
            insert_data['source_id'] = source_id
            insert_data['id'] = str(uuid4())

            update_clause = ", ".join([f"{key} = VALUES({key})" for key in updates.keys()])
            
            query = f"""
                INSERT INTO legislation_enrichment_status ({", ".join(insert_data.keys())})
                VALUES ({", ".join(['%s'] * len(insert_data))})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            
            cursor.execute(query, list(insert_data.values()))
            
            if cursor.rowcount == 1:
                logging.info(f"Inserted new enrichment status for source_id {source_id}.")
            else:
                logging.info(f"Updated enrichment status for source_id {source_id}.")

            self.conn.commit()
            return True
            
        except mysql.connector.Error as err:
            logging.error(f"Failed to upsert enrichment status: {err}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()