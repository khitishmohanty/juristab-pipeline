import mysql.connector
from uuid import uuid4
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseManager:
    """
    Manages database connections and operations for legal case law data.
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
    
    def check_and_upsert_caselaw_metadata(self, metadata, source_id, expected_columns):
        """
        Checks if a record exists for a given source_id and either updates it
        or inserts a new record.
        
        Args:
            metadata (dict): The dictionary of extracted metadata.
            source_id (str): The unique identifier for the case law.
            expected_columns (list): A canonical list of expected database column names.
        
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if not self._get_connection():
            return False

        cursor = self.conn.cursor()
        
        try:
            # Check if record exists
            query = "SELECT COUNT(*) FROM caselaw_metadata WHERE source_id = %s"
            cursor.execute(query, (source_id,))
            record_exists = cursor.fetchone()[0] > 0

            record_id = str(uuid4())
            filtered_metadata = {key.lower(): metadata[key] for key in expected_columns if key in metadata}
            
            columns = filtered_metadata.keys()
            values = list(filtered_metadata.values())
            
            columns_with_id = list(columns) + ['source_id']
            values_with_id = values + [source_id]

            if record_exists:
                update_query = f"UPDATE caselaw_metadata SET {', '.join([f'{col} = %s' for col in columns])} WHERE source_id = %s"
                cursor.execute(update_query, values + [source_id])
                logging.info(f"Updated caselaw_metadata record for source_id: {source_id}")
            else:
                insert_query = f"INSERT INTO caselaw_metadata ({', '.join(columns_with_id)}, id) VALUES ({', '.join(['%s'] * (len(columns_with_id) + 1))})"
                cursor.execute(insert_query, values_with_id + [record_id])
                logging.info(f"Created new caselaw_metadata record for source_id: {source_id}")

            self.conn.commit()
            return True

        except mysql.connector.Error as err:
            logging.error(f"Database operation failed: {err}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()
    
    def insert_counsel_firm_mapping(self, mappings, source_id):
        """
        Inserts a list of counsel/firm mappings into the mapping_counsel_firm table.
        
        Args:
            mappings (list): A list of dictionaries with 'counsel' and 'law_firm_agency' keys.
            source_id (str): The unique identifier for the case law to associate the mappings with.
            
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        if not self._get_connection():
            return False
            
        cursor = self.conn.cursor()
        try:
            insert_query = "INSERT INTO mapping_counsel_firm (id, counsel, law_firm_agency, source_id) VALUES (%s, %s, %s, %s)"
            
            records_to_insert = []
            for mapping in mappings:
                record_id = str(uuid4())
                counsel = mapping.get('counsel')
                law_firm = mapping.get('law_firm_agency')
                
                if not counsel and not law_firm:
                    continue

                records_to_insert.append((record_id, counsel, law_firm, source_id))

            if records_to_insert:
                cursor.executemany(insert_query, records_to_insert)
                
            self.conn.commit()
            logging.info(f"Successfully inserted {len(records_to_insert)} counsel/firm mappings for source_id {source_id}.")
            return True
            
        except mysql.connector.Error as err:
            logging.error(f"Mapping insertion failed: {err}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()

    def update_enrichment_status(self, source_id, updates):
        """
        Inserts or updates the caselaw_enrichment_status table using an atomic "upsert".
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
            # Prepare data for insertion, including a new UUID for the 'id' column
            insert_data = updates.copy()
            insert_data['source_id'] = source_id
            insert_data['id'] = str(uuid4())

            # Prepare the ON DUPLICATE KEY UPDATE clause
            # This will update the specified columns if the source_id already exists
            update_clause = ", ".join([f"{key} = VALUES({key})" for key in updates.keys()])
            
            # Construct the full "upsert" query
            query = f"""
                INSERT INTO caselaw_enrichment_status ({", ".join(insert_data.keys())})
                VALUES ({", ".join(['%s'] * len(insert_data))})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            
            cursor.execute(query, list(insert_data.values()))
            
            # MySQL returns 1 for an INSERT, 2 for an UPDATE
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