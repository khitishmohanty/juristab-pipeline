import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv # For loading .env file (recommended)
from sqlalchemy import create_engine
import glob
import boto3
import re
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from sqlalchemy import text

# --- Database Configuration ---
# Load environment variables from .env file if present
load_dotenv()

DB_DIALECT = os.getenv("DB_DIALECT", "mysql")  # e.g., mysql, postgresql, mssql
DB_DRIVER = os.getenv("DB_DRIVER", "mysqlconnector") # e.g., mysqlconnector, psycopg2, pyodbc
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT") # Optional, defaults are usually fine
DB_NAME = os.getenv("DB_NAME")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "legal_store")
S3_DEST_FOLDER = os.getenv("S3_DEST_FOLDER", "crawl_configs/") # Make sure it ends with a slash

# Ensure S3_DEST_FOLDER ends with a slash if it's not empty
if S3_DEST_FOLDER and not S3_DEST_FOLDER.endswith('/'):
    S3_DEST_FOLDER += '/'
    
# --- Local Configuration ---
TRACKED_SITES_PARENT_DIR_NAME = "tracked_sites"
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOCAL_SITES_BASE_PATH = os.path.join(PROJECT_ROOT, TRACKED_SITES_PARENT_DIR_NAME)

def get_db_connection_url():
    """Constructs the database connection URL from environment variables."""
    if not all([DB_DIALECT, DB_DRIVER, DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        print("❌ Error: Missing one or more database connection environment variables.")
        print("❌ Required: DB_DIALECT, DB_DRIVER, DB_USER, DB_PASSWORD, DB_HOST, DB_NAME")
        return None

    url = f"{DB_DIALECT}+{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}"
    if DB_PORT:
        url += f":{DB_PORT}"
    url += f"/{DB_NAME}"
    return url

def create_db_engine():
    """
    Creates a SQLAlchemy engine using a connection URL from environment variables.
    """
    connection_url = get_db_connection_url()
    if not connection_url:
        return None

    try:
        engine = create_engine(connection_url)
        # Test the connection
        with engine.connect() as connection:
            print(f"✅ SQLAlchemy engine created and connection successful to {DB_DIALECT} database.")
        return engine
    except Exception as e:
        print(f"❌ The error '{e}' occurred while creating the engine or connecting.")
        return None


def update_db_record(connection, table_name, identifier_column_name, identifier_value, columns_to_update):
    """
    Updates specified columns for a record in a given table, identified by a specific column and value.

    :param connection: SQLAlchemy connection object.
    :param table_name: Name of the table to update (e.g., "parent_urls").
    :param identifier_column_name: Name of the column to use in the WHERE clause (e.g., "base_url" or "id").
    :param identifier_value: Value to match in the identifier_column_name.
    :param columns_to_update: A dictionary where keys are column names and values are their new values.
                            Example: {"robots_file_status": "success", "last_processed_at": datetime.now()}
    :return: True if update was successful, False otherwise.
    """
    if not columns_to_update:
        print("❌ No columns specified for update. Aborting DB update.")
        return False
    if identifier_value is None:
        print(f"❌ Error: Cannot update record with NULL identifier_value for column '{identifier_column_name}'. Aborting DB update.")
        return False

    try:
        set_clauses = []
        params = {}
        param_idx = 0 # To create unique parameter names for SET clauses

        for col, val in columns_to_update.items():
            # Basic sanitization/check for column names (can be expanded)
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col):
                print(f"❌ Error: Invalid column name '{col}' for update. Aborting.")
                return False
            param_name = f"val_{param_idx}"
            set_clauses.append(f"{col} = :{param_name}")
            params[param_name] = val
            param_idx += 1
        
        # Add the identifier to the parameters for the WHERE clause
        params["identifier_val_in_db"] = identifier_value

        # Basic sanitization for table_name and identifier_column_name
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name) or \
            not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier_column_name):
            print(f"Error: Invalid table name '{table_name}' or identifier column name '{identifier_column_name}'. Update aborted.")
            return False

        set_clause_str = ", ".join(set_clauses)
        
        query_str = f"UPDATE {table_name} SET {set_clause_str} WHERE {identifier_column_name} = :identifier_val_in_db"
        query = text(query_str)
        
        connection.execute(query, params)
        print(f"✅ DB record updated in table '{table_name}' for {identifier_column_name}='{identifier_value}': SET {set_clause_str} with params {params}")
        return True
    except Exception as e:
        print(f"❌ Error updating DB record in table '{table_name}' for {identifier_column_name}='{identifier_value}': {e}")
        return False
    
def get_s3_client():
    """
    Initializes and returns a Boto3 S3 client.
    Returns None if client initialization fails.
    """
    try:
        s3_client = boto3.client('s3')
        print("✅ Successfully initialized Boto3 S3 client.")
        return s3_client
    except Exception as e:
        print(f"❌ Error initializing Boto3 S3 client: {e}")
        print("❌ Ensure Boto3 is installed and AWS credentials (region, keys) might be an issue.")
        return None


def ensure_s3_folder_exists(s3_client, bucket_name, folder_key):
    """
    Ensures a 'folder' exists in S3 by creating an empty object with the folder key.
    The folder_key should end with a '/'.
    This function is generic and can be used for any bucket and folder structure.
    """
    if not folder_key.endswith('/'):
        print(f"⚠️ Warning: Folder key '{folder_key}' should end with a '/' for S3 folder conventions to appear as a folder in the console.")
    try:
        print(f"⚙️ Ensuring S3 folder s3://{bucket_name}/{folder_key} exists (by creating/updating an empty object)...")
        # put_object will create the "folder" (an empty object with the key) if it doesn't exist,
        # or overwrite it if it does. This is suitable for ensuring the folder marker is present.
        s3_client.put_object(Bucket=bucket_name, Key=folder_key, Body='')
        print(f"✅ S3 folder '{folder_key}' ensured in bucket '{bucket_name}'.")
        return True
    except ClientError as e:
        # Catching general ClientError from S3 (e.g., permissions, bucket not found)
        print(f"❌ AWS ClientError ensuring S3 folder '{folder_key}' in bucket '{bucket_name}': {e}")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred while ensuring S3 folder '{folder_key}' in bucket '{bucket_name}': {e}")
        return False


def upload_data_to_s3(s3_client, data_content, bucket_name, s3_object_key, content_type='text/plain'):
    """
    Uploads data content (string or bytes) directly to an S3 object.

    :param s3_client: Initialized Boto3 S3 client.
    :param data_content: The data to upload (can be string or bytes).
    :param bucket_name: Name of the S3 bucket.
    :param s3_object_key: The key (path/filename) for the object in S3.
    :param content_type: The MIME type of the content (e.g., 'text/plain', 'application/json').
    :return: True if upload was successful, False otherwise.
    """
    try:
        if isinstance(data_content, str):
            data_bytes = data_content.encode('utf-8')
        elif isinstance(data_content, bytes):
            data_bytes = data_content
        else:
            print(f"❌ Error: data_content must be str or bytes, got {type(data_content)}")
            return False

        print(f"⚙️ Uploading data to s3://{bucket_name}/{s3_object_key} (Content-Type: {content_type})...")
        s3_client.put_object(Bucket=bucket_name, Key=s3_object_key, Body=data_bytes, ContentType=content_type)
        print("Upload Successful")
        return True
    except NoCredentialsError:
        print("❌ Error: AWS credentials not found. Configure your credentials.")
    except PartialCredentialsError:
        print("❌ Error: Incomplete AWS credentials.")
    except ClientError as e:
        print(f"❌ AWS ClientError during data upload to s3://{bucket_name}/{s3_object_key}: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during data upload: {e}")
    return False