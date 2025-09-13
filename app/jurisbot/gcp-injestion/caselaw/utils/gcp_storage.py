import os
import json
import logging
from google.cloud import storage
from google.oauth2.service_account import Credentials

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_gcp_client(credentials_json_string):
    """
    Initializes and returns a Google Cloud Storage client using service account JSON.
    
    The function now directly parses the JSON string, which is more reliable
    for environments like Fargate where the string is provided in a single line.
    
    Args:
        credentials_json_string (str): The content of the service account key JSON.
        
    Returns:
        storage.Client: An initialized Google Cloud Storage client, or None on failure.
    """
    if not credentials_json_string:
        logging.error("GCP credentials JSON string is empty.")
        return None

    try:
        credentials_dict = json.loads(credentials_json_string)
        credentials = Credentials.from_service_account_info(credentials_dict)
        client = storage.Client(credentials=credentials)
        return client
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse GCP credentials JSON. Please check the string's format: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to create GCP Storage client: {e}")
        return None

def upload_file_from_memory(gcp_client, bucket_name, destination_blob_name, data):
    """
    Uploads a file from an in-memory byte stream to Google Cloud Storage.
    
    Args:
        gcp_client: An initialized google.cloud.storage.Client.
        bucket_name (str): The GCS bucket name.
        destination_blob_name (str): The destination path for the file in GCS.
        data (bytes): The in-memory content of the file.
        
    Returns:
        bool: True on success, False on failure.
    """
    if not gcp_client:
        return False
        
    try:
        bucket = gcp_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Upload the byte stream
        blob.upload_from_string(data)
        
        logging.info(f"Successfully uploaded file to: gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload file to gs://{bucket_name}/{destination_blob_name}: {e}")
        return False
