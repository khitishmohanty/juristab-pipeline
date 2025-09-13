import os
import yaml
import boto3
from botocore.exceptions import ClientError

def load_s3_config():
    """Loads and parses the config.yaml file from the config/ directory."""
    try:
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
            s3_config_map = {
                item['jurisdiction_code']: item for item in config.get('aws', {}).get('s3', [])
            }
            config['aws']['s3_map'] = s3_config_map
            return config
    except FileNotFoundError:
        print("Error: config/config.yaml not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing config.yaml: {e}")
        return None

def fetch_document_from_s3(config, jurisdiction_code, source_id, file_key):
    """
    Fetches a specific document type from S3 based on a file key.
    For the main content view ('source_file'), it prioritizes 'juriscontent.html'
    and falls back to 'miniviewer.html' if the former is not found.
    """
    if not all([config, jurisdiction_code, source_id, file_key]):
        return "Error: Missing required information to fetch document."

    enrichment_files = config.get('enrichment_filenames', {})
    jurisdiction_config = config.get('aws', {}).get('s3_map', {}).get(jurisdiction_code)
    if not jurisdiction_config:
        return f"<h3>Configuration Error</h3><p>No S3 config for jurisdiction '{jurisdiction_code}'.</p>"

    bucket_name = jurisdiction_config.get('bucket_name')
    folder_name = jurisdiction_config.get('folder_name')
    s3_client = boto3.client('s3', region_name=config['aws'].get('default_region', 'ap-southeast-2'))
    html_content = None

    # --- NEW: Logic to try primary file first, then fallback ---
    if file_key == 'source_file':
        primary_filename = "juriscontent.html"
        primary_s3_key = f"{folder_name}/{source_id}/{primary_filename}"
        print(f"Attempting to fetch primary file: s3://{bucket_name}/{primary_s3_key}")
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=primary_s3_key)
            html_content = response['Body'].read().decode('utf-8')
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Primary file '{primary_filename}' not found. Proceeding to fallback.")
                html_content = None # Ensure content is None to trigger fallback logic
            else:
                print(f"An AWS ClientError occurred fetching primary file: {e}")
                return f"<h3>Error</h3><p>An AWS error occurred: {e.response['Error']['Message']}</p>"

    # --- MODIFIED: This block now serves as the fallback for 'source_file' or the default for other files ---
    if html_content is None:
        filename = enrichment_files.get(file_key)
        if not filename:
            return f"<h3>Configuration Error</h3><p>Filename for '{file_key}' not found in config.yaml.</p>"

        s3_key = f"{folder_name}/{source_id}/{filename}"
        if file_key == 'source_file':
            print(f"Attempting to fetch fallback file: s3://{bucket_name}/{s3_key}")
        else:
            print(f"Attempting to fetch: s3://{bucket_name}/{s3_key}")

        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            html_content = response['Body'].read().decode('utf-8')
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return f"<div style='padding: 20px; text-align: center;'><h3>File Not Found</h3><p>Neither the primary nor fallback file could be found.</p></div>"
            else:
                print(f"An AWS ClientError occurred: {e}")
                return f"<h3>Error</h3><p>An AWS error occurred: {e.response['Error']['Message']}</p>"
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return f"<h3>Error</h3><p>An unexpected error occurred: {e}</p>"

    # --- UNCHANGED: Font injection logic for the content tab ---
    if file_key == 'source_file' and html_content:
        font_style_tag = """
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500&display=swap" rel="stylesheet">
        <style> body {{ font-family: 'Poppins', sans-serif !important; }} </style>
        """
        if '</head>' in html_content:
            return html_content.replace('</head>', f'{font_style_tag}</head>')
        else:
            return f'<html><head>{font_style_tag}</head><body>{html_content}</body></html>'
    
    return html_content