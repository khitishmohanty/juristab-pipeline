import re

def get_full_s3_key(source_id, jurisdiction, config, file_purpose: str):
    """
    Constructs the full S3 key based on the provided source_id, jurisdiction,
    and configuration settings.

    Args:
        source_id (str): The unique identifier for the case law.
        jurisdiction (str): The jurisdiction code (e.g., 'NSW').
        config (Config): The application configuration object.
        file_purpose (str): The purpose of the file, either 'rulebased' or 'ai'.
        
    Returns:
        str: The full S3 key (path) to the file, or None if not found.
    """
    s3_configs = config.get('aws', 's3')
    
    # MODIFIED: Select the correct source file based on the purpose
    if file_purpose == 'rulebased':
        source_file = config.get('enrichment_filenames', 'source_file_rulebased')
    elif file_purpose == 'ai':
        source_file = config.get('enrichment_filenames', 'source_file_ai')
    else:
        # If an invalid purpose is given, return None
        return None

    # Find the correct bucket and destination folder based on the jurisdiction
    s3_dest_folder = None
    for s3_config in s3_configs:
        if s3_config.get('jurisdiction_code') == jurisdiction:
            s3_dest_folder = s3_config.get('s3_dest_folder')
            break
            
    if not s3_dest_folder or not source_file:
        return None

    # Construct the final S3 key
    # Key format: <s3_dest_folder><source_id>/<source_file>
    s3_key = f"{s3_dest_folder}{source_id}/{source_file}"
    return s3_key
