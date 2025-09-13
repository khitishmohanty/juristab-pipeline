import re

def get_full_s3_key(source_id, jurisdiction, config):
    """
    Constructs the full S3 key based on the provided source_id, jurisdiction,
    and configuration settings.

    Args:
        source_id (str): The unique identifier for the case law (e.g., 'cfb227b6-8590-47ff-beef-d3cdb55f2f7f').
        jurisdiction (str): The jurisdiction code (e.g., 'NSW').
        config (Config): The application configuration object.
        
    Returns:
        str: The full S3 key (path) to the file, or None if the jurisdiction is not found.
    """
    s3_configs = config.get('aws', 's3')
    source_file = config.get('enrichment_filenames', 'source_file')
    
    # Find the correct bucket and destination folder based on the jurisdiction
    s3_dest_folder = None
    for s3_config in s3_configs:
        if s3_config.get('jurisdiction_code') == jurisdiction:
            s3_dest_folder = s3_config.get('s3_dest_folder')
            break
            
    if not s3_dest_folder:
        return None

    # Construct the final S3 key
    # Key format: <s3_dest_folder><source_id>/<source_file>
    s3_key = f"{s3_dest_folder}{source_id}/{source_file}"
    return s3_key
