# main.py

import os
import time
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables from .env file at the very top
load_dotenv()

from utils.helpers import load_config, DatabaseHandler, S3Handler
from utils.vector_db_handler import VectorDBHandler
from src.embedding_generator import EmbeddingGenerator

def main():
    """
    Main function to orchestrate the caselaw embedding process as a batch job.
    """
    print("Starting Caselaw Embedding Service (Batch Mode)...")
    
    # 1. Load Configuration
    config = load_config('config/config.yaml')
    
    server_pod_price = config.get('server_pod_price', {}).get('hour_price', 0.28)
    print(f"Using server pod hourly price: ${server_pod_price}")

    # 2. Initialize Handlers
    try:
        db_handler = DatabaseHandler(config)
        s3_handler = S3Handler(config)
        embedding_generator = EmbeddingGenerator(config)
        vector_db_handler = VectorDBHandler(config)
    except Exception as e:
        print(f"FATAL: Could not initialize handlers. Error: {e}")
        return

    # 3. Get years and jurisdictions to process from the config
    registry_config = config.get('registry', {}).get('caselaw_registry', {})
    processing_years = registry_config.get('processing_years', [])
    jurisdiction_codes = registry_config.get('jurisdiction_codes', [])

    # If lists are empty, replace them with a list containing 'None'.
    # This ensures the loop runs once and treats the filter as "all-inclusive".
    if not processing_years:
        processing_years = [None] 
    if not jurisdiction_codes:
        jurisdiction_codes = [None]

    print(f"Configured to process years: {processing_years if processing_years != [None] else 'All'}")
    print(f"Configured to process jurisdictions: {jurisdiction_codes if jurisdiction_codes != [None] else 'All'}")

    # 4. Loop through each year and then each jurisdiction from the config
    for year in processing_years:
        for jurisdiction in jurisdiction_codes:
            # Construct a descriptive name for the current processing batch
            year_str = str(year) if year else "All-Years"
            jur_str = jurisdiction if jurisdiction else "All-Jurisdictions"
            
            print(f"\n{'='*25}\nProcessing Year: {year_str}, Jurisdiction: {jur_str}\n{'='*25}")

            # 5. Get list of cases to process for the current year and jurisdiction
            try:
                source_ids_to_process = db_handler.get_cases_to_process(year, jurisdiction)
                if not source_ids_to_process:
                    print(f"No new cases to process for {year_str}-{jur_str}. Continuing.")
                    continue
                print(f"Found {len(source_ids_to_process)} cases to process for {year_str}-{jur_str}.")
            except Exception as e:
                print(f"ERROR: Could not fetch cases for {year_str}-{jur_str}. Skipping. Error: {e}")
                continue

            # 6. Find the S3 folder for each source_id
            id_to_folder_map = db_handler.find_s3_folder_for_ids(
                source_ids_to_process, 
                config['tables']['tables_to_read']
            )

            # 7. Process each case
            desc = f"Processing {year_str}-{jur_str}"
            source_text_filename = config['enrichment_filenames']['source_text']
            embedding_output_filename = config['enrichment_filenames']['embedding_output']
            
            for source_id in tqdm(source_ids_to_process, desc=desc):
                start_time = time.time()
                if source_id not in id_to_folder_map:
                    db_handler.update_embedding_status(source_id, 'fail_mapping', price=None)
                    continue
                try:
                    # Step A: Generate embedding and upload to S3
                    s3_folder = id_to_folder_map[source_id]
                    text_s3_key = f"{s3_folder}{source_id}/{source_text_filename}"
                    embedding_s3_key = f"{s3_folder}{source_id}/{embedding_output_filename}"
                    
                    caselaw_text = s3_handler.get_caselaw_text(text_s3_key)
                    embedding_vector = embedding_generator.generate_embedding_for_text(caselaw_text)
                    
                    if embedding_vector is None:
                        raise ValueError("Embedding generation returned None.")
                    
                    embedding_bytes = embedding_generator.save_embedding_to_bytes(embedding_vector)
                    s3_handler.upload_embedding(embedding_s3_key, embedding_bytes)

                    # Step B: Index document into OpenSearch Vector DB
                    # The inner try/except block has been removed. If this step fails,
                    # the main exception handler below will catch it and mark the status as 'fail'.
                    vector_db_handler.index_document(source_id, embedding_vector)

                    # Step C: Update status in relational DB
                    # This will now only run if both embedding and indexing are successful.
                    duration = time.time() - start_time
                    price = (duration / 3600) * server_pod_price
                    db_handler.update_embedding_status(source_id, 'pass', duration, price)

                except Exception as e:
                    # This block will now correctly handle errors from S3 or OpenSearch.
                    print(f"\nERROR processing source_id {source_id}: {e}")
                    db_handler.update_embedding_status(source_id, 'failed', price=None)

    print("\nCaselaw Embedding Service batch job finished.")


if __name__ == "__main__":
    main()
