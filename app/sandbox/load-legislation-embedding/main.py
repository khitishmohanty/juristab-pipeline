from utils.db_connector import get_db_engine, get_pending_documents
from src.vector_ingestor import VectorIngestor
import time
import pandas as pd
import argparse

def main():
    """
    Main function to run the entire ingestion pipeline.
    It can be configured to run for 'caselaw' or 'legislation'.
    """
    # --- Configuration ---
    # Set the document type you want to process.
    # This can be changed to 'caselaw' to run the pipeline for caselaws.
    doc_type_to_process = 'legislation'
    # -------------------

    print(f"--- Starting Vector DB Ingestion Pipeline for '{doc_type_to_process}' ---")
    start_time = time.time()

    try:
        # 1. Get database connection engine
        db_engine = get_db_engine()

        # 2. Fetch list of documents to process
        pending_docs_list = get_pending_documents(db_engine, doc_type_to_process)
        
        if pending_docs_list:
            # Convert list of dictionaries to a pandas DataFrame
            pending_documents = pd.DataFrame(pending_docs_list)
            
            # 3. Initialize the ingestor with the correct document type
            ingestor = VectorIngestor(db_engine, doc_type_to_process)

            # 4. Run the ingestion process
            ingestor.run_pipeline(pending_documents)
        else:
            print(f"No pending '{doc_type_to_process}' documents found to ingest.")
        
    except Exception as e:
        print(f"A critical error occurred in the pipeline: {e}")
    
    end_time = time.time()
    print(f"--- Pipeline finished in {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()
