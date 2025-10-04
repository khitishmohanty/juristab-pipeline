from utils.db_connector import get_db_engine, get_pending_documents
from src.vector_ingestor import VectorIngestor
from utils.config_loader import config  # <-- Import the config object
import time
import pandas as pd

def main():
    """
    Main function to run the entire ingestion pipeline.
    It reads the config file to determine whether to process 'caselaw', 
    'legislation', or both.
    """
    print("--- Starting Vector DB Ingestion Pipeline ---")
    start_time = time.time()

    # 1. Read the config to see which document types to process
    doc_types_to_process = []
    embedding_config = config.get('embedding', {})
    if embedding_config.get('caselaw'):
        doc_types_to_process.append('caselaw')
    if embedding_config.get('legislation'):
        doc_types_to_process.append('legislation')

    if not doc_types_to_process:
        print("No document types are enabled for embedding in config.yaml. Exiting.")
        return

    # 2. Loop through each enabled document type and run the pipeline
    try:
        db_engine = get_db_engine()
        
        for doc_type in doc_types_to_process:
            print(f"\n--- Processing document type: '{doc_type}' ---")
            
            # Fetch list of documents for the current type
            pending_docs_list = get_pending_documents(db_engine, doc_type)
            
            if pending_docs_list:
                pending_documents = pd.DataFrame(pending_docs_list)
                
                # Initialize the ingestor with the correct document type
                ingestor = VectorIngestor(db_engine, doc_type)

                # Run the ingestion process
                ingestor.run_pipeline(pending_documents)
            else:
                print(f"No pending '{doc_type}' documents found to ingest.")
        
    except Exception as e:
        print(f"A critical error occurred in the pipeline: {e}")
    
    end_time = time.time()
    print(f"\n--- Entire pipeline finished in {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()