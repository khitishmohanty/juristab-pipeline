from utils.db_connector import get_db_engine, get_pending_caselaws
from src.vector_ingestor import VectorIngestor
import time
import pandas as pd # You will likely need pandas to handle the result from get_pending_caselaws

def main():
    """
    Main function to run the entire ingestion pipeline.
    """
    print("--- Starting Vector DB Ingestion Pipeline ---")
    start_time = time.time()

    try:
        # 1. Get database connection engine
        db_engine = get_db_engine()

        # 2. Fetch list of caselaws to process
        # The result from your function is a list of dicts, let's make it a DataFrame
        pending_caselaws_list = get_pending_caselaws(db_engine)
        
        if pending_caselaws_list:
            # Convert to DataFrame as the ingestor expects .iterrows()
            pending_caselaws = pd.DataFrame(pending_caselaws_list)
            
            # 3. Initialize the ingestor
            ingestor = VectorIngestor(db_engine)

            # 4. Run the ingestion process
            ingestor.run_pipeline(pending_caselaws)
        else:
            print("No pending caselaws found to ingest.") # Added a message for the 'else' case
        
    except Exception as e:
        print(f"A critical error occurred in the pipeline: {e}")
    
    end_time = time.time()
    print(f"--- Pipeline finished in {end_time - start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()