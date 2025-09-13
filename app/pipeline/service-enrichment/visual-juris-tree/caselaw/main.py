import os
import sys
# Adjust the path to allow imports from the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from utils.config_manager import ConfigManager
from utils.audit_logger import AuditLogger
from src.ai_processor import AiProcessor

def main():
    """
    Main entry point for the AI Enrichment & Visualization service.
    MODIFIED: This function now iterates through each processing year defined in the 
    config, and for each year, it processes all source tables.
    """
    print("Starting AI Enrichment Service...")
    
    config_path = './config/config.yaml'
    prompt_path = './config/prompt.txt'
    env_path = '.env'
    
    try:
        config_manager = ConfigManager(config_path=config_path, env_path=env_path)
        config = config_manager.get_config()
    except Exception as e:
        print(f"FATAL: Failed to load configuration. Aborting. Error: {e}")
        sys.exit(1)

    # --- MODIFIED: Loop through each processing year, then each source ---
    source_configs = config['tables'].get('tables_to_read', [])
    registry_config = config.get('tables_registry', {})
    processing_years = registry_config.get('processing_years', [])

    if not source_configs:
        print("No source tables found in the configuration. Exiting.")
        sys.exit(0)

    if not processing_years:
        print("No 'processing_years' found under 'tables_registry' in the configuration. Exiting.")
        sys.exit(0)

    # --- Outer loop for years ---
    for year in processing_years:
        print(f"\n=======================================================")
        print(f"========== STARTING PROCESSING FOR YEAR: {year} ==========")
        print(f"=======================================================")

        # --- Inner loop for source tables ---
        for source_info in source_configs:
            log_id = None
            # Add year to the job name for more specific audit logging
            base_job_name = source_info.get('audit_job_name', 'ai_enrichment_job_unnamed')
            job_name = f"{base_job_name}_{year}"
            print(f"\n--- Starting processing for job: {job_name} ---")

            try:
                # Use the 'legal_store' database for audit logging
                audit_db_config = config['database']['destination']
                audit_table_name = config['audit_log']['text_extraction_job']['table']
                
                logger = AuditLogger(db_config=audit_db_config, table_name=audit_table_name)
                log_id = logger.log_start(job_name=job_name)

            except Exception as e:
                print(f"FATAL: Could not start audit logger for job {job_name}. Aborting this job. Error: {e}")
                continue # Skip to the next job

            try:
                # Pass the specific source_info and the current year to the processor
                processor = AiProcessor(
                    config=config, 
                    prompt_path=prompt_path, 
                    source_info=source_info,
                    processing_year=year  # Pass the current year
                )
                processor.process_cases()
                logger.log_end(log_id, status='completed', message=f'{job_name} finished successfully.')
            except Exception as e:
                error_message = f"Job '{job_name}' failed due to an unhandled exception: {str(e)}"
                print(error_message)
                if log_id:
                    logger.log_end(log_id, status='failed', message=error_message)
                continue # Continue to the next job in the list

if __name__ == "__main__":
    main()