#!/usr/bin/env python3
"""
Legal Document Ingestion Pipeline
Main entry point for ingesting caselaw and legislation documents into OpenSearch.
"""

import sys
import time
from datetime import datetime
from utils import get_logger, ConfigLoader
from src.ingestion import CaselawIngestion, LegislationIngestion

def main():
    """Main execution function."""
    # Initialize logger
    logger = get_logger("main")
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config_loader = ConfigLoader()
        config = config_loader.config
        
        logger.info("Starting Legal Document Ingestion Pipeline")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        
        # Track overall execution time
        start_time = time.time()
        
        # Check if any ingestion is enabled
        caselaw_enabled = config.get('caselaw_ingestion', False)
        legislation_enabled = config.get('legislation_ingestion', False)
        
        if not caselaw_enabled and not legislation_enabled:
            logger.warning("Both caselaw and legislation ingestion are disabled. Nothing to do.")
            return 0
        
        # Process caselaw ingestion if enabled
        if caselaw_enabled:
            logger.info("="*50)
            logger.info("CASELAW INGESTION - ENABLED")
            logger.info("="*50)
            
            caselaw_start = time.time()
            try:
                caselaw_ingestion = CaselawIngestion(config)
                caselaw_ingestion.ingest()
                caselaw_duration = time.time() - caselaw_start
                logger.info(f"Caselaw ingestion completed successfully in {caselaw_duration:.2f} seconds")
            except Exception as e:
                logger.error(f"Caselaw ingestion failed: {str(e)}", exc_info=True)
                # Continue with legislation if enabled
        else:
            logger.info("Caselaw ingestion is DISABLED in configuration")
        
        # Process legislation ingestion if enabled
        if legislation_enabled:
            logger.info("="*50)
            logger.info("LEGISLATION INGESTION - ENABLED")
            logger.info("="*50)
            
            legislation_start = time.time()
            try:
                legislation_ingestion = LegislationIngestion(config)
                legislation_ingestion.ingest()
                legislation_duration = time.time() - legislation_start
                logger.info(f"Legislation ingestion completed successfully in {legislation_duration:.2f} seconds")
            except Exception as e:
                logger.error(f"Legislation ingestion failed: {str(e)}", exc_info=True)
        else:
            logger.info("Legislation ingestion is DISABLED in configuration")
        
        # Calculate total execution time
        total_duration = time.time() - start_time
        
        logger.info("="*50)
        logger.info("INGESTION PIPELINE COMPLETED")
        logger.info(f"Total execution time: {total_duration:.2f} seconds")
        logger.info(f"Services executed: Caselaw={caselaw_enabled}, Legislation={legislation_enabled}")
        logger.info("="*50)
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())