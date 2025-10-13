import os
import time
from datetime import datetime
from typing import Optional, List
from src.models import LegislationDocument
from src.services import DatabaseService, S3Service, OpenSearchService
from utils import get_logger

class LegislationIngestion:
    """Handler for legislation document ingestion."""
    
    def __init__(self, config: dict):
        """Initialize legislation ingestion handler."""
        self.logger = get_logger(__name__)
        self.config = config
        self.db_service = DatabaseService(config)
        self.s3_service = S3Service(config)
        
        try:
            self.opensearch_service = OpenSearchService(config)
        except Exception as e:
            self.logger.warning(f"OpenSearch initialization failed: {e}")
            self.opensearch_service = None
        
        self.batch_size = config['ingestion']['legislation'].get('batch_size', 100)
    
    def ingest(self):
        """Main ingestion process for legislation documents."""
        self.logger.info("Starting legislation ingestion process...")
        
        # Get status summary first
        status_summary = self.db_service.get_ingestion_status_summary('legislation')
        self.logger.info("Current ingestion status summary:")
        self.logger.info(f"  - Pass: {status_summary.get('pass', 0)}")
        self.logger.info(f"  - Failed: {status_summary.get('failed', 0)}")
        self.logger.info(f"  - Started: {status_summary.get('started', 0)}")
        self.logger.info(f"  - Not Started: {status_summary.get('not started', 0)}")
        
        # Calculate records to process
        to_process = (
            status_summary.get('failed', 0) + 
            status_summary.get('started', 0) + 
            status_summary.get('not started', 0)
        )
        
        if to_process == 0:
            self.logger.info("All legislation records have been successfully processed (status='pass'). Nothing to do.")
            return
        
        self.logger.info(f"Found {to_process} records to process (excluding 'pass' status)")
        
        # Get filter parameters
        years = self.config['ingestion']['legislation'].get('years', [])
        jurisdiction_codes = self.config['ingestion']['legislation'].get('jurisdiction_codes', [])
        
        # Get records to ingest (excluding pass status)
        records = self.db_service.get_legislation_for_ingestion(
            years=years if years else None,
            jurisdiction_codes=jurisdiction_codes if jurisdiction_codes else None,
            exclude_pass=True  # Only get records that are not 'pass'
        )
        
        if records.empty:
            self.logger.info("No legislation records found for ingestion after filtering")
            return
        
        self.logger.info(f"Processing {len(records)} legislation records (non-pass status)")
        
        # Log status breakdown of records to be processed
        status_counts = records['current_status'].value_counts()
        for status, count in status_counts.items():
            self.logger.info(f"  - {status}: {count} records")
        
        # Process in batches
        total_success = 0
        total_errors = 0
        
        for i in range(0, len(records), self.batch_size):
            batch = records.iloc[i:i+self.batch_size]
            documents = []
            
            for _, row in batch.iterrows():
                source_id = row['source_id']
                
                # Mark as started
                self.db_service.update_ingestion_status(
                    source_id=source_id,
                    status='started',
                    start_time=datetime.now(),
                    doc_type='legislation'
                )
                
                start_time = datetime.now()
                
                try:
                    # Build file path
                    file_path = row['file_path']
                    if not file_path.endswith('/'):
                        file_path += '/'
                    
                    # Replace {section_id} in the file pattern
                    file_name = self.config['tables']['legislation']['source_file_pattern'].replace(
                        '{section_id}', 
                        str(row['section_id'])
                    )
                    file_path += file_name
                    
                    # Read content from S3
                    content = self.s3_service.read_file(file_path)
                    
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    if content:
                        # Create document
                        doc = LegislationDocument(
                            source_id=source_id,
                            section_id=row['section_id'],
                            book_type=row['type_of_document'],
                            book_name=row['book_name'],
                            section_name=row['section_name'],
                            content=content
                        )
                        documents.append(doc.to_dict())
                        
                        # Update status to pass
                        self.db_service.update_ingestion_status(
                            source_id=source_id,
                            status='pass',
                            end_time=end_time,
                            duration=duration,
                            doc_type='legislation'
                        )
                        total_success += 1
                    else:
                        # Update status to failed
                        self.logger.warning(f"No content found for source_id: {source_id}, section_id: {row['section_id']}")
                        self.db_service.update_ingestion_status(
                            source_id=source_id,
                            status='failed',
                            end_time=end_time,
                            duration=duration,
                            doc_type='legislation'
                        )
                        total_errors += 1
                        
                except Exception as e:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    self.logger.error(f"Error processing legislation record {source_id}, section {row['section_id']}: {str(e)}")
                    self.db_service.update_ingestion_status(
                        source_id=source_id,
                        status='failed',
                        end_time=end_time,
                        duration=duration,
                        doc_type='legislation'
                    )
                    total_errors += 1
            
            # Bulk index documents if OpenSearch is available
            if documents and self.opensearch_service:
                try:
                    success, errors = self.opensearch_service.bulk_index_documents(documents)
                    self.logger.info(f"Indexed {success} documents to OpenSearch, {errors} errors")
                except Exception as e:
                    self.logger.error(f"Error indexing to OpenSearch: {e}")
            
            self.logger.info(
                f"Processed batch {i//self.batch_size + 1}/{(len(records)-1)//self.batch_size + 1}: "
                f"{len(documents)} successful, {len(batch) - len(documents)} failed"
            )
        
        # Final summary
        self.logger.info("="*50)
        self.logger.info(f"Legislation ingestion completed:")
        self.logger.info(f"  - Newly successful: {total_success}")
        self.logger.info(f"  - Failed: {total_errors}")
        
        # Get updated status summary
        updated_summary = self.db_service.get_ingestion_status_summary('legislation')
        self.logger.info("Updated status totals:")
        self.logger.info(f"  - Pass: {updated_summary.get('pass', 0)}")
        self.logger.info(f"  - Failed: {updated_summary.get('failed', 0)}")
        self.logger.info(f"  - Started: {updated_summary.get('started', 0)}")
        self.logger.info(f"  - Not Started: {updated_summary.get('not started', 0)}")