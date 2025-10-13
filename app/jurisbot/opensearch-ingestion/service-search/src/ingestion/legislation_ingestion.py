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
            self.opensearch_available = True
        except Exception as e:
            self.logger.warning(f"OpenSearch initialization failed: {e}")
            self.opensearch_service = None
            self.opensearch_available = False
        
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
        total_indexing_errors = 0
        
        for i in range(0, len(records), self.batch_size):
            batch = records.iloc[i:i+self.batch_size]
            documents_to_index = []
            successful_source_ids = []  # Track which documents were successfully prepared
            failed_source_ids = []      # Track which documents failed preparation
            
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
                    
                    if content:
                        # Create document with ONLY the requested metadata fields
                        doc = LegislationDocument(
                            # Required fields
                            source_id=source_id,
                            section_id=row.get('section_id', ''),
                            book_name=row.get('book_name', ''),  # From legislation_registry
                            section_name=row.get('section_name', ''),
                            content=content,
                            
                            # ONLY the requested metadata fields for OpenSearch
                            legislation_number=row.get('legislation_number'),
                            type_of_document=row.get('type_of_document'),
                            enabling_act=row.get('enabling_act'),
                            amended_legislation=row.get('amended_legislation'),
                            administering_agency=row.get('administering_agency'),
                            affected_sectors=row.get('affected_sectors'),
                            practice_areas=row.get('practice_areas'),
                            keywords=row.get('keywords')
                        )
                        
                        documents_to_index.append(doc.to_dict())
                        successful_source_ids.append({
                            'source_id': source_id,
                            'section_id': row['section_id'],
                            'start_time': start_time
                        })
                        
                        # Log sample of metadata being indexed (only for first document in first batch)
                        if i == 0 and _ == batch.index[0]:
                            self.logger.info(f"Sample document metadata for {source_id} (section {row['section_id']}):")
                            doc_dict = doc.to_dict()
                            # Log only the fields that are actually sent to OpenSearch
                            sample_fields = [
                                'book_name', 'section_name', 'legislation_number',
                                'type_of_document', 'enabling_act', 'amended_legislation',
                                'administering_agency', 'affected_sectors', 'practice_areas', 'keywords'
                            ]
                            for key in sample_fields:
                                if key in doc_dict and doc_dict[key]:
                                    value = str(doc_dict[key])[:100] if doc_dict[key] else None
                                    if value:
                                        self.logger.info(f"  - {key}: {value}")
                    else:
                        # No content found - mark as failed
                        end_time = datetime.now()
                        duration = (end_time - start_time).total_seconds()
                        
                        self.logger.warning(f"No content found for source_id: {source_id}, section_id: {row['section_id']}")
                        self.db_service.update_ingestion_status(
                            source_id=source_id,
                            status='failed',
                            end_time=end_time,
                            duration=duration,
                            doc_type='legislation'
                        )
                        failed_source_ids.append(source_id)
                        total_errors += 1
                        
                except Exception as e:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    self.logger.error(f"Error processing legislation record {source_id}, section {row.get('section_id', 'unknown')}: {str(e)}")
                    self.db_service.update_ingestion_status(
                        source_id=source_id,
                        status='failed',
                        end_time=end_time,
                        duration=duration,
                        doc_type='legislation'
                    )
                    failed_source_ids.append(source_id)
                    total_errors += 1
            
            # Bulk index documents if OpenSearch is available and we have documents
            indexing_succeeded = False
            if documents_to_index and self.opensearch_available and self.opensearch_service:
                try:
                    success_count, error_count = self.opensearch_service.bulk_index_documents(documents_to_index)
                    self.logger.info(f"Indexed {success_count} documents to OpenSearch, {error_count} errors")
                    
                    # Check if indexing was successful
                    if success_count > 0 and error_count == 0:
                        indexing_succeeded = True
                    elif success_count > 0:
                        # Partial success
                        indexing_succeeded = True
                        self.logger.warning(f"Partial indexing success: {success_count} succeeded, {error_count} failed")
                    else:
                        indexing_succeeded = False
                        self.logger.error(f"All {error_count} documents failed to index to OpenSearch")
                        
                except Exception as e:
                    self.logger.error(f"Error indexing to OpenSearch: {e}")
                    indexing_succeeded = False
                    total_indexing_errors += len(documents_to_index)
            elif documents_to_index and not self.opensearch_available:
                self.logger.warning("OpenSearch not available, cannot index documents")
                indexing_succeeded = False
            
            # Update status based on indexing result
            for doc_info in successful_source_ids:
                end_time = datetime.now()
                duration = (end_time - doc_info['start_time']).total_seconds()
                
                if indexing_succeeded:
                    # Mark as pass only if indexing succeeded
                    self.db_service.update_ingestion_status(
                        source_id=doc_info['source_id'],
                        status='pass',
                        end_time=end_time,
                        duration=duration,
                        doc_type='legislation'
                    )
                    total_success += 1
                else:
                    # Mark as failed if indexing failed
                    self.db_service.update_ingestion_status(
                        source_id=doc_info['source_id'],
                        status='failed',
                        end_time=end_time,
                        duration=duration,
                        doc_type='legislation'
                    )
                    total_indexing_errors += 1
                    self.logger.error(f"Marking {doc_info['source_id']} (section {doc_info['section_id']}) as failed due to OpenSearch indexing failure")
            
            self.logger.info(
                f"Processed batch {i//self.batch_size + 1}/{(len(records)-1)//self.batch_size + 1}: "
                f"{len(successful_source_ids) if indexing_succeeded else 0} successful, "
                f"{len(failed_source_ids) + (len(successful_source_ids) if not indexing_succeeded else 0)} failed"
            )
        
        # Final summary
        self.logger.info("="*50)
        self.logger.info(f"Legislation ingestion completed:")
        self.logger.info(f"  - Successfully indexed: {total_success}")
        self.logger.info(f"  - Failed (S3/processing): {total_errors}")
        self.logger.info(f"  - Failed (OpenSearch indexing): {total_indexing_errors}")
        
        # Get updated status summary
        updated_summary = self.db_service.get_ingestion_status_summary('legislation')
        self.logger.info("Updated status totals:")
        self.logger.info(f"  - Pass: {updated_summary.get('pass', 0)}")
        self.logger.info(f"  - Failed: {updated_summary.get('failed', 0)}")
        self.logger.info(f"  - Started: {updated_summary.get('started', 0)}")
        self.logger.info(f"  - Not Started: {updated_summary.get('not started', 0)}")
        
        if total_indexing_errors > 0:
            self.logger.warning(
                f"\n*** IMPORTANT: {total_indexing_errors} documents failed to index to OpenSearch. ***\n"
                f"These are marked as 'failed' in the database and will be retried on the next run.\n"
                f"Check OpenSearch configuration and connectivity."
            )