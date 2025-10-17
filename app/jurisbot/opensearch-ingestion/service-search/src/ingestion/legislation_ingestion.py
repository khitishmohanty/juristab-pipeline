import os
import re
import time
from datetime import datetime
from typing import Optional, List, Dict
from src.models import LegislationDocument
from src.services import DatabaseService, S3Service, OpenSearchService
from utils import get_logger
from sqlalchemy import text

class LegislationIngestion:
    """Handler for legislation document ingestion with section-level content."""
    
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
        
        # Regex pattern to match only miniviewer_<integer>.txt files
        self.section_file_pattern = re.compile(r'^miniviewer_(\d+)\.txt$')
    
    def update_ingestion_status_directly(
        self,
        source_id: str,
        status: str,
        start_time: datetime = None,
        end_time: datetime = None,
        duration: float = None
    ) -> bool:
        """
        Direct database update for ingestion status with explicit transaction handling.
        """
        try:
            table_config = self.config['tables']['legislation']
            table_name = table_config['enrichment_status_table']
            fields = table_config['status_tracking_fields']
            
            # Build update query parts
            update_parts = [f"{fields['status_field']} = :status"]
            params = {'source_id': source_id, 'status': status}
            
            if start_time is not None:
                update_parts.append(f"{fields['start_time_field']} = :start_time")
                params['start_time'] = start_time
            
            if end_time is not None:
                update_parts.append(f"{fields['end_time_field']} = :end_time")
                params['end_time'] = end_time
            
            if duration is not None:
                update_parts.append(f"{fields['duration_field']} = :duration")
                params['duration'] = duration
            
            update_query = text(f"""
                UPDATE {table_name}
                SET {', '.join(update_parts)}
                WHERE {fields['source_id']} = :source_id
            """)
            
            # Use explicit transaction
            with self.db_service.dest_engine.begin() as conn:
                result = conn.execute(update_query, params)
                
                if result.rowcount > 0:
                    self.logger.debug(f"Updated status for {source_id}: {status}")
                    return True
                else:
                    self.logger.warning(f"No rows updated for {source_id} - record might not exist")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error updating status for {source_id}: {str(e)}")
            return False
    
    def ingest(self):
        """Main ingestion process for legislation documents."""
        self.logger.info("Starting legislation ingestion process...")
        
        # Get status summary with new criteria
        status_summary = self.get_ingestion_status_summary()
        self.logger.info("Current ingestion status summary (with prerequisite checks):")
        self.logger.info(f"  - Pass: {status_summary.get('pass', 0)}")
        self.logger.info(f"  - Failed: {status_summary.get('failed', 0)}")
        self.logger.info(f"  - Started: {status_summary.get('started', 0)}")
        self.logger.info(f"  - Not Started: {status_summary.get('not started', 0)}")
        self.logger.info(f"  - Prerequisites Not Met: {status_summary.get('prerequisites_not_met', 0)}")
        
        # Calculate records to process
        to_process = (
            status_summary.get('failed', 0) + 
            status_summary.get('started', 0) + 
            status_summary.get('not started', 0)
        )
        
        if to_process == 0:
            if status_summary.get('prerequisites_not_met', 0) > 0:
                self.logger.info(f"All eligible records processed. {status_summary.get('prerequisites_not_met', 0)} records waiting for prerequisites to be met.")
            else:
                self.logger.info("All legislation records have been successfully processed (status='pass'). Nothing to do.")
            return
        
        self.logger.info(f"Found {to_process} records to process (excluding 'pass' status and records without prerequisites)")
        
        # Get filter parameters
        years = self.config['ingestion']['legislation'].get('years', [])
        jurisdiction_codes = self.config['ingestion']['legislation'].get('jurisdiction_codes', [])
        
        # Get records to ingest with new prerequisites check
        records = self.get_legislation_for_ingestion_with_prerequisites(
            years=years if years else None,
            jurisdiction_codes=jurisdiction_codes if jurisdiction_codes else None
        )
        
        if records.empty:
            self.logger.info("No legislation records found for ingestion after filtering")
            return
        
        self.logger.info(f"Processing {len(records)} legislation records with prerequisites met")
        
        # Log status breakdown of records to be processed
        status_counts = records['current_status'].value_counts()
        for status, count in status_counts.items():
            self.logger.info(f"  - {status}: {count} records")
        
        # Process in batches
        total_success = 0
        total_errors = 0
        total_indexing_errors = 0
        total_skipped_files = 0
        
        for i in range(0, len(records), self.batch_size):
            batch = records.iloc[i:i+self.batch_size]
            documents_to_index = []
            # Track documents with their processing info
            document_tracking = []  # List of dicts with source_id, section_id, start_time
            
            for _, row in batch.iterrows():
                source_id = row['source_id']
                section_id = row.get('section_id', '')
                
                # Validate section_id is numeric
                if not str(section_id).isdigit():
                    self.logger.warning(f"Skipping non-numeric section_id: {section_id} for source_id: {source_id}")
                    total_skipped_files += 1
                    continue
                
                # Mark as started
                start_time = datetime.now()
                self.update_ingestion_status_directly(
                    source_id=source_id,
                    status='started',
                    start_time=start_time
                )
                
                try:
                    # Build file path with new structure
                    base_path = row['file_path']
                    if not base_path.endswith('/'):
                        base_path += '/'
                    
                    # Add section-level-content folder
                    file_path = base_path + 'section-level-content/'
                    
                    # Add the specific section file
                    file_name = f"miniviewer_{section_id}.txt"
                    
                    # Validate filename against regex pattern
                    if not self.section_file_pattern.match(file_name):
                        self.logger.warning(f"Skipping file {file_name} - doesn't match pattern miniviewer_<integer>.txt")
                        total_skipped_files += 1
                        continue
                    
                    full_path = file_path + file_name
                    
                    self.logger.debug(f"Reading file: {full_path}")
                    
                    # Read content from S3
                    content = self.s3_service.read_file(full_path)
                    
                    if content:
                        # Create document with ONLY the requested metadata fields
                        doc = LegislationDocument(
                            # Required fields
                            source_id=source_id,
                            section_id=str(section_id),
                            book_name=row.get('book_name', ''),
                            section_name='',  # Always blank as requested
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
                        document_tracking.append({
                            'source_id': source_id,
                            'section_id': section_id,
                            'start_time': start_time
                        })
                        
                        # Log sample of metadata being indexed (only for first document in first batch)
                        if i == 0 and _ == batch.index[0]:
                            self.logger.info(f"Sample document metadata for {source_id} (section {section_id}):")
                            self.logger.info(f"  - File path: {full_path}")
                            doc_dict = doc.to_dict()
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
                        # No content found - mark as failed immediately
                        end_time = datetime.now()
                        duration = (end_time - start_time).total_seconds()
                        
                        self.logger.warning(f"No content found at path: {full_path}")
                        self.update_ingestion_status_directly(
                            source_id=source_id,
                            status='failed',
                            end_time=end_time,
                            duration=duration
                        )
                        total_errors += 1
                        
                except Exception as e:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    
                    self.logger.error(f"Error processing legislation record {source_id}, section {section_id}: {str(e)}")
                    self.update_ingestion_status_directly(
                        source_id=source_id,
                        status='failed',
                        end_time=end_time,
                        duration=duration
                    )
                    total_errors += 1
            
            # Bulk index documents if we have any
            if documents_to_index:
                if self.opensearch_available and self.opensearch_service:
                    try:
                        success_count, error_count = self.opensearch_service.bulk_index_documents(documents_to_index)
                        self.logger.info(f"Indexed {success_count} documents to OpenSearch, {error_count} errors")
                        
                        # Determine overall success for this batch
                        batch_succeeded = (success_count > 0 and error_count == 0)
                        
                        if error_count > 0:
                            self.logger.warning(f"Batch had {error_count} indexing errors out of {len(documents_to_index)} documents")
                        
                        # Update status for ALL documents in this batch based on indexing result
                        for doc_info in document_tracking:
                            end_time = datetime.now()
                            duration = (end_time - doc_info['start_time']).total_seconds()
                            
                            if batch_succeeded:
                                # Only mark as pass if ALL documents in batch succeeded
                                self.update_ingestion_status_directly(
                                    source_id=doc_info['source_id'],
                                    status='pass',
                                    end_time=end_time,
                                    duration=duration
                                )
                                total_success += 1
                            else:
                                # Mark as failed if ANY document in batch failed
                                self.update_ingestion_status_directly(
                                    source_id=doc_info['source_id'],
                                    status='failed',
                                    end_time=end_time,
                                    duration=duration
                                )
                                total_indexing_errors += 1
                                self.logger.error(f"Marking {doc_info['source_id']} (section {doc_info['section_id']}) as failed due to OpenSearch indexing failure")
                        
                    except Exception as e:
                        self.logger.error(f"Critical error during bulk indexing: {str(e)}")
                        
                        # Mark ALL documents in batch as failed
                        for doc_info in document_tracking:
                            end_time = datetime.now()
                            duration = (end_time - doc_info['start_time']).total_seconds()
                            
                            self.update_ingestion_status_directly(
                                source_id=doc_info['source_id'],
                                status='failed',
                                end_time=end_time,
                                duration=duration
                            )
                            total_indexing_errors += 1
                            self.logger.error(f"Marking {doc_info['source_id']} as failed due to OpenSearch exception: {str(e)}")
                        
                        # Check if this is a circuit breaker exception
                        if 'circuit_breaking_exception' in str(e).lower():
                            self.logger.critical(
                                "\n*** CRITICAL: OpenSearch circuit breaker activated! ***\n"
                                "OpenSearch is out of memory. You need to:\n"
                                "1. Increase OpenSearch heap memory, OR\n"
                                "2. Delete old documents to free space, OR\n"
                                "3. Increase circuit breaker limits\n"
                                "Stopping ingestion to prevent further failures.\n"
                            )
                            # Stop processing further batches
                            break
                            
                else:
                    self.logger.warning("OpenSearch not available, marking all documents as failed")
                    # Mark all documents as failed if OpenSearch is not available
                    for doc_info in document_tracking:
                        end_time = datetime.now()
                        duration = (end_time - doc_info['start_time']).total_seconds()
                        
                        self.update_ingestion_status_directly(
                            source_id=doc_info['source_id'],
                            status='failed',
                            end_time=end_time,
                            duration=duration
                        )
                        total_indexing_errors += 1
            
            self.logger.info(
                f"Processed batch {i//self.batch_size + 1}/{(len(records)-1)//self.batch_size + 1}: "
                f"Success: {len([d for d in document_tracking if d])} documents prepared, "
                f"Errors: {total_errors + total_indexing_errors} total"
            )
        
        # Final summary
        self.logger.info("="*50)
        self.logger.info(f"Legislation ingestion completed:")
        self.logger.info(f"  - Successfully indexed: {total_success}")
        self.logger.info(f"  - Failed (S3/processing): {total_errors}")
        self.logger.info(f"  - Failed (OpenSearch indexing): {total_indexing_errors}")
        self.logger.info(f"  - Skipped (invalid filenames): {total_skipped_files}")
        
        # Get updated status summary
        updated_summary = self.get_ingestion_status_summary()
        self.logger.info("Updated status totals:")
        self.logger.info(f"  - Pass: {updated_summary.get('pass', 0)}")
        self.logger.info(f"  - Failed: {updated_summary.get('failed', 0)}")
        self.logger.info(f"  - Started: {updated_summary.get('started', 0)}")
        self.logger.info(f"  - Not Started: {updated_summary.get('not started', 0)}")
        self.logger.info(f"  - Prerequisites Not Met: {updated_summary.get('prerequisites_not_met', 0)}")
        
        if total_indexing_errors > 0:
            self.logger.warning(
                f"\n*** IMPORTANT: {total_indexing_errors} documents failed to index to OpenSearch. ***\n"
                f"These are marked as 'failed' in the database and will be retried on the next run.\n"
                f"Check OpenSearch configuration and connectivity."
            )
    
    def list_s3_section_files(self, base_path: str) -> List[int]:
        """List all valid section files in S3 and return section IDs."""
        import boto3
        
        if not base_path.endswith('/'):
            base_path += '/'
        section_path = base_path + 'section-level-content/'
        
        try:
            # List all files in the section-level-content folder
            s3_client = self.s3_service.s3_client
            bucket_name = self.s3_service.bucket_name
            
            # Clean up the path for S3
            if section_path.startswith('s3://'):
                section_path = section_path[5:]
                if section_path.startswith(f'{bucket_name}/'):
                    section_path = section_path[len(bucket_name)+1:]
            if section_path.startswith('/'):
                section_path = section_path[1:]
            
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=section_path
            )
            
            section_ids = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Get just the filename
                    filename = obj['Key'].split('/')[-1]
                    # Check if it matches the pattern
                    match = self.section_file_pattern.match(filename)
                    if match:
                        section_id = int(match.group(1))
                        section_ids.append(section_id)
            
            return sorted(section_ids)
            
        except Exception as e:
            self.logger.error(f"Error listing S3 files in {section_path}: {e}")
            return []
    
    def get_ingestion_status_summary(self) -> dict:
        """Get status summary with prerequisite checks."""
        try:
            query = """
                SELECT 
                    CASE 
                        WHEN juriscontent_html_content_verification_status != 'pass' 
                            OR status_juriscontent_section_extract != 'pass'
                            OR status_juriscontent_html != 'pass' THEN 'prerequisites_not_met'
                        ELSE COALESCE(status_opensearch_ingestion_search_service, 'not started')
                    END as status,
                    COUNT(*) as count
                FROM legislation_enrichment_status
                WHERE status_text_processor = 'pass'
                GROUP BY status
            """
            
            with self.db_service.dest_engine.connect() as conn:
                result = conn.execute(text(query))
                
                summary = {}
                for row in result:
                    status = row[0]
                    count = row[1]
                    summary[status] = count
                
                # Add zero counts for missing statuses
                for status in ['pass', 'failed', 'started', 'not started', 'prerequisites_not_met']:
                    if status not in summary:
                        summary[status] = 0
                
                return summary
                
        except Exception as e:
            self.logger.error(f"Error getting status summary: {str(e)}")
            return {'pass': 0, 'failed': 0, 'started': 0, 'not started': 0, 'prerequisites_not_met': 0}
    
    def get_legislation_for_ingestion_with_prerequisites(
        self,
        years: Optional[List[int]] = None,
        jurisdiction_codes: Optional[List[str]] = None
    ) -> 'pd.DataFrame':
        """Get legislation records that meet all prerequisites and expand with S3 sections."""
        import pandas as pd
        table_config = self.config['tables']['legislation']
        
        # Simpler query without legislation_content table
        query = f"""
        SELECT 
            lr.source_id,
            lr.book_name,
            lr.file_path,
            lr.year as registry_year,
            lr.jurisdiction_code as registry_jurisdiction,
            lm.legislation_number,
            lm.type_of_document,
            lm.enabling_act,
            lm.amended_legislation,
            lm.administering_agency,
            lm.affected_sectors,
            lm.practice_areas,
            lm.keywords,
            COALESCE(les.status_opensearch_ingestion_search_service, 'not started') as current_status
        FROM 
            {table_config['registry_table']} lr
        INNER JOIN 
            {table_config['enrichment_status_table']} les 
            ON lr.source_id = les.source_id
        LEFT JOIN 
            {table_config['metadata_table']} lm
            ON lr.source_id = lm.source_id
        WHERE 
            lr.status_registration = 'pass'
            AND les.status_text_processor = 'pass'
            -- New prerequisite checks
            AND les.juriscontent_html_content_verification_status = 'pass'
            AND les.status_juriscontent_section_extract = 'pass'
            AND les.status_juriscontent_html = 'pass'
            -- Exclude already processed
            AND (les.status_opensearch_ingestion_search_service IS NULL 
                 OR les.status_opensearch_ingestion_search_service != 'pass')
        """
        
        conditions = []
        if years:
            years_str = ','.join(map(str, years))
            conditions.append(f"lr.year IN ({years_str})")
        
        if jurisdiction_codes:
            codes_str = ','.join([f"'{code}'" for code in jurisdiction_codes])
            conditions.append(f"lr.jurisdiction_code IN ({codes_str})")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        self.logger.info(f"Executing legislation query with prerequisite checks")
        
        with self.db_service.dest_engine.connect() as conn:
            base_df = pd.read_sql(text(query), conn)
            
            if base_df.empty:
                return base_df
            
            # Now expand each legislation with its section files from S3
            expanded_rows = []
            for _, row in base_df.iterrows():
                # List all section files in S3 for this legislation
                section_ids = self.list_s3_section_files(row['file_path'])
                
                if not section_ids:
                    self.logger.warning(f"No valid section files found for source_id: {row['source_id']} at path: {row['file_path']}")
                    continue
                
                # Create one row for each section
                for section_id in section_ids:
                    section_row = row.copy()
                    section_row['section_id'] = section_id
                    section_row['section_name'] = ''  # Blank as requested
                    expanded_rows.append(section_row)
                
                self.logger.info(f"Found {len(section_ids)} sections for source_id: {row['source_id']}")
            
            if not expanded_rows:
                return pd.DataFrame()  # Return empty DataFrame if no sections found
            
            # Create final DataFrame with all sections
            result_df = pd.DataFrame(expanded_rows)
            
            # Use registry values where metadata is missing
            if 'year' not in result_df.columns or result_df['year'].isna().all():
                result_df['year'] = result_df['registry_year']
            if 'jurisdiction_code' not in result_df.columns or result_df['jurisdiction_code'].isna().all():
                result_df['jurisdiction_code'] = result_df['registry_jurisdiction']
                
            return result_df