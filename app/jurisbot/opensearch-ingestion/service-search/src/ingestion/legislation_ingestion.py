import os
from typing import Optional, List
from src.models import LegislationDocument
from src.services import DatabaseService, S3Service, OpenSearchService
from utils import get_logger

class LegislationIngestion:
    """Handler for legislation document ingestion."""
    
    def __init__(self, config: dict):
        """
        Initialize legislation ingestion handler.
        
        Args:
            config: Application configuration
        """
        self.logger = get_logger(__name__)
        self.config = config
        self.db_service = DatabaseService(config)
        self.s3_service = S3Service(config)
        self.opensearch_service = OpenSearchService(config)
        self.batch_size = config['ingestion']['legislation'].get('batch_size', 100)
    
    def ingest(self):
        """Main ingestion process for legislation documents."""
        self.logger.info("Starting legislation ingestion process...")
        
        # Get filter parameters
        years = self.config['ingestion']['legislation'].get('years', [])
        jurisdiction_codes = self.config['ingestion']['legislation'].get('jurisdiction_codes', [])
        
        # Get records to ingest
        records = self.db_service.get_legislation_for_ingestion(
            years=years if years else None,
            jurisdiction_codes=jurisdiction_codes if jurisdiction_codes else None
        )
        
        if records.empty:
            self.logger.info("No legislation records found for ingestion")
            return
        
        self.logger.info(f"Found {len(records)} legislation records for ingestion")
        
        # Process in batches
        total_success = 0
        total_errors = 0
        
        for i in range(0, len(records), self.batch_size):
            batch = records.iloc[i:i+self.batch_size]
            documents = []
            
            for _, row in batch.iterrows():
                try:
                    # Build S3 file path
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
                        # Create document
                        doc = LegislationDocument(
                            source_id=row['source_id'],
                            section_id=row['section_id'],
                            book_type=row['type_of_document'],
                            book_name=row['book_name'],
                            section_name=row['section_name'],
                            content=content
                        )
                        documents.append(doc.to_dict())
                    else:
                        self.logger.warning(
                            f"No content found for source_id: {row['source_id']}, "
                            f"section_id: {row['section_id']}"
                        )
                        total_errors += 1
                        
                except Exception as e:
                    self.logger.error(
                        f"Error processing legislation record {row['source_id']}, "
                        f"section {row['section_id']}: {str(e)}"
                    )
                    total_errors += 1
            
            # Bulk index documents
            if documents:
                success, errors = self.opensearch_service.bulk_index_documents(documents)
                total_success += success
                total_errors += errors
            
            self.logger.info(f"Processed batch {i//self.batch_size + 1}: {len(documents)} documents")
        
        self.logger.info(f"Legislation ingestion completed. Success: {total_success}, Errors: {total_errors}")