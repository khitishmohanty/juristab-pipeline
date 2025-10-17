import os
import time
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from utils.config_manager import ConfigManager
from utils.audit_logger import AuditLogger
import sys

class VersionProcessor:
    """
    Handles the versioning of legislation books by grouping records with the same book_name
    and assigning incremental versions based on their dates.
    Also logs the status to legislation_enrichment_status table.
    """
    def __init__(self, config: dict):
        """
        Initializes the VersionProcessor.

        Args:
            config (dict): The application configuration dictionary.
        """
        self.config = config
        self.db = DatabaseConnector(db_config=config['database']['destination'])

    def process_versions(self):
        """
        Main method to run the versioning pipeline.
        It identifies books with multiple records, checks existing versions,
        and assigns new versions where needed.
        """
        registry_config = self.config.get('tables_registry')
        if not registry_config:
            print("FATAL: 'tables_registry' configuration not found in config.yaml. Aborting.")
            return

        registry_table = registry_config['table']
        book_name_col = registry_config['book_name_column']
        book_version_col = registry_config['book_version_column']
        date_col = registry_config['date_column']
        status_col = registry_config['status_column']
        registry_year_col = registry_config.get('year_column')
        
        processing_years = registry_config.get('processing_years', [])
        jurisdictions_to_process = registry_config.get('jurisdiction_codes', [])

        # Get status table configuration
        tables_config = self.config.get('tables', {})
        tables_to_write = tables_config.get('tables_to_write', [])
        if not tables_to_write:
            print("WARNING: 'tables.tables_to_write' configuration not found. Status logging will be skipped.")
            status_table = None
            step_columns_config = None
        else:
            status_table_info = tables_to_write[0]
            status_table = status_table_info['table']
            step_columns_config = status_table_info['step_columns']

        # Build dynamic WHERE clause based on filters
        where_conditions = [f"{status_col} = 'pass'"]
        params = {}
        
        if jurisdictions_to_process:
            placeholders = ','.join([f':juris_{i}' for i in range(len(jurisdictions_to_process))])
            where_conditions.append(f"jurisdiction_code IN ({placeholders})")
            for i, juris in enumerate(jurisdictions_to_process):
                params[f'juris_{i}'] = juris
        
        if processing_years:
            placeholders = ','.join([f':year_{i}' for i in range(len(processing_years))])
            where_conditions.append(f"{registry_year_col} IN ({placeholders})")
            for i, year in enumerate(processing_years):
                params[f'year_{i}'] = year
        
        where_clause = " AND ".join(where_conditions)

        try:
            # Find all book_names that have multiple records
            query = f"""
                SELECT {book_name_col}, COUNT(*) as record_count
                FROM {registry_table}
                WHERE {where_clause}
                GROUP BY {book_name_col}
                HAVING COUNT(*) > 1
            """
            
            print(f"DEBUG: Executing query to find books with multiple versions...")
            books_to_version_df = self.db.read_sql(query, params=params)
            print(f"INFO: Found {len(books_to_version_df)} books with multiple records requiring versioning.")

        except Exception as e:
            print(f"ERROR: Could not query the registry for books. Error: {e}")
            return

        if books_to_version_df.empty:
            print("INFO: No books found with multiple records. Nothing to version.")
            return

        # Process each book
        total_processed = 0
        total_failed = 0
        
        for index, row in books_to_version_df.iterrows():
            book_name = row[book_name_col]
            record_count = row['record_count']
            print(f"\n- Processing book: {book_name} (Total records: {record_count})")
            
            success = self._assign_versions_to_book(
                registry_table, book_name, book_name_col, 
                book_version_col, date_col, status_col,
                status_table, step_columns_config
            )
            
            if success:
                total_processed += 1
            else:
                total_failed += 1
        
        print("\n" + "=" * 60)
        print(f"Version assignment completed!")
        print(f"Total books processed successfully: {total_processed}")
        print(f"Total books failed: {total_failed}")
        print("=" * 60)

    def _assign_versions_to_book(self, table: str, book_name: str, 
                                  book_name_col: str, version_col: str, 
                                  date_col: str, status_col: str,
                                  status_table: str, step_columns_config: dict):
        """
        Assigns versions to all records of a specific book and logs status.
        
        Args:
            table (str): The registry table name
            book_name (str): The name of the book to version
            book_name_col (str): Column name for book name
            version_col (str): Column name for book version
            date_col (str): Column name for date (to order versions)
            status_col (str): Column name for status
            status_table (str): Table name for status logging
            step_columns_config (dict): Column configuration for status logging
            
        Returns:
            bool: True if successful, False otherwise
        """
        start_time_utc = datetime.now(timezone.utc)
        
        try:
            # Get all records for this book, ordered by date
            query = f"""
                SELECT source_id, {date_col}, {version_col}
                FROM {table}
                WHERE {book_name_col} = :book_name 
                AND {status_col} = 'pass'
                ORDER BY {date_col} ASC
            """
            
            records_df = self.db.read_sql(query, params={"book_name": book_name})
            
            if records_df.empty:
                print(f"  No records found for book: {book_name}")
                return False
            
            # Find the highest existing version
            existing_versions = records_df[version_col].dropna()
            if len(existing_versions) > 0:
                max_existing_version = int(existing_versions.max())
                print(f"  Highest existing version found: {max_existing_version}")
                versioned_count = len(existing_versions)
                print(f"  {versioned_count} records already versioned")
            else:
                max_existing_version = 0
                print(f"  No existing versions found. Starting from version 1")
            
            # Assign versions to records without versions
            next_version = max_existing_version + 1
            updates_made = 0
            records_failed = 0
            
            for idx, record in records_df.iterrows():
                current_version = record[version_col]
                source_id = record['source_id']
                
                # Skip if already has a version
                if current_version is not None and str(current_version).strip() != '' and current_version != 0:
                    continue
                
                # Process this individual record
                record_start_time = datetime.now(timezone.utc)
                
                try:
                    # Assign the next version
                    self.db.update_book_version(
                        table_name=table,
                        source_id=source_id,
                        version=next_version,
                        version_col=version_col
                    )
                    
                    record_end_time = datetime.now(timezone.utc)
                    record_duration = (record_end_time - record_start_time).total_seconds()
                    
                    # Log success status
                    if status_table and step_columns_config:
                        self.db.upsert_step_result(
                            table_name=status_table,
                            source_id=source_id,
                            step='book_versioning',
                            status='pass',
                            duration=record_duration,
                            start_time=record_start_time,
                            end_time=record_end_time,
                            step_columns=step_columns_config
                        )
                    
                    print(f"  Assigned version {next_version} to source_id: {source_id}")
                    next_version += 1
                    updates_made += 1
                    
                except Exception as e:
                    record_end_time = datetime.now(timezone.utc)
                    record_duration = (record_end_time - record_start_time).total_seconds()
                    
                    # Log failure status
                    if status_table and step_columns_config:
                        try:
                            self.db.upsert_step_result(
                                table_name=status_table,
                                source_id=source_id,
                                step='book_versioning',
                                status='failed',
                                duration=record_duration,
                                start_time=record_start_time,
                                end_time=record_end_time,
                                step_columns=step_columns_config
                            )
                        except Exception as log_error:
                            print(f"  ERROR: Could not log failure status for source_id {source_id}: {log_error}")
                    
                    print(f"  FAILED to assign version to source_id: {source_id}. Error: {e}")
                    records_failed += 1
            
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            
            if updates_made > 0:
                print(f"  Successfully assigned {updates_made} new versions for book '{book_name}'. Duration: {duration:.2f}s")
            else:
                print(f"  All records for book '{book_name}' already have versions assigned.")
            
            if records_failed > 0:
                print(f"  WARNING: {records_failed} records failed for book '{book_name}'")
            
            return True
                
        except Exception as e:
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            print(f"  FAILED to process book '{book_name}'. Error: {e}")
            
            # Log failure status for all records in this book that don't have versions yet
            if status_table and step_columns_config:
                try:
                    # Get all source_ids for this book that don't have versions
                    query = f"""
                        SELECT source_id
                        FROM {table}
                        WHERE {book_name_col} = :book_name 
                        AND {status_col} = 'pass'
                        AND ({version_col} IS NULL OR {version_col} = 0 OR {version_col} = '')
                    """
                    failed_records_df = self.db.read_sql(query, params={"book_name": book_name})
                    
                    # Log failure for each record
                    for idx, record in failed_records_df.iterrows():
                        source_id = record['source_id']
                        try:
                            self.db.upsert_step_result(
                                table_name=status_table,
                                source_id=source_id,
                                step='book_versioning',
                                status='failed',
                                duration=duration,
                                start_time=start_time_utc,
                                end_time=end_time_utc,
                                step_columns=step_columns_config
                            )
                        except Exception as log_error:
                            print(f"  ERROR: Could not log failure status for source_id {source_id}: {log_error}")
                except Exception as query_error:
                    print(f"  ERROR: Could not retrieve records to log failure: {query_error}")
            
            return False