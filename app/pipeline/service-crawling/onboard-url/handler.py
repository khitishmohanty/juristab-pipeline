import datetime
import re
from urllib.parse import urlparse
from sqlalchemy import text
import uuid # Import the uuid library

# Ensure 'aws_utils.py' is in the same directory or included in your Lambda deployment package.
import aws_utils # This line assumes aws_utils.py can be imported.

# --- Helper function for filename generation ---
def generate_derived_filenames(input_url_str):
    """
    Generates robots_file_name and sitemap_file_name from the input URL's domain.
    Example: "https://www.example.co.uk/path" -> "example_co_uk.txt", "example_co_uk.xml"
    """
    parsed_url = urlparse(input_url_str)
    
    host_part = parsed_url.netloc
    host_part = host_part.split(':')[0] # Remove port if present

    # Remove "www." prefix if it exists at the beginning of the host_part, case-insensitively
    if host_part.lower().startswith("www."):
        host_part = host_part[4:]
    
    # Replace all dots with underscores
    sanitized_base_name = host_part.replace('.', '_')
    
    robots_file_name = f"{sanitized_base_name}.txt"
    sitemap_file_name = f"{sanitized_base_name}.xml"
    
    return robots_file_name, sitemap_file_name

# --- Lambda Handler ---
def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    Processes an input URL to add a record to parent_urls and logs the job in audit_log.
    Returns success status along with the generated audit log ID.
    """
    job_name = "add_parent_url_job" 
    start_time = datetime.datetime.now(datetime.timezone.utc) # Timezone-aware UTC
    end_time = None
    job_status = "failed" # Default to failed
    job_duration_seconds = None
    
    input_url = event.get('url')
    engine = None # Initialize database engine variable

    log_messages = []
    log_messages.append(f"Job '{job_name}' started at {start_time.isoformat()} for URL: {input_url}")

    # Default failure response, will be updated on success or more specific failure
    response = {"status": "failure", "message": "Processing failed."}

    try:
        # 1. Input Validation
        if not input_url:
            response["message"] = "Error: 'url' not provided in the event."
            log_messages.append(f"❌ {response['message']}")
            # 'finally' block will execute to log this message and calculate duration
            return response

        parsed_url = urlparse(input_url)
        if not parsed_url.scheme or not parsed_url.netloc:
            response["message"] = f"Error: Invalid URL '{input_url}'. Must have a scheme (e.g., http, https) and a domain."
            log_messages.append(f"❌ {response['message']}")
            return response
        
        db_base_url = input_url # Use the validated input URL

        # 2. Generate Filenames
        robots_fn, sitemap_fn = generate_derived_filenames(db_base_url)
        log_messages.append(f"⚙️ Generated filenames: robots='{robots_fn}', sitemap='{sitemap_fn}'")

        # 3. Database Connection
        engine = aws_utils.create_db_engine()
        if not engine:
            response["message"] = "Error: Failed to create database engine. Check DB credentials, connectivity, and aws_utils.py."
            log_messages.append(f"❌ {response['message']}")
            return response
        log_messages.append("✅ Database engine created successfully.")

        # Generate UUID for parent_urls.id
        parent_urls_record_id = str(uuid.uuid4())
        log_messages.append(f"⚙️ Generated ID for parent_urls record: {parent_urls_record_id}")

        # 4. Create Record in parent_urls Table
        parent_urls_data = {
            "id": parent_urls_record_id, # Include the generated ID for parent_urls
            "base_url": db_base_url,
            "crawl_status": "pending",
            "robots_file_name": robots_fn,
            "sitemap_file_name": sitemap_fn,
            "config_file_fetch_status": "pending"
            # Assuming 'created_at' and 'updated_at' are handled by the database (e.g., DEFAULT CURRENT_TIMESTAMP)
        }

        with engine.connect() as connection:
            with connection.begin(): # Start a transaction
                table_name_parent_urls = "parent_urls"
                # Basic validation for table name (fixed in this context but good practice)
                if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name_parent_urls):
                    raise ValueError(f"FATAL: Invalid hardcoded table name '{table_name_parent_urls}'.")

                # Prepare columns and placeholders for the SQL query
                columns = []
                placeholders = []
                for col_name in parent_urls_data.keys():
                    # Basic validation for column names (derived from fixed keys of parent_urls_data)
                    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                        raise ValueError(f"FATAL: Invalid hardcoded column name '{col_name}' for table '{table_name_parent_urls}'.")
                    columns.append(f"`{col_name}`") # Use backticks for MySQL column names
                    placeholders.append(f":{col_name}")
                
                columns_str = ", ".join(columns)
                placeholders_str = ", ".join(placeholders)

                # Construct and execute the INSERT query using SQLAlchemy's text() for parameter binding
                insert_query_str = f"INSERT INTO `{table_name_parent_urls}` ({columns_str}) VALUES ({placeholders_str})"
                query = text(insert_query_str)
                
                connection.execute(query, parent_urls_data)
                log_messages.append(f"✅ Record successfully inserted into '{table_name_parent_urls}' (ID: {parent_urls_record_id}) for URL: {db_base_url}")
            
            # If the transaction committed successfully
            job_status = "success"

    except Exception as e:
        job_status = "failed" 
        error_type = type(e).__name__
        error_message = f"{error_type} - {str(e)}"
        response["message"] = f"An error occurred during main processing: {error_message}"
        log_messages.append(f"❌ {response['message']}")
        # The 'finally' block will handle logging this error and calculating duration.

    finally:
        end_time = datetime.datetime.now(datetime.timezone.utc) # Timezone-aware UTC
        job_duration_seconds = (end_time - start_time).total_seconds()
        log_messages.append(f"⚙️ Job '{job_name}' finished at {end_time.isoformat()}. Duration: {job_duration_seconds:.2f} seconds. Status: {job_status}")

        # Generate UUID for audit log ID in Python code
        audit_record_id_for_log = str(uuid.uuid4())

        if engine: # Attempt to log to audit_log table only if DB engine was available
            try:
                with engine.connect() as connection:
                    with connection.begin(): # Transaction for audit log insertion
                        audit_log_data = {
                            "id": audit_record_id_for_log, # Include the Python-generated UUID
                            "job_name": job_name,
                            "start_time": start_time, # Timezone-aware UTC
                            "end_time": end_time,     # Timezone-aware UTC
                            "job_status": job_status,
                            "job_duration": job_duration_seconds
                            # Assuming 'created_at' for audit_log is handled by the database
                        }
                        
                        table_name_audit_log = "audit_log"
                        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name_audit_log):
                             raise ValueError(f"FATAL: Invalid hardcoded table name '{table_name_audit_log}'.")

                        audit_columns = []
                        audit_placeholders = []
                        for col_name in audit_log_data.keys():
                            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", col_name):
                                raise ValueError(f"FATAL: Invalid hardcoded column name '{col_name}' for table '{table_name_audit_log}'.")
                            audit_columns.append(f"`{col_name}`") # Use backticks
                            audit_placeholders.append(f":{col_name}")
                        
                        audit_columns_str = ", ".join(audit_columns)
                        audit_placeholders_str = ", ".join(audit_placeholders)
                        
                        audit_insert_query_str = f"INSERT INTO `{table_name_audit_log}` ({audit_columns_str}) VALUES ({audit_placeholders_str})"
                        audit_query = text(audit_insert_query_str)
                        
                        connection.execute(audit_query, audit_log_data)
                        log_messages.append(f"✅ Audit log record created successfully with ID: {audit_record_id_for_log} for job '{job_name}'.")
            except Exception as audit_e:
                # If audit log fails, the main job status might still be 'success' or 'failed' from the 'try' block.
                # We should ensure the overall returned status reflects any critical failure in audit logging.
                # Even if main job succeeded, if audit fails, we might consider the overall operation problematic.
                original_job_status_before_audit_fail = job_status
                job_status = "failed" # Mark job as failed if audit logging fails critically
                error_type = type(audit_e).__name__
                audit_error_message = f"{error_type} - {str(audit_e)}"
                # Update response message to reflect audit failure, potentially overwriting a success message.
                response["message"] = f"Critical error writing to audit_log: {audit_error_message}. Main job original status was: {original_job_status_before_audit_fail}"
                log_messages.append(f"❌ CRITICAL: Failed to write to audit_log for job '{job_name}'. Error: {audit_error_message}")
            finally:
                # Dispose of the engine to release database connections, especially important in Lambda.
                try:
                    engine.dispose()
                    log_messages.append("ℹ️ Database engine disposed.")
                except Exception as dispose_e:
                    log_messages.append(f"⚠️ Warning: Error disposing database engine: {str(dispose_e)}")
        else: # Engine was not initialized (e.g., input validation failed, or create_db_engine failed)
            log_messages.append(f"ℹ️ Audit Log to DB skipped: DB engine was not available. (Job status: {job_status})")
        
        # Print all accumulated log messages to CloudWatch
        if log_messages: 
            print("\n---\n".join(log_messages)) # Use a separator for readability in logs
        
        # Final response based on job_status
        if job_status == "success" and audit_record_id_for_log: # Ensure audit ID was generated if successful
            response["status"] = "success"
            response["audit_log_id"] = audit_record_id_for_log
            response["message"] = f"Job '{job_name}' completed successfully."
        else:
            response["status"] = "failed"
            # The message would have been set by the specific error or the default one.
            # Ensure a message is present if it's still the default "Processing failed."
            if not response.get("message") or "Processing failed" in response.get("message", ""): 
                response["message"] = "Job failed. Check logs for details."

        return response

# Example usage (for local testing, not part of Lambda deployment normally):
if __name__ == '__main__':
    # Before running locally, ensure environment variables for aws_utils.py are set
    # (DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_DIALECT, DB_DRIVER)
    # For example, by creating a .env file in the same directory as aws_utils.py
    # and ensuring python-dotenv is installed.

    # --- Mocking AWS Lambda event and context ---
    mock_event_success = {
        'url': 'https://jade.io/t/home'
    }
    mock_event_invalid_url = {
        'url': 'not_a_url'
    }
    mock_event_no_url = {}

    print("--- Testing with a valid URL ---")
    result = lambda_handler(mock_event_success, None)
    print(f"Lambda handler returned: {result}\n")

    print("--- Testing with an invalid URL ---")
    result = lambda_handler(mock_event_invalid_url, None)
    print(f"Lambda handler returned: {result}\n")

    # print("--- Testing with no URL ---")
    # result = lambda_handler(mock_event_no_url, None)
    # print(f"Lambda handler returned: {result}\n")

    # print("--- Testing with a URL that might have www and complex TLD ---")
    # mock_event_complex_tld = {
    #     'url': 'http://www.subdomain.example.co.uk/somepage?query=1'
    # }
    result = lambda_handler(mock_event_complex_tld, None)
    print(f"Lambda handler returned: {result}\n")