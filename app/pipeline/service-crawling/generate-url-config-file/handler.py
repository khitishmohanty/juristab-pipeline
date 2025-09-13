import os
import sys
import requests 
from bs4 import BeautifulSoup 
import json 
import time 
from urllib.parse import urljoin
from sqlalchemy import text
import uuid # For audit log ID
from datetime import datetime, timezone # For timestamps

# For Lambda, ensure 'utils' is in the same directory or a subdirectory
# and can be imported directly.
try:
    import aws_utils
except ImportError as e:
    print(f"❌ Error importing utility modules: {e}.")
    S3_CLIENT = None
    DB_ENGINE = None
else:
    S3_CLIENT = aws_utils.get_s3_client()
    DB_ENGINE = aws_utils.create_db_engine()


S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "legal_store")
S3_DEST_FOLDER = os.getenv("S3_DEST_FOLDER", "crawl_configs/") 

if S3_DEST_FOLDER and not S3_DEST_FOLDER.endswith('/'):
    S3_DEST_FOLDER += '/'

WEB_REQUEST_TIMEOUT = 30
USER_AGENT = "DocuDiveJuristabConfigGenerator/1.0 (+http://your-project-url.com)"

# --- Helper functions for web fetching (largely unchanged) ---
def make_web_request(url, is_xml=False):
    print(f"⚙️ Fetching web content from: {url}")
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(url, headers=headers, timeout=WEB_REQUEST_TIMEOUT)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"❌ Error during web request to {url}: {e}")
        return None

def fetch_robots_txt_content(base_url):
    robots_url = urljoin(base_url, "/robots.txt")
    response = make_web_request(robots_url)
    if response:
        return response.text
    return None

def fetch_sitemap_urls_recursive_from_web(sitemap_url, visited_sitemaps=None):
    if visited_sitemaps is None:
        visited_sitemaps = set()
    if sitemap_url in visited_sitemaps:
        return []
    visited_sitemaps.add(sitemap_url)
    all_loc_urls = []
    response = make_web_request(sitemap_url, is_xml=True)

    if response:
        try:
            soup = BeautifulSoup(response.content, 'xml')
            sitemap_tags = soup.find_all('sitemap')
            if sitemap_tags:
                print(f"⚙️ Found sitemap index: {sitemap_url}. Processing sub-sitemaps...")
                for sitemap_tag in sitemap_tags:
                    sub_sitemap_loc = sitemap_tag.find('loc')
                    if sub_sitemap_loc and sub_sitemap_loc.text:
                        sub_sitemap_url = sub_sitemap_loc.text.strip()
                        all_loc_urls.extend(fetch_sitemap_urls_recursive_from_web(sub_sitemap_url, visited_sitemaps))
            else:
                url_tags = soup.find_all('url')
                for url_tag in url_tags:
                    loc = url_tag.find('loc')
                    if loc and loc.text:
                        all_loc_urls.append(loc.text.strip())
                print(f"✅ Processed sitemap: {sitemap_url}, found {len(url_tags)} <url> tags.")
        except Exception as e: 
            print(f"❌ Error parsing sitemap XML from {sitemap_url}: {e}")
    else:
        print(f"❌ Failed to fetch or parse sitemap: {sitemap_url}")
    return all_loc_urls

def generate_sitemap_json_content(base_url):
    initial_sitemap_url = urljoin(base_url, "/sitemap.xml")
    sitemap_urls = fetch_sitemap_urls_recursive_from_web(initial_sitemap_url)
    if sitemap_urls:
        sitemap_data = {
            "source_sitemap": initial_sitemap_url,
            "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "base_url": base_url,
            "urls_count": len(sitemap_urls),
            "urls": sorted(list(set(sitemap_urls)))
        }
        try:
            return json.dumps(sitemap_data, indent=4)
        except Exception as e:
            print(f"❌ Error creating JSON for sitemap data from {base_url}: {e}")
    return None

# --- Audit Log Helper Functions ---
def _insert_initial_audit_record(connection, audit_id, job_name, start_time):
    query = text("""
        INSERT INTO audit_log (id, job_name, start_time, job_status, created_at)
        VALUES (:id, :job_name, :start_time, 'running', :created_at)
    """)
    connection.execute(query, {
        "id": audit_id, "job_name": job_name, 
        "start_time": start_time, "created_at": start_time 
    })

def _update_final_audit_record(connection, audit_id, status, start_time):
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    query = text("""
        UPDATE audit_log SET end_time = :end_time, job_status = :status, job_duration = :duration
        WHERE id = :id
    """)
    connection.execute(query, {
        "id": audit_id, "end_time": end_time, "status": status, "duration": duration
    })

def _log_audit_failure_without_main_transaction(audit_id, job_name, start_time):
    """Tries to log a failure to audit_log using a new DB connection for errors outside main transaction."""
    if not DB_ENGINE: return

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    try:
        with DB_ENGINE.connect() as conn:
            update_query = text("UPDATE audit_log SET end_time = :et, job_status = 'failed', job_duration = :jd WHERE id = :id")
            res = conn.execute(update_query, {"et": end_time, "jd": duration, "id": audit_id})
            
            if res.rowcount == 0: # Initial record was not inserted or ID is wrong.
                insert_query = text("""
                    INSERT INTO audit_log (id, job_name, start_time, end_time, job_status, job_duration, created_at) 
                    VALUES (:id, :job_name, :start_time, :end_time, 'failed', :job_duration, :created_at)
                """)
                conn.execute(insert_query, {
                    "id": audit_id, "job_name": job_name, "start_time": start_time, "end_time": end_time,
                    "job_duration": duration, "created_at": start_time
                })
            conn.commit()
            print(f"ℹ️ Successfully logged audit failure for ID {audit_id} in separate transaction.")
    except Exception as db_err:
        print(f"❌ CRITICAL: Failed to log audit failure for ID {audit_id} to DB in separate transaction: {db_err}")


# --- Lambda Handler ---
def lambda_handler(event, context):
    site_id = event.get('id')
    
    audit_log_id = str(uuid.uuid4())
    job_name = "site_config_processor" 
    process_start_time = datetime.now(timezone.utc)
    # Default final audit status. Will be 'success' if all defined steps complete as expected.
    # For this lambda, "success" means it handled the site_id as per logic (processed, or correctly identified as not pending/not found).
    # An unhandled exception during processing will keep this as 'failed'.
    final_audit_status = 'failed' 

    if S3_BUCKET_NAME == "legal_store" and os.getenv("S3_BUCKET_NAME") is None:
        print("Warning: S3_BUCKET_NAME is using the default value 'legal-store'.")
    if S3_DEST_FOLDER == "crawl_configs/" and os.getenv("S3_DEST_FOLDER") is None:
        print("Warning: S3_DEST_FOLDER is using the default value 'crawl_configs/'.")

    if not site_id:
        print("❌ Error: 'id' not found in event object.")
        # Not attempting audit log here as site_id is fundamental.
        return {"statusCode": 400, "body": json.dumps({"error": "Missing 'id' in event", "site_id": None, "audit_log_id": audit_log_id})}

    print(f"⚙️ Lambda received request for site ID: {site_id}. Audit Log ID: {audit_log_id}")

    if not S3_CLIENT:
        print("❌ Critical Error: S3 client is not initialized.")
        if DB_ENGINE: _log_audit_failure_without_main_transaction(audit_log_id, job_name, process_start_time)
        return {"statusCode": 500, "body": json.dumps({"error": "S3 client initialization failed", "site_id": site_id, "audit_log_id": audit_log_id})}

    if not DB_ENGINE:
        print("❌ Critical Error: Database engine is not initialized. No audit logging possible.")
        return {"statusCode": 500, "body": json.dumps({"error": "Database engine initialization failed", "site_id": site_id, "audit_log_id": audit_log_id})}

    final_operation_summary = {}
    db_update_payload_for_parent_urls = {}
    
    try:
        with DB_ENGINE.connect() as connection:
            _insert_initial_audit_record(connection, audit_log_id, job_name, process_start_time)

            query_str = text("SELECT id, base_url, robots_file_name, sitemap_file_name, config_file_fetch_status FROM parent_urls WHERE id = :site_id_val")
            row = connection.execute(query_str, {"site_id_val": site_id}).fetchone()

            if not row:
                print(f"❌ Site ID '{site_id}' not found.")
                final_audit_status = 'success' # Lambda handled the "not found" case successfully.
                _update_final_audit_record(connection, audit_log_id, final_audit_status, process_start_time)
                connection.commit()
                return {"statusCode": 404, "body": json.dumps({"error": "Site ID not found", "site_id": site_id, "audit_log_id": audit_log_id})}

            if row.config_file_fetch_status != 'pending':
                print(f"ℹ️ Site ID '{site_id}' status '{row.config_file_fetch_status}', not 'pending'. No file fetching action taken.")
                final_audit_status = 'success' # Lambda handled the "not pending" case successfully.
                _update_final_audit_record(connection, audit_log_id, final_audit_status, process_start_time)
                connection.commit()
                return {"statusCode": 200, "body": json.dumps({"message": "Site not pending processing.", "site_id": site_id, "current_status": row.config_file_fetch_status, "audit_log_id": audit_log_id})}

            # --- Site is found and 'pending', proceed with actual processing ---
            base_url_from_db = row.base_url
            db_robot_filename = row.robots_file_name
            db_sitemap_filename = row.sitemap_file_name
            overall_site_processing_ok = True # Tracks success of robots/sitemap operations for this site.

            if not base_url_from_db:
                print(f"❌ Base URL is missing for site ID '{site_id}'. Cannot proceed with file fetching.")
                db_update_payload_for_parent_urls["config_file_fetch_status"] = "failed"
                db_update_payload_for_parent_urls["processing_error_details"] = "Base URL missing in DB"
                overall_site_processing_ok = False
            else:
                print(f"⚙️ Processing site from DB: ID='{site_id}', Base URL='{base_url_from_db}'")
                
                # Ensure S3_DEST_FOLDER (main crawl_configs folder) exists - can be done once, but cheap
                if S3_DEST_FOLDER and S3_DEST_FOLDER != '/':
                    if not aws_utils.ensure_s3_folder_exists(S3_CLIENT, S3_BUCKET_NAME, S3_DEST_FOLDER):
                        print(f"❌ Critical Error: Could not ensure S3 main destination folder '{S3_DEST_FOLDER}'. This may affect multiple sites.")
                        # This is a more global issue, but we'll mark this site's processing as failed due to it.
                        db_update_payload_for_parent_urls["config_file_fetch_status"] = "failed"
                        db_update_payload_for_parent_urls["processing_error_details"] = f"Failed to ensure S3 base folder {S3_DEST_FOLDER}"
                        overall_site_processing_ok = False

                if overall_site_processing_ok: # Proceed only if base URL and S3 base folder are OK
                    s3_site_folder_key = f"{S3_DEST_FOLDER}{str(site_id)}/"
                    if not aws_utils.ensure_s3_folder_exists(S3_CLIENT, S3_BUCKET_NAME, s3_site_folder_key):
                        print(f"❌ Failed to ensure S3 site folder: {s3_site_folder_key}. Marking operations as failed.")
                        if db_robot_filename: db_update_payload_for_parent_urls["robots_file_status"] = "failed"
                        if db_sitemap_filename: db_update_payload_for_parent_urls["sitemap_file_status"] = "failed"
                        # config_file_fetch_status will be set to failed based on overall_site_processing_ok
                        overall_site_processing_ok = False
                        if "processing_error_details" not in db_update_payload_for_parent_urls:
                             db_update_payload_for_parent_urls["processing_error_details"] = f"Failed to create S3 site folder {s3_site_folder_key}"
                    else:
                        # Process robots.txt
                        robot_op_attempted = bool(db_robot_filename)
                        if robot_op_attempted:
                            print(f"⚙️ Fetching robots.txt for {base_url_from_db}...")
                            robots_content = fetch_robots_txt_content(base_url_from_db)
                            if robots_content is not None:
                                s3_robot_key = s3_site_folder_key + db_robot_filename
                                if aws_utils.upload_data_to_s3(S3_CLIENT, robots_content, S3_BUCKET_NAME, s3_robot_key, 'text/plain'):
                                    db_update_payload_for_parent_urls["robots_file_status"] = "success"
                                    final_operation_summary["robots_txt"] = "Success"
                                else:
                                    db_update_payload_for_parent_urls["robots_file_status"] = "failed"
                                    overall_site_processing_ok = False
                                    final_operation_summary["robots_txt"] = "Failed (upload)"
                            else:
                                print(f"❌ Failed to fetch robots.txt for {base_url_from_db}")
                                db_update_payload_for_parent_urls["robots_file_status"] = "failed"
                                overall_site_processing_ok = False
                                final_operation_summary["robots_txt"] = "Failed (fetch)"
                        else:
                            final_operation_summary["robots_txt"] = "Skipped (no filename)"

                        # Process sitemap
                        sitemap_op_attempted = bool(db_sitemap_filename)
                        if sitemap_op_attempted:
                            print(f"⚙️ Generating sitemap JSON for {base_url_from_db}...")
                            sitemap_json_content = generate_sitemap_json_content(base_url_from_db)
                            if sitemap_json_content is not None:
                                s3_sitemap_key = s3_site_folder_key + db_sitemap_filename
                                if aws_utils.upload_data_to_s3(S3_CLIENT, sitemap_json_content, S3_BUCKET_NAME, s3_sitemap_key, 'application/json'):
                                    db_update_payload_for_parent_urls["sitemap_file_status"] = "success"
                                    final_operation_summary["sitemap_json"] = "Success"
                                else:
                                    db_update_payload_for_parent_urls["sitemap_file_status"] = "failed"
                                    overall_site_processing_ok = False
                                    final_operation_summary["sitemap_json"] = "Failed (upload)"
                            else:
                                print(f"❌ Failed to generate sitemap JSON for {base_url_from_db}")
                                db_update_payload_for_parent_urls["sitemap_file_status"] = "failed"
                                overall_site_processing_ok = False
                                final_operation_summary["sitemap_json"] = "Failed (generation)"
                        else:
                            final_operation_summary["sitemap_json"] = "Skipped (no filename)"
            
            # Finalize config_file_fetch_status for parent_urls based on operations
            # This ensures 'config_file_fetch_status' is always set if we attempted processing.
            if overall_site_processing_ok:
                db_update_payload_for_parent_urls["config_file_fetch_status"] = "success"
            else:
                db_update_payload_for_parent_urls["config_file_fetch_status"] = "failed"
                if "processing_error_details" not in db_update_payload_for_parent_urls and base_url_from_db: # Add generic error if no specific one was set
                    db_update_payload_for_parent_urls["processing_error_details"] = "One or more file operations failed."
            
            if db_update_payload_for_parent_urls: # Update parent_urls if there's anything to change
                aws_utils.update_db_record(connection, "parent_urls", "id", site_id, db_update_payload_for_parent_urls)

            final_audit_status = 'success' if overall_site_processing_ok else 'failed'
            
            _update_final_audit_record(connection, audit_log_id, final_audit_status, process_start_time)
            connection.commit()

            # Construct appropriate response based on whether the operations themselves succeeded or failed
            if overall_site_processing_ok:
                return {"statusCode": 200, "body": json.dumps({"message": "Site processed successfully.", "site_id": site_id, "status_summary": final_operation_summary, "final_db_status": db_update_payload_for_parent_urls, "audit_log_id": audit_log_id})}
            else:
                # This path is for when overall_site_processing_ok is false, meaning a defined operation failed (e.g., S3 upload).
                return {"statusCode": 500, "body": json.dumps({"error": "Site processing failed due to operational issues.", "site_id": site_id, "status_summary": final_operation_summary, "final_db_status": db_update_payload_for_parent_urls, "audit_log_id": audit_log_id})}

    except Exception as e:
        print(f"❌ An unexpected critical error occurred while processing site ID {site_id}: {e}")
        # final_audit_status remains 'failed' (its default value)
        _log_audit_failure_without_main_transaction(audit_log_id, job_name, process_start_time)
        
        return {"statusCode": 500, "body": json.dumps({"error": f"An unexpected error occurred: {str(e)}", "site_id": site_id, "audit_log_id": audit_log_id})}

if __name__ == "__main__":
    # Example local test event
    test_event = {"id": "dde888c6-7b5a-4731-b627-502f9404f910"} 
    # Ensure environment variables are set for DB and S3 access for local testing.
    print("🚀 LOCAL TEST RUN STARTING 🚀")
    response = lambda_handler(test_event, None)
    print("\n🚀 LOCAL TEST RUN FINISHED 🚀")
    print(f"Lambda response:\n{json.dumps(response, indent=2)}")