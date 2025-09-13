from datetime import datetime
import uuid
import os
from sqlalchemy import text

NAVIGATION_PATH_DEPTH = int(os.getenv("NAVIGATION_PATH_DEPTH", 3)) # Duplicate checking

def create_audit_log_entry(engine, job_name):
    """Creates a new entry in the audit_log table and returns its ID."""
    audit_id = str(uuid.uuid4())
    print(f"\nCreating audit log entry for job: {job_name} (ID: {audit_id})")
    try:
        with engine.connect() as connection:
            with connection.begin():
                query = text("""
                    INSERT INTO audit_log (id, job_name, start_time, job_status)
                    VALUES (:id, :job_name, :start_time, 'running')
                """)
                params = {"id": audit_id, "job_name": job_name, "start_time": datetime.now()}
                connection.execute(query, params)
        return audit_id
    except Exception as e:
        print(f"  - FATAL ERROR: Could not create audit log entry: {e}")
        return None

def update_audit_log_entry(engine, audit_id, final_status, message):
    """Updates the audit_log entry with the final status and duration."""
    if not audit_id:
        print("  - WARNING: No audit_id provided, cannot update audit log.")
        return

    print(f"\nUpdating audit log entry {audit_id} with status: {final_status}")
    try:
        with engine.connect() as connection:
            with connection.begin():
                start_time_query = text("SELECT start_time FROM audit_log WHERE id = :id")
                start_time_result = connection.execute(start_time_query, {"id": audit_id}).fetchone()
                
                end_time = datetime.now()
                duration = (end_time - start_time_result[0]).total_seconds() if start_time_result else -1.0

                query = text("""
                    UPDATE audit_log 
                    SET end_time = :end_time, job_status = :status, job_duration = :duration, message = :message
                    WHERE id = :id
                """)
                params = {"id": audit_id, "end_time": end_time, "status": final_status, "duration": duration, "message": message}
                connection.execute(query, params)
        print("  - Audit log entry updated successfully.")
    except Exception as e:
        print(f"  - FATAL ERROR: Could not update audit log entry {audit_id}: {e}")