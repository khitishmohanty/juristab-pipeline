import logging
import pandas as pd
from datetime import datetime

def write_audit_log(engine, table_name, job_name, job_id, start_time, end_time, status, message):
    """
    Writes a final log entry to the audit_log table.

    Args:
        engine: The SQLAlchemy engine for the destination database.
        table_name (str): The name of the audit log table.
        job_name (str): The name of the job being logged.
        job_id (str): The unique ID of the job being logged.
        start_time (datetime): The start time of the job.
        end_time (datetime): The end time of the job.
        status (str): The final status of the job ('success' or 'fail').
        message (str): A summary message for the job run.
    """
    try:
        duration = (end_time - start_time).total_seconds()
        audit_record = {
            'job_name': job_name,
            'job_id': job_id,
            'start_time': start_time,
            'end_time': end_time,
            'job_status': status,
            'created_at': end_time,
            'job_duration': duration,
            'message': message
        }
        audit_df = pd.DataFrame([audit_record])
        with engine.connect() as conn:
            audit_df.to_sql(table_name, conn, if_exists='append', index=False)
            conn.commit()
        logging.info(f"Successfully wrote audit log for job '{job_name}' (ID: {job_id}).")
    except Exception as e:
        logging.error(f"Failed to write to audit_log table '{table_name}'. Error: {e}")
