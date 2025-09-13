import os
import uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

class AuditLogger:
    """Handles all logging to the audit_log table."""

    def __init__(self, db_config: dict, table_name: str):
        """
        Initializes the logger with database configuration.

        Args:
            db_config (dict): A dictionary containing connection details for the audit log database.
            table_name (str): The name of the audit log table.
        """
        self.table_name = table_name
        self.engine = self._create_db_engine(db_config)
        self.Session = sessionmaker(bind=self.engine)

    def _create_db_engine(self, db_config: dict):
        """Creates a SQLAlchemy engine from the configuration."""
        try:
            connection_url = URL.create(
                drivername=f"{db_config['dialect']}+{db_config['driver']}",
                username=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['name']
            )
            print(f"AuditLogger creating database engine for: {db_config['name']}")
            return create_engine(connection_url)
        except Exception as e:
            print(f"AuditLogger failed to create database engine: {e}")
            raise

    def log_start(self, job_name: str) -> str:
        """
        Logs the start of a job and returns the unique ID for the log entry.

        Args:
            job_name (str): The name of the job being executed.

        Returns:
            str: The UUID of the new audit log record.
        """
        session = self.Session()
        log_id = str(uuid.uuid4())
        start_time = datetime.now(timezone.utc)
        
        try:
            stmt = text(f"""
                INSERT INTO {self.table_name} (id, job_name, start_time, job_status, created_at)
                VALUES (:id, :job_name, :start_time, 'running', :created_at)
            """)
            session.execute(stmt, {
                "id": log_id,
                "job_name": job_name,
                "start_time": start_time,
                "created_at": start_time
            })
            session.commit()
            print(f"Job '{job_name}' started. Log ID: {log_id}")
            return log_id
        except Exception as e:
            print(f"Failed to log job start: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def log_end(self, log_id: str, status: str, message: str = ""):
        """
        Updates the audit log record to mark the end of a job.

        Args:
            log_id (str): The unique ID of the log entry to update.
            status (str): The final status of the job ('completed' or 'failed').
            message (str, optional): An optional final message or error details.
        """
        session = self.Session()
        end_time = datetime.now(timezone.utc)

        try:
            # First, get the start_time to calculate duration
            get_stmt = text(f"SELECT start_time FROM {self.table_name} WHERE id = :log_id")
            result = session.execute(get_stmt, {"log_id": log_id}).fetchone()
            
            job_duration = None
            if result and result.start_time:
                # FIX: Ensure the start_time from DB is timezone-aware (UTC) before subtraction
                start_time_aware = result.start_time.replace(tzinfo=timezone.utc)
                duration_delta = end_time - start_time_aware
                job_duration = duration_delta.total_seconds()

            # Now, update the record
            update_stmt = text(f"""
                UPDATE {self.table_name}
                SET end_time = :end_time,
                    job_status = :status,
                    job_duration = :duration,
                    message = :message
                WHERE id = :log_id
            """)
            session.execute(update_stmt, {
                "end_time": end_time,
                "status": status,
                "duration": job_duration,
                "message": message,
                "log_id": log_id
            })
            session.commit()
            print(f"Job with Log ID '{log_id}' finished with status: {status}")
        except Exception as e:
            print(f"Failed to log job end for log_id {log_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()