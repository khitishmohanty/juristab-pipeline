from sqlalchemy import text
import uuid
import os
from datetime import datetime


NAVIGATION_PATH_DEPTH = int(os.getenv("NAVIGATION_PATH_DEPTH", 3)) # Duplicate checking

def get_parent_url_details(engine, parent_url_id):
    """Connects to the database and fetches the base_url for the given ID."""
    print(f"\nFetching base_url for parent_url_id: {parent_url_id}...")
    try:
        with engine.connect() as connection:
            query = text("SELECT base_url FROM parent_urls WHERE id = :id")
            result = connection.execute(query, {"id": parent_url_id}).fetchone()
            if result: return result[0]
            print(f"  - FATAL ERROR: No record found for id='{parent_url_id}'")
            return None
    except Exception as e:
        print(f"  - FATAL ERROR: Could not query database: {e}")
        return None

def save_book_links_to_db(engine, scraped_data, parent_url_id, navigation_path_parts, page_num, job_state, destination_tablename):
    """
    Saves a list of scraped book links to the database, checking for duplicates.
    Returns the number of new records inserted.
    """
    if not scraped_data:
        print("  - No data to save for this page.")
        return 0

    human_readable_path = "/".join(navigation_path_parts) + f"/Page/{page_num}"
    
    try:
        with engine.connect() as connection:
            with connection.begin():
                path_prefix_parts = navigation_path_parts[:NAVIGATION_PATH_DEPTH]
                path_prefix = "/".join(path_prefix_parts) + "%"
                print(f"  - Checking for existing records in table '{destination_tablename}' with path prefix: '{path_prefix}'")

                existing_urls_query = text(f"SELECT book_url FROM {destination_tablename} WHERE parent_url_id = :parent_url_id AND navigation_path LIKE :path_prefix")
                existing_urls_result = connection.execute(existing_urls_query, {"parent_url_id": parent_url_id, "path_prefix": path_prefix}).fetchall()
                existing_urls = {row[0] for row in existing_urls_result}
                print(f"  - Found {len(existing_urls)} existing records for this path context.")

                records_to_insert = [item for item in scraped_data if item.get('link') not in existing_urls]

                if not records_to_insert:
                    print("  - All scraped records for this page already exist. Nothing to insert.")
                    return 0

                print(f"  - Found {len(records_to_insert)} new records to insert into '{destination_tablename}'.")
                
                for item in records_to_insert:
                    query = text(f"""
                        INSERT INTO {destination_tablename} (id, parent_url_id, book_name, book_number, book_url, navigation_path, date_collected, is_active)
                        VALUES (:id, :parent_url_id, :book_name, :book_number, :book_url, :navigation_path, :date_collected, :is_active)
                    """)
                    params = {
                        "id": str(uuid.uuid4()), "parent_url_id": parent_url_id,
                        "book_name": item.get('title'), "book_number": item.get('number'),
                        "book_url": item.get('link'), "navigation_path": human_readable_path,
                        "date_collected": datetime.now(), "is_active": 1
                    }
                    connection.execute(query, params)
                
                newly_inserted_count = len(records_to_insert)
                job_state['records_saved'] += newly_inserted_count
                print(f"  - Successfully saved {newly_inserted_count} new records with path: {human_readable_path}")
                return newly_inserted_count

    except Exception as e:
        print(f"  - FATAL ERROR: Failed during database save operation: {e}")
        return 0 # Return 0 on error