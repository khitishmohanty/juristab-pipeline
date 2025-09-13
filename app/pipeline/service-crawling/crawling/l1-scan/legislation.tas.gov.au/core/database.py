from sqlalchemy import text
import uuid
import os
import re
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

def save_scraped_data_to_db(engine, scraped_data, parent_url_id, navigation_path_parts, page_num, destination_table):
    """Saves a list of scraped book links to the specified destination table."""
    if not scraped_data: return 0
    
    if not re.match(r"^[a-zA-Z0-9_]+$", destination_table):
        print(f"  - FATAL ERROR: Invalid destination_table name: '{destination_table}'. Aborting save.")
        return 0

    human_readable_path = "/".join(navigation_path_parts) + f"/Page/{page_num}"
    try:
        with engine.connect() as connection:
            with connection.begin():
                path_prefix_parts = navigation_path_parts[:NAVIGATION_PATH_DEPTH]
                path_prefix = "/".join(path_prefix_parts) + "%"
                
                existing_urls_query_str = f"SELECT book_url FROM {destination_table} WHERE parent_url_id = :parent_url_id AND navigation_path LIKE :path_prefix"
                existing_urls_query = text(existing_urls_query_str)
                
                existing_urls_result = connection.execute(existing_urls_query, {"parent_url_id": parent_url_id, "path_prefix": path_prefix}).fetchall()
                existing_urls = {row[0] for row in existing_urls_result}
                records_to_insert = [item for item in scraped_data if item.get('link') not in existing_urls]
                
                if not records_to_insert:
                    print(f"  - All {len(scraped_data)} scraped records for this page already exist. Nothing to insert.")
                    return 0
                print(f"  - Found {len(records_to_insert)} new records to insert.")

                insert_query_str = f"""
                    INSERT INTO {destination_table} (id, parent_url_id, book_name, book_number, book_url, navigation_path, date_collected, is_active, book_effective_date, book_year)
                    VALUES (:id, :parent_url_id, :book_name, :book_number, :book_url, :navigation_path, :date_collected, :is_active, :book_effective_date, :book_year)
                """
                query = text(insert_query_str)

                for item in records_to_insert:
                    book_year_val, book_effective_date_val = None, None
                    try:
                        if item.get('year'): book_year_val = int(item.get('year'))
                    except (ValueError, TypeError): pass
                    try:
                        if item.get('effective_date'):
                            date_str = item.get('effective_date').strip()
                            book_effective_date_val = datetime.strptime(date_str, '%d/%m/%Y').date()
                    except (ValueError, TypeError) as e: 
                        print(f"  - WARNING: Could not parse date '{item.get('effective_date')}'. Error: {e}")
                        pass
                    
                    params = {
                        "id": str(uuid.uuid4()), "parent_url_id": parent_url_id,
                        "book_name": item.get('title'), "book_number": item.get('number'),
                        "book_url": item.get('link'), "navigation_path": human_readable_path,
                        "date_collected": datetime.now(), "is_active": 1,
                        "book_effective_date": book_effective_date_val,
                        "book_year": book_year_val
                    }
                    connection.execute(query, params)
                print(f"  - Successfully saved {len(records_to_insert)} new records to '{destination_table}'.")
                return len(records_to_insert)
    except Exception as e:
        print(f"  - FATAL ERROR: Failed during database save operation: {e}")
        return 0