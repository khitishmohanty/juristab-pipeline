import pytest
from unittest.mock import patch, MagicMock, call
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from moto import mock_aws
import boto3
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from handler import (
    run_crawler, load_config, create_audit_log_entry,
    update_audit_log_entry, get_parent_url_details,
    save_content_to_s3, save_record_and_get_id, process_step,
    scrape_page_details_and_save
)

# Mock the database engine for all tests
@pytest.fixture
def mock_db_engine():
    """
    Mocks the database engine and its connect context manager.
    This is required to correctly test functions that use `with engine.connect()`.
    """
    with patch('handler.create_db_engine') as mock_create_engine:
        mock_engine = MagicMock(spec=Engine)
        # **THE FIX:** The engine's connect() method must return a context manager mock.
        # This mock will then be used in the `with` statement.
        mock_connection = MagicMock()
        # Make the mock connection's execute return a mock result with fetchone
        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        yield mock_engine

# Mock the selenium webdriver
@pytest.fixture
def mock_driver():
    with patch('handler.webdriver.Chrome') as mock_chrome:
        mock_driver_instance = MagicMock()
        mock_chrome.return_value = mock_driver_instance
        yield mock_driver_instance

# Mock environment variables
@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("PARENT_URL_ID", "test_parent_id")
    monkeypatch.setenv("SITEMAP_FILE_NAME", "test_sitemap.json")
    # Add other DB variables if your functions need them directly
    monkeypatch.setenv("DB_USER", "test") 
    monkeypatch.setenv("DB_PASSWORD", "test")
    monkeypatch.setenv("DB_HOST", "test")
    monkeypatch.setenv("DB_NAME", "test")

def test_load_config_file_not_found():
    """
    Tests that load_config returns None if the config file doesn't exist.
    """
    result = load_config('non_existent_file.json')
    assert result is None

# This is a more advanced test that mocks multiple components
@patch('handler.get_parent_url_details', return_value="http://fake-url.com")
@patch('handler.create_audit_log_entry', return_value="audit_id_123")
@patch('handler.update_audit_log_entry')
@patch('handler.process_step', return_value=True)
@patch('handler.load_config')
def test_run_crawler_happy_path(
    mock_load_config,
    mock_process_step,
    mock_update_audit,
    mock_create_audit,
    mock_get_parent_url,
    mock_db_engine,
    mock_driver
):
    """
    Integration test for the main run_crawler function's happy path.
    This test verifies the overall flow and that key functions are called.
    """
    # Setup mock config
    mock_config_data = {
        "crawler_config": {
            "journeys": [
                {
                    "journey_id": "test_journey",
                    "description": "A test journey",
                    "steps": [{"action": "click", "description": "step 1"}]
                }
            ]
        }
    }
    mock_load_config.return_value = mock_config_data

    # Execute the crawler
    run_crawler("test_parent_id", "test_sitemap.json")

    # Assertions to check the flow
    mock_load_config.assert_called_with('config/test_sitemap.json')
    # ** THE FIX: This assertion is removed because we are mocking the functions
    # that would actually call .connect(). **
    # mock_db_engine.connect.assert_called() 
    mock_get_parent_url.assert_called_with(mock_db_engine, "test_parent_id")
    mock_create_audit.assert_called_with(mock_db_engine, "crawling-jade-test_parent_id")
    
    # Check that the driver was initialized and navigated to the base URL
    mock_driver.get.assert_called_with("http://fake-url.com")
    
    # Check that process_step was called for the step in our mock journey
    mock_process_step.assert_called()

    # Check that the audit log was updated with a success message
    mock_update_audit.assert_called_with(
        mock_db_engine,
        "audit_id_123",
        'success',
        "Successfully processed 0 new records."
    )


def test_create_audit_log_entry(mock_db_engine):
    """
    Tests the creation of an audit log entry.
    """
    audit_id = create_audit_log_entry(mock_db_engine, "test_job")
    assert audit_id is not None
    # Check that the execute method was called on the connection
    mock_connection = mock_db_engine.connect.return_value.__enter__.return_value
    mock_connection.execute.assert_called_once()
    

def test_create_audit_log_entry_db_exception(mock_db_engine):
    mock_db_engine.connect.side_effect = SQLAlchemyError("Connection failed")
    audit_id = create_audit_log_entry(mock_db_engine, "test_job")
    assert audit_id is None

def test_update_audit_log_entry(mock_db_engine):
    """
    Tests the update of an audit log entry.
    """
    # Make the mock result return a start time when fetchone is called
    mock_connection = mock_db_engine.connect.return_value.__enter__.return_value
    mock_connection.execute.return_value.fetchone.return_value = [MagicMock()] # Mock datetime object
    
    update_audit_log_entry(mock_db_engine, "some_id", "success", "message")
    assert mock_connection.execute.call_count == 2 # Called for SELECT then UPDATE

def test_update_audit_log_entry_no_audit_id(mock_db_engine):
    update_audit_log_entry(mock_db_engine, None, "success", "message")
    mock_db_engine.connect.assert_not_called()

def test_update_audit_log_entry_db_exception(mock_db_engine):
    mock_db_engine.connect.side_effect = SQLAlchemyError("Connection failed")
    # The function should handle the exception gracefully and not crash
    update_audit_log_entry(mock_db_engine, "some_id", "failed", "error message")

def test_get_parent_url_details(mock_db_engine):
    """
    Tests fetching parent URL details from the database.
    """
    mock_connection = mock_db_engine.connect.return_value.__enter__.return_value
    mock_connection.execute.return_value.fetchone.return_value = ["http://test.com"]
    
    url = get_parent_url_details(mock_db_engine, "parent_id")
    assert url == "http://test.com"
    mock_connection.execute.assert_called_once()

def test_get_parent_url_details_not_found(mock_db_engine):
    mock_connection = mock_db_engine.connect.return_value.__enter__.return_value
    mock_connection.execute.return_value.fetchone.return_value = None
    url = get_parent_url_details(mock_db_engine, "not_found_id")
    assert url is None

def test_get_parent_url_details_db_exception(mock_db_engine):
    mock_db_engine.connect.side_effect = SQLAlchemyError("Connection failed")
    url = get_parent_url_details(mock_db_engine, "any_id")
    assert url is None

@mock_aws
def test_save_content_to_s3():
    """
    Tests saving content to S3.
    """
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "test-bucket"
    s3.create_bucket(Bucket=bucket)

    with patch('handler.s3_client', s3):
        save_content_to_s3("some content", bucket, "test/key.html")
        obj = s3.get_object(Bucket=bucket, Key="test/key.html")
        assert obj['Body'].read().decode() == "some content"

def test_save_content_to_s3_exception():
    with patch('handler.s3_client.put_object', side_effect=Exception("S3 is down")):
        with pytest.raises(Exception):
            save_content_to_s3("content", "bucket", "key")

def test_save_record_and_get_id_new_record(mock_db_engine):
    """
    Tests saving a new record to the database.
    """
    mock_connection = mock_db_engine.connect.return_value.__enter__.return_value
    # Make it return None to simulate the record not existing
    mock_connection.execute.return_value.fetchone.return_value = None
    
    data = {'book_name': 'Test Book', 'book_context': 'Test Context'}
    record_id = save_record_and_get_id(mock_db_engine, data, "p_id", "path", "table")
    
    assert record_id is not None
    # SELECT to check, INSERT to save
    assert mock_connection.execute.call_count == 2

def test_save_record_and_get_id_existing_record(mock_db_engine):
    """
    Tests that an existing record is not re-inserted.
    """
    mock_connection = mock_db_engine.connect.return_value.__enter__.return_value
    # Make it return a value to simulate the record existing
    mock_connection.execute.return_value.fetchone.return_value = ["existing_id"]
    
    data = {'book_name': 'Test Book', 'book_context': 'Test Context'}
    record_id = save_record_and_get_id(mock_db_engine, data, "p_id", "path", "table")
    
    assert record_id is None
    # Only the SELECT query should be run
    mock_connection.execute.assert_called_once()

def test_save_record_and_get_id_no_book_name(mock_db_engine):
    record_id = save_record_and_get_id(mock_db_engine, {}, "p_id", "path", "table")
    assert record_id is None
    mock_db_engine.connect.assert_not_called()

def test_save_record_and_get_id_db_exception(mock_db_engine):
    mock_db_engine.connect.side_effect = SQLAlchemyError("Connection failed")
    data = {'book_name': 'Test Book', 'book_context': 'Test Context'}
    with pytest.raises(SQLAlchemyError):
        save_record_and_get_id(mock_db_engine, data, "p_id", "path", "table")


@patch('handler.WebDriverWait')
def test_process_step_click(mock_wait, mock_driver):
    mock_element = MagicMock()
    mock_wait.return_value.until.return_value = mock_element
    step = {'action': 'click', 'target': {'value': 'xpath'}, 'description': 'test click'}
    
    result = process_step(mock_driver, step, None, None, None, None, None)
    
    assert result is True
    mock_driver.execute_script.assert_called_with("arguments[0].click();", mock_element)

@patch('time.sleep')
def test_process_step_pause(mock_sleep):
    step = {'action': 'pause', 'duration': 5}
    result = process_step(None, step, None, None, None, None, None)
    assert result is True
    mock_sleep.assert_called_once_with(5)

@patch('handler.process_and_paginate')
@patch('handler.process_navigation_loop')
def test_process_step_delegation(mock_nav_loop, mock_paginate):
    # Test delegation to process_and_paginate
    step_paginate = {'action': 'process_and_paginate'}
    process_step(None, step_paginate, "db", "id", "path", "state", "jstate")
    # **THE FIX:** The function is called with `job_state` ('state'), not `journey_state` ('jstate')
    mock_paginate.assert_called_once_with(None, step_paginate, "db", "id", "path", "state")

    # Test delegation to process_navigation_loop
    step_nav = {'action': 'navigation_loop'}
    process_step(None, step_nav, "db", "id", "path", "state", "jstate")
    mock_nav_loop.assert_called_once_with(None, step_nav, "db", "id", "path", "state", "jstate")

@patch('handler.save_record_and_get_id', return_value="new-record-id")
@patch('handler.save_content_to_s3')
@patch('handler.WebDriverWait')
def test_scrape_page_details_and_save_success(mock_wait, mock_save_s3, mock_save_db, mock_driver):
    # --- Setup mock web elements ---
    mock_row = MagicMock()
    mock_col_element = MagicMock()
    mock_col_element.text = "Book Name"
    mock_tab_button = MagicMock()
    mock_tab_content = MagicMock()
    mock_tab_content.get_attribute.return_value = "<html>Content</html>"
    
    mock_row.find_element.side_effect = [
        mock_col_element, # for book_name
        mock_col_element, # for book_url (will get text)
        mock_col_element, # for book_context
        mock_tab_button,  # for first tab click
        mock_tab_content, # for first tab content
    ]
    
    mock_wait.return_value.until.return_value = [mock_row] # Return a list with one row
    
    # --- Setup config ---
    config = {
        'row_xpath': '//div',
        'columns': [
            {'name': 'book_name', 'xpath': './/h2', 'type': 'text'},
            {'name': 'book_url', 'xpath': './/a', 'type': 'text'},
            {'name': 'book_context', 'xpath': './/span', 'type': 'text'}
        ],
        'content_tabs': {'tabs': [{'name': 'Excerpt', 'click_xpath': './/a', 'content_xpath': './/div'}]},
        'destination_table': 'test_table',
        's3_bucket': 'test-bucket',
        'jurisdiction_folder_name': 'test-jurisdiction'
    }
    job_state = {'records_saved': 0}

    # --- Execute and Assert ---
    result = scrape_page_details_and_save(mock_driver, config, "db_engine", "parent_id", ["nav"], job_state)

    assert result is True
    assert job_state['records_saved'] == 1
    mock_save_db.assert_called_once()
    mock_save_s3.assert_called_once()
    mock_driver.execute_script.assert_called_with("arguments[0].click();", mock_tab_button)

@patch('handler.WebDriverWait')
def test_scrape_page_details_and_save_no_rows(mock_wait, mock_driver):
    # Make the wait time out, simulating no rows found
    mock_wait.return_value.until.side_effect = TimeoutException
    result = scrape_page_details_and_save(mock_driver, {'row_xpath': ''}, None, None, None, None)
    assert result is True

