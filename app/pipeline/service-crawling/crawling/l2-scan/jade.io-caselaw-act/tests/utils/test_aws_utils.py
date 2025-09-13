import pytest
import boto3
import importlib
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from moto import mock_aws
from unittest.mock import patch, MagicMock

# We need to import the module itself to patch its variables
from utils import aws_utils
from utils.aws_utils import (
    get_db_connection_url, create_db_engine, upload_data_to_s3,
    update_db_record, get_s3_client, ensure_s3_folder_exists
)


# Use monkeypatch to directly set the module-level variables
@pytest.fixture(autouse=True)
def mock_module_globals(monkeypatch):
    """
    Directly patches the global variables inside the aws_utils module for testing.
    This is necessary because the variables are set when the module is first imported.
    """
    monkeypatch.setattr(aws_utils, "DB_DIALECT", "mysql")
    monkeypatch.setattr(aws_utils, "DB_DRIVER", "mysqlconnector")
    monkeypatch.setattr(aws_utils, "DB_USER", "testuser")
    monkeypatch.setattr(aws_utils, "DB_PASSWORD", "testpass")
    monkeypatch.setattr(aws_utils, "DB_HOST", "localhost")
    monkeypatch.setattr(aws_utils, "DB_NAME", "testdb")
    monkeypatch.setattr(aws_utils, "DB_PORT", "3306")
    # Set a default value for the S3 folder to prevent errors in other tests
    monkeypatch.setattr(aws_utils, "S3_DEST_FOLDER", "crawl_configs/")


def test_get_db_connection_url_success():
    """
    Tests that a correct database URL is constructed.
    """
    # This now correctly matches the DB_NAME from the mock_module_globals fixture
    expected_url = "mysql+mysqlconnector://testuser:testpass@localhost:3306/testdb"
    assert get_db_connection_url() == expected_url


def test_get_db_connection_url_missing_vars(monkeypatch):
    """
    Tests that None is returned if a required environment variable is missing.
    """
    # **THE FIX:** Set the attribute to None instead of deleting it.
    # This correctly simulates the variable being missing from the environment.
    monkeypatch.setattr(aws_utils, "DB_HOST", None)
    assert get_db_connection_url() is None


@patch('utils.aws_utils.create_engine')
def test_create_db_engine_success(mock_create_engine):
    """
    Tests that the create_engine function is called with the correct URL.
    """
    # The fixture will ensure get_db_connection_url returns the correct test URL
    create_db_engine()
    expected_url = "mysql+mysqlconnector://testuser:testpass@localhost:3306/testdb"
    mock_create_engine.assert_called_once_with(expected_url)


@patch('utils.aws_utils.create_engine', side_effect=Exception("DB connection failed"))
def test_create_db_engine_failure(mock_create_engine):
    """
    Tests that the function handles exceptions from create_engine.
    """
    assert create_db_engine() is None


@patch('utils.aws_utils.get_db_connection_url', return_value=None)
def test_create_db_engine_no_url(mock_get_url):
    """
    Tests that create_db_engine returns None if it doesn't get a connection URL.
    """
    assert create_db_engine() is None
    mock_get_url.assert_called_once()


@mock_aws
def test_upload_data_to_s3_success():
    """
    Tests that data is correctly uploaded to a mock S3 bucket.
    """
    conn = boto3.client("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="test-bucket")

    # Call the function to test
    success = upload_data_to_s3(
        s3_client=conn,
        data_content="Hello World",
        bucket_name="test-bucket",
        s3_object_key="test.txt"
    )

    assert success is True
    # Verify the object was created in the mock bucket
    body = conn.get_object(Bucket="test-bucket", Key="test.txt")['Body'].read().decode('utf-8')
    assert body == "Hello World"


@mock_aws
def test_upload_data_to_s3_bytes_content():
    """
    Tests that the function correctly handles byte string content.
    """
    conn = boto3.client("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="test-bucket")
    
    success = upload_data_to_s3(
        s3_client=conn,
        data_content=b"Hello Bytes",
        bucket_name="test-bucket",
        s3_object_key="test.txt"
    )
    assert success is True
    body = conn.get_object(Bucket="test-bucket", Key="test.txt")['Body'].read()
    assert body == b"Hello Bytes"

def test_upload_data_to_s3_invalid_content_type():
    """
    Tests that the function returns False for unsupported data types.
    """
    mock_s3 = MagicMock()
    success = upload_data_to_s3(
        s3_client=mock_s3,
        data_content=12345, # Invalid type
        bucket_name="test-bucket",
        s3_object_key="test.txt"
    )
    assert success is False
    mock_s3.put_object.assert_not_called()

@mock_aws
def test_upload_data_to_s3_client_error():
    """
    Tests the function's handling of ClientError (e.g., bucket does not exist).
    """
    conn = boto3.client("s3", region_name="us-east-1")
    # We do NOT create the bucket, so this will fail
    success = upload_data_to_s3(
        s3_client=conn,
        data_content="Hello World",
        bucket_name="non-existent-bucket",
        s3_object_key="test.txt"
    )
    assert success is False


@patch('boto3.client')
def test_upload_data_to_s3_no_credentials_error(mock_boto_client):
    """
    Tests the function's handling of NoCredentialsError.
    """
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = NoCredentialsError()
    mock_boto_client.return_value = mock_s3
    
    success = upload_data_to_s3(
        s3_client=mock_s3, data_content="data", bucket_name="b", s3_object_key="k"
    )
    assert success is False

@patch('boto3.client')
def test_upload_data_to_s3_partial_credentials_error(mock_boto_client):
    """
    Tests the function's handling of PartialCredentialsError.
    """
    mock_s3 = MagicMock()
    # FIX: The constructor for PartialCredentialsError requires specific keys for its format string.
    # We provide 'provider' and 'cred_var' which are expected by the exception's message format.
    mock_s3.put_object.side_effect = PartialCredentialsError(provider="test", cred_var="session_token")
    mock_boto_client.return_value = mock_s3
    
    success = upload_data_to_s3(
        s3_client=mock_s3, data_content="data", bucket_name="b", s3_object_key="k"
    )
    assert success is False


@patch('boto3.client')
def test_upload_data_to_s3_general_exception(mock_boto_client):
    """
    Tests the function's handling of a generic exception during upload.
    """
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("A generic error occurred")
    mock_boto_client.return_value = mock_s3
    
    success = upload_data_to_s3(
        s3_client=mock_s3,
        data_content="Hello World",
        bucket_name="any-bucket",
        s3_object_key="test.txt"
    )
    assert success is False


def test_update_db_record_success():
    """
    Tests that update_db_record constructs and executes the correct SQL.
    """
    # Create a mock for the database connection
    mock_connection = MagicMock()
    
    update_db_record(
        connection=mock_connection,
        table_name="my_table",
        identifier_column_name="id",
        identifier_value=123,
        columns_to_update={"status": "complete", "value": 99}
    )

    # Check that the execute method was called
    assert mock_connection.execute.called
    # Get the arguments that the mock was called with
    call_args = mock_connection.execute.call_args
    # The first argument is the query object (TextClause)
    query_string = str(call_args[0][0])
    # The second argument is the parameters dictionary
    params = call_args[0][1]

    # Assert that the SQL string is correctly formatted (order of SET might vary)
    assert "UPDATE my_table" in query_string
    assert "status = :val_0" in query_string
    assert "value = :val_1" in query_string
    assert "WHERE id = :identifier_val_in_db" in query_string
    
    # Assert that the parameters are correct
    assert params == {'val_0': 'complete', 'val_1': 99, 'identifier_val_in_db': 123}


def test_update_db_record_no_columns():
    """
    Tests that the function returns False if no columns are provided to update.
    """
    mock_connection = MagicMock()
    result = update_db_record(
        connection=mock_connection,
        table_name="my_table",
        identifier_column_name="id",
        identifier_value=123,
        columns_to_update={} # Empty dictionary
    )
    assert result is False
    # Ensure the database was not touched
    mock_connection.execute.assert_not_called()

def test_update_db_record_null_identifier():
    """
    Tests that the function returns False if the identifier_value is None.
    """
    mock_connection = MagicMock()
    result = update_db_record(
        connection=mock_connection,
        table_name="my_table",
        identifier_column_name="id",
        identifier_value=None, # Null identifier
        columns_to_update={"status": "failed"}
    )
    assert result is False
    mock_connection.execute.assert_not_called()

def test_update_db_record_db_exception():
    """
    Tests that the function returns False when the DB execute call fails.
    """
    mock_connection = MagicMock()
    mock_connection.execute.side_effect = Exception("Database error")
    
    result = update_db_record(
        connection=mock_connection,
        table_name="my_table",
        identifier_column_name="id",
        identifier_value=123,
        columns_to_update={"status": "failed"}
    )
    assert result is False

def test_update_db_record_invalid_names():
    """
    Tests that the function returns False for invalid table or column names to prevent SQL injection.
    """
    mock_connection = MagicMock()
    
    # Test invalid column name in the update dictionary
    result_invalid_col = update_db_record(
        connection=mock_connection,
        table_name="my_table",
        identifier_column_name="id",
        identifier_value=123,
        columns_to_update={"invalid-col;": "bad_data"}
    )
    assert result_invalid_col is False

    # Test invalid table name
    result_invalid_table = update_db_record(
        connection=mock_connection,
        table_name="my_table;",
        identifier_column_name="id",
        identifier_value=123,
        columns_to_update={"status": "good"}
    )
    assert result_invalid_table is False
    
    mock_connection.execute.assert_not_called()


@patch('boto3.client')
def test_get_s3_client_success(mock_boto_client):
    """
    Tests that the get_s3_client function calls boto3.client correctly.
    """
    get_s3_client()
    mock_boto_client.assert_called_once_with('s3')


@patch('boto3.client', side_effect=Exception("AWS client init failed"))
def test_get_s3_client_failure(mock_boto_client):
    """
    Tests that get_s3_client returns None if boto3.client raises an exception.
    """
    assert get_s3_client() is None

@mock_aws
def test_ensure_s3_folder_exists():
    """
    Tests that a folder marker object is created in S3.
    """
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3.create_bucket(Bucket=bucket_name)

    success = ensure_s3_folder_exists(s3, bucket_name, "my-folder/")
    assert success is True

    # Check that the empty folder object was created
    response = s3.list_objects_v2(Bucket=bucket_name)
    assert response['Contents'][0]['Key'] == "my-folder/"


@mock_aws
def test_ensure_s3_folder_exists_no_trailing_slash():
    """
    Tests that the function works correctly even without a trailing slash.
    """
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3.create_bucket(Bucket=bucket_name)
    
    success = ensure_s3_folder_exists(s3, bucket_name, "my-folder") # No slash
    assert success is True
    
    response = s3.list_objects_v2(Bucket=bucket_name)
    assert response['Contents'][0]['Key'] == "my-folder"


@mock_aws
def test_ensure_s3_folder_exists_client_error():
    """
    Tests that the function returns False on a ClientError (e.g., bucket not found).
    """
    s3 = boto3.client("s3", region_name="us-east-1")
    # We do NOT create the bucket, so the put_object call will fail
    success = ensure_s3_folder_exists(s3, "non-existent-bucket", "my-folder/")
    assert success is False

@patch('boto3.client')
def test_ensure_s3_folder_exists_general_exception(mock_boto_client):
    """
    Tests that the function returns False on a generic exception.
    """
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = Exception("Something went wrong")
    mock_boto_client.return_value = mock_s3
    
    success = ensure_s3_folder_exists(mock_s3, "any-bucket", "any-folder/")
    assert success is False


# --- FINAL TEST TO GET 100% COVERAGE ---
def test_s3_dest_folder_trailing_slash(monkeypatch):
    """
    Tests the module-level code that adds a trailing slash to S3_DEST_FOLDER.
    """
    # **THE FIX:** Use setenv to change the environment variable before reloading the module.
    # This ensures os.getenv() will see our new value.
    monkeypatch.setenv("S3_DEST_FOLDER", "no_slash")
    
    # Reload the module to re-trigger the import-time code
    importlib.reload(aws_utils)
    
    # Assert that the slash was added
    assert aws_utils.S3_DEST_FOLDER == "no_slash/"

