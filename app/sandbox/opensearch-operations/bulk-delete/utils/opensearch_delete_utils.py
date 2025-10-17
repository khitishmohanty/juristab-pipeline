import boto3
import yaml
import os
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


def load_config(path: str = "util/config.yaml") -> dict:
    """
    Load OpenSearch config from a YAML file.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_client(config: dict):
    """
    Returns an OpenSearch client using AWS SigV4 authentication.
    Config should be a dict from load_config().
    """
    host = config["opensearch"]["host"]
    port = config["opensearch"]["port"]
    region = config["opensearch"]["region"]

    session = boto3.Session()
    credentials = session.get_credentials()
    
    # Debug: Verify the credentials being used
    sts = session.client('sts')
    identity = sts.get_caller_identity()
    print(f"Authenticating as: {identity['Arn']}")
    
    auth = AWSV4SignerAuth(credentials, region, service='aoss')  # Make sure service is 'aoss'

    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=auth,
        use_ssl=config["opensearch"].get("use_ssl", True),
        verify_certs=config["opensearch"].get("verify_certs", True),
        ssl_assert_hostname=config["opensearch"].get("ssl_assert_hostname", False),
        ssl_show_warn=config["opensearch"].get("ssl_show_warn", False),
        connection_class=RequestsHttpConnection,
    )


def scroll_query(client, index: str, query: dict, scroll: str = "2m", size: int = 500):
    """Run a scroll search query and return the first batch + scroll_id."""
    return client.search(
        index=index,
        body=query,
        scroll=scroll,
        size=size,
        _source=False,
    )


def fetch_scroll(client, scroll_id: str, scroll: str = "2m"):
    """Fetch next batch of results using scroll_id."""
    return client.scroll(scroll_id=scroll_id, scroll=scroll)


def bulk_delete(client, hits: list):
    """Delete a batch of documents given hits from a scroll."""
    if not hits:
        return 0

    actions = []
    for h in hits:
        actions.append({"delete": {"_index": h["_index"], "_id": h["_id"]}})

    body = "\n".join([str(a).replace("'", '"') for a in actions]) + "\n"
    client.bulk(body=body)
    return len(hits)


def delete_by_document_type(client, index: str, document_type: str, negate: bool = False, batch_size: int = 200):
    """
    Delete documents from an index where `document_type` matches (or does not match if negate=True).
    
    Examples:
      delete_by_document_type(client, "legal-store-search", "caselaw", negate=False)  # deletes all with document_type="caselaw"
      delete_by_document_type(client, "legal-store-search", "caselaw", negate=True)   # deletes all with document_type!="caselaw"
    """
    # Build query - matching the exact format that worked in console
    if negate:
        # This matches your console query for != 'caselaw'
        query_body = {
            "_index": index,
            "_id": "*",  
            "query": {
                "bool": {
                    "must_not": [
                        {"term": {"document_type": document_type}}
                    ]
                }
            }
        }
        print(f"Deleting all documents where document_type != '{document_type}'")
    else:
        query_body = {
            "_index": index,
            "_id": "*",
            "query": {
                "term": {"document_type": document_type}
            }
        }
        print(f"Deleting all documents where document_type = '{document_type}'")
    
    # Count documents first
    count_query = {"query": query_body["query"]}
    count_response = client.count(index=index, body=count_query)
    docs_to_delete = count_response['count']
    print(f"Found {docs_to_delete} documents to delete")
    
    if docs_to_delete == 0:
        print("No documents to delete")
        return
    
    # Confirm before deleting
    response = input(f"Are you sure you want to delete {docs_to_delete} documents? (yes/no): ")
    if response.lower() != 'yes':
        print("Deletion cancelled")
        return
    
    # Prepare the delete operation
    delete_body = {
        "delete": query_body
    }
    
    # Execute the delete - using the same structure as the console
    try:
        # First, let's try with a POST to _bulk endpoint with delete operations
        total_deleted = 0
        while total_deleted < docs_to_delete:
            # Get documents to delete
            search_body = {
                "query": query_body["query"],
                "size": batch_size,
                "_source": False
            }
            
            response = client.search(index=index, body=search_body)
            hits = response['hits']['hits']
            
            if not hits:
                break
            
            # Build bulk delete body
            bulk_operations = []
            for hit in hits:
                bulk_operations.append({"delete": {"_index": hit["_index"], "_id": hit["_id"]}})
            
            # Execute bulk delete
            if bulk_operations:
                bulk_response = client.bulk(body=bulk_operations)
                deleted_count = len([item for item in bulk_response['items'] if item['delete']['status'] == 200])
                total_deleted += deleted_count
                print(f"  Deleted {deleted_count} docs (Total: {total_deleted}/{docs_to_delete})")
                
                # Check for errors
                if bulk_response.get('errors'):
                    for item in bulk_response['items']:
                        if item['delete'].get('error'):
                            print(f"    Error deleting {item['delete']['_id']}: {item['delete']['error']}")
            
            # Small delay to avoid rate limiting
            import time
            time.sleep(0.2)
        
        print(f"✅ Finished. Deleted {total_deleted} documents from {index}")
        
    except Exception as e:
        print(f"Error during deletion: {e}")
        raise
