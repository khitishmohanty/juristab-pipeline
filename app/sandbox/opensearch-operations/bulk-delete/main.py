# test_connection.py
from utils.opensearch_delete_utils import load_config, get_client, delete_by_document_type

# Load config
config = load_config("utils/config.yaml")

# Connect client
client = get_client(config)

# Try a simple search
try:
    response = client.indices.get_alias(index="legal-store-search")
    print("Successfully connected! Index aliases:", response)
except Exception as e:
    print(f"Error: {e}")

# Try to count documents
try:
    response = client.count(index="legal-store-search")
    print(f"Document count: {response['count']}")
except Exception as e:
    print(f"Error counting documents: {e}")


# Delete all non-caselaw documents
delete_by_document_type(client, config["opensearch"]["index_name"], "caselaw", negate=True)