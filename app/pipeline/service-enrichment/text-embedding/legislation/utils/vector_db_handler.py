import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

class VectorDBHandler:
    """Handles all OpenSearch interactions."""
    def __init__(self, config):
        vector_db_config = config['vector_db']
        self.host = vector_db_config['host']
        self.index_name = vector_db_config['index_name']
        self.doc_type = vector_db_config['doc_type']
        region = vector_db_config['default_region']
        
        # Get credentials using boto3 for AWS OpenSearch Serverless
        service = 'aoss'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key, 
            credentials.secret_key, 
            region, 
            service, 
            session_token=credentials.token
        )

        # Create the OpenSearch client
        self.client = OpenSearch(
            hosts=[{'host': self.host, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )
        print(f"VectorDBHandler: Connected to OpenSearch host '{self.host}'")

    def index_document(self, doc_id, embedding_vector):
        """Indexes a single document into OpenSearch."""
        document = {
            "doc_id": doc_id,
            "doc_type": self.doc_type,
            "caselaw_embedding": embedding_vector.tolist() # Convert numpy array to list
        }
        
        try:
            self.client.index(
                index=self.index_name,
                body=document,
                refresh=False 
            )
            print(f"Successfully indexed doc_id {doc_id} into OpenSearch.")
        except Exception as e:
            print(f"ERROR: Failed to index doc_id {doc_id} into OpenSearch.")
            raise e
