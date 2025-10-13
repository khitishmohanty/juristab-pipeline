from typing import List, Dict, Any
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3
from utils import get_logger

class OpenSearchService:
    """Service for OpenSearch operations."""
    
    def __init__(self, config: dict):
        """
        Initialize OpenSearch service.
        
        Args:
            config: OpenSearch configuration
        """
        self.logger = get_logger(__name__)
        self.config = config['opensearch']
        self.index_name = self.config['index_name']
        
        # Create OpenSearch client
        self.client = self._create_client()
        
        # Ensure index exists
        self._ensure_index_exists()
    
    def _create_client(self) -> OpenSearch:
        """Create OpenSearch client with appropriate authentication."""
        host = self.config['host']
        port = self.config['port']
        
        # AWS OpenSearch Serverless uses IAM authentication
        if 'aoss.amazonaws.com' in host:
            self.logger.info("Using AWS OpenSearch Serverless with IAM authentication")
            
            # Use IAM credentials from environment or instance role
            session = boto3.Session()
            credentials = session.get_credentials()
            
            # AWS4Auth for OpenSearch Serverless
            auth = AWSV4SignerAuth(
                credentials,
                self.config.get('region', 'ap-southeast-2'),
                'aoss'  # Service name for OpenSearch Serverless
            )
            
            client = OpenSearch(
                hosts=[{'host': host, 'port': port}],
                http_auth=auth,
                use_ssl=True,
                verify_certs=True,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
                connection_class=RequestsHttpConnection
            )
        else:
            # For standard OpenSearch (non-AWS or self-managed)
            self.logger.info("Using standard OpenSearch with basic authentication")
            
            # Use basic auth - get credentials from environment if available
            import os
            username = os.environ.get('OPENSEARCH_USER', 'admin')
            password = os.environ.get('OPENSEARCH_PASSWORD', '')
            
            if not password:
                self.logger.warning("No OpenSearch password provided - connection may fail")
            
            client = OpenSearch(
                hosts=[{'host': host, 'port': port}],
                http_auth=(username, password),
                use_ssl=self.config.get('use_ssl', True),
                verify_certs=self.config.get('verify_certs', True),
                ssl_assert_hostname=self.config.get('ssl_assert_hostname', False),
                ssl_show_warn=self.config.get('ssl_show_warn', False),
                connection_class=RequestsHttpConnection
            )
        
        # Test connection
        try:
            info = client.info()
            self.logger.info(f"Successfully connected to OpenSearch")
        except Exception as e:
            self.logger.error(f"Failed to connect to OpenSearch: {str(e)}")
            raise
        
        return client
    
    def _ensure_index_exists(self):
        """Ensure the index exists with proper mappings."""
        if not self.client.indices.exists(index=self.index_name):
            self.logger.info(f"Creating index: {self.index_name}")
            
            # Define index mappings
            index_body = {
                "mappings": {
                    "properties": {
                        "source_id": {"type": "keyword"},
                        "section_id": {"type": "keyword"},
                        "book_type": {"type": "keyword"},
                        "book_name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "neutral_citation": {"type": "keyword"},
                        "section_name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "content": {
                            "type": "text",
                            "analyzer": "standard"  # Using standard analyzer for compatibility
                        }
                    }
                },
                "settings": {
                    "number_of_shards": 2,
                    "number_of_replicas": 1
                }
            }
            
            self.client.indices.create(index=self.index_name, body=index_body)
            self.logger.info(f"Index {self.index_name} created successfully")
        else:
            self.logger.info(f"Index {self.index_name} already exists")
    
    def index_document(self, document: Dict[str, Any]) -> bool:
        """
        Index a single document.
        
        Args:
            document: Document to index
        
        Returns:
            True if successful, False otherwise
        """
        try:
            response = self.client.index(
                index=self.index_name,
                body=document,
                id=document.get('source_id')
            )
            return response['result'] in ['created', 'updated']
            
        except Exception as e:
            self.logger.error(f"Error indexing document: {str(e)}")
            return False
    
    def bulk_index_documents(self, documents: List[Dict[str, Any]]) -> tuple:
        """
        Bulk index documents.
        
        Args:
            documents: List of documents to index
        
        Returns:
            Tuple of (success_count, error_count)
        """
        if not documents:
            return 0, 0
        
        success_count = 0
        error_count = 0
        
        # Prepare bulk request body
        bulk_body = []
        for doc in documents:
            # Create unique ID for documents
            doc_id = f"{doc.get('source_id', '')}"
            if 'section_id' in doc and doc['section_id']:
                doc_id += f"_{doc['section_id']}"
            
            bulk_body.append({
                "index": {
                    "_index": self.index_name,
                    "_id": doc_id
                }
            })
            bulk_body.append(doc)
        
        try:
            response = self.client.bulk(body=bulk_body)
            
            # Process response
            for item in response['items']:
                if 'error' not in item.get('index', {}):
                    success_count += 1
                else:
                    error_count += 1
                    self.logger.error(f"Error indexing document: {item['index'].get('error', {})}")
            
            self.logger.info(f"Bulk indexed {success_count} documents, {error_count} errors")
            
        except Exception as e:
            self.logger.error(f"Error during bulk indexing: {str(e)}")
            error_count = len(documents)
        
        return success_count, error_count