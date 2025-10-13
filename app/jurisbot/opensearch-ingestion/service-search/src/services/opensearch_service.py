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
            
            # IMPORTANT: Don't use a profile, use the credentials from environment
            # which are already loaded and verified correct
            session = boto3.Session()
            credentials = session.get_credentials()
            
            # Log the access key ID (partially hidden for security)
            if credentials and credentials.access_key:
                key_preview = credentials.access_key[:4] + "..." + credentials.access_key[-4:]
                self.logger.info(f"Using AWS Access Key: {key_preview}")
                
                # Verify the identity
                import boto3 as boto3_verify
                sts = boto3_verify.client('sts')
                try:
                    identity = sts.get_caller_identity()
                    self.logger.info(f"Confirming identity: {identity['Arn']}")
                    if 'legal-store-service' not in identity['Arn']:
                        self.logger.error(f"ERROR: Wrong user! Using: {identity['Arn']}")
                except Exception as e:
                    self.logger.warning(f"Could not verify identity: {e}")
            
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
                connection_class=RequestsHttpConnection,
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
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
                connection_class=RequestsHttpConnection,
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
        
        # Test connection - but don't fail if info() doesn't work
        try:
            # For OpenSearch Serverless, info() might not be available
            # Try to list indices instead
            indices = client.indices.get_alias(index="*")
            self.logger.info(f"Successfully connected to OpenSearch. Found {len(indices)} indices")
        except Exception as e:
            self.logger.warning(f"Could not get OpenSearch info (this is normal for serverless): {str(e)}")
            # Don't raise the error - connection might still work for indexing
        
        return client
    
    def _ensure_index_exists(self):
        """Ensure the index exists with proper mappings."""
        try:
            if not self.client.indices.exists(index=self.index_name):
                self.logger.info(f"Creating index: {self.index_name}")
                
                # Simplified index mappings for OpenSearch Serverless
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
                            "content": {"type": "text"}
                        }
                    }
                }
                
                # Try to create the index
                try:
                    self.client.indices.create(index=self.index_name, body=index_body)
                    self.logger.info(f"Index {self.index_name} created successfully")
                except Exception as create_error:
                    # If creation fails, try with minimal settings
                    self.logger.warning(f"Could not create index with full mappings: {str(create_error)}")
                    self.logger.info("Attempting to create index with minimal settings...")
                    
                    minimal_body = {
                        "mappings": {
                            "properties": {
                                "source_id": {"type": "keyword"},
                                "content": {"type": "text"}
                            }
                        }
                    }
                    self.client.indices.create(index=self.index_name, body=minimal_body)
                    self.logger.info(f"Index {self.index_name} created with minimal mappings")
            else:
                self.logger.info(f"Index {self.index_name} already exists")
                
        except Exception as e:
            self.logger.error(f"Error checking/creating index: {str(e)}")
            self.logger.warning("Continuing without index creation - it might already exist or will be auto-created on first document")
    
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
            if 'items' in response:
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