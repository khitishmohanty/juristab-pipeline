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
                
                # Full index configuration with analyzers and mappings
                index_body = {
                    "settings": {
                        "analysis": {
                            "analyzer": {
                                "legal_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "stop", "porter_stem"]
                                },
                                "citation_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "keyword",
                                    "filter": ["lowercase", "trim"]
                                },
                                "autocomplete_index_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "edge_ngram"]
                                },
                                "autocomplete_search_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase"]
                                },
                                "suggestion_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding"]
                                }
                            },
                            "filter": {
                                "edge_ngram": {
                                    "type": "edge_ngram",
                                    "min_gram": 2,
                                    "max_gram": 20
                                },
                                "porter_stem": {
                                    "type": "porter_stem"
                                }
                            }
                        },
                        "number_of_shards": 2,
                        "number_of_replicas": 1
                    },
                    "mappings": {
                        "properties": {
                            # Core identification fields
                            "source_id": {"type": "keyword"},
                            "section_id": {"type": "keyword"},
                            "document_type": {
                                "type": "keyword",
                                "copy_to": ["all_search"]
                            },
                            
                            # Main content fields
                            "content": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "term_vector": "with_positions_offsets",
                                "fields": {
                                    "autocomplete": {
                                        "type": "search_as_you_type",
                                        "doc_values": False,
                                        "max_shingle_size": 3
                                    }
                                }
                            },
                            "content_length": {"type": "integer"},
                            
                            # Book/document identifiers
                            "book_name": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 200
                                    }
                                }
                            },
                            "neutral_citation": {
                                "type": "text",
                                "analyzer": "citation_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 100},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 100
                                    }
                                }
                            },
                            
                            # Caselaw-specific fields
                            "file_no": {"type": "keyword"},
                            "presiding_officer": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "counsel": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "law_firm_agency": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 150
                                    }
                                }
                            },
                            "court_type": {
                                "type": "keyword",
                                "copy_to": ["all_search"]
                            },
                            "hearing_location": {
                                "type": "text",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "keywords": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "legislation_cited": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "affected_sectors": {
                                "type": "keyword",
                                "copy_to": ["all_search"]
                            },
                            "practice_areas": {
                                "type": "keyword",
                                "copy_to": ["all_search"]
                            },
                            "citation": {
                                "type": "text",
                                "analyzer": "citation_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 150
                                    }
                                }
                            },
                            "key_issues": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "autocomplete": {
                                        "type": "search_as_you_type",
                                        "doc_values": False,
                                        "max_shingle_size": 3
                                    }
                                }
                            },
                            "panelist": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "cases_cited": {
                                "type": "text",
                                "analyzer": "citation_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "matter_type": {
                                "type": "keyword",
                                "copy_to": ["all_search"]
                            },
                            "category": {
                                "type": "keyword",
                                "copy_to": ["all_search"]
                            },
                            "bjs_number": {"type": "keyword"},
                            "tribunal_name": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 150
                                    }
                                }
                            },
                            "panel_or_division_name": {
                                "type": "text",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "year": {"type": "integer"},
                            "decision_number": {"type": "integer"},
                            "decision_date": {
                                "type": "date",
                                "format": "yyyy-MM-dd||epoch_millis||strict_date_optional_time"
                            },
                            "members": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            
                            # Legislation-specific fields
                            "section_name": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "legislation_number": {
                                "type": "text",
                                "analyzer": "citation_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 100
                                    }
                                }
                            },
                            "type_of_document": {
                                "type": "keyword",
                                "copy_to": ["all_search"]
                            },
                            "enabling_act": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 150
                                    }
                                }
                            },
                            "amended_legislation": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    }
                                }
                            },
                            "administering_agency": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "copy_to": ["all_search"],
                                "fields": {
                                    "keyword": {"type": "keyword"},
                                    "autocomplete": {
                                        "type": "text",
                                        "analyzer": "autocomplete_index_analyzer",
                                        "search_analyzer": "autocomplete_search_analyzer"
                                    },
                                    "suggest": {
                                        "type": "completion",
                                        "analyzer": "suggestion_analyzer",
                                        "preserve_separators": True,
                                        "preserve_position_increments": True,
                                        "max_input_length": 100
                                    }
                                }
                            },
                            
                            # Meta fields
                            "indexed_date": {
                                "type": "date",
                                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS||strict_date_optional_time"
                            },
                            
                            # Combined search field
                            "all_search": {
                                "type": "text",
                                "analyzer": "legal_analyzer",
                                "fields": {
                                    "autocomplete": {
                                        "type": "search_as_you_type",
                                        "doc_values": False,
                                        "max_shingle_size": 3
                                    }
                                }
                            }
                        }
                    }
                }
                
                try:
                    # Try to create index with full configuration
                    self.client.indices.create(index=self.index_name, body=index_body)
                    self.logger.info(f"Index {self.index_name} created successfully with full mappings and analyzers")
                except Exception as create_error:
                    error_msg = str(create_error)
                    
                    # Check if error is due to OpenSearch Serverless limitations
                    if 'ValidationException' in error_msg or 'settings' in error_msg.lower():
                        self.logger.warning(f"Could not create index with custom analyzers (likely OpenSearch Serverless): {error_msg}")
                        self.logger.info("Attempting to create index without custom settings for OpenSearch Serverless...")
                        
                        # Simplified mappings for OpenSearch Serverless (no custom analyzers)
                        serverless_body = {
                            "mappings": index_body["mappings"]
                        }
                        
                        # Remove analyzer references from mappings for serverless
                        for field_name, field_config in serverless_body["mappings"]["properties"].items():
                            if isinstance(field_config, dict):
                                field_config.pop("analyzer", None)
                                if "fields" in field_config:
                                    for subfield_config in field_config["fields"].values():
                                        if isinstance(subfield_config, dict):
                                            subfield_config.pop("analyzer", None)
                                            subfield_config.pop("search_analyzer", None)
                        
                        try:
                            self.client.indices.create(index=self.index_name, body=serverless_body)
                            self.logger.info(f"Index {self.index_name} created with standard analyzers for OpenSearch Serverless")
                        except Exception as serverless_error:
                            self.logger.error(f"Failed to create index even with simplified settings: {serverless_error}")
                            self.logger.warning("Index might need to be created externally with proper configuration")
                    else:
                        self.logger.error(f"Failed to create index: {create_error}")
                        self.logger.warning("Index might need to be created externally with proper configuration")
            else:
                self.logger.info(f"Index {self.index_name} already exists")
                
                # Optionally verify mappings match expected structure
                try:
                    existing_mapping = self.client.indices.get_mapping(index=self.index_name)
                    field_count = len(existing_mapping[self.index_name]['mappings']['properties'])
                    self.logger.info(f"Existing index has {field_count} mapped fields")
                    
                    # Check for critical fields
                    critical_fields = ['source_id', 'content', 'book_name', 'document_type']
                    existing_fields = existing_mapping[self.index_name]['mappings']['properties'].keys()
                    missing_fields = [f for f in critical_fields if f not in existing_fields]
                    
                    if missing_fields:
                        self.logger.warning(f"Index is missing critical fields: {missing_fields}")
                    else:
                        self.logger.info("All critical fields are present in the index")
                        
                except Exception as e:
                    self.logger.warning(f"Could not verify index mappings: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error in index management: {str(e)}")
            self.logger.warning("Proceeding anyway - index might exist or auto-create on first document")
    
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