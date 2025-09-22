import boto3
import numpy as np
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk
from aws_requests_auth.aws_auth import AWSRequestsAuth
from tqdm import tqdm
from utils.config_loader import config
import time
import io

class VectorIngestor:
    """
    A class to handle the ingestion of vector embeddings into OpenSearch Serverless.
    """
    def __init__(self, db_engine, doc_type: str):
        """
        Initializes the VectorIngestor.
        """
        self.engine = db_engine
        self.config = config
        self.doc_type = doc_type
        self.index_name = self.config['opensearch']['index_name']
        self.os_client = self._get_opensearch_client()
        self.s3_client = boto3.client('s3', region_name=self.config['aws']['default_region'])
        print(f"VectorIngestor for '{self.doc_type}' initialized successfully.")
        print(f"Target OpenSearch index: {self.index_name}")

    def _get_opensearch_client(self):
        """Initializes and returns a client for OpenSearch Serverless."""
        host = self.config['opensearch']['host']
        aws_region = self.config['aws']['default_region']

        if not host:
            raise ValueError("OpenSearch host is not configured. Please set OPENSEARCH_HOST.")

        if host.startswith("https://"):
            host = host[8:]
        elif host.startswith("http://"):
            host = host[7:]

        credentials = boto3.Session().get_credentials()
        aws_auth = AWSRequestsAuth(
            aws_access_key=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            aws_token=credentials.token,
            aws_host=host,
            aws_region=aws_region,
            aws_service='aoss'
        )

        client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=120
        )
        print("OpenSearch client created successfully.")
        
        try:
            if client.indices.exists(index=self.index_name):
                print(f"Index '{self.index_name}' exists.")
                count_response = client.count(index=self.index_name)
                print(f"Current document count in index: {count_response['count']}")
            else:
                print(f"WARNING: Index '{self.index_name}' does not exist!")
        except Exception as e:
            print(f"Error checking index status: {e}")
        
        return client

    def _generate_bulk_actions(self, pending_documents):
        """
        Generator function to yield documents for bulk ingestion, compatible with Serverless.
        """
        print("Starting to process files for bulk ingestion...")
        bucket_name = self.config['aws']['s3']['bucket_name']
        embedding_filename = self.config['enrichment_filenames']['embedding_output']
        
        successful_docs = 0
        failed_docs = 0

        for _, row in tqdm(pending_documents.iterrows(), total=pending_documents.shape[0], desc="Processing S3 files"):
            source_id = row['source_id']
            s3_path = row['file_path']
            
            if s3_path.startswith(f"s3://{bucket_name}/"):
                s3_key_prefix = s3_path[len(f"s3://{bucket_name}/"):]
            else:
                print(f"Warning: Skipping unexpected file_path format: {s3_path}")
                failed_docs += 1
                continue
                
            embedding_key = f"{s3_key_prefix}/{embedding_filename}"

            try:
                response = self.s3_client.get_object(Bucket=bucket_name, Key=embedding_key)
                file_content = response['Body'].read()
                embedding = np.load(io.BytesIO(file_content))
                
                if embedding.shape[0] != 1024:
                    print(f"Warning: Embedding for source_id {source_id} has incorrect dimensions. Skipping.")
                    failed_docs += 1
                    continue
                
                doc = {
                    "_op_type": "index",
                    "_index": self.index_name,
                    # No "_id" field for OpenSearch Serverless compatibility
                    "_source": {
                        "doc_id": source_id,
                        "doc_type": self.doc_type,
                        "legal_document_embedding": embedding.tolist()
                    }
                }
                
                successful_docs += 1
                yield doc

            except self.s3_client.exceptions.NoSuchKey:
                print(f"Warning: Embedding file not found for source_id {source_id} at {embedding_key}")
                failed_docs += 1
                continue
            except Exception as e:
                print(f"Error processing source_id {source_id}: {e}")
                failed_docs += 1
                continue
        
        print(f"Pre-processing complete. Ready to ingest: {successful_docs}, Failed: {failed_docs}")

    def run_pipeline(self, pending_documents):
        """
        Executes the full, production-ready bulk data ingestion pipeline.
        Processes documents in chunks of 100.
        """
        if pending_documents.empty:
            print(f"No pending {self.doc_type} documents to ingest.")
            return

        total_docs = len(pending_documents)
        print(f"\nStarting bulk ingestion of {total_docs} documents to OpenSearch...")
        
        try:
            initial_count = self.os_client.count(index=self.index_name)['count']
            print(f"Initial document count: {initial_count}")
            
            success_count = 0
            failed_items = []
            
            chunk_size = 100
            actions = self._generate_bulk_actions(pending_documents)
            
            # Use the bulk helper to efficiently process in chunks
            success, failures = bulk(
                self.os_client,
                actions,
                chunk_size=chunk_size,
                request_timeout=120,
                max_retries=3,
                initial_backoff=2,
                raise_on_error=False,
                raise_on_exception=False
            )
            success_count += success
            if failures:
                failed_items.extend(failures)

            print(f"\nBulk ingestion complete. Success: {success_count}, Failed: {len(failed_items)}")
            
            if failed_items:
                print(f"Sample of failed items (first 5):")
                for item in failed_items[:5]:
                    print(f"  {item}")

            # It can take a few moments for the count to update in Serverless
            print("Waiting for index to refresh...")
            time.sleep(5) 
            
            final_count = self.os_client.count(index=self.index_name)['count']
            print(f"Final document count: {final_count}")
            print(f"Documents added in this run: {final_count - initial_count}")

        except Exception as e:
            print(f"A critical error occurred during bulk ingestion: {e}")
            import traceback
            traceback.print_exc()

