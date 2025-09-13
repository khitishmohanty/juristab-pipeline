import boto3
import numpy as np
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk
from aws_requests_auth.aws_auth import AWSRequestsAuth
from tqdm import tqdm
from utils.config_loader import config
import time

class VectorIngestor:
    """
    A class to handle the ingestion of vector embeddings into OpenSearch.
    """
    def __init__(self, db_engine):
        self.engine = db_engine
        self.config = config
        self.os_client = self._get_opensearch_client()
        self.s3_client = boto3.client('s3', region_name=self.config['aws']['default_region'])
        print("VectorIngestor initialized successfully.")

    def _get_opensearch_client(self):
        """Initializes and returns a client for OpenSearch Serverless."""
        host = self.config['opensearch']['host']
        aws_region = self.config['aws']['default_region']

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
        return client

    def _generate_bulk_actions(self, pending_caselaws):
        """
        Generator function to yield documents for bulk ingestion.
        """
        print("Starting to process files for bulk ingestion...")
        bucket_name = self.config['aws']['s3']['bucket_name'] #
        embedding_filename = self.config['enrichment_filenames']['embedding_output'] #
        index_name = self.config['opensearch']['index_name'] #

        for _, row in tqdm(pending_caselaws.iterrows(), total=pending_caselaws.shape[0], desc="Processing S3 files"): #
            source_id = row['source_id'] #
            s3_path = row['file_path'] #
            
            if s3_path.startswith(f"s3://{bucket_name}/"): #
                s3_key_prefix = s3_path[len(f"s3://{bucket_name}/"):] #
            else:
                print(f"Warning: Skipping unexpected file_path format: {s3_path}") #
                continue
                
            embedding_key = f"{s3_key_prefix}/{embedding_filename}" #

            try:
                response = self.s3_client.get_object(Bucket=bucket_name, Key=embedding_key) #
                file_content = response['Body'].read() #
                embedding = np.frombuffer(file_content, dtype=np.float32).tolist() #

                # FIX: Remove the "_id" field from the yielded dictionary
                yield {
                    "_index": index_name,
                    "_source": {
                        "doc_id": source_id,
                        "doc_type": "case-law",
                        "caselaw_embedding": embedding
                    }
                }
            except self.s3_client.exceptions.NoSuchKey: #
                print(f"Warning: Embedding file not found for source_id {source_id} at {embedding_key}") #
                continue
            except Exception as e: #
                print(f"Error processing source_id {source_id}: {e}") #
                continue

    def run_pipeline(self, pending_caselaws):
        """
        Executes the full data ingestion pipeline from S3 to OpenSearch.
        """
        if pending_caselaws.empty:
            print("No pending caselaws to ingest.")
            return

        total_docs = len(pending_caselaws)
        print(f"\nStarting bulk ingestion of {total_docs} documents to OpenSearch...")
        
        try:
            success_count, failed_items = bulk(
                self.os_client,
                self._generate_bulk_actions(pending_caselaws),
                chunk_size=500,
                request_timeout=60,
                max_retries=3,
                initial_backoff=2,
                refresh=False
            )
            print(f"\nIngestion complete. Success: {success_count}, Failed: {len(failed_items)}")
            if failed_items:
                print("First 5 failed items:", failed_items[:5])

            print("Refreshing index to make new documents searchable...")
            self.os_client.indices.refresh(index=self.config['opensearch']['index_name'])
            print("Index refreshed successfully.")

        except Exception as e:
            print(f"An error occurred during bulk ingestion: {e}")

