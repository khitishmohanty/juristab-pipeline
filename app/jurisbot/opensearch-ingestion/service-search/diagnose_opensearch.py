#!/usr/bin/env python3
"""
Diagnostic script for OpenSearch Serverless connection issues
"""

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import json
from botocore.exceptions import ClientError

def diagnose_opensearch():
    print("=" * 60)
    print("OpenSearch Serverless Diagnostic")
    print("=" * 60)
    
    # 1. Verify AWS credentials
    session = boto3.Session()
    credentials = session.get_credentials()
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    print(f"Using AWS Identity: {identity['Arn']}")
    
    # 2. Check OpenSearch Serverless collections via AWS API
    print("\n" + "=" * 60)
    print("Checking OpenSearch Serverless Collections via AWS API")
    print("=" * 60)
    
    try:
        oss_client = boto3.client('opensearchserverless', region_name='ap-southeast-2')
        
        # List collections
        response = oss_client.list_collections()
        collections = response.get('collectionSummaries', [])
        
        print(f"Found {len(collections)} collections:")
        for collection in collections:
            print(f"\nCollection: {collection.get('name')}")
            print(f"  ID: {collection.get('id')}")
            print(f"  Status: {collection.get('status')}")
            print(f"  Endpoint: {collection.get('collectionEndpoint')}")
            
            # Check if this matches our target
            if '5cl8kjgyy51ybng8oe6j' in collection.get('collectionEndpoint', ''):
                print("  *** This is our target collection! ***")
                
                # Get detailed info
                try:
                    detail_response = oss_client.batch_get_collection(
                        ids=[collection.get('id')]
                    )
                    details = detail_response.get('collectionDetails', [])
                    if details:
                        detail = details[0]
                        print(f"  Type: {detail.get('type')}")
                        print(f"  Created: {detail.get('createdDate')}")
                        print(f"  ARN: {detail.get('arn')}")
                except Exception as e:
                    print(f"  Could not get details: {e}")
                    
    except ClientError as e:
        print(f"Error accessing OpenSearch Serverless API: {e}")
        print("You may need to add the following IAM permissions:")
        print("  - opensearchserverless:ListCollections")
        print("  - opensearchserverless:BatchGetCollection")
    
    # 3. Test direct connection
    print("\n" + "=" * 60)
    print("Testing Direct OpenSearch Connection")
    print("=" * 60)
    
    host = '5cl8kjgyy51ybng8oe6j.ap-southeast-2.aoss.amazonaws.com'
    region = 'ap-southeast-2'
    
    print(f"Endpoint: https://{host}")
    print(f"Region: {region}")
    
    # Create auth
    auth = AWSV4SignerAuth(credentials, region, 'aoss')
    
    # Create client with detailed error handling
    client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30
    )
    
    # Test various operations
    tests = [
        ("List all indices", lambda: client.indices.get_alias(index="*")),
        ("Check specific index", lambda: client.indices.exists(index="legal-store-search")),
        ("Get cluster info", lambda: client.info()),
        ("Cat indices", lambda: client.cat.indices()),
    ]
    
    for test_name, test_func in tests:
        print(f"\nTest: {test_name}")
        try:
            result = test_func()
            print(f"  ✓ Success")
            if isinstance(result, dict):
                print(f"  Result: {json.dumps(result, indent=2)[:200]}...")
            elif isinstance(result, bool):
                print(f"  Result: {result}")
            else:
                print(f"  Result: {str(result)[:200]}...")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            error_str = str(e)
            if "403" in error_str:
                print("  This is an authorization error")
            elif "404" in error_str:
                print("  This endpoint or resource was not found")
            elif "connection" in error_str.lower():
                print("  This is a network connectivity issue")
    
    # 4. Try to create index with minimal settings
    print("\n" + "=" * 60)
    print("Attempting to Create Index")
    print("=" * 60)
    
    index_name = "legal-store-search"
    try:
        if not client.indices.exists(index=index_name):
            print(f"Index '{index_name}' does not exist, creating...")
            
            # Try minimal mapping first
            body = {
                "mappings": {
                    "properties": {
                        "source_id": {"type": "keyword"},
                        "content": {"type": "text"}
                    }
                }
            }
            
            client.indices.create(index=index_name, body=body)
            print(f"✓ Successfully created index '{index_name}'")
        else:
            print(f"Index '{index_name}' already exists")
    except Exception as e:
        print(f"✗ Could not create index: {e}")
        
        # Try without body
        try:
            print("Trying to create index without mappings...")
            client.indices.create(index=index_name)
            print(f"✓ Successfully created index '{index_name}' without mappings")
        except Exception as e2:
            print(f"✗ Still failed: {e2}")
    
    # 5. Summary and recommendations
    print("\n" + "=" * 60)
    print("Diagnosis Summary")
    print("=" * 60)
    
    print("\nIf you're seeing 403 errors:")
    print("1. Verify the collection name in AWS Console matches 'legal-store-search'")
    print("2. Check the collection status is 'ACTIVE' not 'CREATING' or 'FAILED'")
    print("3. Ensure the data access policy includes your IAM user")
    print("4. Wait 2-3 minutes after any policy changes for them to take effect")
    print("\n5. The IAM user needs these permissions for OpenSearch Serverless:")
    print("   - aoss:APIAccessAll on the collection resource")
    print("   - Or specific permissions: aoss:CreateIndex, aoss:DeleteIndex, etc.")

if __name__ == "__main__":
    diagnose_opensearch()