import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

def get_secret(secret_name, region_name):
    """
    Retrieves a secret from AWS Secrets Manager.
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        logging.error(f"Failed to retrieve secret {secret_name}: {e}")
        raise e

    # The secret value is a JSON string or binary
    if 'SecretString' in get_secret_value_response:
        return get_secret_value_response['SecretString']
    else:
        return get_secret_value_response['SecretBinary']