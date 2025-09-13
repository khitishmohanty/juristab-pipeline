import os
import json
# --- FIX IS HERE ---
# The service_account module is part of google.oauth2, not google.auth
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# The scope required for the Discovery Engine API
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

def get_gcp_token():
    """
    Authenticates with Google Cloud using a service account key from an environment variable
    populated by AWS Secrets Manager or a .env file.

    The service account key JSON must be stored in an environment variable named
    'GOOGLE_APPLICATION_CREDENTIALS_JSON'.

    Returns:
        str: A Google Cloud access token.
    """
    # Get the service account key JSON string from the environment variable
    credentials_json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not credentials_json_str:
        raise ValueError("The GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set.")

    try:
        credentials_info = json.loads(credentials_json_str)
        # --- FIX IS HERE ---
        # Use the correctly imported 'service_account' module
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES
        )
    except json.JSONDecodeError:
        raise ValueError("Could not decode the service account JSON. Check the secret value in AWS Secrets Manager or your .env file.")
    except Exception as e:
        raise RuntimeError(f"Could not create credentials from service account info: {e}")

    # Refresh the token if it's expired
    if not credentials.valid:
        credentials.refresh(Request())

    return credentials.token
