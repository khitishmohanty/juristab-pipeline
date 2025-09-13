import requests

# The existing endpoint for performing a full search (This remains unchanged)
API_ENDPOINT = "https://discoveryengine.googleapis.com/v1alpha/projects/534929033323/locations/global/collections/default_collection/engines/juristab-legal-store-searc_1754363845002/servingConfigs/default_search:search"

# --- CORRECTED: The autocomplete endpoint now points to your specific Data Store ID ---
AUTOCOMPLETE_API_ENDPOINT = "https://discoveryengine.googleapis.com/v1alpha/projects/534929033323/locations/global/collections/default_collection/dataStores/legal-store-connector_1754362975406_gcs_store:completeQuery"

def perform_search(query: str, access_token: str):
    """
    Performs a search against the Vertex AI Search API.

    Args:
        query (str): The search term from the user.
        access_token (str): The GCP access token for authentication.

    Returns:
        dict: The JSON response from the API, or an error dictionary.
    """
    if not query:
        return {"error": "Query cannot be empty."}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # This is the JSON payload for the search request.
    # We are requesting snippets and a max of 20 results.
    data = {
        "query": query,
        "pageSize": 20,
        "queryExpansionSpec": {"condition": "AUTO"},
        "spellCorrectionSpec": {"mode": "AUTO"},
        "contentSearchSpec": {
            "summarySpec": {
                "summaryResultCount": 5,
                "includeCitations": True
            },
            "snippetSpec": {
                "returnSnippet": True
            },
            "extractiveContentSpec": {
                "maxExtractiveAnswerCount": 3
            }
        }
    }

    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=data, timeout=20)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        # Try to get more specific error info from the response body
        error_details = response.json() if response.content else {}
        return {"error": f"HTTP error occurred: {http_err}", "details": error_details}
    except requests.exceptions.RequestException as req_err:
        return {"error": f"A request error occurred: {req_err}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {e}"}


def get_suggestions(query: str, access_token: str):
    """
    Fetches autocomplete suggestions from the Vertex AI Search API.
    (This function uses the corrected AUTOCOMPLETE_API_ENDPOINT)
    """
    # --- DEBUG: Print the function call ---
    print(f"\n--- DEBUG: Calling get_suggestions with query: '{query}' ---")
    
    if not query:
        return {"error": "Query cannot be empty."}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    data = {
        "query": query,
        "includeTailSuggestions": True,
        "query_model": "page"
    }

    try:
        response = requests.post(AUTOCOMPLETE_API_ENDPOINT, headers=headers, json=data, timeout=5)
        # --- DEBUG: Print the raw API response ---
        print(f"--- DEBUG: API Response Status: {response.status_code} ---")
        print(f"--- DEBUG: API Response Body: {response.text} ---")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as req_err:
        return {"error": f"A request error occurred during autocomplete: {req_err}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred during autocomplete: {e}"}