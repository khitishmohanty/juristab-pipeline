import os
import requests
from typing import Optional, Dict, Any, List, Union
from dotenv import load_dotenv
import json


load_dotenv()

# Load environment variables
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-2.5-flash-preview-04-17")
GEMINI_BASE_URL = os.getenv("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta")

# Token pricing from environment variables (price per 1M tokens)
GEMINI_INPUT_PRICE_PER_MILLION = float(os.getenv("GEMINI_INPUT_PRICE_PER_MILLION", "0.25"))  # Example: $0.25 per 1M tokens
GEMINI_OUTPUT_PRICE_PER_MILLION = float(os.getenv("GEMINI_OUTPUT_PRICE_PER_MILLION", "0.50"))  # Example: $0.50 per 1M tokens

# Convert to per-token price
GEMINI_INPUT_TOKEN_PRICE = GEMINI_INPUT_PRICE_PER_MILLION / 1_000_000
GEMINI_OUTPUT_TOKEN_PRICE = GEMINI_OUTPUT_PRICE_PER_MILLION / 1_000_000

def _make_gemini_request(
    payload: Dict[str, Any],
    api_key: Optional[str] = None,
    model: Optional[str] = None
) -> Dict[str, Any]:
    """
    Internal helper function to make a request to the Gemini API and handle common response processing.
    """
    api_key_to_use = api_key or GEMINI_API_KEY
    model_to_use = model or GEMINI_MODEL
    
    if not api_key_to_use:
        raise ValueError("❌ GEMINI_API_KEY is not set. Please set it in your .env file or pass it directly.")

    endpoint = f"{GEMINI_BASE_URL}/{model_to_use}:generateContent?key={api_key_to_use}"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()

        text = ""
        if result.get("candidates") and result["candidates"][0].get("content") and result["candidates"][0]["content"].get("parts"):
            # Handle cases where the response might not be 'text' (e.g. function call)
            # For now, we assume the first part is the primary text response.
            # If 'text' is not in the first part, it could be an error or different response type.
            first_part = result["candidates"][0]["content"]["parts"][0]
            if "text" in first_part:
                text = first_part["text"]
            # If the model returns a function call, it won't have 'text' in the same way.
            # elif "functionCall" in first_part:
            #     text = json.dumps(first_part["functionCall"]) # Example: serialize function call
            else:
                print(f"⚠️ Warning: First part of Gemini response does not contain 'text'. Part: {first_part}")
                # Attempt to serialize the whole parts array if no direct text found
                all_parts_content = [part.get("text", str(part)) for part in result["candidates"][0]["content"]["parts"]]
                text = "\n".join(all_parts_content) if all_parts_content else ""


        usage_metadata = result.get("usageMetadata", {})
        input_tokens = usage_metadata.get("promptTokenCount", 0)
        output_tokens = usage_metadata.get("candidatesTokenCount", 0)
        
        cost = (GEMINI_INPUT_TOKEN_PRICE * input_tokens) + (GEMINI_OUTPUT_TOKEN_PRICE * output_tokens)

        return {
            "text": text,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "cost_usd": cost,
            "raw_response": result
        }

    except requests.exceptions.HTTPError as http_err:
        error_content = "No error content in response."
        try:
            error_content = response.json()
        except json.JSONDecodeError:
            error_content = response.text
        print(f"❌ HTTP error occurred: {http_err} - {response.status_code}")
        print(f"Error details: {error_content}")
        raise RuntimeError(f"❌ Gemini API HTTP error: {http_err} - {error_content}") from http_err
    except requests.exceptions.RequestException as req_err:
        print(f"❌ Request error occurred: {req_err}")
        raise RuntimeError(f"❌ Gemini API request failed: {req_err}") from req_err
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        raise RuntimeError(f"❌ Unexpected error processing Gemini response: {e}") from e

def call_gemini_api(
    image_base64: str,
    prompt_parts: list,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    mime_type: str = "image/jpeg"
) -> Dict[str, Any]:
    """
    Calls Gemini API with image and prompt, returns structured response including text, tokens, and cost.
    """
    api_key = api_key or GEMINI_API_KEY
    model = model or GEMINI_MODEL
    endpoint = f"{GEMINI_BASE_URL}/{model}:generateContent?key={api_key}"

    headers = {"Content-Type": "application/json"}

    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "inlineData": {
                            "mimeType": mime_type,
                            "data": image_base64
                        }
                    }
                ] + prompt_parts
            }
        ]
    }

    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()

        # Extract main response text
        text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")

        # Extract token usage metadata (if available)
        usage = result.get("usageMetadata", {})
        input_tokens = usage.get("promptTokenCount", 0)
        output_tokens = usage.get("candidatesTokenCount", 0)

        # Calculate estimated cost
        cost = (
            GEMINI_INPUT_TOKEN_PRICE * input_tokens +
            GEMINI_OUTPUT_TOKEN_PRICE * output_tokens
        )

        return {
            "text": text,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost
        }

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"❌ Failed to call Gemini API: {e}")
    except Exception as e:
        raise RuntimeError(f"❌ Unexpected response structure from Gemini: {e}")


def call_gemini_with_pdf(
    pdf_base64: str,
    enrichment_prompt_dict: dict,
    api_key: Optional[str] = None,
    model: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calls Gemini API with a PDF file and enrichment prompt (as a dictionary).
    """
    api_key = api_key or GEMINI_API_KEY
    model = model or GEMINI_MODEL
    endpoint = f"{GEMINI_BASE_URL}/{model}:generateContent?key={api_key}"

    prompt_details = enrichment_prompt_dict.get("prompt_details", {})
    task_description = prompt_details.get("task_description", "")
    output_format_instructions = prompt_details.get("output_format_instructions", {})

    # Construct prompt parts
    prompt_parts = [
        {"text": task_description},
        {"text": f"Strictly follow the output format:\n{json.dumps(output_format_instructions, indent=2)}"}
    ]

    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "inlineData": {
                            "mimeType": "application/pdf",
                            "data": pdf_base64
                        }
                    }
                ] + prompt_parts
            }
        ]
    }

    try:
        response = requests.post(endpoint, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()

        # Extract main response text
        text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")

        # Extract token usage
        usage = result.get("usageMetadata", {})
        input_tokens = usage.get("promptTokenCount", 0)
        output_tokens = usage.get("candidatesTokenCount", 0)

        # Calculate cost
        cost = GEMINI_INPUT_TOKEN_PRICE * input_tokens + GEMINI_OUTPUT_TOKEN_PRICE * output_tokens

        return {
            "text": text,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost
        }

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"❌ Failed to call Gemini API: {e}")
    except Exception as e:
        raise RuntimeError(f"❌ Unexpected response structure from Gemini: {e}")
    
    
def call_gemini_multimodal_content(
    media_items: Union[Dict[str, str], List[Dict[str, str]]],
    prompt_text: str,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    generation_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Calls Gemini API with a flexible set of media items (images or PDFs) and a text prompt.

    Args:
        media_items (Union[Dict[str, str], List[Dict[str, str]]]): 
            A single media item or a list of media items.
            Each item must be a dictionary with "data" (base64 string) 
            and "mimeType" (e.g., "image/jpeg", "application/pdf").
            Example for single: {"data": "...", "mimeType": "image/png"}
            Example for multiple: [{"data": "...", "mimeType": "application/pdf"}, {"data": "...", "mimeType": "image/jpeg"}]
        prompt_text (str): The main text prompt for the task.
        api_key (Optional[str]): Gemini API key. Defaults to environment variable.
        model (Optional[str]): Gemini model name. Defaults to environment variable.
        generation_config (Optional[Dict[str, Any]]): Configuration for generation,
                                                     e.g., {"responseMimeType": "application/json"}.

    Returns:
        Dict[str, Any]: A dictionary containing the API response ('text', 'input_tokens', etc.).
    """
    if not media_items:
        raise ValueError("❌ media_items cannot be empty.")
    if not prompt_text:
        raise ValueError("❌ prompt_text cannot be empty.")

    parts_list: List[Dict[str, Any]] = []

    # Normalize media_items to always be a list
    if isinstance(media_items, dict):
        items_to_process = [media_items]
    elif isinstance(media_items, list):
        items_to_process = media_items
    else:
        raise TypeError("❌ media_items must be a dictionary or a list of dictionaries.")

    # Add each media item to the parts list
    for item in items_to_process:
        if not isinstance(item, dict) or "data" not in item or "mimeType" not in item:
            raise ValueError("❌ Each media item must be a dictionary with 'data' and 'mimeType' keys.")
        parts_list.append({
            "inlineData": {
                "mimeType": item["mimeType"],
                "data": item["data"]
            }
        })
    
    # Add the main text prompt as the last part
    parts_list.append({"text": prompt_text})

    payload: Dict[str, Any] = {"contents": [{"parts": parts_list}]}

    if generation_config:
        payload["generationConfig"] = generation_config
    # else: # Defaulting to JSON can be done here if most calls expect it
        # payload["generationConfig"] = {"responseMimeType": "application/json"}

    return _make_gemini_request(payload, api_key, model)