import os
import imghdr # Not used in this specific function, but present in the original file context
import json
from openai import OpenAI
from typing import Optional, List, Dict, Any, TypedDict, Literal, Union # Added Union here
from dotenv import load_dotenv

load_dotenv()

# Load config from environment
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

# Pricing: cost per 1 million tokens
OPENAI_INPUT_TOKEN_PRICE = float(os.getenv("OPENAI_INPUT_TOKEN_PRICE_PER_MILLION", "0.00"))
OPENAI_OUTPUT_TOKEN_PRICE = float(os.getenv("OPENAI_OUTPUT_TOKEN_PRICE_PER_MILLION", "0.00"))

# OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)


# Define TypedDicts for more specific type hinting

class FileDetail(TypedDict):
    """Specifies the structure for the 'file' part of a file content block."""
    file_id: Optional[str] # file_id can be None before it's assigned

class FileContentPart(TypedDict):
    """Specifies the structure for a file content block."""
    type: Literal["file"]
    file: FileDetail

class TextContentPart(TypedDict):
    """Specifies the structure for a text content block."""
    type: Literal["text"]
    text: str

# A UserContentItem can be either a FileContentPart or a TextContentPart
UserContentItem = Union[FileContentPart, TextContentPart] # Requires `from typing import Union`

class Message(TypedDict):
    """Specifies the structure for a message object sent to the API."""
    role: Literal["user"] # Assuming only "user" role for this specific function
    content: List[UserContentItem]

class OpenAIUsage(TypedDict): # To represent response.usage if it's not None
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int # Usually present as well

class OpenAIFileCallResponse(TypedDict):
    """Specifies the structure of the dictionary returned by this function."""
    text: str
    input_tokens: int
    output_tokens: int
    cost: float
    model_used: str
    uploaded_file_id: Optional[str]
    
def call_openai_api(prompt: str,
                    image_base64: Optional[str] = None,
                    model: Optional[str] = None,
                    image_path: Optional[str] = None) -> Dict:

    model = model or OPENAI_MODEL

    try:
        # Detect image MIME type if image path is provided
        mime_type = "image/jpeg"
        if image_path and os.path.isfile(image_path):
            ext = imghdr.what(image_path)
            if ext in {"png", "gif", "webp", "jpeg"}:
                mime_type = f"image/{ext}"
            else:
                raise ValueError(f"Unsupported image format detected: {ext}")

        # Construct messages for the chat API
        messages: List[Dict] = [{"role": "user", "content": []}]

        if image_base64:
            messages[0]["content"].append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{mime_type};base64,{image_base64}"
                }
            })

        messages[0]["content"].append({
            "type": "text",
            "text": prompt
        })

        # API call
        response = client.chat.completions.create(
            model=model,
            messages=messages
        )

        # Extract usage data
        text = response.choices[0].message.content
        usage = response.usage
        input_tokens = usage.prompt_tokens
        output_tokens = usage.completion_tokens

        # Calculate cost per million tokens
        cost = (
            input_tokens * OPENAI_INPUT_TOKEN_PRICE +
            output_tokens * OPENAI_OUTPUT_TOKEN_PRICE
        ) / 1_000_000

        return {
            "text": text,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost
        }

    except Exception as e:
        raise RuntimeError(f"Failed to call OpenAI API: {e}")


def call_openai_with_json(json_file_path: str, prompt: str, model: Optional[str] = None) -> Dict:

    try:
        # Load JSON content and format as string
        with open(json_file_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
            json_text = json.dumps(json_data, indent=2)

        # Create full prompt
        combined_prompt = f"{prompt}\n\nHere is the JSON input:\n{json_text}"

        # Use existing function to call OpenAI and calculate cost
        return call_openai_api(prompt=combined_prompt, model=model)

    except Exception as e:
        raise RuntimeError(f"Failed to call OpenAI API with JSON input: {e}")
    
def call_openai_with_pdf(pdf_path: str, prompt: str, model: Optional[str] = None) -> OpenAIFileCallResponse: # Changed return type
    """
    Calls the OpenAI Chat Completions API with a prompt and a PDF file.
    The PDF is uploaded to OpenAI and referenced by its ID.
    The uploaded file is deleted after the API call.
    """
    model_to_use = model or OPENAI_MODEL
    uploaded_file_id: Optional[str] = None

    try:
        # Step 1: Upload the PDF file
        with open(pdf_path, "rb") as pdf_file_obj:
            #print(f"Uploading PDF: {pdf_path}...")
            uploaded_file = client.files.create(file=pdf_file_obj, purpose="user_data") #
            uploaded_file_id = uploaded_file.id #
            #print(f"PDF uploaded successfully. File ID: {uploaded_file_id}")

        # Step 2: Construct messages for the chat API
        user_content: List[UserContentItem] = [ # Changed type hint
            {
                "type": "file",
                "file": { # This inner dict matches FileDetail
                    "file_id": uploaded_file_id
                }
            },
            {
                "type": "text",
                "text": prompt
            }
        ]
        
        messages: List[Message] = [{"role": "user", "content": user_content}] # Changed type hint

        # Step 3: API call
        #print(f"Sending prompt and PDF (ID: {uploaded_file_id}) to model: {model_to_use}...")
        response = client.chat.completions.create(
            model=model_to_use,
            messages=messages, # type: ignore # May need to ignore if OpenAI's SDK types are not perfectly aligned with our TypedDict
            max_tokens=4000
        )

        # Step 4: Extract usage data
        text_response = response.choices[0].message.content if response.choices[0].message.content else ""
        
        # The 'usage' object from the OpenAI API response might be None.
        # It's good practice to access its attributes only if it's not None.
        usage_data = response.usage # This object could be None.
        
        input_tokens = 0
        output_tokens = 0

        if usage_data: # Check if usage_data is not None
            input_tokens = usage_data.prompt_tokens
            output_tokens = usage_data.completion_tokens #
        
        #print(f"Received response. Input tokens: {input_tokens}, Output tokens: {output_tokens}")

        # Calculate cost
        cost = (
            (input_tokens * OPENAI_INPUT_TOKEN_PRICE) +
            (output_tokens * OPENAI_OUTPUT_TOKEN_PRICE)
        ) / 1_000_000

        # Construct the return dictionary according to OpenAIFileCallResponse
        result: OpenAIFileCallResponse = {
            "text": text_response,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost,
            "model_used": model_to_use,
            "uploaded_file_id": uploaded_file_id
        }
        return result

    except Exception as e:
        print(f"An error occurred: {e}")
        raise RuntimeError(f"Failed to call OpenAI API with PDF: {e}")
    
    finally:
        if uploaded_file_id:
            try:
                #print(f"Deleting uploaded file: {uploaded_file_id}...")
                client.files.delete(file_id=uploaded_file_id) #
                #print(f"File {uploaded_file_id} deleted successfully.")
            except Exception as delete_error:
                print(f"Error deleting file {uploaded_file_id}: {delete_error}")