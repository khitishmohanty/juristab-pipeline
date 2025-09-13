
import ollama
import base64
import os
from pathlib import Path


DEFAULT_OLLAMA_MODEL = 'deepseek-r1:671b'
OLLAMA_MODEL_NAME = os.getenv('DEEPSEEK_MODEL', DEFAULT_OLLAMA_MODEL)

PROMPT_FILE_PATH = 'prompt.txt'
# Replace 'your_image.png' with the actual path to your image file
DEFAULT_IMAGE_PATH = 'document_image.png'

def read_prompt_from_file(file_path):

    try:
        with open(file_path, 'r') as f:
            prompt_text = f.read().strip()
        return prompt_text
    except FileNotFoundError:
        print(f"Error: Prompt file '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error reading prompt file: {e}")
        return None

def image_to_base64(image_path):

    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        return encoded_string
    except FileNotFoundError:
        print(f"Error: Image file '{image_path}' not found.")
        return None
    except Exception as e:
        print(f"Error encoding image: {e}")
        return None



prompt_text ="""
Perform layout analysis from the image.

1. Identify and label each structural element in the **order it appears** on the page using one of these tags:
   - 'Enum', 'Figure', 'Footnote', 'Header', 'Heading', 'List', 'Paragraph', 'Table', 'Table of Contents', 'Title', 'Subtitle', 'Footer', 'Page number', 'Endnotes', 'Glossary', 'Clause'
2. For **Table of Contents**:
   - Return the entire TOC as a single block.
   - In `content`, return a JSON string representing an array of entries with fields:
     - `item_text`, `page_number`, and optional `sub_items` (for nesting)
3. For **Tables**:
   - In `content`, return either:
     - A JSON array of rows and cells
     - Or a Markdown formatted table
     - or if the content has hyperlink, return the content with the hyperlink information
"""

def call_vlm_client(prompt_text, image_path, model_to_use=OLLAMA_MODEL_NAME):
    
    if not prompt_text:
        print("Error: Prompt text is empty.")
        return None

    base64_image = image_to_base64(image_path)
    if not base64_image:
        return None # Error message already printed by image_to_base64

    print(f"Using Ollama model: {model_to_use}")
    print(f"Prompt: {prompt_text[:100]}...") # Print first 100 chars of prompt
    print(f"Image: {image_path} (encoded)")

    try:
        response = ollama.chat(
            model=model_to_use,
            messages=[
                {
                    "role": "system",
                    "content": prompt_text,
                    "images": [base64_image]
                }
            ]
        )
        print("Response received from Ollama API.")
        #ollama_response_content = response.get('message', {}).get('content')
        ollama_response_content = response['message']['content']
        if ollama_response_content:
            return ollama_response_content
        else:
            print("Error: No content found in the model's response.")
            print(f"Full response: {response}")
            return None
    except Exception as e:
        print(f"An error occurred while calling Ollama API: {e}")
        return None


def call_llm_client(prompt_text, model_to_use=OLLAMA_MODEL_NAME):
    
    if not prompt_text:
        print("Error: Prompt text is empty.")
        return None

    print(f"Using Ollama model: {model_to_use}")
    print(f"Prompt: {prompt_text[:100]}...") # Print first 100 chars of prompt

    try:
        response = ollama.chat(
            model=model_to_use,
            messages=[
                {
                    "role": "system",
                    "content": prompt_text
                }
            ]
        )
        print("Response received from Ollama API.")
        #ollama_response_content = response.get('message', {}).get('content')
        ollama_response_content = response['message']['content']
        if ollama_response_content:
            return ollama_response_content
        else:
            print("Error: No content found in the model's response.")
            print(f"Full response: {response}")
            return None
    except Exception as e:
        print(f"An error occurred while calling Ollama API: {e}")
        return None
    
    
def main():

    print(f"Script will attempt to use Ollama model: {OLLAMA_MODEL_NAME}")
    if OLLAMA_MODEL_NAME == DEFAULT_OLLAMA_MODEL and not os.getenv('DEEPSEEK_MODEL'):
        print(f" (This is the default. Set the 'DEEPSEEK_MODEL' environment variable to use a different model.)")
    
    
    # Read the prompt from the file
    prompt = prompt_text
    if not prompt:
        return

    # 4. Specify the image path
    base_dir = Path(__file__).resolve().parent
    project_root_path = base_dir.parents[1]
  

    print(f"Project root determined as: from pathlib import Path{project_root_path}")

    image_file_path = project_root_path /  "tests" / "assets" / "inputs" / "sample.png"
    # Call the DeepSeek client function
    # The OLLAMA_MODEL_NAME (read from env or default) is used by default by call_deepseek_client
    print("\nAttempting to call Ollama model...")
    model_response = call_vlm_client(prompt, image_file_path)

    if model_response:
        print("\n--- Model Response ---")
        print(model_response)
        print("----------------------")
    else:
        print("\nFailed to get a response from the model.")

if __name__ == '__main__':
    main() 



"""
import ollama


desiremodel = 'deepseek-r1:14b'
prompt = 'can you extract the layout of a document image of 1 page?'

response = ollama.chat(model = desiremodel, messages=[
    {
        "role": "system",
        "content": prompt
    }
])

ollamaResponse=response['message']['content']
print(ollamaResponse)

"""