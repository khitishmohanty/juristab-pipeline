import os
import json
import requests
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LlamaClient:
    """
    A client to interact with a Hugging Face compatible API for Llama models.
    """
    def __init__(self, model_name: str, base_url: str):
        """
        Initializes the LlamaClient.

        Args:
            model_name (str): The name of the model to use.
            base_url (str): The base URL for the API endpoint.
        """
        self.model_name = model_name
        self.api_url = f"{base_url}/chat/completions"
        self.api_key = os.getenv("HF_API_KEY")
        if not self.api_key:
            raise ValueError("HF_API_KEY environment variable not set.")
        
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        logging.info("LlamaClient initialized successfully.")

    def generate_json_from_text(self, prompt: str, text_content: str) -> Optional[str]:
        """
        Sends text content to the Llama model and requests a JSON response.

        Args:
            prompt (str): The instructional prompt for the model.
            text_content (str): The case law text to be analyzed.

        Returns:
            str: The raw string response from the model, expected to be JSON, or None on failure.
        """
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"--- CASE LAW TEXT ---\n\n{text_content}"}
            ],
            "response_format": {"type": "json_object"}
        }

        try:
            logging.info(f"Generating content with model: {self.model_name}")
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=300)
            response.raise_for_status()

            response_data = response.json()
            raw_response = response_data['choices'][0]['message']['content']
            
            logging.info("Successfully received response from Llama API.")
            return raw_response.strip()

        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred while calling the Llama API: {e}")
            return None
        except (KeyError, IndexError) as e:
            logging.error(f"Failed to parse response from Llama API: {e}")
            return None

    @staticmethod
    def is_valid_json(data: str) -> bool:
        """
        Verifies if a string is a valid JSON object.

        Args:
            data (str): The string to validate.

        Returns:
            bool: True if the string is valid JSON, False otherwise.
        """
        if not data:
            return False
        try:
            json.loads(data)
            logging.info("JSON validation successful.")
            return True
        except json.JSONDecodeError:
            logging.error("JSON validation failed.")
            return False