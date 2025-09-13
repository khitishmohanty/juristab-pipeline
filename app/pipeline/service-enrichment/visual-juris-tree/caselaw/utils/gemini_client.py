import os
import json
import google.generativeai as genai
from typing import Tuple

class GeminiClient:
    """
    A client to interact with the Google Gemini API.
    """
    def __init__(self, model_name: str):
        """
        Initializes the Gemini client and configures the API key.

        Args:
            model_name (str): The name of the Gemini model to use (e.g., 'gemini-1.5-flash').
        """
        self.model_name = model_name
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set.")
        
        genai.configure(api_key=self.api_key)
        print("GeminiClient initialized successfully.")

    def generate_json_from_text(self, prompt: str, text_content: str) -> Tuple[str, int, int]:
        """
        Sends text content to the Gemini API and requests a JSON response.

        Args:
            prompt (str): The instructional prompt for the model.
            text_content (str): The case law text to be analyzed.

        Returns:
            A tuple containing:
            - str: The raw string response from the model, expected to be JSON.
            - int: The number of input tokens used.
            - int: The number of output tokens generated.
        """
        try:
            print(f"Generating content with model: {self.model_name}")
            model = genai.GenerativeModel(self.model_name)
            
            full_prompt = f"{prompt}\n\n--- CASE LAW TEXT ---\n\n{text_content}"
            
            # Count input tokens before sending
            input_token_count = model.count_tokens(full_prompt).total_tokens
            
            response = model.generate_content(full_prompt)
            
            # Get output token count from response metadata
            output_token_count = response.usage_metadata.candidates_token_count
            
            raw_response = response.text
            if raw_response.strip().startswith("```json"):
                clean_response = raw_response.strip()[7:-3].strip()
            else:
                clean_response = raw_response.strip()

            print("Successfully received response from Gemini API.")
            return clean_response, input_token_count, output_token_count
            
        except Exception as e:
            print(f"An error occurred while calling the Gemini API: {e}")
            raise

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
            print("JSON validation successful.")
            return True
        except json.JSONDecodeError:
            print("JSON validation failed.")
            return False