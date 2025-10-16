import os
import google.generativeai as genai
from typing import Tuple
import logging

logger = logging.getLogger(__name__)

class GeminiClient:
    """Client to interact with Google Gemini API for HTML generation."""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set.")
        
        genai.configure(api_key=self.api_key)
        logger.info(f"GeminiClient initialized with model: {model_name}")

    def generate_html_with_headings(self, prompt: str, html_content: str) -> Tuple[str, int, int]:
        """
        Sends HTML content to Gemini API to generate HTML with embedded headings.
        
        Args:
            prompt: Instruction prompt for heading detection and HTML generation
            html_content: The HTML content to analyze
            
        Returns:
            Tuple of (html_with_headings, input_tokens, output_tokens)
        """
        try:
            logger.info(f"Generating HTML with headings using model: {self.model_name}")
            model = genai.GenerativeModel(self.model_name)
            
            full_prompt = f"{prompt}\n\n--- HTML CONTENT TO PROCESS ---\n\n{html_content}"
            
            # Count input tokens
            input_token_count = model.count_tokens(full_prompt).total_tokens
            logger.info(f"Input tokens: {input_token_count}")
            
            response = model.generate_content(full_prompt)
            
            # Get output token count
            output_token_count = response.usage_metadata.candidates_token_count
            logger.info(f"Output tokens: {output_token_count}")
            
            raw_response = response.text
            
            # Clean markdown code fences if present
            html_output = raw_response.strip()
            if html_output.startswith("```html"):
                html_output = html_output[7:]
            elif html_output.startswith("```"):
                html_output = html_output[3:]
            
            if html_output.endswith("```"):
                html_output = html_output[:-3]
            
            html_output = html_output.strip()
            
            logger.info("Successfully received HTML with embedded headings from Gemini")
            logger.info(f"Output HTML length: {len(html_output)} characters")
            
            return html_output, input_token_count, output_token_count
            
        except Exception as e:
            logger.error(f"Error calling Gemini API for HTML generation: {e}")
            raise

    def validate_html_output(self, html_content: str) -> bool:
        """
        Validate that the output is proper HTML with heading tags.
        
        Args:
            html_content: The HTML to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not html_content or len(html_content) < 100:
            logger.error("HTML output is too short or empty")
            return False
        
        # Check for basic HTML structure
        has_html_tag = '<html' in html_content.lower() or '<body' in html_content.lower()
        has_heading_tag = any(f'<h{i}' in html_content.lower() for i in range(1, 7))
        
        if not has_heading_tag:
            logger.warning("No heading tags found in Gemini output")
            return False
        
        logger.info(f"HTML validation passed - contains heading tags")
        return True