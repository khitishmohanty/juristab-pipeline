from bs4 import BeautifulSoup
from src.juriscontent_generator import JuriscontentGenerator
from utils.gemini_client import GeminiClient
from utils.token_pricing_calculator import TokenPricingCalculator
from typing import Tuple, Optional
from datetime import datetime, timezone
import json
import logging
import re

logger = logging.getLogger(__name__)

class HtmlTransformer:
    """
    Orchestrates the HTML transformation pipeline with anchor tag embedding.
    
    Flow:
    1. Check if miniviewer.html has heading tags
    2a. If YES: Add anchor tags → Generate juriscontent.html
    2b. If NO and genai_extract=True: Use Gemini → miniviewer_genai.html → Add anchor tags → Generate juriscontent.html
    2c. If NO and genai_extract=False: Skip Gemini → Add anchor tags → Generate juriscontent.html
    """
    
    def __init__(self, config: dict):
        """
        Initialize transformer with configuration.
        
        Args:
            config: Application configuration dictionary
        """
        self.config = config
        self.juriscontent_generator = JuriscontentGenerator()
        
        # Check if Gemini extraction is enabled
        heading_config = config['heading_detection']
        self.genai_extract_enabled = heading_config.get('genai_extract', True)  # Default: True
        
        if self.genai_extract_enabled:
            # Initialize Gemini client only if enabled
            model_config = config['models']['gemini']
            
            self.gemini_client = GeminiClient(model_name=model_config['model'])
            
            # Load prompt
            self.prompt = self._load_prompt(heading_config['prompt_path'])
            
            # Initialize pricing calculator
            self.pricing_calculator = TokenPricingCalculator(model_config['pricing'])
            
            logger.info("HtmlTransformer initialized WITH Gemini HTML generation (genai_extract=True)")
        else:
            self.gemini_client = None
            self.prompt = None
            self.pricing_calculator = None
            logger.info("HtmlTransformer initialized WITHOUT Gemini HTML generation (genai_extract=False)")
    
    def _load_prompt(self, prompt_path: str) -> str:
        """Load the heading detection prompt from file."""
        try:
            with open(prompt_path, 'r') as f:
                prompt = f.read()
            logger.info(f"Loaded heading detection prompt from {prompt_path}")
            return prompt
        except Exception as e:
            logger.error(f"Failed to load prompt from {prompt_path}: {e}")
            raise
    
    def _has_headings(self, soup: BeautifulSoup) -> bool:
        """Check if document contains any h1-h6 tags."""
        return bool(soup.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']))
    
    def _count_h1_headings(self, html_content: str) -> int:
        """Count only H1 heading tags in HTML."""
        soup = BeautifulSoup(html_content, 'html.parser')
        return len(soup.find_all('h1'))
    
    def _add_anchor_tags_to_headings(self, html_content: str) -> str:
        """
        Add id attributes (anchor tags) to headings that don't have them.
        
        IMPORTANT: This preserves existing anchor tags and only adds new ones
        where missing. Uses section-based numbering for consistency.
        
        Args:
            html_content: HTML with heading tags
            
        Returns:
            HTML with anchor tags added to headings without disturbing existing ones
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all H1 headings (primary sections)
        h1_headings = soup.find_all('h1')
        
        anchor_count = 0
        section_number = 1
        
        for h1 in h1_headings:
            if not h1.get('id'):
                # Generate section-based anchor ID
                heading_id = f"section_{section_number}"
                h1['id'] = heading_id
                anchor_count += 1
                logger.debug(f"Added anchor: {heading_id} to H1")
            else:
                logger.debug(f"Preserved existing anchor: {h1.get('id')} on H1")
            
            section_number += 1
        
        # Also add anchors to other heading levels (H2-H6) for navigation
        other_headings = soup.find_all(['h2', 'h3', 'h4', 'h5', 'h6'])
        subsection_counter = {}
        
        for heading in other_headings:
            if not heading.get('id'):
                level = heading.name[1]  # Get level number (2, 3, 4, etc.)
                
                # Initialize counter for this level if needed
                if level not in subsection_counter:
                    subsection_counter[level] = 1
                
                # Generate hierarchical ID
                heading_id = f"section-h{level}-{subsection_counter[level]}"
                heading['id'] = heading_id
                subsection_counter[level] += 1
                anchor_count += 1
                logger.debug(f"Added anchor: {heading_id} to {heading.name}")
            else:
                logger.debug(f"Preserved existing anchor: {heading.get('id')} on {heading.name}")
        
        if anchor_count > 0:
            logger.info(f"✓ Added {anchor_count} new anchor tags (preserved existing ones)")
        else:
            logger.info("✓ All headings already have anchor tags (none added)")
        
        return str(soup)
    
    def _create_gemini_response_data(self, html_output: Optional[str],
                                    input_tokens: int, output_tokens: int,
                                    generation_success: bool, 
                                    error: Optional[str] = None) -> dict:
        """
        Create structured response data for saving to S3.
        
        Args:
            html_output: Generated HTML (or None if failed)
            input_tokens: Number of input tokens used
            output_tokens: Number of output tokens used
            generation_success: Whether generation succeeded
            error: Error message if generation failed
            
        Returns:
            Dictionary with complete response metadata
        """
        input_price, output_price = self.pricing_calculator.calculate_cost(
            input_tokens, output_tokens
        )
        
        response_data = {
            "request_timestamp": datetime.now(timezone.utc).isoformat(),
            "model": self.config['models']['gemini']['model'],
            "tokens": {
                "input": input_tokens,
                "output": output_tokens,
                "input_price": input_price,
                "output_price": output_price,
                "total_price": input_price + output_price
            },
            "generation_success": generation_success,
            "output_length": len(html_output) if html_output else 0
        }
        
        if error:
            response_data["error"] = error
        
        return response_data
    
    def transform(self, html_content: str) -> Tuple[str, Optional[str], Optional[dict], Optional[str]]:
        """
        Process HTML with AI-powered or existing heading structure + anchor tags.
        
        Flow:
        1. Check if headings already exist in miniviewer.html
        2a. If YES:
            - Add anchor tags to existing headings
            - Apply juriscontent styling
        2b. If NO and genai_extract=True:
            - Use Gemini to generate HTML with headings → miniviewer_genai.html
            - Add anchor tags to generated headings
            - Apply juriscontent styling
        2c. If NO and genai_extract=False:
            - Skip Gemini entirely
            - Apply juriscontent styling without headings
            - Section extractor will create single section
        
        Returns:
            Tuple of (transformed_html, intermediate_html, token_info, gemini_response_json)
            - transformed_html: The final juriscontent.html
            - intermediate_html: The miniviewer_genai.html (or None if not generated)
            - token_info: Dict with token counts and pricing, or None if headings exist or Gemini disabled
            - gemini_response_json: JSON string of Gemini response for saving to S3
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        token_info = None
        gemini_response_json = None
        processed_html = html_content
        intermediate_html = None  # Track intermediate HTML for saving
        
        # Count H1 headings BEFORE processing
        before_h1_count = self._count_h1_headings(html_content)
        logger.info(f"H1 headings in source HTML (before processing): {before_h1_count}")
        
        if self._has_headings(soup):
            # Path 1: Existing headings present - no AI needed
            logger.info("✓ Semantic headings found. Skipping AI generation.")
            logger.info("→ Adding anchor tags to existing headings...")
            
            # Add anchor tags to existing headings
            processed_html = self._add_anchor_tags_to_headings(html_content)
            
            # Count H1 headings after processing
            after_h1_count = self._count_h1_headings(processed_html)
            
            # Create metadata for this path
            token_info = {
                'input_tokens': 0,
                'output_tokens': 0,
                'input_price': 0.0,
                'output_price': 0.0,
                'generation_success': False,
                'headings_found': after_h1_count,
                'before_processing_heading_count': before_h1_count,
                'after_processing_heading_count': after_h1_count,
                'genai_path_used': False,
                'path': 'existing_headings'
            }
            
        elif not self.genai_extract_enabled:
            # Path 2: No headings AND Gemini disabled - skip AI entirely
            logger.info("✗ No semantic headings found.")
            logger.info("⚠ Gemini extraction DISABLED (genai_extract=False)")
            logger.info("→ Proceeding without headings. Section extraction will create single section.")
            
            # Use original HTML as-is (no heading generation)
            processed_html = html_content
            
            # Create metadata for this path
            token_info = {
                'input_tokens': 0,
                'output_tokens': 0,
                'input_price': 0.0,
                'output_price': 0.0,
                'generation_success': False,
                'headings_found': 0,
                'before_processing_heading_count': before_h1_count,
                'after_processing_heading_count': before_h1_count,
                'genai_path_used': False,
                'path': 'gemini_disabled'
            }
            
        else:
            # Path 3: No headings AND Gemini enabled - use Gemini to generate HTML with headings
            logger.info("✗ No semantic headings found.")
            logger.info("→ Gemini extraction ENABLED (genai_extract=True)")
            logger.info("→ Using Gemini to generate HTML with headings...")
            
            try:
                html_with_headings, input_tokens, output_tokens = self.gemini_client.generate_html_with_headings(
                    self.prompt, html_content
                )
                
                # Validate the output
                if not self.gemini_client.validate_html_output(html_with_headings):
                    logger.warning("⚠ Gemini HTML validation failed. Proceeding without headings.")
                    
                    response_data = self._create_gemini_response_data(
                        html_output=None,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        generation_success=False,
                        error="HTML validation failed - no heading tags found"
                    )
                    gemini_response_json = json.dumps(response_data, indent=2)
                    
                    token_info = {
                        'input_tokens': input_tokens,
                        'output_tokens': output_tokens,
                        'input_price': response_data['tokens']['input_price'],
                        'output_price': response_data['tokens']['output_price'],
                        'generation_success': False,
                        'headings_found': 0,
                        'before_processing_heading_count': before_h1_count,
                        'after_processing_heading_count': before_h1_count,
                        'genai_path_used': True,
                        'path': 'gemini_validation_failed',
                        'error': 'HTML validation failed'
                    }
                    
                    # ✅ FALLBACK: Use original miniviewer.html
                    processed_html = html_content
                    
                else:
                    # Success - use the generated HTML
                    intermediate_html = html_with_headings  # Save for S3 (before anchor tags)
                    
                    # Count H1 headings in output
                    h1_count_generated = self._count_h1_headings(html_with_headings)
                    
                    logger.info(f"✓ Gemini generated HTML with {h1_count_generated} H1 heading tags")
                    
                    # Add anchor tags to generated headings
                    logger.info("→ Adding anchor tags to generated headings...")
                    processed_html = self._add_anchor_tags_to_headings(html_with_headings)
                    
                    # Count H1 headings after anchor tag addition
                    after_h1_count = self._count_h1_headings(processed_html)
                    
                    response_data = self._create_gemini_response_data(
                        html_output=html_with_headings,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        generation_success=True
                    )
                    gemini_response_json = json.dumps(response_data, indent=2)
                    
                    token_info = {
                        'input_tokens': input_tokens,
                        'output_tokens': output_tokens,
                        'input_price': response_data['tokens']['input_price'],
                        'output_price': response_data['tokens']['output_price'],
                        'generation_success': True,
                        'headings_found': after_h1_count,
                        'before_processing_heading_count': before_h1_count,
                        'after_processing_heading_count': after_h1_count,
                        'genai_path_used': True,
                        'path': 'gemini_success'
                    }
                    
                    total_cost = response_data['tokens']['total_price']
                    logger.info(f"✓ HTML generation complete. "
                              f"Tokens: {input_tokens} in / {output_tokens} out. "
                              f"Cost: ${total_cost:.6f}")
                    
            except Exception as e:
                logger.error(f"⚠ Gemini API error during HTML generation: {e}")
                logger.warning("Proceeding without headings.")
                
                response_data = self._create_gemini_response_data(
                    html_output=None,
                    input_tokens=0,
                    output_tokens=0,
                    generation_success=False,
                    error=str(e)
                )
                gemini_response_json = json.dumps(response_data, indent=2)
                
                token_info = {
                    'input_tokens': 0,
                    'output_tokens': 0,
                    'input_price': 0.0,
                    'output_price': 0.0,
                    'generation_success': False,
                    'headings_found': 0,
                    'before_processing_heading_count': before_h1_count,
                    'after_processing_heading_count': before_h1_count,
                    'genai_path_used': True,
                    'path': 'gemini_api_error',
                    'error': str(e)
                }
                
                # ✅ FALLBACK: Use original miniviewer.html
                processed_html = html_content
        
        # Apply standard juriscontent generation (collapsible sections, navigation, styling)
        logger.info("→ Applying juriscontent styling (collapsible sections + navigation)...")
        try:
            final_html = self.juriscontent_generator.generate(processed_html)
            logger.info("✓ Juriscontent generation complete")
        except Exception as gen_error:
            logger.error(f"Error in juriscontent generation: {gen_error}")
            # Fallback to processed HTML with basic styling
            final_html = self._apply_basic_styling(processed_html)
        
        return final_html, intermediate_html, token_info, gemini_response_json
    
    def _apply_basic_styling(self, html_content: str) -> str:
        """
        Fallback method to apply minimal styling if juriscontent generation fails.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        if not soup.find('head'):
            head = soup.new_tag('head')
            style = soup.new_tag('style')
            style.string = """
                body { 
                    font-family: Arial, sans-serif; 
                    font-size: 14px; 
                    line-height: 1.6; 
                    padding: 2rem; 
                    max-width: 1200px;
                    margin: 0 auto;
                }
                p { margin-bottom: 1em; }
                h1, h2, h3, h4, h5, h6 { 
                    margin-top: 1.5em; 
                    margin-bottom: 0.5em; 
                    font-weight: 600;
                }
            """
            head.append(style)
            
            if soup.html:
                soup.html.insert(0, head)
            else:
                soup.insert(0, head)
        
        return str(soup)