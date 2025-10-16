from bs4 import BeautifulSoup
from src.juriscontent_generator import JuriscontentGenerator
from src.heading_hierarchy_processor import HeadingHierarchyProcessor
from src.headless_html_processor import HeadlessHtmlProcessor
from utils.gemini_client import GeminiClient
from utils.token_pricing_calculator import TokenPricingCalculator
from typing import Tuple, Optional
from datetime import datetime, timezone
import json
import logging
import re
import yaml

logger = logging.getLogger(__name__)

class HtmlTransformer:
    """
    Orchestrates the HTML transformation pipeline with multi-tier heading detection.
    
    Flow (3-tier logic with character threshold):
    1. Check if miniviewer.html has heading tags
       → YES: Add anchor tags → Generate juriscontent.html (PATH: 'original')
    2. If NO headings and genai_extract=True and tokens <= max_input_tokens:
       → Use Gemini → miniviewer_genai.html → Add anchor tags → Generate juriscontent.html (PATH: 'genai')
    3. If tokens > max_input_tokens OR genai_extract=False:
       → Check character count from legislation_metadata
       → If char_count < min_char_threshold: Skip rules (PATH: 'no rules applied')
       → If char_count >= min_char_threshold: Apply rule-based detection (PATH: 'rulebased')
    4. If all fails:
       → Apply styling only → Generate juriscontent.html (PATH: 'no rules applied')
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
        self.genai_extract_enabled = heading_config.get('genai_extract', True)
        self.max_input_tokens = heading_config.get('max_input_tokens', 100000)
        
        # Initialize rule-based processors
        rule_config = config.get('rule_based_heading_detection', {})
        self.rule_based_enabled = rule_config.get('enabled', True)
        self.min_char_threshold = rule_config.get('min_char_threshold', 5000)  # NEW
        
        if self.rule_based_enabled:
            try:
                hierarchy_rules_path = rule_config.get('heading_hierarchy_rules_path', 
                                                       'config/heading_hierarchy_rules.yaml')
                headless_rules_path = rule_config.get('headless_rules_path', 
                                                      'config/headless_rules.yaml')
                
                self.hierarchy_processor = HeadingHierarchyProcessor(hierarchy_rules_path)
                
                # Load headless rules
                with open(headless_rules_path, 'r') as f:
                    headless_rules = yaml.safe_load(f)
                self.headless_processor = HeadlessHtmlProcessor(headless_rules)
                
                logger.info("Rule-based heading detection initialized")
                logger.info(f"  - Hierarchy rules: {hierarchy_rules_path}")
                logger.info(f"  - Headless rules: {headless_rules_path}")
                logger.info(f"  - Min character threshold: {self.min_char_threshold:,}")  # NEW
            except Exception as e:
                logger.error(f"Failed to initialize rule-based processors: {e}")
                self.rule_based_enabled = False
        
        if self.genai_extract_enabled:
            # Initialize Gemini client only if enabled
            model_config = config['models']['gemini']
            
            self.gemini_client = GeminiClient(model_name=model_config['model'])
            
            # Load prompt
            self.prompt = self._load_prompt(heading_config['prompt_path'])
            
            # Initialize pricing calculator
            self.pricing_calculator = TokenPricingCalculator(model_config['pricing'])
            
            logger.info("HtmlTransformer initialized WITH Gemini HTML generation")
            logger.info(f"  - Max input tokens: {self.max_input_tokens:,}")
        else:
            self.gemini_client = None
            self.prompt = None
            self.pricing_calculator = None
            logger.info("HtmlTransformer initialized WITHOUT Gemini HTML generation")
        
        logger.info("="*70)
        logger.info("HEADING DETECTION STRATEGY:")
        logger.info("  1. Original headings found → Use as-is")
        if self.genai_extract_enabled:
            logger.info(f"  2. No headings + tokens ≤ {self.max_input_tokens:,} → Gemini AI")
        logger.info(f"  3. No headings + (tokens > limit OR Gemini disabled):")
        logger.info(f"     - If char_count < {self.min_char_threshold:,} → Skip rule-based")
        logger.info(f"     - If char_count ≥ {self.min_char_threshold:,} → Rule-based detection")
        logger.info("  4. All methods fail → No heading structure")
        logger.info("="*70)
    
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
    
    def _estimate_token_count(self, html_content: str) -> int:
        """
        Estimate token count for HTML content.
        Rough estimation: 1 token ≈ 4 characters for English text.
        This is conservative; actual tokens may be less.
        """
        # Simple character-based estimation
        char_count = len(html_content)
        estimated_tokens = char_count // 4
        logger.debug(f"Token estimation: {char_count} chars → ~{estimated_tokens:,} tokens")
        return estimated_tokens
    
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
    
    def _apply_rule_based_heading_detection(self, html_content: str) -> Tuple[Optional[str], int]:
        """
        Apply rule-based heading detection using hierarchy and headless processors.
        
        Returns:
            Tuple of (processed_html, heading_count)
        """
        try:
            logger.info("→ Applying rule-based heading detection...")
            
            # Step 1: Apply headless HTML processing (style-based inference)
            logger.debug("  Step 1: Processing headless HTML (style-based)")
            html_with_inferred = self.headless_processor.process(html_content)
            
            # Step 2: Apply heading hierarchy rules (pattern-based)
            logger.debug("  Step 2: Applying heading hierarchy rules (pattern-based)")
            html_with_hierarchy = self.hierarchy_processor.process_document(html_with_inferred)
            
            # Count headings after processing
            heading_count = self._count_h1_headings(html_with_hierarchy)
            
            if heading_count > 0:
                logger.info(f"✓ Rule-based detection successful: {heading_count} H1 headings created")
                return html_with_hierarchy, heading_count
            else:
                logger.warning("⚠ Rule-based detection produced no H1 headings")
                return None, 0
                
        except Exception as e:
            logger.error(f"Error in rule-based heading detection: {e}", exc_info=True)
            return None, 0
    
    def _create_response_data(self, html_output: Optional[str],
                             input_tokens: int, output_tokens: int,
                             generation_success: bool,
                             structuring_path: str,
                             error: Optional[str] = None) -> dict:
        """
        Create structured response data for saving to S3.
        
        Args:
            html_output: Generated HTML (or None if failed)
            input_tokens: Number of input tokens used
            output_tokens: Number of output tokens used
            generation_success: Whether generation succeeded
            structuring_path: Method used ('original', 'genai', 'rulebased', 'no rules applied')
            error: Error message if generation failed
            
        Returns:
            Dictionary with complete response metadata
        """
        # Calculate pricing only if Gemini was used
        if self.pricing_calculator and input_tokens > 0:
            input_price, output_price = self.pricing_calculator.calculate_cost(
                input_tokens, output_tokens
            )
        else:
            input_price = 0.0
            output_price = 0.0
        
        response_data = {
            "request_timestamp": datetime.now(timezone.utc).isoformat(),
            "structuring_path": structuring_path,
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
        
        if structuring_path == 'genai':
            response_data["model"] = self.config['models']['gemini']['model']
        
        if error:
            response_data["error"] = error
        
        return response_data
    
    def transform(self, html_content: str, char_count: Optional[int] = None) -> Tuple[str, Optional[str], Optional[dict], Optional[str]]:
        """
        Process HTML with multi-tier heading detection + anchor tags.
        
        NEW: Character count threshold check for rule-based path
        
        3-Tier Flow:
        1. Check if headings already exist
           → YES: Add anchor tags + juriscontent styling (PATH: 'original')
        2. If NO headings:
           a. Check token count and genai_extract setting
              → tokens ≤ max AND genai_extract=True: Use Gemini (PATH: 'genai')
              → tokens > max OR genai_extract=False: Check character threshold
                 → char_count < min_threshold: Skip rules (PATH: 'no rules applied')
                 → char_count >= min_threshold: Use rule-based (PATH: 'rulebased')
        3. If all methods fail or produce no headings:
           → Apply styling only (PATH: 'no rules applied')
        
        Args:
            html_content: HTML content to transform
            char_count: Optional character count from legislation_metadata.count_char
        
        Returns:
            Tuple of (transformed_html, intermediate_html, token_info, response_json)
            - transformed_html: The final juriscontent.html
            - intermediate_html: The intermediate HTML (genai or rulebased)
            - token_info: Dict with token counts, pricing, and structuring path
            - response_json: JSON string of processing response for S3
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        token_info = None
        response_json = None
        processed_html = html_content
        intermediate_html = None
        structuring_path = 'not started'
        
        # Count H1 headings BEFORE processing
        before_h1_count = self._count_h1_headings(html_content)
        logger.info(f"H1 headings in source HTML (before processing): {before_h1_count}")
        
        # Log character count if provided
        if char_count is not None:
            logger.info(f"Character count from metadata: {char_count:,}")
        
        # ==================== TIER 1: ORIGINAL HEADINGS ====================
        if self._has_headings(soup):
            logger.info("✓ Semantic headings found. Using original structure.")
            logger.info("→ Adding anchor tags to existing headings...")
            
            processed_html = self._add_anchor_tags_to_headings(html_content)
            after_h1_count = self._count_h1_headings(processed_html)
            
            structuring_path = 'original'
            token_info = {
                'input_tokens': 0,
                'output_tokens': 0,
                'input_price': 0.0,
                'output_price': 0.0,
                'generation_success': True,
                'headings_found': after_h1_count,
                'before_processing_heading_count': before_h1_count,
                'after_processing_heading_count': after_h1_count,
                'structuring_path': structuring_path,
                'path': 'existing_headings'
            }
            
            response_data = self._create_response_data(
                html_output=processed_html,
                input_tokens=0,
                output_tokens=0,
                generation_success=True,
                structuring_path=structuring_path
            )
            response_json = json.dumps(response_data, indent=2)
        
        # ==================== NO HEADINGS - CHECK TOKEN COUNT ====================
        else:
            logger.info("✗ No semantic headings found.")
            
            # Estimate token count
            estimated_tokens = self._estimate_token_count(html_content)
            logger.info(f"Estimated input tokens: {estimated_tokens:,}")
            
            # Decide which path to take based on tokens and configuration
            use_gemini = (self.genai_extract_enabled and 
                         estimated_tokens <= self.max_input_tokens)
            
            # ==================== TIER 2: GEMINI AI ====================
            if use_gemini:
                logger.info(f"→ Tokens ({estimated_tokens:,}) ≤ limit ({self.max_input_tokens:,})")
                logger.info("→ Using Gemini AI for heading detection...")
                
                try:
                    html_with_headings, input_tokens, output_tokens = self.gemini_client.generate_html_with_headings(
                        self.prompt, html_content
                    )
                    
                    # Validate the output
                    if not self.gemini_client.validate_html_output(html_with_headings):
                        logger.warning("⚠ Gemini HTML validation failed. Falling back to rule-based...")
                        
                        structuring_path = 'genai'  # Attempted but failed
                        response_data = self._create_response_data(
                            html_output=None,
                            input_tokens=input_tokens,
                            output_tokens=output_tokens,
                            generation_success=False,
                            structuring_path=structuring_path,
                            error="HTML validation failed - falling back to rule-based"
                        )
                        response_json = json.dumps(response_data, indent=2)
                        
                        # Fall through to rule-based
                        use_gemini = False
                        
                    else:
                        # Gemini success
                        intermediate_html = html_with_headings
                        h1_count_generated = self._count_h1_headings(html_with_headings)
                        
                        logger.info(f"✓ Gemini generated HTML with {h1_count_generated} H1 headings")
                        logger.info("→ Adding anchor tags to generated headings...")
                        processed_html = self._add_anchor_tags_to_headings(html_with_headings)
                        
                        after_h1_count = self._count_h1_headings(processed_html)
                        structuring_path = 'genai'
                        
                        response_data = self._create_response_data(
                            html_output=html_with_headings,
                            input_tokens=input_tokens,
                            output_tokens=output_tokens,
                            generation_success=True,
                            structuring_path=structuring_path
                        )
                        response_json = json.dumps(response_data, indent=2)
                        
                        token_info = {
                            'input_tokens': input_tokens,
                            'output_tokens': output_tokens,
                            'input_price': response_data['tokens']['input_price'],
                            'output_price': response_data['tokens']['output_price'],
                            'generation_success': True,
                            'headings_found': after_h1_count,
                            'before_processing_heading_count': before_h1_count,
                            'after_processing_heading_count': after_h1_count,
                            'structuring_path': structuring_path,
                            'path': 'gemini_success'
                        }
                        
                        total_cost = response_data['tokens']['total_price']
                        logger.info(f"✓ Gemini processing complete. "
                                  f"Tokens: {input_tokens} in / {output_tokens} out. "
                                  f"Cost: ${total_cost:.6f}")
                        
                except Exception as e:
                    logger.error(f"⚠ Gemini API error: {e}")
                    logger.info("→ Falling back to rule-based detection...")
                    
                    structuring_path = 'genai'  # Attempted but failed
                    response_data = self._create_response_data(
                        html_output=None,
                        input_tokens=0,
                        output_tokens=0,
                        generation_success=False,
                        structuring_path=structuring_path,
                        error=str(e)
                    )
                    response_json = json.dumps(response_data, indent=2)
                    
                    # Fall through to rule-based
                    use_gemini = False
            
            # ==================== TIER 3: CHARACTER THRESHOLD CHECK ====================
            if not use_gemini:
                if not use_gemini and self.genai_extract_enabled:
                    logger.info(f"→ Tokens ({estimated_tokens:,}) > limit ({self.max_input_tokens:,}) OR Gemini failed")
                else:
                    logger.info("→ Gemini disabled (genai_extract=False)")
                
                # NEW: Check character count threshold
                if char_count is not None:
                    if char_count < self.min_char_threshold:
                        logger.info(f"→ Character count ({char_count:,}) < threshold ({self.min_char_threshold:,})")
                        logger.info("→ Skipping rule-based detection for short document")
                        
                        processed_html = html_content
                        structuring_path = 'no rules applied'
                        
                        response_data = self._create_response_data(
                            html_output=None,
                            input_tokens=0,
                            output_tokens=0,
                            generation_success=False,
                            structuring_path=structuring_path,
                            error=f"Document too short ({char_count:,} < {self.min_char_threshold:,})"
                        )
                        response_json = json.dumps(response_data, indent=2)
                        
                        token_info = {
                            'input_tokens': 0,
                            'output_tokens': 0,
                            'input_price': 0.0,
                            'output_price': 0.0,
                            'generation_success': False,
                            'headings_found': 0,
                            'before_processing_heading_count': before_h1_count,
                            'after_processing_heading_count': before_h1_count,
                            'structuring_path': structuring_path,
                            'path': 'char_threshold_not_met'
                        }
                        
                        # Skip to final styling
                        use_gemini = None  # Mark to skip rule-based section
                    else:
                        logger.info(f"→ Character count ({char_count:,}) ≥ threshold ({self.min_char_threshold:,})")
                        logger.info("→ Proceeding with rule-based detection")
                else:
                    logger.warning("→ Character count not provided - proceeding with rule-based detection")
                
                # ==================== TIER 3B: RULE-BASED (if threshold met) ====================
                if use_gemini is False:  # Not None (which means skip)
                    if self.rule_based_enabled:
                        logger.info("→ Using rule-based heading detection...")
                        
                        rule_based_html, heading_count = self._apply_rule_based_heading_detection(html_content)
                        
                        if rule_based_html and heading_count > 0:
                            # Rule-based success
                            intermediate_html = rule_based_html
                            logger.info("→ Adding anchor tags to rule-based headings...")
                            processed_html = self._add_anchor_tags_to_headings(rule_based_html)
                            
                            after_h1_count = self._count_h1_headings(processed_html)
                            structuring_path = 'rulebased'
                            
                            response_data = self._create_response_data(
                                html_output=rule_based_html,
                                input_tokens=0,
                                output_tokens=0,
                                generation_success=True,
                                structuring_path=structuring_path
                            )
                            response_json = json.dumps(response_data, indent=2)
                            
                            token_info = {
                                'input_tokens': 0,
                                'output_tokens': 0,
                                'input_price': 0.0,
                                'output_price': 0.0,
                                'generation_success': True,
                                'headings_found': after_h1_count,
                                'before_processing_heading_count': before_h1_count,
                                'after_processing_heading_count': after_h1_count,
                                'structuring_path': structuring_path,
                                'path': 'rulebased_success'
                            }
                            
                            logger.info("✓ Rule-based heading detection complete")
                        else:
                            # Rule-based failed
                            logger.warning("⚠ Rule-based detection produced no headings")
                            logger.info("→ Proceeding with no heading structure")
                            
                            processed_html = html_content
                            structuring_path = 'no rules applied'
                            
                            response_data = self._create_response_data(
                                html_output=None,
                                input_tokens=0,
                                output_tokens=0,
                                generation_success=False,
                                structuring_path=structuring_path,
                                error="Rule-based detection produced no headings"
                            )
                            response_json = json.dumps(response_data, indent=2)
                            
                            token_info = {
                                'input_tokens': 0,
                                'output_tokens': 0,
                                'input_price': 0.0,
                                'output_price': 0.0,
                                'generation_success': False,
                                'headings_found': 0,
                                'before_processing_heading_count': before_h1_count,
                                'after_processing_heading_count': before_h1_count,
                                'structuring_path': structuring_path,
                                'path': 'no_rules_applied'
                            }
                    else:
                        # Rule-based disabled
                        logger.warning("⚠ Rule-based detection disabled in configuration")
                        logger.info("→ Proceeding with no heading structure")
                        
                        processed_html = html_content
                        structuring_path = 'no rules applied'
                        
                        response_data = self._create_response_data(
                            html_output=None,
                            input_tokens=0,
                            output_tokens=0,
                            generation_success=False,
                            structuring_path=structuring_path,
                            error="Rule-based detection disabled"
                        )
                        response_json = json.dumps(response_data, indent=2)
                        
                        token_info = {
                            'input_tokens': 0,
                            'output_tokens': 0,
                            'input_price': 0.0,
                            'output_price': 0.0,
                            'generation_success': False,
                            'headings_found': 0,
                            'before_processing_heading_count': before_h1_count,
                            'after_processing_heading_count': before_h1_count,
                            'structuring_path': structuring_path,
                            'path': 'rule_based_disabled'
                        }
        
        # ==================== APPLY JURISCONTENT STYLING ====================
        logger.info("→ Applying juriscontent styling (collapsible sections + navigation)...")
        try:
            final_html = self.juriscontent_generator.generate(processed_html)
            logger.info(f"✓ Juriscontent generation complete (path: {structuring_path})")
        except Exception as gen_error:
            logger.error(f"Error in juriscontent generation: {gen_error}")
            final_html = self._apply_basic_styling(processed_html)
        
        return final_html, intermediate_html, token_info, response_json
    
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