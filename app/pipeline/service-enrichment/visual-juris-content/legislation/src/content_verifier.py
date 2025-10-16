import re
import logging
from difflib import SequenceMatcher
from typing import Tuple

logger = logging.getLogger(__name__)

class ContentVerifier:
    """
    Verifies that concatenated section content matches the original source text
    using fuzzy matching algorithms.
    
    All line breaks are replaced with single spaces before comparison to focus
    on content rather than formatting differences.
    """
    
    def __init__(self, pass_threshold: float = 0.85):
        """
        Initialize the content verifier.
        
        Args:
            pass_threshold (float): Minimum similarity score (0.0-1.0) to mark as 'pass'
        """
        if not 0.0 <= pass_threshold <= 1.0:
            raise ValueError("pass_threshold must be between 0.0 and 1.0")
        
        self.pass_threshold = pass_threshold
        logger.info(f"ContentVerifier initialized with pass_threshold={pass_threshold}")
        logger.info("Normalization: All line breaks will be replaced with single spaces")
    
    def strip_barnet_jade_header(self, text: str) -> str:
        """
        Strip the BarNet Jade header from the beginning of the text if present.
        
        Args:
            text (str): Text that may contain the header
            
        Returns:
            str: Text with header removed
        """
        # Pattern to match "Content extract - BarNet Jade" at the start
        # This pattern is case-insensitive and handles variations in spacing
        pattern = r'^Content\s+extract\s*-\s*BarNet\s+Jade\s*'
        
        stripped_text = re.sub(pattern, '', text, count=1, flags=re.IGNORECASE)
        
        if stripped_text != text:
            logger.debug("Stripped 'Content extract - BarNet Jade' header from text")
        
        return stripped_text
    
    def normalize_text(self, text: str) -> str:
        """
        Normalize text for comparison by:
        - Stripping BarNet Jade header if present
        - Converting to lowercase
        - Replacing all line breaks with single spaces
        - Removing extra whitespace
        - Removing special characters (keeping alphanumeric and basic punctuation)
        
        Args:
            text (str): Text to normalize
            
        Returns:
            str: Normalized text
        """
        original_length = len(text)
        
        # First strip BarNet Jade header
        text = self.strip_barnet_jade_header(text)
        
        # Convert to lowercase
        text = text.lower()
        
        # Replace HTML entities
        text = text.replace('&nbsp;', ' ').replace('\u00a0', ' ')
        
        # Count line breaks before removal (for logging)
        line_break_count = text.count('\n') + text.count('\r')
        
        # CRITICAL: Replace ALL line breaks (newlines, carriage returns) with single space
        # This ensures comparison is based on content, not formatting
        text = text.replace('\r\n', ' ')  # Windows line endings
        text = text.replace('\n', ' ')    # Unix/Mac line endings
        text = text.replace('\r', ' ')    # Old Mac line endings
        
        # Replace multiple spaces/tabs with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        normalized_length = len(text)
        
        logger.debug(
            f"Text normalization: {original_length} chars → {normalized_length} chars "
            f"({line_break_count} line breaks replaced with spaces)"
        )
        
        return text
    
    def calculate_similarity_score(self, text1: str, text2: str) -> float:
        """
        Calculate similarity score between two texts using SequenceMatcher.
        
        This uses Python's difflib.SequenceMatcher which implements a 
        Ratcliff/Obershelp algorithm for finding the longest contiguous 
        matching subsequence.
        
        All line breaks are replaced with single spaces before comparison,
        so the comparison focuses on content rather than formatting.
        
        Args:
            text1 (str): First text to compare
            text2 (str): Second text to compare
            
        Returns:
            float: Similarity ratio between 0.0 (completely different) and 1.0 (identical)
        """
        # Normalize both texts (includes replacing all line breaks with spaces)
        normalized_text1 = self.normalize_text(text1)
        normalized_text2 = self.normalize_text(text2)
        
        # Handle empty strings
        if not normalized_text1 or not normalized_text2:
            logger.warning("One or both texts are empty after normalization")
            return 0.0
        
        # Calculate similarity using SequenceMatcher
        matcher = SequenceMatcher(None, normalized_text1, normalized_text2)
        similarity = matcher.ratio()
        
        logger.debug(f"Text lengths after normalization: {len(normalized_text1)} vs {len(normalized_text2)}")
        logger.debug(f"Similarity score: {similarity:.4f}")
        
        return similarity
    
    def verify_content(self, original_text: str, concatenated_sections: str) -> Tuple[float, str]:
        """
        Verify that concatenated section content matches the original text.
        
        All line breaks are replaced with single spaces in both texts before comparison.
        
        Args:
            original_text (str): Original source text (miniviewer.txt)
            concatenated_sections (str): Concatenated content from all section files
            
        Returns:
            Tuple of (similarity_score, status)
            - similarity_score (float): Score between 0.0 and 1.0
            - status (str): 'pass' if score >= threshold, 'failed' otherwise
        """
        try:
            logger.info("Normalizing texts: replacing all line breaks with single spaces...")
            
            # Calculate similarity score
            similarity_score = self.calculate_similarity_score(original_text, concatenated_sections)
            
            # Determine pass/fail status
            status = 'pass' if similarity_score >= self.pass_threshold else 'failed'
            
            # Log results
            original_len = len(original_text)
            sections_len = len(concatenated_sections)
            length_diff = abs(original_len - sections_len)
            length_diff_pct = (length_diff / original_len * 100) if original_len > 0 else 0
            
            # Count line breaks in original texts (before normalization)
            original_line_breaks = original_text.count('\n') + original_text.count('\r')
            sections_line_breaks = concatenated_sections.count('\n') + concatenated_sections.count('\r')
            
            logger.info(f"Content Verification Results:")
            logger.info(f"  Original text length: {original_len:,} chars ({original_line_breaks} line breaks)")
            logger.info(f"  Concatenated sections length: {sections_len:,} chars ({sections_line_breaks} line breaks)")
            logger.info(f"  Length difference (before normalization): {length_diff:,} chars ({length_diff_pct:.1f}%)")
            logger.info(f"  Similarity score (after normalization): {similarity_score:.4f}")
            logger.info(f"  Threshold: {self.pass_threshold:.4f}")
            logger.info(f"  Status: {status.upper()}")
            
            if status == 'failed':
                logger.warning(
                    f"Content verification FAILED: score {similarity_score:.4f} "
                    f"is below threshold {self.pass_threshold:.4f}"
                )
            else:
                logger.info(f"✅ Content verification PASSED")
            
            return similarity_score, status
            
        except Exception as e:
            logger.error(f"Error during content verification: {e}", exc_info=True)
            # Return low score and failed status on error
            return 0.0, 'failed'
    
    def concatenate_section_contents(self, section_contents: list) -> str:
        """
        Concatenate multiple section contents into a single string.
        
        Args:
            section_contents (list): List of section content strings
            
        Returns:
            str: Concatenated content with sections separated by double newlines
        """
        if not section_contents:
            logger.warning("No section contents provided for concatenation")
            return ""
        
        # Join sections with double newline separator
        concatenated = '\n\n'.join(content.strip() for content in section_contents if content.strip())
        
        logger.debug(f"Concatenated {len(section_contents)} sections into {len(concatenated)} chars")
        
        return concatenated
    
    def get_detailed_comparison(self, text1: str, text2: str, context_lines: int = 3) -> str:
        """
        Get a detailed comparison showing differences between texts.
        Useful for debugging when verification fails.
        
        Note: This comparison uses the NORMALIZED texts (with line breaks replaced).
        
        Args:
            text1 (str): First text
            text2 (str): Second text
            context_lines (int): Number of context lines to show around differences
            
        Returns:
            str: Human-readable comparison report
        """
        from difflib import unified_diff
        
        # Strip BarNet Jade header and normalize both texts
        text1_normalized = self.normalize_text(text1)
        text2_normalized = self.normalize_text(text2)
        
        # For better readability in diff, split on sentences instead of lines
        # Since we replaced all line breaks, we split on periods followed by spaces
        text1_sentences = re.split(r'(?<=\.) ', text1_normalized)
        text2_sentences = re.split(r'(?<=\.) ', text2_normalized)
        
        # Add line endings back for unified_diff format
        text1_lines = [s + '\n' for s in text1_sentences]
        text2_lines = [s + '\n' for s in text2_sentences]
        
        # Generate unified diff
        diff = unified_diff(
            text1_lines, 
            text2_lines, 
            fromfile='original (normalized)', 
            tofile='concatenated (normalized)',
            n=context_lines
        )
        
        return ''.join(diff)