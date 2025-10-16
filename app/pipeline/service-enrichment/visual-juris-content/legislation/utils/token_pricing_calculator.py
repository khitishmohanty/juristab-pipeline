import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class TokenPricingCalculator:
    """
    Calculates pricing for token usage based on model pricing configuration.
    """
    
    def __init__(self, pricing_config: Dict[str, float]):
        """
        Initialize calculator with pricing config.
        
        Args:
            pricing_config: Dict with 'input_per_million' and 'output_per_million'
        """
        self.input_price_per_million = pricing_config['input_per_million']
        self.output_price_per_million = pricing_config['output_per_million']
    
    def calculate_cost(self, input_tokens: int, output_tokens: int) -> Tuple[float, float]:
        """
        Calculate cost for given token usage.
        
        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            
        Returns:
            Tuple of (input_cost, output_cost) in dollars
        """
        input_cost = (input_tokens / 1_000_000) * self.input_price_per_million
        output_cost = (output_tokens / 1_000_000) * self.output_price_per_million
        
        logger.debug(f"Token pricing: {input_tokens} input tokens = ${input_cost:.6f}, "
                    f"{output_tokens} output tokens = ${output_cost:.6f}")
        
        return input_cost, output_cost
    
    def get_total_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Get total cost for token usage."""
        input_cost, output_cost = self.calculate_cost(input_tokens, output_tokens)
        return input_cost + output_cost