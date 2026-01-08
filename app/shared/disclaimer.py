"""
GenoMAX² DSHEA Disclaimer Utilities
Provides deterministic rendering helpers for FDA disclaimer display.

RULES (LOCKED):
1. Singular: "This statement has not been evaluated..."  -> claim_count == 1
2. Plural: "These statements have not been evaluated..." -> claim_count > 1

NOTE: claim_count is provided by page/label composer. 
This module only handles prefix selection and text assembly.
"""

# Standard DSHEA disclaimer templates
DSHEA_DISCLAIMER_SINGULAR = (
    "This statement has not been evaluated by the Food and Drug Administration. "
    "This product is not intended to diagnose, treat, cure, or prevent any disease."
)

DSHEA_DISCLAIMER_PLURAL = (
    "These statements have not been evaluated by the Food and Drug Administration. "
    "This product is not intended to diagnose, treat, cure, or prevent any disease."
)


def choose_disclaimer_prefix(claim_count: int) -> str:
    """
    Return the appropriate DSHEA disclaimer text based on claim count.
    
    Args:
        claim_count: Number of health claims on the page/label.
                     Must be >= 1 for supplement products.
    
    Returns:
        Full DSHEA disclaimer text with correct singular/plural prefix.
    
    Rules:
        - claim_count == 1  -> "This statement..."
        - claim_count > 1   -> "These statements..."
        - claim_count <= 0  -> Returns plural (defensive default)
    
    Example:
        >>> choose_disclaimer_prefix(1)
        "This statement has not been evaluated..."
        
        >>> choose_disclaimer_prefix(3)
        "These statements have not been evaluated..."
    """
    if claim_count == 1:
        return DSHEA_DISCLAIMER_SINGULAR
    return DSHEA_DISCLAIMER_PLURAL


def render_disclaimer_block(
    disclaimer_text: str,
    disclaimer_symbol: str = "*",
    claim_count: int = 1,
    applicability: str = "SUPPLEMENT"
) -> str | None:
    """
    Render the complete disclaimer block for display.
    
    Args:
        disclaimer_text: The disclaimer text from DB (stored without symbol).
        disclaimer_symbol: The symbol to prepend (default "*").
        claim_count: Number of claims to determine singular/plural.
        applicability: "SUPPLEMENT" or "TOPICAL".
    
    Returns:
        Formatted disclaimer string, or None for TOPICAL products.
    
    Behavior:
        - SUPPLEMENT: Returns "{symbol} {text}" with correct singular/plural
        - TOPICAL: Returns None (no DSHEA disclaimer required)
    
    Example:
        >>> render_disclaimer_block("...", "*", 2, "SUPPLEMENT")
        "* These statements have not been evaluated..."
        
        >>> render_disclaimer_block("...", "*", 1, "TOPICAL")
        None
    """
    if applicability == "TOPICAL":
        return None
    
    # Use the claim-count-aware text
    text = choose_disclaimer_prefix(claim_count)
    
    return f"{disclaimer_symbol} {text}"


def should_require_disclaimer(applicability: str) -> bool:
    """
    Determine if a module requires fda_disclaimer based on applicability.
    
    Used by QA Design Gate to conditionally enforce disclaimer presence.
    
    Args:
        applicability: "SUPPLEMENT" or "TOPICAL"
    
    Returns:
        True if fda_disclaimer is required, False otherwise.
    
    Rules:
        - SUPPLEMENT -> True (DSHEA required)
        - TOPICAL    -> False (cosmetics exempt)
    """
    return applicability == "SUPPLEMENT"


# Admin guidance for setting TOPICAL on specific SKUs:
# 
# Example SQL to mark cosmetic products:
#
#   UPDATE os_modules_v3_1
#   SET disclaimer_applicability = 'TOPICAL',
#       updated_at = NOW()
#   WHERE shopify_handle IN (
#       'kojic-acid-turmeric-soap-maxima',
#       'kojic-acid-turmeric-soap-maximo'
#   );
#
# After update, verify:
#   SELECT shopify_handle, disclaimer_applicability
#   FROM os_modules_v3_1
#   WHERE disclaimer_applicability = 'TOPICAL';
