"""
GenoMAX² Constraint Translator
Translates Bloodwork Engine constraints into routing/matching enforcement

Version: 1.0.0

This module is PURE and DETERMINISTIC:
- No diagnosis, no dosing, no recommendation logic
- Only translates constraint codes into canonical enforcement fields
- Sorting is always alphabetical for stable hashing

Principle: "Blood does not negotiate."
Bloodwork constraints cannot be removed or overridden by downstream layers.
"""

from typing import List, Optional, Dict, Any
from .translator import (
    ConstraintTranslator,
    TranslatedConstraints,
    get_translator,
    translate,
    filter_products_by_constraints,
    annotate_products_with_constraints,
)
from .mappings import CONSTRAINT_MAPPINGS, get_mapping_version

__version__ = "1.0.0"


def translate_constraints(
    constraint_codes: List[str],
    sex: Optional[str] = None,
    additional_context: Optional[Dict[str, Any]] = None
) -> TranslatedConstraints:
    """
    Convenience function to translate constraints using default translator.
    
    This is the primary API for constraint translation.
    
    Args:
        constraint_codes: List of constraint codes from Bloodwork Engine
        sex: Optional sex for gender-specific rules
        additional_context: Optional context for custom logic
        
    Returns:
        TranslatedConstraints with all enforcement fields populated
    """
    # Map additional_context to context parameter expected by translate()
    return translate(constraint_codes, sex, context=additional_context)


def merge_constraints(
    bloodwork_constraints: TranslatedConstraints,
    other_constraints: dict = None
) -> TranslatedConstraints:
    """
    Merge bloodwork constraints with other sources.
    
    IMPORTANT: Bloodwork blocks CANNOT be removed or overridden.
    This enforces "Blood does not negotiate" principle.
    
    Args:
        bloodwork_constraints: Constraints from bloodwork translation
        other_constraints: Optional dict with additional constraints
        
    Returns:
        Merged TranslatedConstraints (bloodwork always takes precedence)
    """
    if not other_constraints:
        return bloodwork_constraints
    
    from datetime import datetime
    
    # Start with bloodwork constraints
    merged = TranslatedConstraints(
        blocked_ingredients=list(bloodwork_constraints.blocked_ingredients),
        blocked_categories=list(bloodwork_constraints.blocked_categories),
        blocked_targets=list(bloodwork_constraints.blocked_targets),
        caution_flags=list(bloodwork_constraints.caution_flags),
        reason_codes=list(bloodwork_constraints.reason_codes),
        recommended_ingredients=list(bloodwork_constraints.recommended_ingredients),
        translator_version=bloodwork_constraints.translator_version,
        input_hash=bloodwork_constraints.input_hash,
    )
    
    # Add (but never remove) from other constraints
    if "blocked_ingredients" in other_constraints:
        for ing in other_constraints["blocked_ingredients"]:
            if ing not in merged.blocked_ingredients:
                merged.blocked_ingredients.append(ing)
    
    if "caution_flags" in other_constraints:
        for flag in other_constraints["caution_flags"]:
            if flag not in merged.caution_flags:
                merged.caution_flags.append(flag)
    
    if "reason_codes" in other_constraints:
        for code in other_constraints["reason_codes"]:
            if code not in merged.reason_codes:
                merged.reason_codes.append(code)
    
    # Re-sort for determinism
    merged.blocked_ingredients = sorted(merged.blocked_ingredients)
    merged.blocked_categories = sorted(merged.blocked_categories)
    merged.blocked_targets = sorted(merged.blocked_targets)
    merged.caution_flags = sorted(merged.caution_flags)
    merged.reason_codes = sorted(merged.reason_codes)
    merged.recommended_ingredients = sorted(merged.recommended_ingredients)
    
    # Update timestamp
    merged.translated_at = datetime.utcnow().isoformat() + "Z"
    
    return merged


__all__ = [
    "ConstraintTranslator",
    "TranslatedConstraints",
    "CONSTRAINT_MAPPINGS",
    "get_mapping_version",
    "get_translator",
    "translate",
    "translate_constraints",
    "merge_constraints",
    "filter_products_by_constraints",
    "annotate_products_with_constraints",
    "__version__",
]
