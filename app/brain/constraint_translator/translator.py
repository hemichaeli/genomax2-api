"""
GenoMAX² Constraint Translator: Core Translation Logic
======================================================
Pure, deterministic translation from constraint codes to enforcement fields.

Principle: "Blood does not negotiate."
- Same input always produces same output
- All lists sorted alphabetically for stable hashing
- No external dependencies during translation
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Any
from datetime import datetime
import hashlib
import json

from .mappings import CONSTRAINT_MAPPINGS, get_mapping_version

__version__ = "1.0.0"


@dataclass
class TranslatedConstraints:
    """
    Canonical enforcement object produced by ConstraintTranslator.
    
    All list fields are SORTED for deterministic hashing.
    """
    # Hard blocks: ingredients that MUST NOT be included
    blocked_ingredients: List[str] = field(default_factory=list)
    
    # Category blocks: entire categories to exclude
    blocked_categories: List[str] = field(default_factory=list)
    
    # Target blocks: biological targets to avoid
    blocked_targets: List[str] = field(default_factory=list)
    
    # Caution flags: warnings requiring attention
    caution_flags: List[str] = field(default_factory=list)
    
    # Reason codes: audit trail
    reason_codes: List[str] = field(default_factory=list)
    
    # Recommended ingredients: priority based on bloodwork
    recommended_ingredients: List[str] = field(default_factory=list)
    
    # Metadata
    translator_version: str = __version__
    mapping_version: str = ""
    input_hash: str = ""
    output_hash: str = ""
    translated_at: str = ""
    input_constraint_codes: List[str] = field(default_factory=list)
    unknown_codes: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary with sorted fields."""
        return {
            "blocked_ingredients": self.blocked_ingredients,
            "blocked_categories": self.blocked_categories,
            "blocked_targets": self.blocked_targets,
            "caution_flags": self.caution_flags,
            "reason_codes": self.reason_codes,
            "recommended_ingredients": self.recommended_ingredients,
            "metadata": {
                "translator_version": self.translator_version,
                "mapping_version": self.mapping_version,
                "input_hash": self.input_hash,
                "output_hash": self.output_hash,
                "translated_at": self.translated_at,
                "input_constraint_codes": self.input_constraint_codes,
                "unknown_codes": self.unknown_codes,
            },
            "summary": {
                "total_blocked_ingredients": len(self.blocked_ingredients),
                "total_blocked_categories": len(self.blocked_categories),
                "total_blocked_targets": len(self.blocked_targets),
                "total_caution_flags": len(self.caution_flags),
                "total_reason_codes": len(self.reason_codes),
                "total_recommended": len(self.recommended_ingredients),
                "has_blocks": len(self.blocked_ingredients) > 0 or len(self.blocked_categories) > 0,
                "has_cautions": len(self.caution_flags) > 0,
                "has_recommendations": len(self.recommended_ingredients) > 0,
            }
        }
    
    def is_empty(self) -> bool:
        """Check if no constraints were generated."""
        return (
            len(self.blocked_ingredients) == 0 and
            len(self.blocked_categories) == 0 and
            len(self.blocked_targets) == 0 and
            len(self.caution_flags) == 0 and
            len(self.recommended_ingredients) == 0
        )
    
    def is_ingredient_blocked(self, ingredient: str) -> bool:
        """Check if ingredient is blocked."""
        return ingredient.lower() in [i.lower() for i in self.blocked_ingredients]
    
    def is_category_blocked(self, category: str) -> bool:
        """Check if category is blocked."""
        return category.lower() in [c.lower() for c in self.blocked_categories]
    
    def has_caution(self, flag: str) -> bool:
        """Check if caution flag is present."""
        return flag.lower() in [f.lower() for f in self.caution_flags]


class ConstraintTranslator:
    """
    Pure, deterministic translator for bloodwork constraints.
    
    Usage:
        translator = ConstraintTranslator()
        result = translator.translate(["BLOCK_IRON", "CAUTION_RENAL"])
        print(result.blocked_ingredients)  # ["iron", "iron_bisglycinate", ...]
    """
    
    def __init__(self, custom_mappings: Optional[Dict] = None):
        """
        Initialize translator.
        
        Args:
            custom_mappings: Optional additional mappings to merge
        """
        self.mappings = CONSTRAINT_MAPPINGS.copy()
        if custom_mappings:
            self.mappings.update(custom_mappings)
        self.version = __version__
        self.mapping_version = get_mapping_version()
    
    def _compute_hash(self, data: Any) -> str:
        """Compute deterministic SHA-256 hash."""
        serialized = json.dumps(data, sort_keys=True, separators=(',', ':'))
        return f"sha256:{hashlib.sha256(serialized.encode()).hexdigest()[:16]}"
    
    def translate(
        self,
        constraint_codes: List[str],
        sex: Optional[str] = None,
        context: Optional[Dict] = None
    ) -> TranslatedConstraints:
        """
        Translate constraint codes into enforcement semantics.
        
        This is the CORE FUNCTION. It is PURE and DETERMINISTIC:
        - Same input always produces same output
        - No side effects
        - No external calls
        
        Args:
            constraint_codes: List of constraint codes from Bloodwork Engine
            sex: Optional sex for gender-specific rules (not currently used)
            context: Optional additional context (not currently used)
            
        Returns:
            TranslatedConstraints with all enforcement fields populated
        """
        # Normalize input: uppercase, deduplicate, sort
        codes = sorted(set(code.upper().strip() for code in constraint_codes if code))
        
        # Compute input hash
        input_data = {"constraint_codes": codes, "sex": sex}
        input_hash = self._compute_hash(input_data)
        
        # Initialize collectors
        blocked_ingredients: Set[str] = set()
        blocked_categories: Set[str] = set()
        blocked_targets: Set[str] = set()
        caution_flags: Set[str] = set()
        reason_codes: Set[str] = set()
        recommended_ingredients: Set[str] = set()
        unknown_codes: List[str] = []
        
        # Process each constraint code
        for code in codes:
            if code not in self.mappings:
                unknown_codes.append(code)
                reason_codes.add(f"UNKNOWN_CONSTRAINT_{code}")
                continue
            
            mapping = self.mappings[code]
            
            # Merge all fields
            blocked_ingredients.update(mapping.get("blocked_ingredients", []))
            blocked_categories.update(mapping.get("blocked_categories", []))
            blocked_targets.update(mapping.get("blocked_targets", []))
            caution_flags.update(mapping.get("caution_flags", []))
            reason_codes.update(mapping.get("reason_codes", []))
            recommended_ingredients.update(mapping.get("recommended_ingredients", []))
        
        # IMPORTANT: Remove any recommended that are also blocked
        # "Blood does not negotiate" - blocks always win
        recommended_ingredients -= blocked_ingredients
        
        # Build result with SORTED lists (for determinism)
        result = TranslatedConstraints(
            blocked_ingredients=sorted(blocked_ingredients),
            blocked_categories=sorted(blocked_categories),
            blocked_targets=sorted(blocked_targets),
            caution_flags=sorted(caution_flags),
            reason_codes=sorted(reason_codes),
            recommended_ingredients=sorted(recommended_ingredients),
            translator_version=self.version,
            mapping_version=self.mapping_version,
            input_hash=input_hash,
            translated_at=datetime.utcnow().isoformat() + "Z",
            input_constraint_codes=codes,
            unknown_codes=unknown_codes,
        )
        
        # Compute output hash
        output_data = {
            "blocked_ingredients": result.blocked_ingredients,
            "blocked_categories": result.blocked_categories,
            "blocked_targets": result.blocked_targets,
            "caution_flags": result.caution_flags,
            "reason_codes": result.reason_codes,
            "recommended_ingredients": result.recommended_ingredients,
        }
        result.output_hash = self._compute_hash(output_data)
        
        return result
    
    def get_mapping(self, code: str) -> Optional[Dict]:
        """Get mapping for a specific constraint code."""
        return self.mappings.get(code.upper())
    
    def list_codes(self) -> List[str]:
        """List all known constraint codes."""
        return sorted(self.mappings.keys())
    
    def validate_codes(self, codes: List[str]) -> Dict[str, Any]:
        """Validate constraint codes and return status."""
        valid = []
        invalid = []
        for code in codes:
            if code.upper() in self.mappings:
                valid.append(code.upper())
            else:
                invalid.append(code)
        return {
            "valid": valid,
            "invalid": invalid,
            "all_valid": len(invalid) == 0,
        }


# =========================================================================
# Module-Level Singleton
# =========================================================================

_default_translator: Optional[ConstraintTranslator] = None


def get_translator() -> ConstraintTranslator:
    """Get default translator instance (singleton)."""
    global _default_translator
    if _default_translator is None:
        _default_translator = ConstraintTranslator()
    return _default_translator


def translate(
    constraint_codes: List[str],
    sex: Optional[str] = None,
    context: Optional[Dict] = None
) -> TranslatedConstraints:
    """Convenience function using default translator."""
    return get_translator().translate(constraint_codes, sex, context)


# =========================================================================
# Enforcement Helpers (for Routing/Matching layers)
# =========================================================================

def filter_products_by_constraints(
    products: List[Dict],
    constraints: TranslatedConstraints,
    ingredient_key: str = "ingredient_tags"
) -> List[Dict]:
    """
    Filter products based on translated constraints.
    
    Returns products that are NOT blocked.
    
    Args:
        products: List of product dicts
        constraints: TranslatedConstraints from translate()
        ingredient_key: Key in product dict containing ingredient list
        
    Returns:
        List of allowed products with metadata
    """
    allowed = []
    blocked_set = set(i.lower() for i in constraints.blocked_ingredients)
    
    for product in products:
        ingredients = product.get(ingredient_key, [])
        if isinstance(ingredients, str):
            ingredients = [i.strip().lower() for i in ingredients.split(",")]
        else:
            ingredients = [str(i).lower() for i in ingredients]
        
        # Check for blocked ingredients
        blocked_found = blocked_set.intersection(set(ingredients))
        
        if not blocked_found:
            # Product is allowed
            product_copy = product.copy()
            product_copy["_constraint_status"] = "ALLOWED"
            product_copy["_blocked_ingredients"] = []
            allowed.append(product_copy)
    
    return allowed


def annotate_products_with_constraints(
    products: List[Dict],
    constraints: TranslatedConstraints,
    ingredient_key: str = "ingredient_tags"
) -> List[Dict]:
    """
    Annotate ALL products with constraint status (for UI display).
    
    Args:
        products: List of product dicts
        constraints: TranslatedConstraints from translate()
        ingredient_key: Key in product dict containing ingredient list
        
    Returns:
        List of products with constraint annotations
    """
    blocked_set = set(i.lower() for i in constraints.blocked_ingredients)
    caution_set = set(f.lower() for f in constraints.caution_flags)
    recommended_set = set(i.lower() for i in constraints.recommended_ingredients)
    
    annotated = []
    for product in products:
        ingredients = product.get(ingredient_key, [])
        if isinstance(ingredients, str):
            ingredients = [i.strip().lower() for i in ingredients.split(",")]
        else:
            ingredients = [str(i).lower() for i in ingredients]
        
        ingredients_set = set(ingredients)
        
        # Find blocked ingredients in this product
        blocked_found = list(blocked_set.intersection(ingredients_set))
        
        # Find recommended ingredients in this product
        recommended_found = list(recommended_set.intersection(ingredients_set))
        
        # Determine status
        if blocked_found:
            status = "BLOCKED"
        elif recommended_found:
            status = "RECOMMENDED"
        else:
            status = "NEUTRAL"
        
        # Annotate product
        product_copy = product.copy()
        product_copy["_constraint_status"] = status
        product_copy["_blocked_ingredients"] = blocked_found
        product_copy["_recommended_ingredients"] = recommended_found
        product_copy["_caution_flags"] = list(caution_set) if caution_set else []
        
        annotated.append(product_copy)
    
    return annotated
