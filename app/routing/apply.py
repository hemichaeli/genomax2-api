"""
Routing Layer Application Logic (Issue #6)

Pure Safety Elimination - applies biological constraints to valid SKUs.

This module implements the core routing function that:
1. Checks each SKU against blocked_ingredients
2. Checks each SKU against blocked_categories
3. Propagates caution_flags (without blocking)
4. Tracks requirements fulfillment
5. Produces deterministic, auditable output

PRINCIPLE: Blood does not negotiate.

Version: routing_layer_v1
"""

from typing import List, Tuple, Set
from datetime import datetime

from .models import (
    SkuInput,
    RoutingConstraints,
    RoutingResult,
    AllowedSKU,
    BlockedSKU,
    RoutingAudit,
)


def apply_routing_constraints(
    valid_skus: List[SkuInput],
    constraints: RoutingConstraints
) -> RoutingResult:
    """
    Apply routing constraints to valid SKUs.
    
    This is the core routing function. It is:
    - PURE: no side effects
    - ELIMINATIVE: only removes, never adds
    - DUMB: no intelligence, just rule application
    - DETERMINISTIC: same input -> same output
    
    Args:
        valid_skus: SKUs that passed catalog validation (from Issue #5)
        constraints: Routing constraints from Brain Orchestrate
        
    Returns:
        RoutingResult with allowed/blocked SKUs and full audit
    """
    allowed_skus: List[AllowedSKU] = []
    blocked_skus: List[BlockedSKU] = []
    
    # Track statistics
    blocked_by_blood = 0
    blocked_by_metadata = 0
    blocked_by_category = 0
    caution_count = 0
    
    # Track requirements
    requirements_set = set(constraints.requirements)
    fulfilled_requirements: Set[str] = set()
    
    # Normalize constraints to lowercase for matching
    blocked_ingredients_lower = {ing.lower() for ing in constraints.blocked_ingredients}
    blocked_categories_lower = {cat.lower() for cat in constraints.blocked_categories}
    caution_flags_lower = {flag.lower() for flag in constraints.caution_flags}
    
    for sku in valid_skus:
        # Normalize SKU tags for matching
        sku_ingredients_lower = {tag.lower() for tag in sku.ingredient_tags}
        sku_categories_lower = {tag.lower() for tag in sku.category_tags}
        sku_risk_tags_lower = {tag.lower() for tag in sku.risk_tags}
        
        # Check for metadata blocks (from Issue #5)
        metadata_block_reasons = []
        if "blocked_ingredient" in sku_risk_tags_lower:
            metadata_block_reasons.append("BLOCKED_BY_EVIDENCE")
        if "auto_blocked" in sku_risk_tags_lower:
            metadata_block_reasons.append("AUTO_BLOCKED_METADATA")
        
        # Check for blood-based ingredient blocks
        blood_block_ingredients = blocked_ingredients_lower & sku_ingredients_lower
        blood_block_reasons = []
        if blood_block_ingredients:
            for ing in blood_block_ingredients:
                blood_block_reasons.append(f"BLOCK_INGREDIENT_{ing.upper()}")
        
        # Check for category blocks
        category_block = blocked_categories_lower & sku_categories_lower
        category_block_reasons = []
        if category_block:
            for cat in category_block:
                category_block_reasons.append(f"BLOCK_CATEGORY_{cat.upper()}")
        
        # Aggregate all block reasons
        all_block_reasons = metadata_block_reasons + blood_block_reasons + category_block_reasons
        
        if all_block_reasons:
            # SKU is BLOCKED
            # Determine primary blocking source
            if metadata_block_reasons:
                blocked_by = "metadata"
                blocked_by_metadata += 1
            elif blood_block_reasons:
                blocked_by = "blood"
                blocked_by_blood += 1
            else:
                blocked_by = "category"
                blocked_by_category += 1
            
            blocked_skus.append(BlockedSKU(
                sku_id=sku.sku_id,
                product_name=sku.product_name,
                reason_codes=sorted(all_block_reasons),
                blocked_by=blocked_by,
                blocked_ingredients=sorted(list(blood_block_ingredients)),
                blocked_categories=sorted(list(category_block)),
            ))
        else:
            # SKU is ALLOWED
            # Check for caution flags
            sku_caution_flags = caution_flags_lower & sku_ingredients_lower
            caution_reasons = []
            if sku_caution_flags:
                for flag in sku_caution_flags:
                    caution_reasons.append(f"CAUTION_{flag.upper()}")
                caution_count += 1
            
            # Check which requirements this SKU fulfills
            fulfills = []
            for req in requirements_set:
                if req.lower() in sku_ingredients_lower:
                    fulfills.append(req)
                    fulfilled_requirements.add(req)
            
            allowed_skus.append(AllowedSKU(
                sku_id=sku.sku_id,
                product_name=sku.product_name,
                ingredient_tags=sku.ingredient_tags,
                category_tags=sku.category_tags,
                gender_line=sku.gender_line,
                evidence_tier=sku.evidence_tier,
                caution_flags=sorted(list(sku_caution_flags)),
                caution_reasons=sorted(caution_reasons),
                fulfills_requirements=sorted(fulfills),
            ))
    
    # Sort for determinism
    allowed_skus = sorted(allowed_skus, key=lambda s: s.sku_id)
    blocked_skus = sorted(blocked_skus, key=lambda s: s.sku_id)
    
    # Determine which constraints were applied
    constraints_applied = []
    if constraints.blocked_ingredients:
        constraints_applied.append("blocked_ingredients")
    if constraints.blocked_categories:
        constraints_applied.append("blocked_categories")
    if constraints.caution_flags:
        constraints_applied.append("caution_flags")
    if constraints.requirements:
        constraints_applied.append("requirements")
    
    # Determine missing requirements
    missing_requirements = sorted(list(requirements_set - fulfilled_requirements))
    
    # Build audit
    audit = RoutingAudit(
        total_input_skus=len(valid_skus),
        allowed_count=len(allowed_skus),
        blocked_count=len(blocked_skus),
        blocked_by_blood=blocked_by_blood,
        blocked_by_metadata=blocked_by_metadata,
        blocked_by_category=blocked_by_category,
        constraints_applied=sorted(constraints_applied),
        requirements_in_catalog=sorted(list(fulfilled_requirements)),
        requirements_missing=missing_requirements,
        caution_count=caution_count,
        processed_at=datetime.utcnow().isoformat(),
    )
    
    # Compute hash
    routing_hash = RoutingResult.compute_hash(allowed_skus, blocked_skus)
    
    return RoutingResult(
        allowed_skus=allowed_skus,
        blocked_skus=blocked_skus,
        routing_hash=routing_hash,
        audit=audit,
    )


def filter_by_gender(
    allowed_skus: List[AllowedSKU],
    target_gender: str
) -> List[AllowedSKU]:
    """
    Filter allowed SKUs by gender target.
    
    This is a UTILITY function for downstream use (Issue #7).
    NOT part of core routing - provided for convenience.
    
    Args:
        allowed_skus: SKUs that passed routing
        target_gender: "male", "female", or "unisex"
        
    Returns:
        Filtered list of SKUs matching gender target
    """
    target_lower = target_gender.lower()
    
    if target_lower == "male":
        valid_lines = {"maximo2", "unisex", None}
    elif target_lower == "female":
        valid_lines = {"maxima2", "unisex", None}
    else:
        # Unisex or unknown - return all
        return allowed_skus
    
    return [
        sku for sku in allowed_skus
        if (sku.gender_line or "").lower() in valid_lines or sku.gender_line is None
    ]


def get_requirements_coverage(
    allowed_skus: List[AllowedSKU],
    requirements: List[str]
) -> dict:
    """
    Analyze how well allowed SKUs cover requirements.
    
    Utility function for downstream analysis.
    
    Args:
        allowed_skus: SKUs that passed routing
        requirements: List of required ingredient tags
        
    Returns:
        Coverage report with fulfilled/missing/coverage_pct
    """
    requirements_set = set(req.lower() for req in requirements)
    fulfilled = set()
    
    for sku in allowed_skus:
        for req in requirements_set:
            if req in [t.lower() for t in sku.ingredient_tags]:
                fulfilled.add(req)
    
    missing = requirements_set - fulfilled
    coverage_pct = len(fulfilled) / len(requirements_set) * 100 if requirements_set else 100.0
    
    return {
        "total_requirements": len(requirements_set),
        "fulfilled": sorted(list(fulfilled)),
        "missing": sorted(list(missing)),
        "coverage_pct": round(coverage_pct, 2),
    }
