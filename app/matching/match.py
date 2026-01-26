"""
Matching Layer Core Logic (Issue #7)

Intent → SKU Matching (Protocol Assembly)

This module implements the core matching function that:
1. Filters SKUs by gender target (MAXimo² / MAXima²)
2. Filters SKUs through catalog wiring (Issue #15) - ONLY purchasable SKUs
3. Matches intents to SKUs via ingredient overlap
4. Fulfills requirements from bloodwork deficiencies
5. Propagates caution warnings
6. Produces deterministic, auditable output

PRINCIPLE: Blood does not negotiate. But once safe, we optimize for outcomes.

Version: matching_layer_v2 (catalog-aware)
"""

from typing import List, Dict, Set, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass, field
import logging

from .models import (
    MatchingInput,
    MatchingResult,
    ProtocolItem,
    UnmatchedIntent,
    MatchingAudit,
    AllowedSKUInput,
    IntentInput,
    UserContext,
)

# Import catalog wiring (Issue #15)
try:
    from app.catalog.wiring import get_catalog, CatalogWiringError
    CATALOG_WIRING_AVAILABLE = True
except ImportError:
    CATALOG_WIRING_AVAILABLE = False
    CatalogWiringError = Exception  # Fallback

logger = logging.getLogger(__name__)


@dataclass
class MatchCandidate:
    """Internal tracking of a potential match."""
    sku: AllowedSKUInput
    intents: List[IntentInput] = field(default_factory=list)
    matched_ingredients: Set[str] = field(default_factory=set)
    is_requirement: bool = False
    requirement_ingredients: Set[str] = field(default_factory=set)


def filter_by_catalog(
    skus: List[AllowedSKUInput],
    require_catalog: bool = True
) -> Tuple[List[AllowedSKUInput], Dict]:
    """
    Filter SKUs through catalog wiring to ensure purchasability.
    
    CRITICAL (Issue #15): Only recommend products that can actually be sold.
    
    Args:
        skus: SKUs to filter
        require_catalog: If True, raise error if catalog unavailable
        
    Returns:
        Tuple of (filtered_skus, audit_info)
        
    Raises:
        CatalogWiringError: If require_catalog=True and catalog unavailable
    """
    audit_info = {
        "catalog_wiring_available": CATALOG_WIRING_AVAILABLE,
        "catalog_loaded": False,
        "input_count": len(skus),
        "catalog_filtered_count": 0,
        "removed_by_catalog": [],
    }
    
    if not CATALOG_WIRING_AVAILABLE:
        logger.warning("Catalog wiring not available - SKUs not verified as purchasable")
        audit_info["catalog_filtered_count"] = len(skus)
        return skus, audit_info
    
    try:
        catalog = get_catalog()
        
        if not catalog.is_loaded:
            # Try to load the catalog
            catalog.load()
        
        audit_info["catalog_loaded"] = catalog.is_loaded
        
        if not catalog.is_loaded:
            if require_catalog:
                raise CatalogWiringError(
                    "CATALOG_NOT_LOADED: Cannot verify SKU purchasability"
                )
            logger.warning("Catalog not loaded - SKUs not verified as purchasable")
            audit_info["catalog_filtered_count"] = len(skus)
            return skus, audit_info
        
        # Filter SKUs through catalog
        filtered = []
        removed = []
        
        for sku in skus:
            if catalog.is_purchasable(sku.sku_id):
                filtered.append(sku)
            else:
                removed.append(sku.sku_id)
        
        audit_info["catalog_filtered_count"] = len(filtered)
        audit_info["removed_by_catalog"] = removed
        
        if removed:
            logger.info(
                f"Catalog wiring removed {len(removed)} non-purchasable SKUs: {removed}"
            )
        
        return filtered, audit_info
        
    except CatalogWiringError:
        raise
    except Exception as e:
        logger.error(f"Catalog wiring error: {e}")
        if require_catalog:
            raise CatalogWiringError(f"CATALOG_ERROR: {str(e)}")
        audit_info["catalog_filtered_count"] = len(skus)
        audit_info["catalog_error"] = str(e)
        return skus, audit_info


def filter_by_gender(
    skus: List[AllowedSKUInput],
    user_context: UserContext
) -> List[AllowedSKUInput]:
    """
    Filter SKUs by gender target.
    
    - MAXimo² users: see male + unisex SKUs
    - MAXima² users: see female + unisex SKUs
    
    Args:
        skus: All allowed SKUs from routing layer
        user_context: User's biological context
        
    Returns:
        SKUs matching the user's gender target
    """
    product_line = user_context.product_line or "MAXimo2"
    product_line_lower = product_line.lower()
    
    if product_line_lower in ("maximo2", "maximo²"):
        valid_lines = {"maximo2", "maximo²", "male", "unisex", None, ""}
    elif product_line_lower in ("maxima2", "maxima²"):
        valid_lines = {"maxima2", "maxima²", "female", "unisex", None, ""}
    else:
        # Unknown product line - return all (safety fallback)
        return skus
    
    filtered = []
    for sku in skus:
        gender_line = (sku.gender_line or "").lower()
        if gender_line in valid_lines or gender_line == "":
            filtered.append(sku)
    
    return filtered


def calculate_match_score(
    sku_ingredients: Set[str],
    intent_targets: Set[str]
) -> Tuple[float, Set[str]]:
    """
    Calculate how well a SKU covers an intent.
    
    Score = overlap / total_targets
    
    Args:
        sku_ingredients: Normalized ingredient tags from SKU
        intent_targets: Normalized ingredient targets from intent
        
    Returns:
        (match_score, overlapping_ingredients)
    """
    if not intent_targets:
        return 0.0, set()
    
    overlap = sku_ingredients & intent_targets
    if not overlap:
        return 0.0, set()
    
    score = len(overlap) / len(intent_targets)
    return round(score, 4), overlap


def match_intents_to_skus(
    skus: List[AllowedSKUInput],
    intents: List[IntentInput]
) -> Tuple[Dict[str, MatchCandidate], List[IntentInput]]:
    """
    Match intents to SKUs based on ingredient overlap.
    
    For each intent:
    - Find SKUs with overlapping ingredient tags
    - Track which intents each SKU satisfies
    - Track unmatched intents
    
    Args:
        skus: Gender-filtered SKUs
        intents: Prioritized intents from Brain Compose
        
    Returns:
        (sku_candidates dict by sku_id, unmatched intents list)
    """
    candidates: Dict[str, MatchCandidate] = {}
    unmatched: List[IntentInput] = []
    
    # Pre-compute normalized ingredient sets for SKUs
    sku_ingredients: Dict[str, Set[str]] = {}
    for sku in skus:
        sku_ingredients[sku.sku_id] = {
            tag.lower().strip() for tag in sku.ingredient_tags
        }
        candidates[sku.sku_id] = MatchCandidate(sku=sku)
    
    # Process intents in priority order
    for intent in sorted(intents, key=lambda x: x.priority):
        intent_targets = {
            target.lower().strip() for target in intent.ingredient_targets
        }
        
        if not intent_targets:
            unmatched.append(intent)
            continue
        
        matched_any = False
        
        for sku_id, sku_ings in sku_ingredients.items():
            score, overlap = calculate_match_score(sku_ings, intent_targets)
            
            if score > 0:
                matched_any = True
                candidates[sku_id].intents.append(intent)
                candidates[sku_id].matched_ingredients.update(overlap)
        
        if not matched_any:
            unmatched.append(intent)
    
    return candidates, unmatched


def fulfill_requirements(
    candidates: Dict[str, MatchCandidate],
    requirements: List[str]
) -> Tuple[List[str], List[str]]:
    """
    Mark which SKUs fulfill required ingredients.
    
    Requirements come from bloodwork deficiencies and MUST be included.
    
    Args:
        candidates: Current match candidates
        requirements: Required ingredient tags from deficiencies
        
    Returns:
        (fulfilled_requirements, unfulfilled_requirements)
    """
    requirements_lower = {req.lower().strip() for req in requirements}
    fulfilled: Set[str] = set()
    
    for sku_id, candidate in candidates.items():
        sku_ings = {tag.lower().strip() for tag in candidate.sku.ingredient_tags}
        overlap = sku_ings & requirements_lower
        
        if overlap:
            candidate.is_requirement = True
            candidate.requirement_ingredients = overlap
            fulfilled.update(overlap)
    
    unfulfilled = sorted(requirements_lower - fulfilled)
    return sorted(fulfilled), unfulfilled


def build_protocol(
    candidates: Dict[str, MatchCandidate]
) -> List[ProtocolItem]:
    """
    Build the final protocol from matched candidates.
    
    Only includes SKUs that either:
    - Match at least one intent
    - Fulfill at least one requirement
    
    Args:
        candidates: All match candidates
        
    Returns:
        List of ProtocolItem sorted by priority
    """
    protocol: List[ProtocolItem] = []
    
    for sku_id, candidate in candidates.items():
        # Skip if no match and not a requirement
        if not candidate.intents and not candidate.is_requirement:
            continue
        
        # Determine reason
        has_intents = len(candidate.intents) > 0
        is_req = candidate.is_requirement
        
        if has_intents and is_req:
            reason = "both"
        elif is_req:
            reason = "requirement"
        else:
            reason = "intent_match"
        
        # Calculate aggregate match score
        if has_intents:
            intent_targets = set()
            for intent in candidate.intents:
                intent_targets.update(
                    target.lower().strip() for target in intent.ingredient_targets
                )
            sku_ings = {
                tag.lower().strip() for tag in candidate.sku.ingredient_tags
            }
            overlap = sku_ings & intent_targets
            match_score = len(overlap) / len(intent_targets) if intent_targets else 0
        else:
            match_score = 1.0  # Pure requirement fulfillment
            overlap = candidate.requirement_ingredients
        
        # Combine all matched ingredients
        all_matched = candidate.matched_ingredients | candidate.requirement_ingredients
        
        # Get priority rank (lowest = best)
        if candidate.intents:
            priority_rank = min(i.priority for i in candidate.intents)
        else:
            priority_rank = 999  # Requirements without intent match
        
        # Build warnings from caution flags
        warnings = []
        if candidate.sku.caution_flags:
            for flag in candidate.sku.caution_flags:
                warnings.append(f"CAUTION: {flag.upper()}")
        if candidate.sku.caution_reasons:
            for reason_code in candidate.sku.caution_reasons:
                if reason_code not in warnings:
                    warnings.append(reason_code)
        
        protocol.append(ProtocolItem(
            sku_id=sku_id,
            product_name=candidate.sku.product_name,
            matched_intents=sorted([i.code for i in candidate.intents]),
            matched_ingredients=sorted(list(all_matched)),
            match_score=round(match_score, 4),
            reason=reason,
            warnings=sorted(warnings),
            evidence_tier=candidate.sku.evidence_tier,
            priority_rank=priority_rank,
        ))
    
    # Sort by priority rank (ascending), then by match score (descending)
    protocol.sort(key=lambda p: (p.priority_rank, -p.match_score))
    
    return protocol


def resolve_matching(
    input_data: MatchingInput,
    require_catalog: bool = False
) -> MatchingResult:
    """
    Main matching function - resolves intents to SKUs.
    
    This is the core matching logic. It is:
    - DETERMINISTIC: same input -> same output
    - TRANSPARENT: full audit trail
    - ASSUMES SAFETY: trusts routing layer already applied blocks
    - GENDER-AWARE: MAXimo² vs MAXima² product lines
    - CATALOG-AWARE (Issue #15): only purchasable SKUs when enabled
    
    Pipeline:
    1. Filter by gender target
    2. Filter by catalog availability (Issue #15) - if enabled
    3. Match intents to SKUs via ingredient overlap
    4. Mark requirement fulfillment
    5. Build protocol with scores and warnings
    6. Generate audit trail
    
    Args:
        input_data: MatchingInput with allowed_skus, intents, context
        require_catalog: If True, fail if catalog wiring unavailable
        
    Returns:
        MatchingResult with protocol, unmatched intents, audit
    """
    processed_at = datetime.utcnow().isoformat()
    
    # Step 1: Filter by gender
    gender_filtered = filter_by_gender(
        input_data.allowed_skus,
        input_data.user_context
    )
    
    # Step 2: Filter by catalog availability (Issue #15)
    catalog_filtered, catalog_audit = filter_by_catalog(
        gender_filtered,
        require_catalog=require_catalog
    )
    
    # Step 3: Match intents to SKUs
    candidates, unmatched_intents = match_intents_to_skus(
        catalog_filtered,
        input_data.prioritized_intents
    )
    
    # Step 4: Fulfill requirements
    fulfilled_reqs, unfulfilled_reqs = fulfill_requirements(
        candidates,
        input_data.requirements
    )
    
    # Step 5: Build protocol
    protocol = build_protocol(candidates)
    
    # Step 6: Build unmatched intent objects
    unmatched_objects = [
        UnmatchedIntent(
            code=intent.code,
            priority=intent.priority,
            ingredient_targets=intent.ingredient_targets,
            reason="No SKU with matching ingredient tags available"
        )
        for intent in unmatched_intents
    ]
    
    # Count items with warnings
    caution_count = sum(1 for p in protocol if p.warnings)
    
    # Build audit
    audit = MatchingAudit(
        total_allowed_skus=len(input_data.allowed_skus),
        gender_filtered_count=len(gender_filtered),
        catalog_filtered_count=catalog_audit.get("catalog_filtered_count", len(gender_filtered)),
        catalog_removed_skus=catalog_audit.get("removed_by_catalog", []),
        intents_processed=len(input_data.prioritized_intents),
        intents_matched=len(input_data.prioritized_intents) - len(unmatched_intents),
        intents_unmatched=len(unmatched_intents),
        requirements_total=len(input_data.requirements),
        requirements_fulfilled=fulfilled_reqs,
        requirements_unfulfilled=unfulfilled_reqs,
        protocol_items_count=len(protocol),
        caution_warnings_count=caution_count,
        user_context_applied={
            "sex": input_data.user_context.sex,
            "product_line": input_data.user_context.product_line,
            "age": input_data.user_context.age,
            "cycle_phase": input_data.user_context.cycle_phase,
        },
        processed_at=processed_at,
        catalog_wiring=catalog_audit,
    )
    
    # Compute hash
    match_hash = MatchingResult.compute_hash(protocol, unmatched_objects)
    
    return MatchingResult(
        protocol=protocol,
        unmatched_intents=unmatched_objects,
        match_hash=match_hash,
        audit=audit,
    )


# Backward-compatible alias
def resolve_matching_legacy(input_data: MatchingInput) -> MatchingResult:
    """Legacy function without catalog requirement."""
    return resolve_matching(input_data, require_catalog=False)
