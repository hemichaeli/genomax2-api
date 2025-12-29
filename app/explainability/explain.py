"""
Explainability Core Logic (Issue #8)

Generates explanations from existing protocol output.
Does NOT modify decisions - only explains them.

Principle: The Brain decides. The UX explains. Never the reverse.

Version: explainability_v1
"""

from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from .models import (
    ExplainabilityBlock,
    ConfidenceLevel,
    ProtocolConfidence,
    BlockedItemExplanation,
    ProtocolExplainability,
    DisclaimerSet,
    ExplainabilityRequest,
)


# ============================================================
# REASON CODE TO HUMAN-READABLE MAPPING
# ============================================================

REASON_CODE_EXPLANATIONS = {
    # Blood-based blocks
    "BLOCK_IRON_FERRITIN_HIGH": "Iron is blocked due to elevated ferritin levels in your bloodwork.",
    "BLOCK_IRON_TRANSFERRIN_HIGH": "Iron is blocked due to elevated transferrin saturation.",
    "BLOCK_POTASSIUM_HIGH": "Potassium supplementation is blocked due to elevated blood potassium.",
    "BLOCK_CALCIUM_HIGH": "Calcium is blocked due to elevated blood calcium levels.",
    "BLOCK_VITAMIN_D_HIGH": "Vitamin D supplementation is blocked due to already-high levels.",
    
    # Caution codes (non-blocking)
    "CAUTION_VITAMIN_D_CALCIUM_HIGH": "Vitamin D should be used with caution due to calcium levels.",
    "CAUTION_ZINC_HIGH_DOSE": "High-dose zinc products flagged for monitoring.",
    "CAUTION_MAGNESIUM_KIDNEY": "Magnesium requires monitoring with kidney considerations.",
    
    # Metadata blocks
    "INSUFFICIENT_METADATA": "This item lacks required safety data and cannot be included.",
    "MISSING_INGREDIENT_TAGS": "This item's ingredient data is incomplete.",
    "MISSING_CATEGORY_TAGS": "This item's category data is incomplete.",
    
    # Gender blocks
    "GENDER_MISMATCH_MALE": "This product is formulated for males only.",
    "GENDER_MISMATCH_FEMALE": "This product is formulated for females only.",
    
    # Safety blocks
    "HEPATOTOXIC_RISK": "This item is blocked due to potential liver impact.",
    "INTERACTION_RISK": "This item has potential interactions with other supplements.",
}

# Fallback for unknown codes
DEFAULT_EXPLANATION = "This item is blocked based on your assessment results."


# ============================================================
# CONFIDENCE CALCULATION
# ============================================================

def calculate_confidence(
    has_bloodwork: bool,
    bloodwork_complete: bool,
    intent_count: int,
    caution_count: int
) -> ProtocolConfidence:
    """
    Calculate protocol confidence level based on rules.
    
    Rules (NOT ML):
    - HIGH: Bloodwork + complete panel + ≥2 intents + no cautions
    - MEDIUM: Missing bloodwork OR partial panel OR cautions present
    - LOW: Lifestyle-only OR sparse data (0-1 intents)
    
    Returns ProtocolConfidence with badge text and explanation.
    """
    
    factors = {
        "has_bloodwork": has_bloodwork,
        "bloodwork_complete": bloodwork_complete,
        "multiple_intents": intent_count >= 2,
        "no_cautions": caution_count == 0,
    }
    
    recommendations = []
    
    # Determine level
    if has_bloodwork and bloodwork_complete and intent_count >= 2 and caution_count == 0:
        level = ConfidenceLevel.HIGH
        badge_text = "High Confidence"
        explanation = "Your protocol is based on complete bloodwork and multiple matched health goals with no caution flags."
    
    elif not has_bloodwork:
        level = ConfidenceLevel.LOW
        badge_text = "Limited Data"
        explanation = "Your protocol is based on lifestyle data only. Adding bloodwork would significantly improve personalization."
        recommendations.append("Add bloodwork analysis for personalized biomarker-based recommendations")
    
    elif not bloodwork_complete:
        level = ConfidenceLevel.MEDIUM
        badge_text = "Partial Data"
        explanation = "Your bloodwork panel is incomplete. Full panel analysis would improve recommendations."
        recommendations.append("Complete a full bloodwork panel for comprehensive analysis")
    
    elif intent_count < 2:
        level = ConfidenceLevel.LOW
        badge_text = "Limited Goals"
        explanation = "Your protocol addresses few health goals. Consider expanding your assessment."
        recommendations.append("Add more health goals to your assessment for broader coverage")
    
    elif caution_count > 0:
        level = ConfidenceLevel.MEDIUM
        badge_text = "Caution Flagged"
        explanation = f"Your protocol includes {caution_count} caution flag(s) that require attention."
        recommendations.append("Review caution flags and consider consulting a healthcare provider")
    
    else:
        level = ConfidenceLevel.MEDIUM
        badge_text = "Moderate Confidence"
        explanation = "Your protocol has good data coverage with some areas for improvement."
    
    return ProtocolConfidence(
        level=level,
        badge_text=badge_text,
        explanation=explanation,
        factors=factors,
        recommendations_to_improve=recommendations
    )


# ============================================================
# ITEM EXPLANATION GENERATION
# ============================================================

def generate_item_explanation(
    item: Dict[str, Any],
    routing_constraints: Optional[Dict[str, Any]] = None,
    confidence_level: ConfidenceLevel = ConfidenceLevel.MEDIUM
) -> ExplainabilityBlock:
    """
    Generate explanation for a single protocol item.
    
    Args:
        item: Protocol item from matching layer
        routing_constraints: Constraints that were applied
        confidence_level: Protocol-level confidence
    
    Returns:
        ExplainabilityBlock with why_included and why_not_blocked
    """
    
    why_included = []
    why_not_blocked = []
    caution_notes = []
    
    # Extract item data
    sku_id = item.get("sku_id", item.get("item_id", "unknown"))
    product_name = item.get("product_name", "Unknown Product")
    matched_intents = item.get("matched_intents", [])
    matched_ingredients = item.get("matched_ingredients", [])
    warnings = item.get("warnings", [])
    reason = item.get("reason", "")
    match_score = item.get("match_score", 0)
    
    # Generate "why included" reasons
    if matched_intents:
        for intent in matched_intents[:3]:  # Limit to top 3
            intent_name = intent.replace("INTENT_", "").replace("_", " ").title()
            why_included.append(f"Matches your goal: {intent_name}")
    
    if matched_ingredients:
        ingredient_list = ", ".join(matched_ingredients[:5])  # Limit to 5
        why_included.append(f"Contains key ingredients: {ingredient_list}")
    
    if reason == "requirement":
        why_included.append("Required based on your assessment results")
    elif reason == "both":
        why_included.append("Matches goals AND fulfills requirements")
    
    if match_score >= 0.8:
        why_included.append("Strong match for your profile")
    elif match_score >= 0.5:
        why_included.append("Good match for your profile")
    
    # Generate "why not blocked" reasons
    if routing_constraints:
        blocked_ingredients = routing_constraints.get("blocked_ingredients", [])
        blocked_categories = routing_constraints.get("blocked_categories", [])
        
        if blocked_ingredients:
            why_not_blocked.append("Does not contain any blocked ingredients")
        
        if blocked_categories:
            why_not_blocked.append("Not in any blocked categories")
    
    why_not_blocked.append("No blood-based contraindications identified")
    why_not_blocked.append("Passed all safety checks")
    
    # Handle caution warnings (non-blocking)
    for warning in warnings:
        if "CAUTION" in warning.upper():
            caution_code = warning.replace("CAUTION: ", "").replace("CAUTION_", "")
            caution_notes.append(f"Note: {caution_code.replace('_', ' ').title()}")
    
    return ExplainabilityBlock(
        item_id=sku_id,
        product_name=product_name,
        why_included=why_included,
        why_not_blocked=why_not_blocked,
        confidence_level=confidence_level,
        matched_intents=matched_intents,
        matched_ingredients=matched_ingredients,
        caution_notes=caution_notes
    )


# ============================================================
# BLOCKED ITEM EXPLANATION
# ============================================================

def generate_blocked_explanation(
    blocked_item: Dict[str, Any]
) -> BlockedItemExplanation:
    """
    Generate explanation for why an item was blocked.
    
    Critical for trust and reducing support tickets.
    """
    
    sku_id = blocked_item.get("sku_id", "unknown")
    product_name = blocked_item.get("product_name", "Unknown Product")
    reason_codes = blocked_item.get("reason_codes", [])
    blocked_by = blocked_item.get("blocked_by", "safety")
    
    # Get human-readable reason
    if reason_codes:
        primary_code = reason_codes[0]
        reason = REASON_CODE_EXPLANATIONS.get(primary_code, DEFAULT_EXPLANATION)
    else:
        reason = DEFAULT_EXPLANATION
    
    # Determine if block can change
    can_change = False
    change_hint = None
    
    if blocked_by == "blood":
        can_change = True
        change_hint = "This may change with updated bloodwork results"
    elif blocked_by == "metadata":
        can_change = False
        change_hint = None  # Metadata blocks are permanent until product data is updated
    elif blocked_by == "gender":
        can_change = False
        change_hint = None  # Gender-based blocks don't change
    
    return BlockedItemExplanation(
        sku_id=sku_id,
        product_name=product_name,
        reason=reason,
        reason_code=reason_codes[0] if reason_codes else "UNKNOWN",
        blocked_by=blocked_by,
        can_change=can_change,
        change_hint=change_hint
    )


# ============================================================
# MAIN ENTRY POINT
# ============================================================

def generate_explainability(request: ExplainabilityRequest) -> ProtocolExplainability:
    """
    Generate complete explainability output for a protocol.
    
    This is the main entry point for the explainability module.
    Takes existing protocol data and generates explanations.
    
    Does NOT modify any decisions.
    """
    
    # Calculate confidence
    confidence = calculate_confidence(
        has_bloodwork=request.has_bloodwork,
        bloodwork_complete=request.bloodwork_complete,
        intent_count=request.intent_count,
        caution_count=request.caution_count
    )
    
    # Generate item explanations
    item_explanations = []
    for item in request.protocol_items:
        explanation = generate_item_explanation(
            item=item,
            routing_constraints=request.routing_constraints,
            confidence_level=confidence.level
        )
        item_explanations.append(explanation)
    
    # Generate blocked item explanations
    blocked_explanations = []
    if request.blocked_items:
        for blocked in request.blocked_items:
            explanation = generate_blocked_explanation(blocked)
            blocked_explanations.append(explanation)
    
    # Get locked disclaimers
    disclaimers = get_disclaimers()
    
    return ProtocolExplainability(
        protocol_id=request.protocol_id,
        confidence=confidence,
        item_explanations=item_explanations,
        blocked_explanations=blocked_explanations,
        disclaimers=disclaimers,
        generated_at=datetime.utcnow().isoformat(),
        version="explainability_v1"
    )


def get_disclaimers() -> DisclaimerSet:
    """
    Return the locked disclaimer set.
    
    ❌ These cannot be modified
    ❌ These cannot be A/B tested
    ❌ These cannot be localized without legal review
    """
    return DisclaimerSet()
