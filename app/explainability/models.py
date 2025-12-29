"""
Explainability Models (Issue #8)

Pydantic models for protocol explainability output.

CRITICAL CONSTRAINTS:
- No dosage information
- No diagnostic claims
- No "recommended by doctor" language
- Disclaimers are LOCKED - no A/B testing

Version: explainability_v1
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ConfidenceLevel(str, Enum):
    """
    Protocol confidence level based on data completeness.
    
    Calculated by rules, NOT by ML/AI.
    """
    HIGH = "high"      # Bloodwork + ≥2 intents + no cautions
    MEDIUM = "medium"  # Missing bloodwork / partial panel
    LOW = "low"        # Lifestyle-only / sparse data


# ============================================================
# LOCKED DISCLAIMERS - DO NOT MODIFY
# ============================================================

class DisclaimerSet(BaseModel):
    """
    Locked disclaimer copy that MUST appear in all protocol outputs.
    
    ❌ Cannot be modified by UI
    ❌ Cannot be A/B tested
    ❌ Cannot be localized without legal review
    """
    
    allowed_not_recommended: str = Field(
        default="Allowed ≠ Recommended. Inclusion in your protocol means this item passed safety checks, not that it is specifically recommended for you.",
        description="Clarifies that allowed != recommended"
    )
    
    blocked_biological: str = Field(
        default="Some items are blocked due to biological constraints identified in your assessment. This is a safety feature.",
        description="Explains why items may be blocked"
    )
    
    not_medical_advice: str = Field(
        default="This is not medical advice. GenoMAX² provides personalized supplement guidance based on your inputs, not medical diagnosis or treatment. Consult a healthcare provider for medical concerns.",
        description="Standard medical disclaimer"
    )
    
    blood_precedence: str = Field(
        default="Blood biomarker results take precedence over all other factors. Items blocked by bloodwork analysis cannot be overridden.",
        description="Explains blood-first principle"
    )
    
    version: str = Field(
        default="disclaimer_v1.0",
        description="Disclaimer version for audit trail"
    )


# ============================================================
# EXPLAINABILITY BLOCKS
# ============================================================

class ExplainabilityBlock(BaseModel):
    """
    Explanation block for a single protocol item.
    
    Answers:
    - WHY was this included?
    - WHY was this NOT blocked?
    """
    
    item_id: str = Field(
        ...,
        description="SKU ID of the protocol item"
    )
    
    product_name: str = Field(
        ...,
        description="Human-readable product name"
    )
    
    why_included: List[str] = Field(
        default_factory=list,
        description="Reasons for inclusion (intent match, ingredient match, etc.)"
    )
    
    why_not_blocked: List[str] = Field(
        default_factory=list,
        description="Reasons item passed safety checks"
    )
    
    confidence_level: ConfidenceLevel = Field(
        default=ConfidenceLevel.MEDIUM,
        description="Item-level confidence"
    )
    
    matched_intents: List[str] = Field(
        default_factory=list,
        description="Intents this item addresses"
    )
    
    matched_ingredients: List[str] = Field(
        default_factory=list,
        description="Key ingredients that triggered match"
    )
    
    caution_notes: List[str] = Field(
        default_factory=list,
        description="Any caution flags to display (non-blocking)"
    )


class BlockedItemExplanation(BaseModel):
    """
    Explanation for why an item was BLOCKED.
    
    Critical for trust and support reduction.
    Prevents "why didn't you suggest X" questions.
    """
    
    sku_id: str = Field(
        ...,
        description="SKU ID of blocked item"
    )
    
    product_name: str = Field(
        ...,
        description="Human-readable product name"
    )
    
    reason: str = Field(
        ...,
        description="Human-readable blocking reason"
    )
    
    reason_code: str = Field(
        ...,
        description="Machine-readable reason code"
    )
    
    blocked_by: str = Field(
        ...,
        description="What blocked it: 'blood', 'metadata', 'safety', 'gender'"
    )
    
    can_change: bool = Field(
        default=False,
        description="Whether this block could change with different inputs"
    )
    
    change_hint: Optional[str] = Field(
        default=None,
        description="Hint about what would need to change (e.g., 'retest bloodwork')"
    )


# ============================================================
# PROTOCOL-LEVEL CONFIDENCE
# ============================================================

class ProtocolConfidence(BaseModel):
    """
    System-level confidence calculation.
    
    Rule-based, NOT ML-based:
    - HIGH: Bloodwork + ≥2 intents + no cautions
    - MEDIUM: Missing bloodwork OR partial panel
    - LOW: Lifestyle-only OR sparse data
    """
    
    level: ConfidenceLevel = Field(
        ...,
        description="Overall confidence level"
    )
    
    badge_text: str = Field(
        ...,
        description="Display text for confidence badge"
    )
    
    explanation: str = Field(
        ...,
        description="Why this confidence level"
    )
    
    factors: Dict[str, bool] = Field(
        default_factory=dict,
        description="Factors that contributed to confidence"
    )
    
    recommendations_to_improve: List[str] = Field(
        default_factory=list,
        description="What user could do to improve confidence"
    )


# ============================================================
# FULL EXPLAINABILITY OUTPUT
# ============================================================

class ProtocolExplainability(BaseModel):
    """
    Complete explainability output for a protocol.
    
    This wraps all explanations for frontend consumption.
    """
    
    protocol_id: str = Field(
        ...,
        description="ID of the protocol being explained"
    )
    
    confidence: ProtocolConfidence = Field(
        ...,
        description="Overall protocol confidence"
    )
    
    item_explanations: List[ExplainabilityBlock] = Field(
        default_factory=list,
        description="Explanation for each included item"
    )
    
    blocked_explanations: List[BlockedItemExplanation] = Field(
        default_factory=list,
        description="Explanation for each blocked item"
    )
    
    disclaimers: DisclaimerSet = Field(
        default_factory=DisclaimerSet,
        description="Locked disclaimer copy"
    )
    
    generated_at: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(),
        description="When explanations were generated"
    )
    
    version: str = Field(
        default="explainability_v1",
        description="Explainability module version"
    )


# ============================================================
# INPUT MODEL (for API)
# ============================================================

class ExplainabilityRequest(BaseModel):
    """
    Request model for generating explainability.
    
    Takes existing protocol output - does NOT modify it.
    """
    
    protocol_id: str = Field(
        ...,
        description="Protocol ID to explain"
    )
    
    protocol_items: List[Dict[str, Any]] = Field(
        ...,
        description="Protocol items from matching layer"
    )
    
    blocked_items: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Blocked items from routing layer"
    )
    
    routing_constraints: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Routing constraints that were applied"
    )
    
    has_bloodwork: bool = Field(
        default=False,
        description="Whether bloodwork was included"
    )
    
    bloodwork_complete: bool = Field(
        default=False,
        description="Whether bloodwork panel was complete"
    )
    
    intent_count: int = Field(
        default=0,
        description="Number of matched intents"
    )
    
    caution_count: int = Field(
        default=0,
        description="Number of caution flags"
    )
