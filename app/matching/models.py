"""
Matching Layer Models (Issue #7)

Pydantic models for intent-to-SKU matching inputs, outputs, and audit trails.

This layer takes ALLOWED SKUs (from routing) and matches them to user INTENTS.

Version: matching_layer_v1
"""

from datetime import datetime
from typing import List, Optional, Dict, Literal
from pydantic import BaseModel, Field
import hashlib
import json


class IntentInput(BaseModel):
    """
    A user intent with priority from Brain Compose.
    
    Intents represent what the user wants to achieve.
    """
    code: str = Field(
        description="Intent code e.g. 'INTENT_ENERGY', 'INTENT_SLEEP'"
    )
    priority: int = Field(
        ge=1,
        description="Priority rank (1 = highest)"
    )
    ingredient_targets: List[str] = Field(
        default_factory=list,
        description="Ingredient tags this intent targets e.g. ['b12', 'iron', 'coq10']"
    )
    source: Optional[str] = Field(
        default=None,
        description="Where this intent came from: 'goal', 'painpoint', 'blood'"
    )
    confidence: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Confidence score for this intent"
    )
    
    class Config:
        extra = "allow"


class UserContext(BaseModel):
    """
    User biological context for matching.
    
    Determines which product line (MAXimo² / MAXima²) to use.
    """
    sex: Literal["male", "female"]
    product_line: Literal["MAXimo2", "MAXima2"] = Field(
        default=None,
        description="Derived from sex if not provided"
    )
    age: Optional[int] = Field(
        default=None,
        ge=0,
        le=120
    )
    cycle_phase: Optional[str] = Field(
        default=None,
        description="For MAXima² users: follicular, ovulatory, luteal, menstrual"
    )
    
    class Config:
        extra = "allow"
    
    def __init__(self, **data):
        # Auto-derive product_line from sex if not provided
        if "product_line" not in data or data.get("product_line") is None:
            sex = data.get("sex", "male")
            data["product_line"] = "MAXimo2" if sex == "male" else "MAXima2"
        super().__init__(**data)


class AllowedSKUInput(BaseModel):
    """
    SKU that passed routing layer (from Issue #6).
    
    These are ALREADY safety-filtered.
    """
    sku_id: str
    product_name: str
    ingredient_tags: List[str] = Field(default_factory=list)
    category_tags: List[str] = Field(default_factory=list)
    gender_line: Optional[str] = Field(
        default=None,
        description="MAXimo2, MAXima2, or UNISEX"
    )
    evidence_tier: Optional[str] = Field(
        default=None,
        description="TIER_1, TIER_2, TIER_3"
    )
    caution_flags: List[str] = Field(
        default_factory=list,
        description="Caution flags from routing layer"
    )
    caution_reasons: List[str] = Field(
        default_factory=list,
        description="Reason codes for caution flags"
    )
    fulfills_requirements: List[str] = Field(
        default_factory=list,
        description="Required ingredients this SKU provides"
    )
    
    class Config:
        extra = "allow"


class MatchingInput(BaseModel):
    """
    Complete input to the matching layer.
    """
    allowed_skus: List[AllowedSKUInput] = Field(
        description="SKUs that passed routing (from Issue #6)"
    )
    prioritized_intents: List[IntentInput] = Field(
        description="User intents ranked by priority (from Brain Compose)"
    )
    user_context: UserContext = Field(
        description="User biological context"
    )
    requirements: List[str] = Field(
        default_factory=list,
        description="Must-include ingredients from bloodwork deficiencies"
    )
    
    class Config:
        extra = "forbid"


class ProtocolItem(BaseModel):
    """
    A SKU selected for the user's protocol.
    """
    sku_id: str
    product_name: str
    matched_intents: List[str] = Field(
        description="Intent codes this SKU satisfies"
    )
    matched_ingredients: List[str] = Field(
        description="Ingredient tags that matched"
    )
    match_score: float = Field(
        ge=0.0,
        le=1.0,
        description="How well SKU covers intent (overlap / total targets)"
    )
    reason: Literal["intent_match", "requirement", "both"] = Field(
        description="Why this SKU was selected"
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="Caution-derived warnings"
    )
    evidence_tier: Optional[str] = Field(
        default=None,
        description="Evidence tier if available"
    )
    priority_rank: int = Field(
        default=0,
        description="Lowest priority rank among matched intents (1 = highest)"
    )
    
    class Config:
        extra = "forbid"


class UnmatchedIntent(BaseModel):
    """
    An intent that could not be matched to any available SKU.
    """
    code: str
    priority: int
    ingredient_targets: List[str]
    reason: str = Field(
        description="Why matching failed"
    )
    
    class Config:
        extra = "forbid"


class MatchingAudit(BaseModel):
    """
    Audit trail for matching decisions.
    """
    total_allowed_skus: int
    gender_filtered_count: int = Field(
        description="SKUs remaining after gender filter"
    )
    intents_processed: int
    intents_matched: int
    intents_unmatched: int
    requirements_total: int
    requirements_fulfilled: List[str]
    requirements_unfulfilled: List[str]
    protocol_items_count: int
    caution_warnings_count: int = Field(
        default=0,
        description="Items with caution warnings"
    )
    user_context_applied: Dict = Field(
        default_factory=dict,
        description="User context that was applied"
    )
    processed_at: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat()
    )
    
    class Config:
        extra = "forbid"


class MatchingResult(BaseModel):
    """
    Complete output from the matching layer.
    """
    protocol: List[ProtocolItem] = Field(
        description="SKUs selected for the protocol"
    )
    unmatched_intents: List[UnmatchedIntent] = Field(
        description="Intents that could not be matched"
    )
    match_hash: str = Field(
        description="Deterministic hash of the matching result"
    )
    audit: MatchingAudit
    version: str = "matching_layer_v1"
    
    class Config:
        extra = "forbid"
    
    @classmethod
    def compute_hash(cls, protocol: List[ProtocolItem], unmatched: List[UnmatchedIntent]) -> str:
        """
        Compute deterministic hash of matching result.
        
        Uses sorted SKU IDs and intent codes for determinism.
        """
        hash_input = {
            "protocol": sorted([p.sku_id for p in protocol]),
            "unmatched": sorted([u.code for u in unmatched]),
        }
        hash_str = json.dumps(hash_input, sort_keys=True)
        return f"sha256:{hashlib.sha256(hash_str.encode()).hexdigest()[:16]}"


# Response models for API endpoints

class MatchingResponse(BaseModel):
    """API response wrapper for matching results."""
    success: bool = True
    result: MatchingResult
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class MatchingHealthResponse(BaseModel):
    """Health check response for matching module."""
    status: str = "ok"
    module: str = "matching_layer"
    version: str = "matching_layer_v1"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
