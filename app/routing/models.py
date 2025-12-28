"""
Routing Layer Models (Issue #6)

Pydantic models for routing inputs, outputs, and audit trails.

Version: routing_layer_v1
"""

from datetime import datetime
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
import hashlib
import json


class RoutingConstraints(BaseModel):
    """
    Constraints from Brain Orchestrate that determine SKU eligibility.
    
    These come from bloodwork analysis and biological rules.
    """
    blocked_ingredients: List[str] = Field(
        default_factory=list,
        description="Ingredient tags that must be blocked (e.g., ['iron', 'potassium'])"
    )
    blocked_categories: List[str] = Field(
        default_factory=list,
        description="Category tags that must be blocked (e.g., ['hepatotoxic'])"
    )
    caution_flags: List[str] = Field(
        default_factory=list,
        description="Ingredient tags requiring caution (do not block, just flag)"
    )
    requirements: List[str] = Field(
        default_factory=list,
        description="Required ingredient tags based on deficiency (e.g., ['b12', 'omega3'])"
    )
    reason_codes: List[str] = Field(
        default_factory=list,
        description="Reason codes explaining constraints (e.g., ['BLOCK_IRON_FERRITIN_HIGH'])"
    )
    
    class Config:
        extra = "forbid"


class SkuInput(BaseModel):
    """
    Minimal SKU representation for routing input.
    
    This is what comes from catalog governance (Issue #5).
    """
    sku_id: str
    product_name: str
    ingredient_tags: List[str] = Field(default_factory=list)
    category_tags: List[str] = Field(default_factory=list)
    risk_tags: List[str] = Field(default_factory=list)
    gender_line: Optional[str] = None  # "MAXimo2", "MAXima2", "UNISEX"
    evidence_tier: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields from catalog


class RoutingInput(BaseModel):
    """
    Complete input to the routing layer.
    """
    valid_skus: List[SkuInput] = Field(
        description="SKUs that passed catalog validation (from Issue #5)"
    )
    routing_constraints: RoutingConstraints = Field(
        description="Constraints from Brain Orchestrate"
    )
    
    class Config:
        extra = "forbid"


class AllowedSKU(BaseModel):
    """
    A SKU that passed all routing checks.
    
    May have caution flags attached but is not blocked.
    """
    sku_id: str
    product_name: str
    ingredient_tags: List[str]
    category_tags: List[str]
    gender_line: Optional[str] = None
    evidence_tier: Optional[str] = None
    # Caution handling
    caution_flags: List[str] = Field(
        default_factory=list,
        description="Caution flags from constraints that apply to this SKU"
    )
    caution_reasons: List[str] = Field(
        default_factory=list,
        description="Reason codes for caution flags"
    )
    # Requirements tracking
    fulfills_requirements: List[str] = Field(
        default_factory=list,
        description="Which required ingredients this SKU provides"
    )
    
    class Config:
        extra = "forbid"


class BlockedSKU(BaseModel):
    """
    A SKU that was eliminated by routing constraints.
    """
    sku_id: str
    product_name: str
    reason_codes: List[str] = Field(
        description="All reason codes explaining why blocked"
    )
    blocked_by: Literal["blood", "metadata", "safety", "category"] = Field(
        description="Primary blocking source"
    )
    blocked_ingredients: List[str] = Field(
        default_factory=list,
        description="Which ingredient tags caused the block"
    )
    blocked_categories: List[str] = Field(
        default_factory=list,
        description="Which category tags caused the block"
    )
    
    class Config:
        extra = "forbid"


class RoutingAudit(BaseModel):
    """
    Audit trail for routing decisions.
    """
    total_input_skus: int
    allowed_count: int
    blocked_count: int
    blocked_by_blood: int = Field(
        description="SKUs blocked by blood-based constraints"
    )
    blocked_by_metadata: int = Field(
        description="SKUs blocked by metadata issues (from Issue #5)"
    )
    blocked_by_category: int = Field(
        description="SKUs blocked by category constraints"
    )
    constraints_applied: List[str] = Field(
        description="List of constraint types that were applied"
    )
    requirements_in_catalog: List[str] = Field(
        default_factory=list,
        description="Required ingredients that have at least one available SKU"
    )
    requirements_missing: List[str] = Field(
        default_factory=list,
        description="Required ingredients with no available SKUs"
    )
    caution_count: int = Field(
        default=0,
        description="Number of allowed SKUs with caution flags"
    )
    processed_at: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat()
    )
    
    class Config:
        extra = "forbid"


class RoutingResult(BaseModel):
    """
    Complete output from the routing layer.
    """
    allowed_skus: List[AllowedSKU]
    blocked_skus: List[BlockedSKU]
    routing_hash: str = Field(
        description="Deterministic hash of the routing result"
    )
    audit: RoutingAudit
    version: str = "routing_layer_v1"
    
    class Config:
        extra = "forbid"
    
    @classmethod
    def compute_hash(cls, allowed_skus: List[AllowedSKU], blocked_skus: List[BlockedSKU]) -> str:
        """
        Compute deterministic hash of routing result.
        
        Uses sorted SKU IDs for determinism.
        """
        hash_input = {
            "allowed": sorted([s.sku_id for s in allowed_skus]),
            "blocked": sorted([s.sku_id for s in blocked_skus]),
        }
        hash_str = json.dumps(hash_input, sort_keys=True)
        return f"sha256:{hashlib.sha256(hash_str.encode()).hexdigest()[:16]}"


# Response models for API endpoints

class RoutingResponse(BaseModel):
    """API response wrapper for routing results."""
    success: bool = True
    result: RoutingResult
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class RoutingHealthResponse(BaseModel):
    """Health check response for routing module."""
    status: str = "ok"
    module: str = "routing_layer"
    version: str = "routing_layer_v1"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
