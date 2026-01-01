"""
GenoMAX² Product Intake System - Models

Pydantic models for the intake workflow:
- IntakeRequest: Input for creating new intakes
- IntakeStatus: Workflow states
- DraftModule: Proposed OS module
- DraftCopy: Generated label copy
- ValidationFlags: Warnings/blockers from validation
- IntakeResponse: Full intake record response

Version: intake_system_v1
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
import re


class IntakeStatus(str, Enum):
    """Intake workflow states."""
    DRAFT = "draft"
    APPROVED = "approved"
    REJECTED = "rejected"


class OSEnvironment(str, Enum):
    """Valid OS environments."""
    MAXIMO = "MAXimo²"      # Male-optimized
    MAXIMA = "MAXima²"      # Female-optimized
    BOTH = "BOTH"           # Generates M + F modules


class OSLayer(str, Enum):
    """OS module layers based on research tier."""
    CORE = "Core"           # TIER 1: Strong evidence
    ADAPTIVE = "Adaptive"   # TIER 2: Moderate evidence
    EXPERIMENTAL = "Experimental"  # TIER 3: Limited evidence


class BiologicalDomain(str, Enum):
    """Valid biological domains (taxonomy only, no benefits)."""
    CARDIOVASCULAR = "Cardiovascular"
    NEUROLOGICAL = "Neurological"
    METABOLIC = "Metabolic"
    IMMUNE = "Immune"
    MUSCULOSKELETAL = "Musculoskeletal"
    ENDOCRINE = "Endocrine"
    DIGESTIVE = "Digestive"
    INTEGUMENTARY = "Integumentary"
    RESPIRATORY = "Respiratory"
    GENERAL_WELLNESS = "General Wellness"


class ParsedIngredient(BaseModel):
    """Normalized ingredient from supplier data."""
    raw_name: str
    canonical_name: Optional[str] = None
    amount: Optional[str] = None
    unit: Optional[str] = None
    daily_value_percent: Optional[float] = None
    matched_ingredient_id: Optional[int] = None
    tier_classification: Optional[str] = None


class ParsedPayload(BaseModel):
    """Normalized product data from supplier."""
    product_name: str
    ingredients: List[ParsedIngredient] = Field(default_factory=list)
    serving_size: Optional[str] = None
    servings_per_container: Optional[int] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    base_price: Optional[float] = None
    shipping_restrictions: Optional[List[str]] = None
    raw_ingredients_text: Optional[str] = None


class DraftModule(BaseModel):
    """
    Proposed OS module row.
    
    NOT inserted until approved.
    For BOTH os_environment, two DraftModules are generated (M + F).
    """
    module_code: str
    os_environment: str  # MAXimo² or MAXima²
    os_layer: str        # Core, Adaptive, or Experimental
    biological_domain: Optional[str] = None
    shopify_store: Optional[str] = None
    shopify_handle: Optional[str] = None
    product_name: str
    ingredient_tags: Optional[str] = None
    category_tags: Optional[str] = None
    ingredients_raw_text: Optional[str] = None
    suggested_use_full: Optional[str] = None
    safety_notes: Optional[str] = None
    contraindications: Optional[str] = None
    drug_interactions: Optional[str] = None
    wholesale_price: Optional[float] = None
    shipping_restriction: Optional[str] = None
    fda_disclaimer: str = "These statements have not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease."


class DraftCopy(BaseModel):
    """Generated label and marketing copy."""
    front_label_text: Optional[str] = None
    back_label_text: Optional[str] = None
    shopify_title: Optional[str] = None
    shopify_body: Optional[str] = None


class ValidationFlag(BaseModel):
    """Single validation warning or blocker."""
    code: str
    severity: str  # "warning" or "blocker"
    message: str
    field: Optional[str] = None


class ValidationFlags(BaseModel):
    """Collection of validation results."""
    warnings: List[ValidationFlag] = Field(default_factory=list)
    blockers: List[ValidationFlag] = Field(default_factory=list)
    duplicate_check: Optional[Dict[str, Any]] = None
    is_valid: bool = True


# =============================================
# Request Models
# =============================================

class IntakeCreateRequest(BaseModel):
    """Request to create a new intake."""
    supplier: str = Field(default="supliful", description="Supplier identifier")
    product_url: str = Field(..., description="Supliful product URL")
    
    @field_validator('product_url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        # Basic URL validation
        if not v.startswith('http'):
            raise ValueError("product_url must be a valid URL starting with http(s)")
        return v


class IntakeApproveRequest(BaseModel):
    """Request to approve an intake."""
    intake_id: str = Field(..., description="UUID of the intake to approve")
    approved_by: str = Field(..., description="Admin name approving the intake")


class IntakeRejectRequest(BaseModel):
    """Request to reject an intake."""
    intake_id: str = Field(..., description="UUID of the intake to reject")
    reason: str = Field(..., description="Reason for rejection")


# =============================================
# Response Models
# =============================================

class IntakeCreateResponse(BaseModel):
    """Response from intake creation."""
    status: str = "success"
    intake_id: str
    supplier: str
    product_url: str
    parsed_payload: Optional[ParsedPayload] = None
    draft_modules: List[DraftModule] = Field(default_factory=list)
    draft_copy: Optional[DraftCopy] = None
    validation_flags: ValidationFlags
    workflow_status: IntakeStatus = IntakeStatus.DRAFT
    created_at: datetime
    next_step: str = "approve"


class IntakeApproveResponse(BaseModel):
    """Response from intake approval."""
    status: str = "success"
    intake_id: str
    approved_by: str
    approved_at: datetime
    inserted_modules: List[str]  # module_codes inserted
    snapshot_id: Optional[str] = None
    snapshot_version: Optional[str] = None


class IntakeRejectResponse(BaseModel):
    """Response from intake rejection."""
    status: str = "success"
    intake_id: str
    rejection_reason: str
    workflow_status: IntakeStatus = IntakeStatus.REJECTED


class IntakeListItem(BaseModel):
    """Intake summary for list views."""
    intake_id: str
    supplier: str
    product_url: str
    product_name: Optional[str] = None
    status: IntakeStatus
    validation_warnings: int = 0
    validation_blockers: int = 0
    created_at: datetime
    updated_at: datetime


class IntakeListResponse(BaseModel):
    """Response for listing intakes."""
    status: str = "success"
    total: int
    intakes: List[IntakeListItem]


class IntakeDetailResponse(BaseModel):
    """Full intake details."""
    status: str = "success"
    intake_id: str
    supplier: str
    product_url: str
    supplier_payload: Optional[Dict[str, Any]] = None
    parsed_payload: Optional[ParsedPayload] = None
    draft_modules: List[DraftModule] = Field(default_factory=list)
    draft_copy: Optional[DraftCopy] = None
    validation_flags: ValidationFlags
    workflow_status: IntakeStatus
    created_at: datetime
    updated_at: datetime
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None


class SnapshotResponse(BaseModel):
    """Response for snapshot operations."""
    status: str = "success"
    snapshot_id: str
    version_tag: str
    module_count: int
    generated_files: Dict[str, str]
    created_at: datetime
    generated_by: str
