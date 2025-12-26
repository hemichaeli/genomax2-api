"""
GenoMAX² Brain Contract v1.0
Canonical Pydantic Schemas for the Resolver Layer

This module defines the stable contract between:
- Bloodwork Engine -> Resolver
- Lifestyle Engine -> Resolver
- Goals/Painpoints Engine -> Resolver
- Resolver -> Route

IMPORTANT: These schemas are versioned. Any breaking changes
require a version bump (e.g., "1.0" -> "2.0").

Usage:
    from app.brain.contracts import (
        AssessmentContext,
        RoutingConstraints,
        ProtocolIntents,
        ResolverInput,
        ResolverOutput,
    )
"""

from __future__ import annotations
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


# =============================================================================
# CONTRACT VERSION
# =============================================================================
CONTRACT_VERSION = "1.0"


# =============================================================================
# ENUMS
# =============================================================================

class GenderEnum(str, Enum):
    MALE = "male"
    FEMALE = "female"


# =============================================================================
# ASSESSMENT CONTEXT
# =============================================================================

class AssessmentContext(BaseModel):
    """
    Context from the assessment phase.
    Passed through orchestrate_v2 and used by all downstream phases.
    
    This is NOT raw bloodwork data - it's the processed context
    that informs routing decisions.
    """
    contract_version: str = Field(default=CONTRACT_VERSION, description="Schema version")
    
    # Identifiers
    protocol_id: str = Field(..., description="UUID of the protocol run")
    run_id: str = Field(..., description="UUID of the brain run")
    
    # User Context
    gender: GenderEnum = Field(..., description="Biological sex for OS environment selection")
    age: Optional[int] = Field(None, ge=0, le=120, description="Age in years")
    height_cm: Optional[float] = Field(None, ge=50, le=300, description="Height in centimeters")
    weight_kg: Optional[float] = Field(None, ge=20, le=500, description="Weight in kilograms")
    
    # Medical Context
    meds: Optional[List[str]] = Field(default_factory=list, description="Current medications (lowercase, normalized)")
    conditions: Optional[List[str]] = Field(default_factory=list, description="Known health conditions")
    allergies: Optional[List[str]] = Field(default_factory=list, description="Known allergies")
    
    # Flags
    flags: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional context flags")
    
    @field_validator('gender', mode='before')
    @classmethod
    def normalize_gender(cls, v):
        if isinstance(v, str):
            v_lower = v.lower().strip()
            if v_lower in ('male', 'm'):
                return GenderEnum.MALE
            elif v_lower in ('female', 'f'):
                return GenderEnum.FEMALE
        return v

    class Config:
        use_enum_values = True
        json_schema_extra = {
            "example": {
                "contract_version": "1.0",
                "protocol_id": "550e8400-e29b-41d4-a716-446655440000",
                "run_id": "550e8400-e29b-41d4-a716-446655440001",
                "gender": "male",
                "age": 35,
                "height_cm": 180.0,
                "weight_kg": 75.0,
                "meds": ["metformin"],
                "conditions": [],
                "allergies": [],
                "flags": {}
            }
        }


# =============================================================================
# ROUTING CONSTRAINTS
# =============================================================================

class TargetDetail(BaseModel):
    """Details about a specific target's constraint status."""
    gate_status: str = Field(..., description="allowed, blocked, or caution")
    reason: str = Field(..., description="Human-readable reason")
    blocking_biomarkers: List[str] = Field(default_factory=list)
    caution_biomarkers: List[str] = Field(default_factory=list)
    source: Optional[str] = Field(None, description="Source engine: bloodwork, lifestyle, etc.")


class RoutingConstraints(BaseModel):
    """
    Constraints that gate supplement selection.
    
    Produced by:
    - Bloodwork Engine (from biomarker analysis)
    - Lifestyle Engine (from lifestyle factors)
    - Merged by Resolver
    
    Consumed by:
    - Route phase (to filter OS modules)
    """
    contract_version: str = Field(default=CONTRACT_VERSION, description="Schema version")
    
    # Target-level constraints
    blocked_targets: List[str] = Field(default_factory=list, description="Targets that MUST NOT be supplemented")
    caution_targets: List[str] = Field(default_factory=list, description="Targets requiring caution/warning")
    allowed_targets: List[str] = Field(default_factory=list, description="Explicitly allowed targets")
    
    # Ingredient-level constraints
    blocked_ingredients: List[str] = Field(default_factory=list, description="Specific ingredients to block")
    
    # Flags
    has_critical_flags: bool = Field(default=False, description="True if any critical flags present")
    global_flags: List[str] = Field(default_factory=list, description="Global flag IDs")
    
    # Detailed constraint info
    target_details: Dict[str, TargetDetail] = Field(default_factory=dict, description="Per-target constraint details")
    
    class Config:
        json_schema_extra = {
            "example": {
                "contract_version": "1.0",
                "blocked_targets": ["iron_boost", "ferritin_elevated"],
                "caution_targets": ["high_dose_vitamin_d"],
                "allowed_targets": ["magnesium", "omega3"],
                "blocked_ingredients": ["iron", "ashwagandha"],
                "has_critical_flags": False,
                "global_flags": [],
                "target_details": {}
            }
        }


# =============================================================================
# PROTOCOL INTENTS
# =============================================================================

class ProtocolIntentItem(BaseModel):
    """
    A single supplement intent.
    
    Represents the desire to address a specific target with supplements.
    Priority determines selection order when multiple intents compete.
    """
    intent_id: str = Field(..., description="Unique intent identifier (e.g., 'magnesium_for_sleep')")
    target_id: str = Field(..., description="Target this intent addresses (e.g., 'sleep_quality')")
    priority: float = Field(..., ge=0.0, le=1.0, description="Priority score (0.0-1.0, higher = more important)")
    source_goal: Optional[str] = Field(None, description="The goal that generated this intent")
    source_painpoint: Optional[str] = Field(None, description="The painpoint that generated this intent")
    blocked: bool = Field(default=False, description="Whether this intent is blocked by constraints")
    
    class Config:
        json_schema_extra = {
            "example": {
                "intent_id": "magnesium_for_sleep",
                "target_id": "sleep_quality",
                "priority": 0.85,
                "source_goal": "sleep",
                "source_painpoint": None,
                "blocked": False
            }
        }


class LifestyleIntent(BaseModel):
    """A lifestyle intervention intent (generic for now)."""
    intent_id: str
    priority: float = Field(ge=0.0, le=1.0)
    category: Optional[str] = None
    description: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)


class NutritionIntent(BaseModel):
    """A nutrition intervention intent (generic for now)."""
    intent_id: str
    priority: float = Field(ge=0.0, le=1.0)
    category: Optional[str] = None
    description: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)


class ProtocolIntents(BaseModel):
    """
    All intents for a protocol.
    
    Produced by:
    - Goals Engine (from selected goals)
    - Painpoints Engine (from reported painpoints)
    - Merged by Resolver
    
    Consumed by:
    - Route phase (supplements only)
    - Future: MAXync² (lifestyle/nutrition)
    """
    contract_version: str = Field(default=CONTRACT_VERSION, description="Schema version")
    
    # Keep lifestyle/nutrition generic for now (list[dict])
    # Only supplements are strict because they drive SKU routing
    lifestyle: List[Dict[str, Any]] = Field(default_factory=list, description="Lifestyle intervention intents")
    nutrition: List[Dict[str, Any]] = Field(default_factory=list, description="Nutrition intervention intents")
    supplements: List[ProtocolIntentItem] = Field(default_factory=list, description="Supplement intents (strict schema)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "contract_version": "1.0",
                "lifestyle": [
                    {"intent_id": "improve_sleep_quality", "priority": 0.85}
                ],
                "nutrition": [
                    {"intent_id": "evening_carb_timing", "priority": 0.60}
                ],
                "supplements": [
                    {
                        "intent_id": "magnesium_for_sleep",
                        "target_id": "sleep_quality",
                        "priority": 0.80,
                        "source_goal": "sleep",
                        "blocked": False
                    }
                ]
            }
        }


# =============================================================================
# RESOLVER INPUT / OUTPUT
# =============================================================================

class ResolverInput(BaseModel):
    """
    Input to the Resolver.
    
    Aggregates all inputs needed for deterministic constraint
    and intent resolution.
    """
    contract_version: str = Field(default=CONTRACT_VERSION, description="Schema version")
    
    # Core context
    assessment_context: AssessmentContext
    
    # Constraints from engines
    bloodwork_constraints: RoutingConstraints = Field(
        default_factory=RoutingConstraints,
        description="Constraints from Bloodwork Engine (or mock)"
    )
    lifestyle_constraints: RoutingConstraints = Field(
        default_factory=RoutingConstraints,
        description="Constraints from Lifestyle Engine (or mock)"
    )
    
    # Raw inputs for intent generation
    raw_goals: List[str] = Field(default_factory=list, description="Selected goals from user")
    raw_painpoints: List[str] = Field(default_factory=list, description="Reported painpoints from user")
    
    # Pre-computed intents (optional, if compose already ran)
    goals_intents: Optional[ProtocolIntents] = Field(None, description="Pre-computed intents from goals")
    painpoint_intents: Optional[ProtocolIntents] = Field(None, description="Pre-computed intents from painpoints")
    
    class Config:
        json_schema_extra = {
            "example": {
                "contract_version": "1.0",
                "assessment_context": {
                    "protocol_id": "550e8400-e29b-41d4-a716-446655440000",
                    "run_id": "550e8400-e29b-41d4-a716-446655440001",
                    "gender": "male",
                    "age": 35
                },
                "bloodwork_constraints": {
                    "blocked_targets": [],
                    "caution_targets": [],
                    "blocked_ingredients": []
                },
                "lifestyle_constraints": {
                    "blocked_targets": [],
                    "caution_targets": [],
                    "blocked_ingredients": []
                },
                "raw_goals": ["sleep", "energy"],
                "raw_painpoints": ["fatigue", "brain_fog"]
            }
        }


class ResolverAudit(BaseModel):
    """Audit trail for the resolver output."""
    resolver_version: str = Field(default="1.0.0")
    resolved_at: str = Field(..., description="ISO timestamp")
    input_hash: str = Field(..., description="Hash of ResolverInput")
    output_hash: str = Field(..., description="Hash of resolved output")
    
    # Counts for verification
    bloodwork_blocked_count: int = Field(default=0)
    lifestyle_blocked_count: int = Field(default=0)
    merged_blocked_count: int = Field(default=0)
    
    goals_intents_count: int = Field(default=0)
    painpoint_intents_count: int = Field(default=0)
    merged_intents_count: int = Field(default=0)
    
    # Deduplication stats
    intents_deduplicated: int = Field(default=0, description="Number of duplicate intents removed")
    priority_conflicts_resolved: int = Field(default=0, description="Number of priority conflicts resolved")


class ResolverOutput(BaseModel):
    """
    Output from the Resolver.
    
    Contains deterministically merged constraints and intents,
    ready for the Route phase.
    """
    contract_version: str = Field(default=CONTRACT_VERSION, description="Schema version")
    
    # Identifiers
    protocol_id: str
    run_id: Optional[str] = None
    
    # Merged results
    resolved_constraints: RoutingConstraints
    resolved_intents: ProtocolIntents
    
    # Assessment context (passed through)
    assessment_context: AssessmentContext
    
    # Audit trail
    audit: ResolverAudit
    
    class Config:
        json_schema_extra = {
            "example": {
                "contract_version": "1.0",
                "protocol_id": "550e8400-e29b-41d4-a716-446655440000",
                "run_id": "550e8400-e29b-41d4-a716-446655440001",
                "resolved_constraints": {
                    "blocked_targets": ["iron_boost"],
                    "caution_targets": [],
                    "blocked_ingredients": ["iron"]
                },
                "resolved_intents": {
                    "supplements": [
                        {
                            "intent_id": "magnesium_for_sleep",
                            "target_id": "sleep_quality",
                            "priority": 0.85
                        }
                    ]
                },
                "audit": {
                    "resolver_version": "1.0.0",
                    "resolved_at": "2025-06-15T10:30:00Z",
                    "input_hash": "sha256:abc123",
                    "output_hash": "sha256:def456",
                    "merged_blocked_count": 1,
                    "merged_intents_count": 1
                }
            }
        }


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def empty_routing_constraints() -> RoutingConstraints:
    """Factory for empty routing constraints."""
    return RoutingConstraints()


def empty_protocol_intents() -> ProtocolIntents:
    """Factory for empty protocol intents."""
    return ProtocolIntents()
