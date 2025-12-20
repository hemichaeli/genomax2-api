"""
GenoMAX2 Brain API Models
Pydantic models for request/response validation.
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class ConstraintType(str, Enum):
    BLOCKED = "blocked"
    CAUTION = "caution"
    REQUIRED = "required"


class Severity(str, Enum):
    HARD = "hard"  # Cannot be overridden
    SOFT = "soft"  # Can be overridden with warning


class MarkerStatus(str, Enum):
    DEFICIENT = "deficient"
    SUBOPTIMAL = "suboptimal"
    OPTIMAL = "optimal"
    ELEVATED = "elevated"


class RoutingConstraint(BaseModel):
    """A routing constraint derived from bloodwork"""
    ingredient_class: str
    constraint_type: ConstraintType
    reason: str
    source_marker: str
    source_value: float
    severity: Severity


class MarkerAnalysis(BaseModel):
    """Analysis result for a single marker"""
    marker: str
    value: float
    unit: str
    status: MarkerStatus


class AssessmentContext(BaseModel):
    """Context built from bloodwork analysis"""
    user_id: str
    gender: str = "unknown"
    test_date: Optional[str] = None
    lab_source: Optional[str] = None
    markers_analyzed: int
    summary: Dict[str, int]
    deficient: List[MarkerAnalysis] = []
    suboptimal: List[MarkerAnalysis] = []
    optimal: List[MarkerAnalysis] = []
    elevated: List[MarkerAnalysis] = []


class OrchestrateRequest(BaseModel):
    """Request to orchestrate phase"""
    user_id: str = Field(..., description="Unique user identifier")
    signal_data: Dict[str, Any] = Field(..., description="Bloodwork signal with markers")
    signal_hash: Optional[str] = Field(None, description="Hash for immutability verification")
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user-123",
                "signal_data": {
                    "gender": "male",
                    "test_date": "2025-01-15",
                    "markers": {
                        "vitamin_d": 25,
                        "b12": 350,
                        "ferritin": 45
                    }
                }
            }
        }


class OrchestrateResponse(BaseModel):
    """Response from orchestrate phase"""
    run_id: str
    status: str
    phase: str = "orchestrate"
    signal_hash: str
    routing_constraints: List[Dict[str, Any]]
    override_allowed: bool
    assessment_context: Dict[str, Any]
    next_phase: str = "compose"
    audit: Dict[str, Any]


class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
