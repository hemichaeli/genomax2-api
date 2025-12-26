"""
GenoMAX2 Brain API v1.2.0
Full brain pipeline with Orchestrate, Compose, and Pipeline integration.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime

from app.brain.orchestrate import run_orchestrate, OrchestrateStatus
from app.brain.compose import (
    PainpointInput,
    LifestyleInput,
    Intent,
    compose,
    ComposeResult,
    compose_result_to_dict
)
from app.brain.pipeline import (
    run_brain,
    run_brain_pipeline,
    BrainPipelineInput,
    BrainPipelineResult,
    brain_result_to_dict
)
from app.shared.hashing import canonicalize_and_hash


# ============================================
# Router Setup
# ============================================

brain_router = APIRouter(prefix="/api/v1/brain", tags=["Brain"])


# ============================================
# Request/Response Models
# ============================================

class OrchestrateRequest(BaseModel):
    user_id: str
    signal_data: Dict[str, Any]
    signal_hash: Optional[str] = None
    
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
                        "ferritin": 45,
                        "magnesium": 1.9
                    }
                },
                "signal_hash": None
            }
        }


class PainpointInputModel(BaseModel):
    id: str
    severity: int = Field(ge=1, le=3, default=2)


class LifestyleInputModel(BaseModel):
    sleep_hours: float = Field(ge=0, le=12, default=7)
    sleep_quality: int = Field(ge=1, le=10, default=7)
    stress_level: int = Field(ge=1, le=10, default=5)
    activity_level: str = Field(default="moderate", pattern="^(sedentary|light|moderate|high)$")
    caffeine_intake: str = Field(default="low", pattern="^(none|low|medium|high)$")
    alcohol_intake: str = Field(default="none", pattern="^(none|low|medium|high)$")
    work_schedule: str = Field(default="day", pattern="^(day|night|rotating)$")
    meals_per_day: int = Field(ge=0, le=5, default=3)
    sugar_intake: str = Field(default="low", pattern="^(low|medium|high)$")
    smoking: bool = False


class BrainPipelineRequest(BaseModel):
    user_id: str
    signal_data: Dict[str, Any]
    painpoints: Optional[List[PainpointInputModel]] = None
    lifestyle: Optional[LifestyleInputModel] = None
    goals: Optional[List[str]] = None
    signal_hash: Optional[str] = None
    
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
                },
                "painpoints": [
                    {"id": "fatigue", "severity": 2},
                    {"id": "sleep_issues", "severity": 3}
                ],
                "lifestyle": {
                    "sleep_hours": 5,
                    "sleep_quality": 4,
                    "stress_level": 8,
                    "activity_level": "sedentary",
                    "caffeine_intake": "high",
                    "alcohol_intake": "low",
                    "work_schedule": "day",
                    "meals_per_day": 2,
                    "sugar_intake": "high",
                    "smoking": False
                },
                "goals": ["energy", "sleep"]
            }
        }


class ComposeOnlyRequest(BaseModel):
    painpoints: Optional[List[PainpointInputModel]] = None
    lifestyle: Optional[LifestyleInputModel] = None
    goals: Optional[List[str]] = None
    blood_blocks: Optional[Dict[str, Any]] = None


# ============================================
# Endpoints
# ============================================

@brain_router.get("/health")
async def brain_health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "brain",
        "version": "brain_1.2.0"
    }


@brain_router.get("/info")
async def brain_info():
    """API information"""
    return {
        "name": "GenoMAX2 Brain",
        "version": "brain_1.2.0",
        "description": "Full brain pipeline for personalized supplementation",
        "phases": [
            {"name": "orchestrate", "status": "active", "description": "Signal validation and routing constraints"},
            {"name": "compose", "status": "active", "description": "Painpoints + Lifestyle → Prioritized Intents"},
            {"name": "route", "status": "planned", "description": "Map intents to SKUs"},
            {"name": "check-interactions", "status": "planned", "description": "Drug interaction verification"},
            {"name": "finalize", "status": "planned", "description": "Build final protocol"}
        ],
        "endpoints": {
            "POST /run": "Full pipeline (recommended)",
            "POST /orchestrate": "Phase 1 only",
            "POST /compose": "Phase 2 only (testing)"
        }
    }


@brain_router.post("/run")
async def run_full_pipeline(request: BrainPipelineRequest):
    """
    🧠 Full Brain Pipeline
    
    Runs the complete brain pipeline:
    1. Orchestrate: Bloodwork → Routing Constraints
    2. Compose: Painpoints + Lifestyle + Goals → Prioritized Intents
    
    Blood constraints ALWAYS override painpoints and lifestyle.
    
    Returns prioritized intents ready for routing phase.
    """
    # Get DB connection if available
    db_conn = None
    try:
        from api_server import get_db
        db_conn = get_db()
    except:
        pass
    
    # Prepare signal data with user_id
    signal_data = request.signal_data.copy()
    signal_data["user_id"] = request.user_id
    
    # Convert Pydantic models to dicts
    painpoints = None
    if request.painpoints:
        painpoints = [{"id": p.id, "severity": p.severity} for p in request.painpoints]
    
    lifestyle = None
    if request.lifestyle:
        lifestyle = request.lifestyle.model_dump()
    
    # Run full pipeline
    result = run_brain(
        signal_data=signal_data,
        painpoints=painpoints,
        lifestyle=lifestyle,
        goals=request.goals,
        signal_hash=request.signal_hash,
        db_conn=db_conn
    )
    
    # Close DB connection
    if db_conn:
        try:
            db_conn.close()
        except:
            pass
    
    # Handle errors
    if result.error and "validation" in result.status.lower():
        raise HTTPException(status_code=400, detail=result.error)
    
    if result.error and "immutability" in result.status.lower():
        raise HTTPException(status_code=409, detail=result.error)
    
    # Return success response
    return {
        "run_id": result.run_id,
        "status": result.status,
        "phase": "compose",
        "signal_hash": result.signal_hash,
        
        # Orchestrate outputs
        "routing_constraints": result.routing_constraints,
        "assessment_context": result.assessment_context,
        
        # Compose outputs
        "intents": result.intents,
        "painpoints_applied": result.painpoints_applied,
        "lifestyle_rules_applied": result.lifestyle_rules_applied,
        "confidence_adjustments": result.confidence_adjustments,
        
        # Next steps
        "next_phase": "route",
        "audit": result.audit
    }


@brain_router.post("/orchestrate")
async def orchestrate(request: OrchestrateRequest):
    """
    Phase 1: Orchestrate
    
    Validates bloodwork signal, verifies immutability, and generates routing constraints.
    
    Returns routing_constraints that tell compose phase what is:
    - blocked: Cannot include this ingredient class
    - caution: Include with warning
    - required: Must include this ingredient class
    
    Does NOT return: SKUs, dosages, or recommendations.
    """
    db_conn = None
    try:
        from api_server import get_db
        db_conn = get_db()
    except:
        pass
    
    # Add user_id to signal_data
    signal_data = request.signal_data.copy()
    signal_data["user_id"] = request.user_id
    
    result = run_orchestrate(
        signal_data=signal_data,
        provided_hash=request.signal_hash,
        db_conn=db_conn
    )
    
    if db_conn:
        try:
            db_conn.close()
        except:
            pass
    
    if result.status == OrchestrateStatus.VALIDATION_ERROR:
        raise HTTPException(status_code=400, detail=result.error)
    
    if result.status == OrchestrateStatus.IMMUTABILITY_VIOLATION:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "IMMUTABILITY_VIOLATION",
                "message": result.error,
                "expected_hash": result.audit.get("expected_hash"),
                "computed_hash": result.audit.get("computed_hash")
            }
        )
    
    return {
        "run_id": result.run_id,
        "status": result.status.value,
        "phase": "orchestrate",
        "signal_hash": result.signal_hash,
        "routing_constraints": result.routing_constraints,
        "override_allowed": result.override_allowed,
        "assessment_context": result.assessment_context,
        "next_phase": "compose",
        "audit": result.audit
    }


@brain_router.post("/compose")
async def compose_only(request: ComposeOnlyRequest):
    """
    Phase 2: Compose (Standalone)
    
    For testing compose phase independently.
    In production, use /run for full pipeline.
    
    Takes painpoints, lifestyle, goals, and optional blood_blocks.
    Returns prioritized intents.
    """
    # Parse inputs
    painpoints = None
    if request.painpoints:
        painpoints = [
            PainpointInput(id=p.id, severity=p.severity) 
            for p in request.painpoints
        ]
    
    lifestyle = None
    if request.lifestyle:
        data = request.lifestyle.model_dump()
        lifestyle = LifestyleInput(**data)
    
    goal_intents = None
    if request.goals:
        from app.brain.pipeline import parse_goals_to_intents
        goal_intents = parse_goals_to_intents(request.goals)
    
    # Run compose
    result = compose(
        painpoints_input=painpoints,
        lifestyle_input=lifestyle,
        goal_intents=goal_intents,
        blood_blocks=request.blood_blocks
    )
    
    return {
        "phase": "compose",
        "intents": [
            {
                "id": i.id,
                "priority": i.priority,
                "source": i.source,
                "confidence": i.confidence,
                "applied_modifiers": i.applied_modifiers
            }
            for i in result.intents
        ],
        "painpoints_applied": result.painpoints_applied,
        "lifestyle_rules_applied": result.lifestyle_rules_applied,
        "confidence_adjustments": result.confidence_adjustments,
        "audit_log": result.audit_log
    }


@brain_router.get("/painpoints")
async def list_painpoints():
    """List available painpoints with their mappings."""
    from app.brain.compose import load_painpoints_dictionary
    
    try:
        dictionary = load_painpoints_dictionary()
        return {
            "count": len(dictionary),
            "painpoints": [
                {
                    "id": key,
                    "label": val.get("label"),
                    "mapped_intents": list(val.get("mapped_intents", {}).keys())
                }
                for key, val in dictionary.items()
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load painpoints: {str(e)}")


@brain_router.get("/lifestyle-schema")
async def get_lifestyle_schema():
    """Get the lifestyle questionnaire schema."""
    return {
        "questions": [
            {"field": "sleep_hours", "label": "Average sleep per night", "type": "number", "min": 0, "max": 12},
            {"field": "sleep_quality", "label": "Sleep quality (self-rated)", "type": "scale", "min": 1, "max": 10},
            {"field": "stress_level", "label": "Stress level", "type": "scale", "min": 1, "max": 10},
            {"field": "activity_level", "label": "Physical activity level", "type": "enum", "options": ["sedentary", "light", "moderate", "high"]},
            {"field": "caffeine_intake", "label": "Caffeine intake", "type": "enum", "options": ["none", "low", "medium", "high"]},
            {"field": "alcohol_intake", "label": "Alcohol consumption", "type": "enum", "options": ["none", "low", "medium", "high"]},
            {"field": "work_schedule", "label": "Work schedule", "type": "enum", "options": ["day", "night", "rotating"]},
            {"field": "meals_per_day", "label": "Meals per day", "type": "number", "min": 0, "max": 5},
            {"field": "sugar_intake", "label": "Added sugar intake", "type": "enum", "options": ["low", "medium", "high"]},
            {"field": "smoking", "label": "Smoking", "type": "boolean"}
        ]
    }


@brain_router.get("/orchestrate/{run_id}")
async def get_orchestrate_result(run_id: str):
    """Retrieve a previous orchestrate result by run_id"""
    raise HTTPException(
        status_code=501,
        detail="Run retrieval not yet implemented. Use POST endpoints."
    )
