"""
GenoMAX2 Brain API v1.3.0
Full brain pipeline with Orchestrate, Compose, and Pipeline integration.

v1.3.0: Added Bloodwork Handoff Integration (Strict Mode)
- POST /orchestrate/v2 with bloodwork_input support
- Strict mode: 503 on bloodwork unavailability
"""

from fastapi import APIRouter, HTTPException, Depends, Response
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
from app.brain.orchestrate_v2_bloodwork import (
    BloodworkInputV2,
    MarkerInput,
    orchestrate_with_bloodwork_input,
    build_bloodwork_error_response
)
from app.brain.bloodwork_handoff import (
    BloodworkHandoffException,
    BloodworkHandoffError
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


class MarkerInputModel(BaseModel):
    """Single biomarker input for V2 endpoint."""
    code: str = Field(..., description="Marker code (e.g., 'ferritin', 'vitamin_d')")
    value: float = Field(..., description="Numeric value")
    unit: str = Field(..., description="Unit of measurement (e.g., 'ng/mL')")


class BloodworkInputModel(BaseModel):
    """Bloodwork input for orchestrate/v2 - calls Bloodwork Engine directly."""
    markers: List[MarkerInputModel] = Field(
        ..., 
        min_items=1,
        description="Array of biomarker readings"
    )
    lab_profile: str = Field(
        default="GLOBAL_CONSERVATIVE",
        description="Lab profile for reference ranges"
    )
    sex: Optional[str] = Field(
        default=None,
        description="Biological sex (male/female) for sex-specific ranges"
    )
    age: Optional[int] = Field(
        default=None,
        ge=0,
        le=150,
        description="Age in years"
    )


class OrchestrateV2Request(BaseModel):
    """
    Request model for orchestrate/v2 endpoint.
    
    Supports two modes:
    1. bloodwork_signal: Pre-computed bloodwork signal (legacy)
    2. bloodwork_input: Raw markers to send to Bloodwork Engine (recommended)
    
    If bloodwork_input is provided, it takes precedence and will call
    the Bloodwork Engine to process markers.
    """
    bloodwork_signal: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Pre-computed bloodwork signal (legacy mode)"
    )
    bloodwork_input: Optional[BloodworkInputModel] = Field(
        default=None,
        description="Raw markers to send to Bloodwork Engine (recommended)"
    )
    assessment_context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Assessment context (gender, goals, etc.)"
    )
    selected_goals: List[str] = Field(
        default_factory=list,
        description="User-selected goals"
    )
    verify_hash: bool = Field(
        default=False,
        description="Enable hash verification for immutability checks"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "bloodwork_input": {
                    "markers": [
                        {"code": "ferritin", "value": 400, "unit": "ng/mL"},
                        {"code": "vitamin_d", "value": 35, "unit": "ng/mL"},
                        {"code": "b12", "value": 500, "unit": "pg/mL"}
                    ],
                    "lab_profile": "GLOBAL_CONSERVATIVE",
                    "sex": "male",
                    "age": 35
                },
                "selected_goals": ["energy", "sleep"],
                "assessment_context": {"gender": "male"}
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
        "version": "brain_1.3.0",
        "bloodwork_handoff": "strict_mode"
    }


@brain_router.get("/info")
async def brain_info():
    """API information"""
    return {
        "name": "GenoMAX2 Brain",
        "version": "brain_1.3.0",
        "description": "Full brain pipeline for personalized supplementation",
        "phases": [
            {"name": "orchestrate", "status": "active", "description": "Signal validation and routing constraints"},
            {"name": "orchestrate/v2", "status": "active", "description": "With Bloodwork Engine integration (STRICT MODE)"},
            {"name": "compose", "status": "active", "description": "Painpoints + Lifestyle → Prioritized Intents"},
            {"name": "route", "status": "planned", "description": "Map intents to SKUs"},
            {"name": "check-interactions", "status": "planned", "description": "Drug interaction verification"},
            {"name": "finalize", "status": "planned", "description": "Build final protocol"}
        ],
        "endpoints": {
            "POST /run": "Full pipeline (recommended)",
            "POST /orchestrate": "Phase 1 only (legacy)",
            "POST /orchestrate/v2": "Phase 1 with Bloodwork Engine integration",
            "POST /compose": "Phase 2 only (testing)"
        },
        "bloodwork_handoff": {
            "mode": "strict",
            "description": "Blood does not negotiate. Unavailability = 503 hard abort."
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
    Phase 1: Orchestrate (Legacy)
    
    Validates bloodwork signal, verifies immutability, and generates routing constraints.
    
    Returns routing_constraints that tell compose phase what is:
    - blocked: Cannot include this ingredient class
    - caution: Include with warning
    - required: Must include this ingredient class
    
    Does NOT return: SKUs, dosages, or recommendations.
    
    NOTE: For new integrations, use POST /orchestrate/v2 with bloodwork_input.
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


@brain_router.post("/orchestrate/v2")
async def orchestrate_v2(request: OrchestrateV2Request, response: Response):
    """
    Phase 1: Orchestrate V2 with Bloodwork Engine Integration
    
    ⚠️ STRICT MODE: Blood does not negotiate.
    
    Supports two modes:
    1. bloodwork_input (RECOMMENDED): Sends raw markers to Bloodwork Engine
    2. bloodwork_signal (LEGACY): Uses pre-computed signal
    
    When bloodwork_input is provided:
    - Calls POST /api/v1/bloodwork/process internally
    - Validates handoff against JSON Schema
    - Persists full handoff to decision_outputs
    - Returns merged routing constraints
    
    Error handling:
    - Bloodwork unavailable → 503 BLOODWORK_UNAVAILABLE (hard abort)
    - Invalid handoff schema → 502 BLOODWORK_INVALID_HANDOFF (hard abort)
    - No graceful degradation permitted
    """
    import uuid
    
    run_id = str(uuid.uuid4())
    
    # Get DB connection if available
    db_conn = None
    try:
        from api_server import get_db
        db_conn = get_db()
    except:
        pass
    
    # MODE 1: bloodwork_input provided - call Bloodwork Engine
    if request.bloodwork_input is not None:
        try:
            # Convert to internal model
            bloodwork_input = BloodworkInputV2(
                markers=[
                    MarkerInput(code=m.code, value=m.value, unit=m.unit)
                    for m in request.bloodwork_input.markers
                ],
                lab_profile=request.bloodwork_input.lab_profile,
                sex=request.bloodwork_input.sex,
                age=request.bloodwork_input.age
            )
            
            # Run integration with bloodwork engine
            result = orchestrate_with_bloodwork_input(
                bloodwork_input=bloodwork_input,
                run_id=run_id,
                db_conn=db_conn
            )
            
            # Close DB connection
            if db_conn:
                try:
                    db_conn.close()
                except:
                    pass
            
            return {
                "run_id": run_id,
                "status": "success",
                "phase": "orchestrate_v2",
                "mode": "bloodwork_input",
                "routing_constraints": result.merged_constraints,
                "bloodwork_handoff": {
                    "version": result.handoff.handoff_version,
                    "source": result.handoff.source,
                    "signal_flags": result.handoff.output.get("signal_flags", []),
                    "safety_gates_triggered": len(result.handoff.output.get("safety_gates", [])),
                    "unknown_biomarkers": result.handoff.output.get("unknown_biomarkers", [])
                },
                "selected_goals": request.selected_goals,
                "assessment_context": request.assessment_context,
                "next_phase": "compose",
                "audit": {
                    "input_hash": result.handoff.audit.get("input_hash"),
                    "output_hash": result.persistence_data["output_hash"],
                    "ruleset_version": result.handoff.audit.get("ruleset_version"),
                    "processed_at": result.handoff.audit.get("processed_at")
                }
            }
            
        except BloodworkHandoffException as e:
            # Close DB connection on error
            if db_conn:
                try:
                    db_conn.close()
                except:
                    pass
            
            # STRICT MODE: Return error with appropriate HTTP code
            error_response = build_bloodwork_error_response(e)
            response.status_code = e.http_code
            return error_response
    
    # MODE 2: bloodwork_signal provided (legacy mode)
    elif request.bloodwork_signal is not None:
        # Fall back to legacy orchestrate logic
        signal_data = request.bloodwork_signal.copy()
        signal_data["user_id"] = run_id
        
        result = run_orchestrate(
            signal_data=signal_data,
            db_conn=db_conn
        )
        
        if db_conn:
            try:
                db_conn.close()
            except:
                pass
        
        if result.status != OrchestrateStatus.SUCCESS:
            raise HTTPException(status_code=400, detail=result.error)
        
        return {
            "run_id": run_id,
            "status": "success",
            "phase": "orchestrate_v2",
            "mode": "bloodwork_signal",
            "routing_constraints": result.routing_constraints,
            "selected_goals": request.selected_goals,
            "assessment_context": request.assessment_context,
            "next_phase": "compose",
            "audit": result.audit
        }
    
    # Neither provided - error
    else:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "MISSING_INPUT",
                "message": "Either bloodwork_input or bloodwork_signal must be provided"
            }
        )


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
