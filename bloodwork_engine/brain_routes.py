"""
GenoMAX² Brain Pipeline FastAPI Routes
======================================
API endpoints for Brain orchestrator operations.

Endpoints:
- POST /brain/run - Execute Brain pipeline with bloodwork data
- GET /brain/run/{run_id} - Get Brain run status/results
- POST /brain/evaluate - Quick deficiency evaluation (no module scoring)
- GET /brain/config - Get Brain configuration (thresholds, goals)
- GET /brain/health - Health check

Version: 1.1.0
"""

import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel, Field

from .brain_orchestrator import (
    get_orchestrator,
    detect_deficiencies,
    BIOMARKER_DEFICIENCY_THRESHOLDS,
    LIFECYCLE_RECOMMENDATIONS,
    BrainRunStatus,
    DeficiencyLevel
)
from .bloodwork_brain import (
    NormalizedMarker,
    BloodworkCanonical,
    evaluate_safety_gates,
    create_canonical_handoff
)

router = APIRouter(prefix="/api/v1/brain", tags=["brain"])

# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class BrainRunRequest(BaseModel):
    """Request to execute Brain pipeline."""
    submission_id: str = Field(..., description="Bloodwork submission ID")
    user_id: str = Field(..., description="User ID")
    
    # Markers from bloodwork
    markers: List[Dict[str, Any]] = Field(
        ..., 
        description="Normalized markers from Bloodwork Engine"
    )
    
    # Safety constraints from Bloodwork Engine
    blocked_ingredients: List[str] = Field(
        default_factory=list,
        description="Ingredients blocked by safety gates"
    )
    caution_ingredients: List[str] = Field(
        default_factory=list,
        description="Ingredients requiring caution"
    )
    
    # User context
    gender: str = Field(..., description="male or female")
    age: Optional[int] = Field(None, description="User age in years")
    lifecycle_phase: Optional[str] = Field(
        None,
        description="Lifecycle phase: pregnant, breastfeeding, perimenopause, postmenopausal, athletic"
    )
    goals: List[str] = Field(
        default_factory=list,
        description="Health goals: energy, sleep, stress, immunity, heart_health, brain_health, etc."
    )
    excluded_ingredients: List[str] = Field(
        default_factory=list,
        description="User-requested ingredient exclusions (allergies, preferences)"
    )
    
    # Quality metrics
    confidence_score: float = Field(
        default=1.0,
        ge=0.0, le=1.0,
        description="Bloodwork data confidence score"
    )

class DeficiencyInfo(BaseModel):
    """Detected biomarker deficiency."""
    marker_code: str
    value: float
    unit: str
    level: str
    distance_from_optimal: float
    target_ingredients: List[str]
    priority_weight: float

class ModuleRecommendation(BaseModel):
    """Recommended supplement module."""
    module_id: int
    module_name: str
    category: str
    final_score: float
    matched_deficiencies: List[str]
    reasons: List[str]
    caution: bool = False
    caution_reasons: List[str] = []

class BlockedModule(BaseModel):
    """Module blocked by safety gates."""
    module_id: int
    module_name: str
    category: str
    block_reason: str

class BrainRunResponse(BaseModel):
    """Response from Brain pipeline execution."""
    run_id: str
    submission_id: str
    user_id: str
    status: str
    
    # Results
    markers_processed: int
    priority_markers_found: int
    deficiencies: List[DeficiencyInfo]
    recommended_modules: List[ModuleRecommendation]
    blocked_modules: List[BlockedModule]
    
    # Safety summary
    blocked_ingredients: List[str]
    caution_ingredients: List[str]
    safety_gates_triggered: int
    
    # Context
    gender: str
    lifecycle_phase: Optional[str]
    goals: List[str]
    
    # Metadata
    processing_time_ms: int
    created_at: datetime
    completed_at: Optional[datetime]
    error_message: Optional[str] = None

class QuickEvaluateRequest(BaseModel):
    """Request for quick deficiency evaluation."""
    markers: List[Dict[str, Any]]
    gender: str

class QuickEvaluateResponse(BaseModel):
    """Response from quick deficiency evaluation."""
    deficiencies: List[DeficiencyInfo]
    deficiency_count: int
    severe_count: int
    priority_markers_analyzed: int

class BrainConfigResponse(BaseModel):
    """Brain configuration information."""
    biomarker_thresholds: Dict[str, Any]
    lifecycle_phases: List[str]
    supported_goals: List[str]
    version: str

class BrainHealthResponse(BaseModel):
    """Brain health check response."""
    status: str
    database_connected: bool
    modules_available: int
    version: str
    timestamp: datetime

# =============================================================================
# ENDPOINTS
# =============================================================================

@router.post("/run", response_model=BrainRunResponse)
async def execute_brain_run(request: BrainRunRequest):
    """
    Execute Brain pipeline with bloodwork data.
    
    This is the main entry point for bloodwork-to-supplement recommendations.
    
    Flow:
    1. Receive BloodworkCanonical data
    2. Detect biomarker deficiencies
    3. Score supplement modules
    4. Apply safety constraints
    5. Return ranked recommendations
    
    "Blood does not negotiate" - blocked ingredients are never recommended.
    """
    try:
        orchestrator = await get_orchestrator()
        
        result = await orchestrator.run(
            submission_id=request.submission_id,
            user_id=request.user_id,
            markers=request.markers,
            blocked_ingredients=request.blocked_ingredients,
            caution_ingredients=request.caution_ingredients,
            gender=request.gender,
            age=request.age,
            lifecycle_phase=request.lifecycle_phase,
            goals=request.goals,
            excluded_ingredients=request.excluded_ingredients,
            confidence_score=request.confidence_score
        )
        
        # Convert result to response format
        deficiencies = [
            DeficiencyInfo(
                marker_code=d.marker_code,
                value=d.value,
                unit=d.unit,
                level=d.level.value,
                distance_from_optimal=d.distance_from_optimal,
                target_ingredients=d.target_ingredients,
                priority_weight=d.priority_weight
            )
            for d in result.deficiencies_detected
        ]
        
        recommended = [
            ModuleRecommendation(
                module_id=m.module_id,
                module_name=m.module_name,
                category=m.category,
                final_score=m.final_score,
                matched_deficiencies=m.matched_deficiencies,
                reasons=m.reasons,
                caution=m.caution,
                caution_reasons=m.caution_reasons
            )
            for m in result.recommended_modules
        ]
        
        blocked = [
            BlockedModule(
                module_id=m.module_id,
                module_name=m.module_name,
                category=m.category,
                block_reason=m.block_reason or "Blocked by safety gate"
            )
            for m in result.blocked_modules
        ]
        
        return BrainRunResponse(
            run_id=result.run_id,
            submission_id=result.submission_id,
            user_id=result.user_id,
            status=result.status.value,
            markers_processed=result.markers_processed,
            priority_markers_found=result.priority_markers_found,
            deficiencies=deficiencies,
            recommended_modules=recommended,
            blocked_modules=blocked,
            blocked_ingredients=result.blocked_ingredients,
            caution_ingredients=result.caution_ingredients,
            safety_gates_triggered=result.safety_gates_triggered,
            gender=result.gender,
            lifecycle_phase=result.lifecycle_phase,
            goals=result.goals,
            processing_time_ms=result.processing_time_ms,
            created_at=result.created_at,
            completed_at=result.completed_at,
            error_message=result.error_message
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Brain pipeline execution failed: {str(e)}"
        )

@router.get("/run/{run_id}")
async def get_brain_run_status(run_id: str):
    """
    Get status and results of a Brain run.
    
    Returns full run details including:
    - Detected deficiencies
    - Recommended modules with scores
    - Blocked modules with reasons
    - Safety summary
    """
    try:
        orchestrator = await get_orchestrator()
        result = await orchestrator.get_run_status(run_id)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Brain run not found: {run_id}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving Brain run: {str(e)}"
        )

@router.post("/evaluate", response_model=QuickEvaluateResponse)
async def quick_deficiency_evaluation(request: QuickEvaluateRequest):
    """
    Quick deficiency evaluation without full module scoring.
    
    Useful for:
    - Immediate feedback after bloodwork upload
    - Preview of deficiencies before full Brain run
    - Testing/debugging marker normalization
    """
    try:
        deficiencies = detect_deficiencies(request.markers, request.gender)
        
        deficiency_infos = [
            DeficiencyInfo(
                marker_code=d.marker_code,
                value=d.value,
                unit=d.unit,
                level=d.level.value,
                distance_from_optimal=d.distance_from_optimal,
                target_ingredients=d.target_ingredients,
                priority_weight=d.priority_weight
            )
            for d in deficiencies
        ]
        
        severe_count = sum(
            1 for d in deficiencies 
            if d.level in [DeficiencyLevel.DEFICIENT, DeficiencyLevel.ELEVATED]
        )
        
        priority_analyzed = sum(
            1 for m in request.markers
            if m.get("code") in BIOMARKER_DEFICIENCY_THRESHOLDS
        )
        
        return QuickEvaluateResponse(
            deficiencies=deficiency_infos,
            deficiency_count=len(deficiencies),
            severe_count=severe_count,
            priority_markers_analyzed=priority_analyzed
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Deficiency evaluation failed: {str(e)}"
        )

@router.post("/canonical-handoff")
async def create_and_run_brain(
    submission_id: str,
    user_id: str,
    markers: List[NormalizedMarker],
    gender: str,
    age: Optional[int] = None,
    lifecycle_phase: Optional[str] = None,
    goals: List[str] = None,
    excluded_ingredients: List[str] = None,
    source: str = "api"
):
    """
    Complete pipeline: Create canonical handoff and execute Brain run.
    
    This is the full integration endpoint that:
    1. Creates BloodworkCanonical from markers
    2. Evaluates safety gates
    3. Executes Brain pipeline
    4. Returns recommendations
    
    Use this when you have raw normalized markers and want
    complete end-to-end processing.
    """
    try:
        # Step 1: Create canonical handoff
        canonical = await create_canonical_handoff(
            submission_id=submission_id,
            user_id=user_id,
            source=source,
            markers=markers
        )
        
        # Step 2: Execute Brain pipeline
        orchestrator = await get_orchestrator()
        
        result = await orchestrator.run(
            submission_id=submission_id,
            user_id=user_id,
            markers=[m.dict() for m in markers],
            blocked_ingredients=canonical.blocked_ingredients,
            caution_ingredients=canonical.caution_ingredients,
            gender=gender,
            age=age,
            lifecycle_phase=lifecycle_phase,
            goals=goals or [],
            excluded_ingredients=excluded_ingredients or [],
            confidence_score=canonical.confidence_score
        )
        
        return {
            "canonical": canonical.dict(),
            "brain_result": {
                "run_id": result.run_id,
                "status": result.status.value,
                "deficiencies_detected": len(result.deficiencies_detected),
                "recommended_modules": len(result.recommended_modules),
                "blocked_modules": len(result.blocked_modules),
                "processing_time_ms": result.processing_time_ms
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Canonical handoff failed: {str(e)}"
        )

@router.get("/config", response_model=BrainConfigResponse)
async def get_brain_configuration():
    """
    Get Brain configuration including biomarker thresholds and supported options.
    
    Useful for:
    - Frontend UI configuration
    - Understanding scoring parameters
    - Documentation
    """
    return BrainConfigResponse(
        biomarker_thresholds={
            code: {
                "target_ingredients": config["target_ingredients"],
                "priority_weight": config["priority_weight"],
                "inverse": config.get("inverse", False)
            }
            for code, config in BIOMARKER_DEFICIENCY_THRESHOLDS.items()
        },
        lifecycle_phases=list(LIFECYCLE_RECOMMENDATIONS.keys()),
        supported_goals=[
            "energy", "sleep", "stress", "immunity", 
            "heart_health", "brain_health", "bone_health",
            "skin_health", "gut_health", "muscle_recovery"
        ],
        version="1.1.0"
    )

@router.get("/health", response_model=BrainHealthResponse)
async def brain_health_check():
    """
    Brain pipeline health check.
    
    Verifies:
    - Database connectivity
    - Module availability
    - Configuration validity
    """
    try:
        orchestrator = await get_orchestrator()
        
        # Check database connection
        db_connected = False
        modules_count = 0
        
        if orchestrator.pool:
            try:
                async with orchestrator.pool.acquire() as conn:
                    # Query os_modules_v3_1 (production schema)
                    result = await conn.fetchval(
                        "SELECT COUNT(*) FROM os_modules_v3_1 WHERE governance_status = 'active' OR governance_status IS NULL"
                    )
                    modules_count = result or 0
                    db_connected = True
            except:
                pass
        
        return BrainHealthResponse(
            status="healthy" if db_connected else "degraded",
            database_connected=db_connected,
            modules_available=modules_count,
            version="1.1.0",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        return BrainHealthResponse(
            status="unhealthy",
            database_connected=False,
            modules_available=0,
            version="1.1.0",
            timestamp=datetime.utcnow()
        )

@router.get("/deficiency-thresholds")
async def list_deficiency_thresholds():
    """
    List all biomarker deficiency thresholds.
    
    Shows the exact values used for deficiency detection
    including gender-specific thresholds.
    """
    return {
        "thresholds": BIOMARKER_DEFICIENCY_THRESHOLDS,
        "count": len(BIOMARKER_DEFICIENCY_THRESHOLDS),
        "priority_markers": list(BIOMARKER_DEFICIENCY_THRESHOLDS.keys())
    }

@router.get("/lifecycle-recommendations")
async def list_lifecycle_recommendations():
    """
    List lifecycle-specific supplement recommendations.
    
    Shows required, recommended, and blocked ingredients
    for each lifecycle phase.
    """
    return {
        "phases": LIFECYCLE_RECOMMENDATIONS,
        "supported_phases": list(LIFECYCLE_RECOMMENDATIONS.keys())
    }

# =============================================================================
# INTEGRATION NOTES
# =============================================================================
"""
ROUTER INTEGRATION:
    from bloodwork_engine.brain_routes import router as brain_router
    app.include_router(brain_router)

TYPICAL FLOW:
1. Bloodwork submission (OCR or Junction) → normalized markers
2. POST /api/v1/brain/run with markers + user context
3. Brain detects deficiencies, scores modules, applies constraints
4. Response includes ranked recommendations

QUICK EVALUATION:
- POST /api/v1/brain/evaluate for deficiency preview
- No database writes, no module scoring
- Fast feedback for UI

FULL PIPELINE:
- POST /api/v1/brain/canonical-handoff for complete processing
- Creates canonical object + runs Brain
- Single endpoint for end-to-end

SAFETY ENFORCEMENT:
- Blocked ingredients from bloodwork_brain.py safety gates
- User exclusions (allergies/preferences)
- Lifecycle blocks (e.g., no vitamin A retinol during pregnancy)
- All blocks are absolute - "Blood does not negotiate"
"""
