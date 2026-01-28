"""
GenoMAX² Brain Orchestrator
===========================
Main orchestration layer connecting bloodwork to personalized protocols.

Pipeline Flow:
1. Receive BloodworkCanonical from Bloodwork Engine
2. Translate to routing constraints
3. Generate module recommendations
4. Build personalized protocol with SKUs
5. Store and return results

"Blood does not negotiate" - deterministic execution.

Version: 1.0.0
"""

import os
import uuid
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from enum import Enum

# Import pipeline components
from .constraint_translator import (
    ConstraintTranslator,
    ConstraintSet,
    translate_bloodwork_to_constraints
)
from .recommendation_engine import (
    RecommendationEngine,
    RecommendationResult,
    generate_recommendations
)
from .protocol_builder import (
    ProtocolBuilder,
    Protocol,
    build_protocol
)

router = APIRouter(prefix="/api/v1/brain", tags=["brain"])

# =============================================================================
# MODELS
# =============================================================================

class BrainRunStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    TRANSLATING = "translating"
    RECOMMENDING = "recommending"
    BUILDING = "building"
    COMPLETED = "completed"
    FAILED = "failed"

class BrainRunRequest(BaseModel):
    """Request to execute Brain pipeline."""
    submission_id: str
    user_id: str
    gender: str  # male, female
    age: Optional[int] = None
    lifecycle_phase: Optional[str] = None  # pregnant, postmenopausal, etc.
    goals: List[str] = []
    excluded_ingredients: List[str] = []
    
    # Optional: Pass bloodwork directly instead of fetching
    bloodwork_canonical: Optional[Dict[str, Any]] = None

class BrainRunResponse(BaseModel):
    """Response from Brain pipeline execution."""
    run_id: str
    submission_id: str
    user_id: str
    status: BrainRunStatus
    
    # Results (populated when completed)
    protocol: Optional[Protocol] = None
    recommendation_result: Optional[RecommendationResult] = None
    constraint_set: Optional[Dict[str, Any]] = None
    
    # Timing
    created_at: datetime
    completed_at: Optional[datetime] = None
    processing_time_ms: Optional[int] = None
    
    # Errors
    error_message: Optional[str] = None

class BrainRunSummary(BaseModel):
    """Summary of a Brain run for listing."""
    run_id: str
    submission_id: str
    user_id: str
    gender: str
    status: BrainRunStatus
    total_recommendations: int
    total_protocol_items: int
    created_at: datetime

# =============================================================================
# IN-MEMORY STORE (Production uses database)
# =============================================================================

# Temporary in-memory storage for brain runs
_brain_runs: Dict[str, Dict[str, Any]] = {}

# =============================================================================
# BRAIN ORCHESTRATOR
# =============================================================================

class BrainOrchestrator:
    """
    Main orchestration class for the Brain pipeline.
    
    Coordinates:
    1. Constraint translation
    2. Recommendation generation
    3. Protocol building
    """
    
    def __init__(self):
        self.translator = ConstraintTranslator()
        self.recommender = RecommendationEngine()
        self.builder = ProtocolBuilder()
    
    async def execute(
        self,
        bloodwork_canonical: Dict[str, Any],
        user_id: str,
        submission_id: str,
        gender: str,
        age: Optional[int] = None,
        lifecycle_phase: Optional[str] = None,
        goals: List[str] = [],
        excluded_ingredients: List[str] = []
    ) -> BrainRunResponse:
        """
        Execute the complete Brain pipeline.
        
        Args:
            bloodwork_canonical: BloodworkCanonical dict from Bloodwork Engine
            user_id: User identifier
            submission_id: Bloodwork submission ID
            gender: male/female
            age: User age (optional)
            lifecycle_phase: pregnant, postmenopausal, etc.
            goals: Health goals
            excluded_ingredients: User-excluded ingredients
        
        Returns:
            BrainRunResponse with complete results
        """
        run_id = f"BRAIN-{uuid.uuid4().hex[:12].upper()}"
        start_time = datetime.utcnow()
        
        try:
            # Step 1: Translate bloodwork to constraints
            constraint_set = self.translator.translate(
                markers=[m if isinstance(m, dict) else m.dict() for m in bloodwork_canonical.get("markers", [])],
                safety_gates=[g if isinstance(g, dict) else g.dict() for g in bloodwork_canonical.get("safety_gates", [])],
                blocked_ingredients=bloodwork_canonical.get("blocked_ingredients", []),
                caution_ingredients=bloodwork_canonical.get("caution_ingredients", []),
                user_id=user_id,
                submission_id=submission_id,
                gender=gender,
                lifecycle_phase=lifecycle_phase,
                excluded_by_user=excluded_ingredients
            )
            
            # Step 2: Generate recommendations
            recommendation_result = self.recommender.generate_recommendations(
                constraint_set=constraint_set.dict() if hasattr(constraint_set, 'dict') else constraint_set,
                markers=bloodwork_canonical.get("markers", []),
                gender=gender,
                lifecycle_phase=lifecycle_phase,
                goals=goals,
                max_recommendations=8
            )
            
            # Step 3: Build protocol
            recommendations_list = [
                r.dict() if hasattr(r, 'dict') else r 
                for r in recommendation_result.recommendations
            ]
            
            protocol = self.builder.build_protocol(
                recommendations=recommendations_list,
                constraint_set=constraint_set.dict() if hasattr(constraint_set, 'dict') else constraint_set,
                user_id=user_id,
                submission_id=submission_id,
                gender=gender
            )
            
            # Calculate processing time
            end_time = datetime.utcnow()
            processing_ms = int((end_time - start_time).total_seconds() * 1000)
            
            return BrainRunResponse(
                run_id=run_id,
                submission_id=submission_id,
                user_id=user_id,
                status=BrainRunStatus.COMPLETED,
                protocol=protocol,
                recommendation_result=recommendation_result,
                constraint_set=constraint_set.dict() if hasattr(constraint_set, 'dict') else dict(constraint_set),
                created_at=start_time,
                completed_at=end_time,
                processing_time_ms=processing_ms
            )
            
        except Exception as e:
            return BrainRunResponse(
                run_id=run_id,
                submission_id=submission_id,
                user_id=user_id,
                status=BrainRunStatus.FAILED,
                created_at=start_time,
                error_message=str(e)
            )

# Global orchestrator instance
_orchestrator = BrainOrchestrator()

# =============================================================================
# API ENDPOINTS
# =============================================================================

@router.post("/run", response_model=BrainRunResponse)
async def execute_brain_run(request: BrainRunRequest):
    """
    Execute Brain pipeline with bloodwork data.
    
    This is the main entry point for generating personalized protocols.
    
    Flow:
    1. Receives bloodwork canonical (directly or fetches from DB)
    2. Translates to routing constraints
    3. Generates module recommendations
    4. Builds protocol with SKU selection
    5. Returns complete results
    """
    # If bloodwork not provided directly, would fetch from DB
    if not request.bloodwork_canonical:
        # TODO: Fetch from database
        # submission = await db.bloodwork_submissions.find_one({"id": request.submission_id})
        # if not submission:
        #     raise HTTPException(status_code=404, detail="Submission not found")
        # bloodwork_canonical = submission.to_canonical()
        
        raise HTTPException(
            status_code=400,
            detail="bloodwork_canonical must be provided directly (DB fetch not yet implemented)"
        )
    
    # Execute pipeline
    result = await _orchestrator.execute(
        bloodwork_canonical=request.bloodwork_canonical,
        user_id=request.user_id,
        submission_id=request.submission_id,
        gender=request.gender,
        age=request.age,
        lifecycle_phase=request.lifecycle_phase,
        goals=request.goals,
        excluded_ingredients=request.excluded_ingredients
    )
    
    # Store result
    _brain_runs[result.run_id] = result.dict()
    
    return result

@router.get("/run/{run_id}", response_model=BrainRunResponse)
async def get_brain_run(run_id: str):
    """Get status and results of a Brain run."""
    if run_id not in _brain_runs:
        raise HTTPException(status_code=404, detail="Brain run not found")
    
    return BrainRunResponse(**_brain_runs[run_id])

@router.get("/runs", response_model=List[BrainRunSummary])
async def list_brain_runs(
    user_id: Optional[str] = None,
    limit: int = 20
):
    """List recent Brain runs."""
    runs = list(_brain_runs.values())
    
    if user_id:
        runs = [r for r in runs if r.get("user_id") == user_id]
    
    # Sort by created_at descending
    runs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    # Build summaries
    summaries = []
    for r in runs[:limit]:
        protocol = r.get("protocol") or {}
        rec_result = r.get("recommendation_result") or {}
        
        summaries.append(BrainRunSummary(
            run_id=r["run_id"],
            submission_id=r["submission_id"],
            user_id=r["user_id"],
            gender=protocol.get("gender", "unknown"),
            status=BrainRunStatus(r["status"]),
            total_recommendations=len(rec_result.get("recommendations", [])),
            total_protocol_items=protocol.get("total_items", 0),
            created_at=r["created_at"]
        ))
    
    return summaries

@router.post("/translate-constraints")
async def translate_constraints(
    bloodwork_canonical: Dict[str, Any],
    gender: str,
    lifecycle_phase: Optional[str] = None,
    excluded_ingredients: List[str] = []
):
    """
    Translate bloodwork to routing constraints (debug/testing endpoint).
    
    Use this to see what constraints would be generated from bloodwork
    without running the full pipeline.
    """
    constraint_set = translate_bloodwork_to_constraints(
        canonical_handoff=bloodwork_canonical,
        gender=gender,
        lifecycle_phase=lifecycle_phase,
        excluded_by_user=excluded_ingredients
    )
    
    return {
        "constraint_set": constraint_set.dict() if hasattr(constraint_set, 'dict') else constraint_set,
        "summary": {
            "total_constraints": len(constraint_set.constraints) if hasattr(constraint_set, 'constraints') else 0,
            "blocked_count": len(constraint_set.blocked_ingredients) if hasattr(constraint_set, 'blocked_ingredients') else 0,
            "boosted_count": len(constraint_set.boosted_ingredients) if hasattr(constraint_set, 'boosted_ingredients') else 0,
            "required_count": len(constraint_set.required_ingredients) if hasattr(constraint_set, 'required_ingredients') else 0,
        }
    }

@router.post("/generate-recommendations")
async def generate_recommendations_endpoint(
    constraint_set: Dict[str, Any],
    markers: List[Dict[str, Any]],
    gender: str,
    lifecycle_phase: Optional[str] = None,
    goals: List[str] = [],
    max_recommendations: int = 8
):
    """
    Generate recommendations from constraints (debug/testing endpoint).
    
    Use this to see what recommendations would be generated from constraints
    without building the full protocol.
    """
    result = generate_recommendations(
        constraint_set=constraint_set,
        markers=markers,
        gender=gender,
        lifecycle_phase=lifecycle_phase,
        goals=goals,
        max_recommendations=max_recommendations
    )
    
    return result.dict() if hasattr(result, 'dict') else result

@router.post("/build-protocol")
async def build_protocol_endpoint(
    recommendations: List[Dict[str, Any]],
    constraint_set: Dict[str, Any],
    user_id: str,
    submission_id: str,
    gender: str
):
    """
    Build protocol from recommendations (debug/testing endpoint).
    
    Use this to see what protocol would be built from recommendations
    without running the full pipeline.
    """
    protocol = build_protocol(
        recommendations=recommendations,
        constraint_set=constraint_set,
        user_id=user_id,
        submission_id=submission_id,
        gender=gender
    )
    
    return protocol.dict() if hasattr(protocol, 'dict') else protocol

@router.get("/health")
async def brain_health():
    """Brain service health check."""
    return {
        "status": "healthy",
        "service": "brain_orchestrator",
        "version": "1.0.0",
        "components": {
            "constraint_translator": "ready",
            "recommendation_engine": "ready",
            "protocol_builder": "ready"
        },
        "runs_in_memory": len(_brain_runs),
        "timestamp": datetime.utcnow().isoformat()
    }

@router.get("/capabilities")
async def brain_capabilities():
    """List Brain pipeline capabilities."""
    return {
        "pipeline_stages": [
            {
                "stage": 1,
                "name": "constraint_translation",
                "description": "Translates bloodwork signals to routing constraints",
                "inputs": ["BloodworkCanonical", "gender", "lifecycle_phase"],
                "outputs": ["ConstraintSet"]
            },
            {
                "stage": 2,
                "name": "recommendation_generation",
                "description": "Scores and ranks supplement modules",
                "inputs": ["ConstraintSet", "markers", "goals"],
                "outputs": ["RecommendationResult"]
            },
            {
                "stage": 3,
                "name": "protocol_building",
                "description": "Routes to SKUs and builds protocol",
                "inputs": ["RecommendationResult", "ConstraintSet", "gender"],
                "outputs": ["Protocol"]
            }
        ],
        "supported_genders": ["male", "female"],
        "supported_lifecycle_phases": [
            "pregnant", "breastfeeding", "postmenopausal", "perimenopause"
        ],
        "product_lines": ["MAXimo²", "MAXima²", "Universal"],
        "max_recommendations": 8,
        "safety_gates": 9,
        "priority_biomarkers": 13
    }

# =============================================================================
# INTEGRATION NOTES
# =============================================================================
"""
USAGE:

1. From Bloodwork Engine after submission:
   - Call /api/v1/brain/run with BloodworkCanonical
   - Receives complete Protocol with SKU selection

2. Debug/Testing:
   - /translate-constraints: See constraint generation
   - /generate-recommendations: See recommendation scoring
   - /build-protocol: See protocol building

3. Integration with main.py:
   from brain.brain_orchestrator import router as brain_router
   app.include_router(brain_router)

PIPELINE FLOW:
   BloodworkCanonical
        ↓
   ConstraintTranslator
        ↓
   ConstraintSet (blocked/boosted/required ingredients)
        ↓
   RecommendationEngine
        ↓
   RecommendationResult (scored modules)
        ↓
   ProtocolBuilder
        ↓
   Protocol (SKUs, dosages, schedule, Supliful order)

SAFETY:
- "Blood does not negotiate" - blocked ingredients never override
- Safety gates evaluated first (priority 1)
- Lifecycle constraints applied (priority 2)
- User exclusions respected (priority 1)
- Dosage modifiers calculated from biomarker levels
"""
