"""
Brain Pipeline API Routes
Version 1.1.0

Exposes Brain orchestrator functionality through FastAPI endpoints.
Handles the Route → Compose → Confirm → Finalize phases.
"""

import logging
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field
import asyncpg
import os

# Import the brain orchestrator
from bloodwork_engine.brain_orchestrator import (
    BrainOrchestrator,
    BrainInput,
    SexType,
    ConstraintType
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/brain", tags=["Brain Pipeline"])

# =============================================================================
# Pydantic Models for API
# =============================================================================

class BiomarkerReading(BaseModel):
    """Single biomarker reading from bloodwork"""
    biomarker_code: str = Field(..., description="Biomarker code (e.g., 'VITD', 'FERRITIN')")
    value: float = Field(..., description="Measured value")
    unit: str = Field(..., description="Unit of measurement")
    reference_low: Optional[float] = Field(None, description="Reference range low")
    reference_high: Optional[float] = Field(None, description="Reference range high")

class Constraint(BaseModel):
    """Safety constraint from bloodwork engine"""
    constraint_type: str = Field(..., description="Type: BLOCK, LIMIT, CAUTION, BOOST")
    constraint_code: str = Field(..., description="Constraint identifier")
    reason: str = Field(..., description="Human-readable reason")
    affected_ingredients: List[str] = Field(default_factory=list)
    max_dose_mg: Optional[float] = Field(None, description="Maximum dose if LIMIT type")

class BloodworkHandoff(BaseModel):
    """
    Canonical handoff object from Bloodwork Engine to Brain.
    This is the contract between the two systems.
    """
    user_id: str = Field(..., description="User identifier")
    sex: str = Field(..., description="Biological sex: 'male' or 'female'")
    biomarkers: List[BiomarkerReading] = Field(..., description="All biomarker readings")
    constraints: List[Constraint] = Field(default_factory=list, description="Safety constraints")
    deficiencies: List[str] = Field(default_factory=list, description="Identified deficiencies")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    bloodwork_engine_version: str = Field(default="1.0.0")
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_123",
                "sex": "male",
                "biomarkers": [
                    {"biomarker_code": "VITD", "value": 18.0, "unit": "ng/mL", "reference_low": 30, "reference_high": 100},
                    {"biomarker_code": "FERRITIN", "value": 250.0, "unit": "ng/mL", "reference_low": 30, "reference_high": 400}
                ],
                "constraints": [
                    {"constraint_type": "BLOCK", "constraint_code": "IRON_BLOCK", "reason": "Ferritin adequate", "affected_ingredients": ["iron"]}
                ],
                "deficiencies": ["vitamin_d"],
                "timestamp": "2025-01-28T12:00:00Z",
                "bloodwork_engine_version": "1.0.0"
            }
        }

class RouteRequest(BaseModel):
    """Request for Route phase - identifies candidate modules"""
    handoff: BloodworkHandoff
    max_modules: int = Field(default=10, ge=1, le=20)

class ComposeRequest(BaseModel):
    """Request for Compose phase - builds protocol from selected modules"""
    user_id: str
    sex: str
    selected_module_ids: List[str]
    constraints: List[Constraint] = Field(default_factory=list)

class UserSelections(BaseModel):
    """User's module selections for Confirm phase"""
    confirmed_module_ids: List[str]
    rejected_module_ids: List[str] = Field(default_factory=list)
    notes: Optional[str] = None

class ConfirmRequest(BaseModel):
    """Request for Confirm phase - user confirms selections"""
    user_id: str
    sex: str
    selections: UserSelections
    constraints: List[Constraint] = Field(default_factory=list)

class FinalizeRequest(BaseModel):
    """Request for Finalize phase - generates final protocol"""
    user_id: str
    confirmed_protocol_id: str

# Response Models
class CandidateModule(BaseModel):
    """A candidate module from Route phase"""
    module_id: str
    name: str
    relevance_score: float
    addresses_deficiencies: List[str]
    blocked: bool = False
    block_reason: Optional[str] = None

class RouteResponse(BaseModel):
    """Response from Route phase"""
    user_id: str
    candidates: List[CandidateModule]
    total_candidates: int
    constraints_applied: int
    phase: str = "route"
    timestamp: datetime

class ProtocolModule(BaseModel):
    """A module in a composed protocol"""
    module_id: str
    name: str
    dosage: str
    frequency: str
    timing: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)

class ComposeResponse(BaseModel):
    """Response from Compose phase"""
    user_id: str
    protocol_id: str
    modules: List[ProtocolModule]
    total_daily_pills: int
    interaction_warnings: List[str]
    phase: str = "compose"
    timestamp: datetime

class ConfirmResponse(BaseModel):
    """Response from Confirm phase"""
    user_id: str
    protocol_id: str
    confirmed_modules: List[str]
    ready_for_finalize: bool
    phase: str = "confirm"
    timestamp: datetime

class FinalProtocol(BaseModel):
    """Final protocol ready for fulfillment"""
    protocol_id: str
    user_id: str
    modules: List[ProtocolModule]
    sku_list: List[str]
    total_monthly_cost: Optional[float] = None
    fulfillment_ready: bool
    created_at: datetime

class FinalizeResponse(BaseModel):
    """Response from Finalize phase"""
    user_id: str
    protocol: FinalProtocol
    phase: str = "finalize"
    timestamp: datetime

# =============================================================================
# Helper Functions
# =============================================================================

def convert_handoff_to_brain_input(handoff: BloodworkHandoff) -> BrainInput:
    """Convert API handoff model to internal BrainInput"""
    
    # Convert sex string to enum
    sex = SexType.MALE if handoff.sex.lower() == "male" else SexType.FEMALE
    
    # Convert biomarkers to dict format
    biomarkers = {}
    for b in handoff.biomarkers:
        biomarkers[b.biomarker_code] = {
            "value": b.value,
            "unit": b.unit,
            "reference_low": b.reference_low,
            "reference_high": b.reference_high
        }
    
    # Convert constraints to internal format
    constraints = []
    for c in handoff.constraints:
        constraint_type = ConstraintType[c.constraint_type] if c.constraint_type in ConstraintType.__members__ else ConstraintType.CAUTION
        constraints.append({
            "type": constraint_type,
            "code": c.constraint_code,
            "reason": c.reason,
            "affected_ingredients": c.affected_ingredients,
            "max_dose_mg": c.max_dose_mg
        })
    
    return BrainInput(
        user_id=handoff.user_id,
        sex=sex,
        biomarkers=biomarkers,
        constraints=constraints,
        deficiencies=handoff.deficiencies,
        bloodwork_engine_version=handoff.bloodwork_engine_version
    )

# =============================================================================
# API Endpoints
# =============================================================================

@router.post("/route", response_model=RouteResponse, summary="Route Phase - Identify Candidates")
async def route_phase(request: RouteRequest):
    """
    **Route Phase**: First phase of Brain pipeline.
    
    Takes bloodwork handoff and identifies candidate supplement modules
    based on deficiencies, biomarker status, and sex-specific needs.
    
    Applies safety constraints to filter/flag inappropriate modules.
    """
    try:
        logger.info(f"Route phase started for user {request.handoff.user_id}")
        
        # Convert to internal format
        brain_input = convert_handoff_to_brain_input(request.handoff)
        
        # Initialize orchestrator and run route phase
        orchestrator = BrainOrchestrator()
        result = await orchestrator.route(brain_input, max_modules=request.max_modules)
        
        # Convert to response format
        candidates = []
        for module in result.get("candidates", []):
            candidates.append(CandidateModule(
                module_id=module["module_id"],
                name=module["name"],
                relevance_score=module.get("relevance_score", 0.0),
                addresses_deficiencies=module.get("addresses_deficiencies", []),
                blocked=module.get("blocked", False),
                block_reason=module.get("block_reason")
            ))
        
        return RouteResponse(
            user_id=request.handoff.user_id,
            candidates=candidates,
            total_candidates=len(candidates),
            constraints_applied=result.get("constraints_applied", 0),
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Route phase error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/compose", response_model=ComposeResponse, summary="Compose Phase - Build Protocol")
async def compose_phase(request: ComposeRequest):
    """
    **Compose Phase**: Second phase of Brain pipeline.
    
    Takes selected module IDs and composes them into a coherent protocol,
    checking for interactions and optimizing dosages.
    """
    try:
        logger.info(f"Compose phase started for user {request.user_id}")
        
        # Convert constraints
        constraints = []
        for c in request.constraints:
            constraint_type = ConstraintType[c.constraint_type] if c.constraint_type in ConstraintType.__members__ else ConstraintType.CAUTION
            constraints.append({
                "type": constraint_type,
                "code": c.constraint_code,
                "reason": c.reason,
                "affected_ingredients": c.affected_ingredients,
                "max_dose_mg": c.max_dose_mg
            })
        
        sex = SexType.MALE if request.sex.lower() == "male" else SexType.FEMALE
        
        orchestrator = BrainOrchestrator()
        result = await orchestrator.compose(
            user_id=request.user_id,
            sex=sex,
            module_ids=request.selected_module_ids,
            constraints=constraints
        )
        
        # Convert to response format
        modules = []
        for m in result.get("modules", []):
            modules.append(ProtocolModule(
                module_id=m["module_id"],
                name=m["name"],
                dosage=m.get("dosage", "As directed"),
                frequency=m.get("frequency", "Daily"),
                timing=m.get("timing"),
                warnings=m.get("warnings", [])
            ))
        
        return ComposeResponse(
            user_id=request.user_id,
            protocol_id=result.get("protocol_id", f"proto_{request.user_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"),
            modules=modules,
            total_daily_pills=result.get("total_daily_pills", len(modules)),
            interaction_warnings=result.get("interaction_warnings", []),
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Compose phase error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/confirm", response_model=ConfirmResponse, summary="Confirm Phase - User Confirmation")
async def confirm_phase(request: ConfirmRequest):
    """
    **Confirm Phase**: Third phase of Brain pipeline.
    
    Records user's confirmation of selected modules.
    Validates selections against safety constraints.
    """
    try:
        logger.info(f"Confirm phase started for user {request.user_id}")
        
        # Convert constraints
        constraints = []
        for c in request.constraints:
            constraint_type = ConstraintType[c.constraint_type] if c.constraint_type in ConstraintType.__members__ else ConstraintType.CAUTION
            constraints.append({
                "type": constraint_type,
                "code": c.constraint_code,
                "reason": c.reason,
                "affected_ingredients": c.affected_ingredients,
                "max_dose_mg": c.max_dose_mg
            })
        
        sex = SexType.MALE if request.sex.lower() == "male" else SexType.FEMALE
        
        orchestrator = BrainOrchestrator()
        result = await orchestrator.confirm(
            user_id=request.user_id,
            sex=sex,
            confirmed_ids=request.selections.confirmed_module_ids,
            rejected_ids=request.selections.rejected_module_ids,
            constraints=constraints
        )
        
        return ConfirmResponse(
            user_id=request.user_id,
            protocol_id=result.get("protocol_id", f"proto_{request.user_id}_confirmed"),
            confirmed_modules=result.get("confirmed_modules", request.selections.confirmed_module_ids),
            ready_for_finalize=result.get("ready_for_finalize", True),
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Confirm phase error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/finalize", response_model=FinalizeResponse, summary="Finalize Phase - Generate Final Protocol")
async def finalize_phase(request: FinalizeRequest):
    """
    **Finalize Phase**: Final phase of Brain pipeline.
    
    Generates the final protocol ready for fulfillment,
    including SKU mapping and cost calculation.
    """
    try:
        logger.info(f"Finalize phase started for user {request.user_id}")
        
        orchestrator = BrainOrchestrator()
        result = await orchestrator.finalize(
            user_id=request.user_id,
            protocol_id=request.confirmed_protocol_id
        )
        
        # Build final protocol
        modules = []
        for m in result.get("modules", []):
            modules.append(ProtocolModule(
                module_id=m["module_id"],
                name=m["name"],
                dosage=m.get("dosage", "As directed"),
                frequency=m.get("frequency", "Daily"),
                timing=m.get("timing"),
                warnings=m.get("warnings", [])
            ))
        
        protocol = FinalProtocol(
            protocol_id=request.confirmed_protocol_id,
            user_id=request.user_id,
            modules=modules,
            sku_list=result.get("sku_list", []),
            total_monthly_cost=result.get("total_monthly_cost"),
            fulfillment_ready=result.get("fulfillment_ready", True),
            created_at=datetime.utcnow()
        )
        
        return FinalizeResponse(
            user_id=request.user_id,
            protocol=protocol,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Finalize phase error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# Full Pipeline Endpoint
# =============================================================================

@router.post("/process", summary="Full Pipeline - All Phases")
async def full_pipeline(
    handoff: BloodworkHandoff,
    auto_confirm: bool = Query(default=True, description="Auto-confirm top candidates"),
    max_modules: int = Query(default=6, ge=1, le=15, description="Maximum modules to include")
):
    """
    **Full Pipeline**: Runs all Brain phases in sequence.
    
    Convenience endpoint that executes Route → Compose → Confirm → Finalize
    in a single request. Use for automated processing.
    
    Set auto_confirm=False to stop after Compose phase for manual review.
    """
    try:
        logger.info(f"Full pipeline started for user {handoff.user_id}")
        
        # Convert to internal format
        brain_input = convert_handoff_to_brain_input(handoff)
        
        # Initialize orchestrator
        orchestrator = BrainOrchestrator()
        
        # Run full pipeline
        result = await orchestrator.process_full_pipeline(
            brain_input=brain_input,
            max_modules=max_modules,
            auto_confirm=auto_confirm
        )
        
        return {
            "user_id": handoff.user_id,
            "status": "complete" if auto_confirm else "pending_confirmation",
            "phases_completed": result.get("phases_completed", []),
            "protocol": result.get("protocol"),
            "candidates_evaluated": result.get("candidates_evaluated", 0),
            "modules_selected": result.get("modules_selected", 0),
            "constraints_applied": result.get("constraints_applied", 0),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Full pipeline error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# Utility Endpoints
# =============================================================================

@router.get("/health", summary="Brain Pipeline Health Check")
async def health_check():
    """Check Brain pipeline health and database connectivity"""
    try:
        # Check database connection
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            return {
                "status": "degraded",
                "brain_pipeline": "operational",
                "database": "no_connection_string",
                "version": "1.1.0",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        conn = await asyncpg.connect(database_url)
        try:
            # Query os_modules_v3_1 (production schema)
            result = await conn.fetchval(
                "SELECT COUNT(*) FROM os_modules_v3_1 WHERE governance_status = 'active' OR governance_status IS NULL"
            )
            return {
                "status": "healthy",
                "brain_pipeline": "operational",
                "database": "connected",
                "modules_available": result,
                "version": "1.1.0",
                "timestamp": datetime.utcnow().isoformat()
            }
        finally:
            await conn.close()
            
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {
            "status": "unhealthy",
            "brain_pipeline": "error",
            "database": "error",
            "error": str(e),
            "version": "1.1.0",
            "timestamp": datetime.utcnow().isoformat()
        }

@router.get("/modules", summary="List Available Modules")
async def list_modules(
    sex: Optional[str] = Query(None, description="Filter by sex: male, female, or unisex"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(50, ge=1, le=200)
):
    """List available supplement modules in the Brain catalog"""
    try:
        orchestrator = BrainOrchestrator()
        
        sex_filter = None
        if sex:
            sex_filter = SexType.MALE if sex.lower() == "male" else SexType.FEMALE if sex.lower() == "female" else None
        
        modules = await orchestrator.get_available_modules(
            sex=sex_filter,
            category=category,
            limit=limit
        )
        
        return {
            "modules": modules,
            "total": len(modules),
            "filters_applied": {
                "sex": sex,
                "category": category
            }
        }
        
    except Exception as e:
        logger.error(f"List modules error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/constraints/types", summary="List Constraint Types")
async def list_constraint_types():
    """List all supported constraint types and their meanings"""
    return {
        "constraint_types": [
            {
                "type": "BLOCK",
                "description": "Hard block - ingredient/module must be excluded",
                "example": "Iron blocked due to elevated ferritin"
            },
            {
                "type": "LIMIT",
                "description": "Dose limitation - reduce to specified maximum",
                "example": "Vitamin A limited to 2500 IU due to liver markers"
            },
            {
                "type": "CAUTION",
                "description": "Warning flag - include with monitoring note",
                "example": "B12 supplementation - monitor for interactions"
            },
            {
                "type": "BOOST",
                "description": "Increase priority - deficiency identified",
                "example": "Vitamin D boosted due to severe deficiency"
            }
        ]
    }

@router.get("/schema/handoff", summary="Get Handoff Schema")
async def get_handoff_schema():
    """Get the JSON schema for BloodworkHandoff object"""
    return BloodworkHandoff.model_json_schema()
