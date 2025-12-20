"""
GenoMAX2 Brain API v1.1.0
Routing constraints engine - no SKUs, no doses, no recommendations.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, Optional
from pydantic import BaseModel
from datetime import datetime

from app.brain.orchestrate import run_orchestrate, OrchestrateStatus
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
    signal_hash: Optional[str] = None  # For immutability verification
    
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


# ============================================
# Endpoints
# ============================================

@brain_router.get("/health")
async def brain_health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "brain",
        "version": "brain_1.1.0"
    }


@brain_router.get("/info")
async def brain_info():
    """API information"""
    return {
        "name": "GenoMAX2 Brain",
        "version": "brain_1.1.0",
        "description": "Routing constraints engine for personalized supplementation",
        "phases": [
            {"name": "orchestrate", "status": "active", "description": "Signal validation and routing constraints"},
            {"name": "compose", "status": "planned", "description": "Convert constraints to ingredient intents"},
            {"name": "route", "status": "planned", "description": "Map intents to SKUs"},
            {"name": "check-interactions", "status": "planned", "description": "Drug interaction verification"},
            {"name": "finalize", "status": "planned", "description": "Build final protocol"}
        ]
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
    # Get DB connection if available
    db_conn = None
    try:
        from api_server import get_db
        db_conn = get_db()
    except:
        pass  # DB optional for now
    
    # Run orchestrate
    result = run_orchestrate(
        signal_data=request.signal_data,
        provided_hash=request.signal_hash,
        db_conn=db_conn
    )
    
    # Close DB connection
    if db_conn:
        try:
            db_conn.close()
        except:
            pass
    
    # Handle errors
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
    
    if result.status == OrchestrateStatus.DB_ERROR:
        # Log but don't fail - DB is optional
        print(f"DB Error (non-fatal): {result.error}")
    
    # Return success response
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


@brain_router.get("/orchestrate/{run_id}")
async def get_orchestrate_result(run_id: str):
    """Retrieve a previous orchestrate result by run_id"""
    # TODO: Implement DB lookup
    raise HTTPException(
        status_code=501,
        detail="Run retrieval not yet implemented. Use POST /orchestrate."
    )
