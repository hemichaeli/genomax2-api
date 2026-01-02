"""
GenoMAX² Safety Gate Admin Endpoints v1.0
API endpoints for Safety Gate inspection and management

Endpoints:
- GET /api/v1/safety/status - Get current safety gate status
- GET /api/v1/safety/blocked-ingredients - List all rejected ingredients
- GET /api/v1/safety/blocked-modules - List all blocked modules
- POST /api/v1/safety/check - Check specific modules for safety
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import os
import psycopg2
from psycopg2.extras import RealDictCursor

from app.brain.safety_gate import (
    SAFETY_GATE_VERSION,
    get_safety_blocked_ingredients,
    get_all_blocked_modules,
    check_modules_safety,
    get_blocked_ingredient_names,
    build_safety_gate_output,
    now_iso,
)

router = APIRouter(prefix="/api/v1/safety", tags=["safety-gate"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    """Get database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class SafetyCheckRequest(BaseModel):
    """Request to check module safety."""
    module_codes: List[str] = Field(..., min_length=1, max_length=100)


class ModuleSafetyItem(BaseModel):
    """Single module safety result."""
    module_code: str
    status: str  # PASS or BLOCKED
    blocked_by: Optional[str] = None
    reason: Optional[str] = None


class SafetyCheckResponse(BaseModel):
    """Response from safety check."""
    checked_at: str
    gate_version: str
    total_checked: int
    passed_count: int
    blocked_count: int
    results: List[ModuleSafetyItem]


class RejectedIngredientItem(BaseModel):
    """Rejected ingredient details."""
    ingredient_id: int
    name: str
    tier_classification: str
    rejection_reason: str
    rejection_date: Optional[str] = None


class BlockedModuleItem(BaseModel):
    """Blocked module details."""
    module_code: str
    product_name: Optional[str] = None
    blocked_by: str
    rejection_reason: str


class SafetyStatusResponse(BaseModel):
    """Overall safety gate status."""
    gate_version: str
    status: str
    checked_at: str
    rejected_ingredients_count: int
    blocked_modules_count: int
    blocked_ingredient_names: List[str]


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.get("/status", response_model=SafetyStatusResponse)
def get_safety_status():
    """
    Get current safety gate status.
    
    Returns count of rejected ingredients and blocked modules.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        rejected = get_safety_blocked_ingredients(conn)
        blocked_modules = get_all_blocked_modules(conn)
        blocked_names = list(get_blocked_ingredient_names(conn))
        
        conn.close()
        
        return SafetyStatusResponse(
            gate_version=SAFETY_GATE_VERSION,
            status="OPERATIONAL",
            checked_at=now_iso(),
            rejected_ingredients_count=len(rejected),
            blocked_modules_count=len(blocked_modules),
            blocked_ingredient_names=sorted(blocked_names)
        )
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/blocked-ingredients", response_model=List[RejectedIngredientItem])
def list_blocked_ingredients():
    """
    List all permanently rejected ingredients.
    
    These ingredients are blocked from all recommendations due to
    safety concerns (hepatotoxicity, severe adverse events, etc.).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        rejected = get_safety_blocked_ingredients(conn)
        conn.close()
        
        return [
            RejectedIngredientItem(
                ingredient_id=ri.ingredient_id,
                name=ri.name,
                tier_classification=ri.tier_classification,
                rejection_reason=ri.rejection_reason,
                rejection_date=ri.rejection_date
            )
            for ri in rejected
        ]
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/blocked-modules", response_model=List[BlockedModuleItem])
def list_blocked_modules():
    """
    List all modules blocked due to rejected ingredients.
    
    These modules contain ingredients that have been permanently
    rejected and cannot be recommended.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        blocked = get_all_blocked_modules(conn)
        conn.close()
        
        return [
            BlockedModuleItem(
                module_code=m['module_code'],
                product_name=m.get('product_name'),
                blocked_by=m['blocked_by'],
                rejection_reason=m['rejection_reason']
            )
            for m in blocked
        ]
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/check", response_model=SafetyCheckResponse)
def check_module_safety(request: SafetyCheckRequest):
    """
    Check safety status for specific modules.
    
    Use this endpoint to validate candidate modules before
    including them in recommendations.
    
    Returns PASS or BLOCKED status for each module, with
    blocking ingredient and reason if blocked.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        results = check_modules_safety(conn, request.module_codes)
        conn.close()
        
        passed = [r for r in results if r.status == 'PASS']
        blocked = [r for r in results if r.status == 'BLOCKED']
        
        return SafetyCheckResponse(
            checked_at=now_iso(),
            gate_version=SAFETY_GATE_VERSION,
            total_checked=len(results),
            passed_count=len(passed),
            blocked_count=len(blocked),
            results=[
                ModuleSafetyItem(
                    module_code=r.module_code,
                    status=r.status,
                    blocked_by=r.blocked_by,
                    reason=r.reason
                )
                for r in results
            ]
        )
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
def safety_gate_health():
    """Health check for safety gate service."""
    conn = get_db()
    if not conn:
        return {
            "status": "degraded",
            "gate_version": SAFETY_GATE_VERSION,
            "database": "disconnected"
        }
    
    try:
        # Quick query to verify database access
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as cnt FROM ingredients WHERE safety_status = 'REJECTED'")
        row = cur.fetchone()
        rejected_count = row['cnt'] if row else 0
        cur.close()
        conn.close()
        
        return {
            "status": "healthy",
            "gate_version": SAFETY_GATE_VERSION,
            "database": "connected",
            "rejected_ingredients_count": rejected_count
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return {
            "status": "degraded",
            "gate_version": SAFETY_GATE_VERSION,
            "database": "error",
            "error": str(e)
        }
