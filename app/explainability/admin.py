"""
Explainability API Endpoints (Issue #8)

FastAPI router for protocol explainability.

Endpoints:
- POST /api/v1/explainability/explain - Generate full explainability
- GET /api/v1/explainability/disclaimers - Get locked disclaimers
- GET /api/v1/explainability/health - Module health check
- GET /api/v1/explainability/test - Test with mock data

Version: explainability_v1
"""

from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .models import (
    ExplainabilityRequest,
    ProtocolExplainability,
    DisclaimerSet,
    ConfidenceLevel,
)
from .explain import (
    generate_explainability,
    calculate_confidence,
    get_disclaimers,
)


router = APIRouter(
    prefix="/api/v1/explainability",
    tags=["explainability"]
)


# ============================================================
# HEALTH CHECK
# ============================================================

@router.get("/health")
def explainability_health():
    """
    Module health check.
    """
    return {
        "status": "ok",
        "module": "explainability",
        "version": "explainability_v1",
        "timestamp": datetime.utcnow().isoformat(),
        "principle": "The Brain decides. The UX explains. Never the reverse."
    }


# ============================================================
# MAIN EXPLAINABILITY ENDPOINT
# ============================================================

@router.post("/explain", response_model=ProtocolExplainability)
def explain_protocol(request: ExplainabilityRequest):
    """
    Generate complete explainability for a protocol.
    
    Takes existing protocol output and generates:
    - Item explanations (why included, why not blocked)
    - Blocked item explanations (negative explainability)
    - Confidence level (rule-based)
    - Locked disclaimers
    
    Does NOT modify any decisions.
    """
    try:
        result = generate_explainability(request)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate explainability: {str(e)}"
        )


# ============================================================
# DISCLAIMERS ENDPOINT
# ============================================================

@router.get("/disclaimers", response_model=DisclaimerSet)
def get_disclaimers_endpoint():
    """
    Get the locked disclaimer set.
    
    ❌ These cannot be modified by UI
    ❌ These cannot be A/B tested
    ❌ These must appear in all protocol displays
    """
    return get_disclaimers()


# ============================================================
# CONFIDENCE CALCULATION ENDPOINT
# ============================================================

class ConfidenceRequest(BaseModel):
    """Request model for confidence calculation."""
    has_bloodwork: bool = False
    bloodwork_complete: bool = False
    intent_count: int = 0
    caution_count: int = 0


@router.post("/confidence")
def calculate_confidence_endpoint(request: ConfidenceRequest):
    """
    Calculate confidence level based on data completeness.
    
    Rules (NOT ML):
    - HIGH: Bloodwork + complete + ≥2 intents + no cautions
    - MEDIUM: Missing bloodwork OR partial panel
    - LOW: Lifestyle-only OR sparse data
    """
    confidence = calculate_confidence(
        has_bloodwork=request.has_bloodwork,
        bloodwork_complete=request.bloodwork_complete,
        intent_count=request.intent_count,
        caution_count=request.caution_count
    )
    return confidence


# ============================================================
# TEST ENDPOINT
# ============================================================

@router.get("/test")
def test_explainability():
    """
    Test explainability with mock data.
    
    Returns a complete explainability output for verification.
    """
    
    # Mock protocol items (as if from Issue #7 matching)
    mock_protocol_items = [
        {
            "sku_id": "TEST-SKU-001",
            "product_name": "Energy Support Complex",
            "matched_intents": ["INTENT_ENERGY", "INTENT_FOCUS"],
            "matched_ingredients": ["b12", "coq10", "iron"],
            "match_score": 0.85,
            "reason": "both",
            "warnings": []
        },
        {
            "sku_id": "TEST-SKU-002",
            "product_name": "Sleep & Recovery Formula",
            "matched_intents": ["INTENT_SLEEP"],
            "matched_ingredients": ["magnesium", "glycine"],
            "match_score": 0.72,
            "reason": "intent_match",
            "warnings": ["CAUTION: MAGNESIUM_HIGH_DOSE"]
        }
    ]
    
    # Mock blocked items (as if from Issue #6 routing)
    mock_blocked_items = [
        {
            "sku_id": "TEST-SKU-BLOCKED-001",
            "product_name": "Iron Boost Plus",
            "reason_codes": ["BLOCK_IRON_FERRITIN_HIGH"],
            "blocked_by": "blood"
        },
        {
            "sku_id": "TEST-SKU-BLOCKED-002",
            "product_name": "Unknown Supplement",
            "reason_codes": ["INSUFFICIENT_METADATA"],
            "blocked_by": "metadata"
        }
    ]
    
    # Mock routing constraints
    mock_constraints = {
        "blocked_ingredients": ["iron"],
        "blocked_categories": [],
        "caution_flags": ["magnesium"],
        "requirements": ["b12"],
        "reason_codes": ["BLOCK_IRON_FERRITIN_HIGH"]
    }
    
    # Create request
    request = ExplainabilityRequest(
        protocol_id="TEST-PROTOCOL-001",
        protocol_items=mock_protocol_items,
        blocked_items=mock_blocked_items,
        routing_constraints=mock_constraints,
        has_bloodwork=True,
        bloodwork_complete=True,
        intent_count=3,
        caution_count=1
    )
    
    # Generate explainability
    result = generate_explainability(request)
    
    return {
        "test_input": {
            "protocol_items_count": len(mock_protocol_items),
            "blocked_items_count": len(mock_blocked_items),
            "has_bloodwork": True,
            "intent_count": 3
        },
        "result": result.model_dump()
    }


# ============================================================
# SUMMARY ENDPOINT (Lightweight)
# ============================================================

@router.post("/summary")
def explainability_summary(request: ExplainabilityRequest):
    """
    Get a lightweight summary of explainability.
    
    Returns only:
    - Confidence level and badge
    - Item count with explanations
    - Blocked count with reasons
    - Disclaimer version
    
    Use /explain for full output.
    """
    result = generate_explainability(request)
    
    return {
        "protocol_id": result.protocol_id,
        "confidence": {
            "level": result.confidence.level.value,
            "badge_text": result.confidence.badge_text,
        },
        "items_explained": len(result.item_explanations),
        "items_blocked": len(result.blocked_explanations),
        "has_cautions": any(
            len(item.caution_notes) > 0 
            for item in result.item_explanations
        ),
        "disclaimer_version": result.disclaimers.version,
        "generated_at": result.generated_at
    }
