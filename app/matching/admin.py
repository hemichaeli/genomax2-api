"""
Matching Layer Admin Endpoints (Issue #7)

API endpoints for intent-to-SKU matching.

POST /api/v1/matching/resolve - Main matching endpoint
GET  /api/v1/matching/health  - Health check

Version: matching_layer_v1
"""

import os
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Header, HTTPException, Depends
from pydantic import BaseModel, Field

from .models import (
    MatchingInput,
    MatchingResult,
    MatchingResponse,
    MatchingHealthResponse,
    AllowedSKUInput,
    IntentInput,
    UserContext,
    ProtocolItem,
    UnmatchedIntent,
)
from .match import resolve_matching

# Try to import routing layer for integration
try:
    from app.routing.models import AllowedSKU, RoutingResult
    ROUTING_AVAILABLE = True
except ImportError:
    ROUTING_AVAILABLE = False


# Router
router = APIRouter(
    prefix="/api/v1/matching",
    tags=["matching"],
)


# Request models

class ResolveMatchingRequest(BaseModel):
    """Request to resolve intent-to-SKU matching."""
    
    # Option 1: Direct SKU input
    allowed_skus: Optional[List[AllowedSKUInput]] = Field(
        default=None,
        description="SKUs that passed routing (from Issue #6)"
    )
    
    # Option 2: Routing result input (will extract allowed_skus)
    routing_result: Optional[dict] = Field(
        default=None,
        description="Full routing result (will extract allowed_skus)"
    )
    
    # Required: Intents
    prioritized_intents: List[IntentInput] = Field(
        description="User intents ranked by priority"
    )
    
    # Required: User context
    user_context: UserContext = Field(
        description="User biological context (sex, product_line)"
    )
    
    # Optional: Requirements from bloodwork
    requirements: List[str] = Field(
        default_factory=list,
        description="Must-include ingredients from bloodwork deficiencies"
    )


class ResolveMatchingResponse(BaseModel):
    """Response from matching resolution."""
    success: bool = True
    result: MatchingResult
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class ProtocolSummaryResponse(BaseModel):
    """Simplified protocol summary for frontend."""
    success: bool = True
    protocol_count: int
    unmatched_count: int
    requirements_fulfilled: List[str]
    requirements_unfulfilled: List[str]
    has_warnings: bool
    match_hash: str
    items: List[dict]


# Endpoints

@router.get("/health", response_model=MatchingHealthResponse)
async def matching_health():
    """
    Health check for matching module.
    
    Does not require authentication.
    """
    return MatchingHealthResponse(
        status="ok",
        module="matching_layer",
        version="matching_layer_v1",
        timestamp=datetime.utcnow().isoformat(),
    )


@router.post("/resolve", response_model=ResolveMatchingResponse)
async def resolve_matching_endpoint(request: ResolveMatchingRequest):
    """
    Resolve intent-to-SKU matching.
    
    This is the main matching endpoint. It:
    1. Takes allowed SKUs (from routing layer)
    2. Takes prioritized intents (from Brain Compose)
    3. Matches intents to SKUs via ingredient overlap
    4. Respects gender-specific product lines
    5. Fulfills requirements from deficiencies
    6. Returns protocol with full audit trail
    
    Input options:
    - Provide allowed_skus directly
    - OR provide routing_result (will extract allowed_skus)
    
    Does not require admin key - this is a core operational endpoint.
    """
    try:
        # Extract allowed_skus from input
        allowed_skus = None
        
        if request.allowed_skus:
            allowed_skus = request.allowed_skus
        elif request.routing_result:
            # Extract from routing result
            routing_allowed = request.routing_result.get("allowed_skus", [])
            allowed_skus = [
                AllowedSKUInput(**sku) if isinstance(sku, dict) else sku
                for sku in routing_allowed
            ]
        
        if not allowed_skus:
            raise HTTPException(
                status_code=400,
                detail="Must provide allowed_skus or routing_result"
            )
        
        # Build matching input
        matching_input = MatchingInput(
            allowed_skus=allowed_skus,
            prioritized_intents=request.prioritized_intents,
            user_context=request.user_context,
            requirements=request.requirements,
        )
        
        # Resolve matching
        result = resolve_matching(matching_input)
        
        return ResolveMatchingResponse(
            success=True,
            result=result,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Matching error: {str(e)}")


@router.post("/resolve/summary", response_model=ProtocolSummaryResponse)
async def resolve_matching_summary(request: ResolveMatchingRequest):
    """
    Resolve matching and return simplified summary.
    
    Same as /resolve but returns frontend-friendly format.
    """
    try:
        # Extract allowed_skus from input
        allowed_skus = None
        
        if request.allowed_skus:
            allowed_skus = request.allowed_skus
        elif request.routing_result:
            routing_allowed = request.routing_result.get("allowed_skus", [])
            allowed_skus = [
                AllowedSKUInput(**sku) if isinstance(sku, dict) else sku
                for sku in routing_allowed
            ]
        
        if not allowed_skus:
            raise HTTPException(
                status_code=400,
                detail="Must provide allowed_skus or routing_result"
            )
        
        # Build matching input
        matching_input = MatchingInput(
            allowed_skus=allowed_skus,
            prioritized_intents=request.prioritized_intents,
            user_context=request.user_context,
            requirements=request.requirements,
        )
        
        # Resolve matching
        result = resolve_matching(matching_input)
        
        # Build simplified items
        items = [
            {
                "sku_id": item.sku_id,
                "product_name": item.product_name,
                "intents": item.matched_intents,
                "score": item.match_score,
                "reason": item.reason,
                "has_warnings": len(item.warnings) > 0,
            }
            for item in result.protocol
        ]
        
        return ProtocolSummaryResponse(
            success=True,
            protocol_count=len(result.protocol),
            unmatched_count=len(result.unmatched_intents),
            requirements_fulfilled=result.audit.requirements_fulfilled,
            requirements_unfulfilled=result.audit.requirements_unfulfilled,
            has_warnings=result.audit.caution_warnings_count > 0,
            match_hash=result.match_hash,
            items=items,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Matching error: {str(e)}")


@router.get("/test-match")
async def test_match(
    intent_code: str = "INTENT_ENERGY",
    sex: str = "male",
):
    """
    Test endpoint to verify matching logic.
    
    Creates a simple test case with mock data.
    """
    # Mock allowed SKUs
    mock_skus = [
        AllowedSKUInput(
            sku_id="TEST-SKU-001",
            product_name="Test Energy Supplement",
            ingredient_tags=["b12", "coq10", "iron"],
            category_tags=["energy"],
            gender_line="UNISEX",
            evidence_tier="TIER_1",
            caution_flags=[],
            caution_reasons=[],
            fulfills_requirements=["b12"],
        ),
        AllowedSKUInput(
            sku_id="TEST-SKU-002",
            product_name="Test Sleep Aid",
            ingredient_tags=["magnesium", "melatonin"],
            category_tags=["sleep"],
            gender_line="UNISEX",
            evidence_tier="TIER_2",
            caution_flags=[],
            caution_reasons=[],
            fulfills_requirements=[],
        ),
        AllowedSKUInput(
            sku_id="TEST-SKU-003",
            product_name="Test Male Vitality",
            ingredient_tags=["zinc", "d3", "b12"],
            category_tags=["vitality"],
            gender_line="MAXimo2",
            evidence_tier="TIER_1",
            caution_flags=["zinc_high"],
            caution_reasons=["CAUTION_ZINC_HIGH_DOSE"],
            fulfills_requirements=["b12", "d3"],
        ),
    ]
    
    # Mock intent
    mock_intent = IntentInput(
        code=intent_code,
        priority=1,
        ingredient_targets=["b12", "coq10"],
        source="goal",
    )
    
    # Mock context
    mock_context = UserContext(
        sex=sex,
    )
    
    # Build input
    matching_input = MatchingInput(
        allowed_skus=mock_skus,
        prioritized_intents=[mock_intent],
        user_context=mock_context,
        requirements=["b12"],
    )
    
    # Resolve
    result = resolve_matching(matching_input)
    
    return {
        "test_params": {
            "intent_code": intent_code,
            "sex": sex,
        },
        "result": result.model_dump(),
    }
