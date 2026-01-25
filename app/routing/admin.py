"""
Routing Layer Admin Endpoints (Issue #6)

API endpoints for applying routing constraints and testing.

Security: POST endpoints require valid input, health check is public.

Version: routing_layer_v1.1 (fixed metadata attribute name)
"""

import os
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Header, HTTPException, Depends
from pydantic import BaseModel, Field

from .models import (
    RoutingInput,
    RoutingConstraints,
    RoutingResult,
    AllowedSKU,
    BlockedSKU,
    RoutingAudit,
    SkuInput,
)
from .apply import (
    apply_routing_constraints,
    filter_by_gender,
    get_requirements_coverage,
)

# Import catalog mapper to load valid SKUs
try:
    from app.catalog.mapper import CatalogMapper
    from app.catalog.validate import validate_catalog_snapshot
    CATALOG_AVAILABLE = True
except ImportError:
    CATALOG_AVAILABLE = False


# Router
router = APIRouter(
    prefix="/api/v1/routing",
    tags=["routing"],
)


# Request/Response models

class ApplyRoutingRequest(BaseModel):
    """Request to apply routing constraints."""
    routing_constraints: RoutingConstraints
    use_catalog: bool = Field(
        default=True,
        description="If true, load valid SKUs from catalog. If false, must provide valid_skus."
    )
    valid_skus: Optional[List[SkuInput]] = Field(
        default=None,
        description="Optional manual SKU list (used if use_catalog=False)"
    )


class ApplyRoutingResponse(BaseModel):
    """Response from routing application."""
    success: bool = True
    result: RoutingResult
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class GenderFilterRequest(BaseModel):
    """Request to filter by gender."""
    routing_result: RoutingResult
    target_gender: str = Field(description="male, female, or unisex")


class GenderFilterResponse(BaseModel):
    """Response from gender filtering."""
    success: bool = True
    filtered_skus: List[AllowedSKU]
    original_count: int
    filtered_count: int
    target_gender: str


class RequirementsCoverageRequest(BaseModel):
    """Request requirements coverage analysis."""
    routing_result: RoutingResult
    requirements: List[str]


class RequirementsCoverageResponse(BaseModel):
    """Response from requirements coverage analysis."""
    success: bool = True
    total_requirements: int
    fulfilled: List[str]
    missing: List[str]
    coverage_pct: float


# Admin key verification (optional - for protected operations)
def verify_admin_key(x_admin_api_key: str = Header(None, alias="X-Admin-API-Key")) -> str:
    """Verify admin API key from header."""
    expected_key = os.environ.get("ADMIN_API_KEY")
    
    if not expected_key:
        return "dev_mode"
    
    if not x_admin_api_key:
        raise HTTPException(status_code=401, detail="Missing X-Admin-API-Key header")
    
    if x_admin_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid admin API key")
    
    return x_admin_api_key


# Endpoints

@router.get("/health")
async def routing_health():
    """
    Health check for routing module.
    
    Does not require authentication.
    """
    return {
        "status": "ok",
        "module": "routing_layer",
        "version": "routing_layer_v1.1",
        "catalog_available": CATALOG_AVAILABLE,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.post("/apply", response_model=ApplyRoutingResponse)
async def apply_routing(request: ApplyRoutingRequest):
    """
    Apply routing constraints to catalog SKUs.
    
    This is the main routing endpoint. It:
    1. Loads valid SKUs from catalog (or uses provided list)
    2. Applies all blocking rules
    3. Propagates caution flags
    4. Returns allowed/blocked SKUs with full audit
    
    Does not require admin key - this is a core operational endpoint.
    """
    try:
        if request.use_catalog:
            if not CATALOG_AVAILABLE:
                raise HTTPException(
                    status_code=503,
                    detail="Catalog module not available. Provide valid_skus manually."
                )
            
            # Load valid SKUs from catalog
            mapper = CatalogMapper()
            sku_data = mapper.load_catalog_auto()
            
            # Validate and filter to only valid SKUs
            from app.catalog.validate import validate_catalog_snapshot
            results, coverage = validate_catalog_snapshot(mapper)
            
            # Convert valid results to SkuInput
            # NOTE: SkuValidationResult has 'metadata' field, not 'meta'
            valid_skus = []
            for result in results:
                if result.status.value == "VALID":
                    valid_skus.append(SkuInput(
                        sku_id=result.sku_id,
                        product_name=result.product_name,
                        ingredient_tags=result.metadata.ingredient_tags if result.metadata else [],
                        category_tags=result.metadata.category_tags if result.metadata else [],
                        risk_tags=result.metadata.risk_tags if result.metadata else [],
                        gender_line=result.metadata.gender_line.value if result.metadata and result.metadata.gender_line else None,
                        evidence_tier=result.metadata.evidence_tier if result.metadata else None,
                    ))
        else:
            if not request.valid_skus:
                raise HTTPException(
                    status_code=400,
                    detail="Must provide valid_skus when use_catalog=False"
                )
            valid_skus = request.valid_skus
        
        # Apply routing constraints
        result = apply_routing_constraints(
            valid_skus=valid_skus,
            constraints=request.routing_constraints,
        )
        
        return ApplyRoutingResponse(
            success=True,
            result=result,
        )
        
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Routing error: {str(e)}")


@router.post("/filter-gender", response_model=GenderFilterResponse)
async def filter_gender(request: GenderFilterRequest):
    """
    Filter allowed SKUs by gender target.
    
    Utility endpoint for downstream processing.
    """
    try:
        filtered = filter_by_gender(
            allowed_skus=request.routing_result.allowed_skus,
            target_gender=request.target_gender,
        )
        
        return GenderFilterResponse(
            success=True,
            filtered_skus=filtered,
            original_count=len(request.routing_result.allowed_skus),
            filtered_count=len(filtered),
            target_gender=request.target_gender,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Filter error: {str(e)}")


@router.post("/requirements-coverage", response_model=RequirementsCoverageResponse)
async def requirements_coverage(request: RequirementsCoverageRequest):
    """
    Analyze how well allowed SKUs cover requirements.
    
    Utility endpoint for downstream processing.
    """
    try:
        coverage = get_requirements_coverage(
            allowed_skus=request.routing_result.allowed_skus,
            requirements=request.requirements,
        )
        
        return RequirementsCoverageResponse(
            success=True,
            total_requirements=coverage["total_requirements"],
            fulfilled=coverage["fulfilled"],
            missing=coverage["missing"],
            coverage_pct=coverage["coverage_pct"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Coverage error: {str(e)}")


@router.get("/test-blocking")
async def test_blocking(
    ingredient: Optional[str] = None,
    category: Optional[str] = None,
    admin_key: str = Depends(verify_admin_key)
):
    """
    Test which SKUs would be blocked by a specific constraint.
    
    Admin endpoint for debugging.
    """
    if not CATALOG_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Catalog module not available"
        )
    
    try:
        # Build test constraints
        constraints = RoutingConstraints(
            blocked_ingredients=[ingredient] if ingredient else [],
            blocked_categories=[category] if category else [],
        )
        
        # Load and validate catalog
        mapper = CatalogMapper()
        from app.catalog.validate import validate_catalog_snapshot
        results, coverage = validate_catalog_snapshot(mapper)
        
        # Convert valid results to SkuInput
        # NOTE: SkuValidationResult has 'metadata' field, not 'meta'
        valid_skus = []
        for result in results:
            if result.status.value == "VALID":
                valid_skus.append(SkuInput(
                    sku_id=result.sku_id,
                    product_name=result.product_name,
                    ingredient_tags=result.metadata.ingredient_tags if result.metadata else [],
                    category_tags=result.metadata.category_tags if result.metadata else [],
                    risk_tags=result.metadata.risk_tags if result.metadata else [],
                ))
        
        # Apply routing
        result = apply_routing_constraints(valid_skus, constraints)
        
        return {
            "test_constraints": constraints.dict(),
            "would_block": [
                {"sku_id": s.sku_id, "product_name": s.product_name, "reasons": s.reason_codes}
                for s in result.blocked_skus
            ],
            "would_allow": len(result.allowed_skus),
            "blocked_count": len(result.blocked_skus),
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Test error: {str(e)}")
