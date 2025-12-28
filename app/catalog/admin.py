"""
Catalog Admin Endpoints (Issue #5)

Read-only admin endpoints for catalog governance.

Security: Requires ADMIN_API_KEY header for all endpoints.

Version: catalog_governance_v1
"""

import os
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Dict

from .models import (
    CatalogCoverageReportV1,
    SkuValidationResult,
    SkuValidationStatus,
)
from .validate import (
    validate_catalog_snapshot,
    create_validation_run,
    get_blocked_skus,
    get_skus_missing_field,
    get_unknown_ingredients_summary,
)
from .mapper import CatalogMapper


# Router
router = APIRouter(
    prefix="/api/v1/admin/catalog",
    tags=["admin", "catalog"],
)


# Security
def verify_admin_key(x_admin_api_key: str = Header(None, alias="X-Admin-API-Key")) -> str:
    """
    Verify admin API key from header.
    
    Raises 401 if missing or invalid.
    """
    expected_key = os.environ.get("ADMIN_API_KEY")
    
    if not expected_key:
        # Fail open in dev if ADMIN_API_KEY not set (log warning in production)
        return "dev_mode"
    
    if not x_admin_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing X-Admin-API-Key header"
        )
    
    if x_admin_api_key != expected_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid admin API key"
        )
    
    return x_admin_api_key


# Response models
class CoverageResponse(BaseModel):
    """Coverage report response."""
    success: bool = True
    coverage: CatalogCoverageReportV1
    run_id: Optional[str] = None
    results_hash: Optional[str] = None
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class MissingMetadataItem(BaseModel):
    """Single SKU with missing metadata."""
    sku_id: str
    product_name: str
    missing_fields: List[str]
    reason_codes: List[str]


class MissingMetadataResponse(BaseModel):
    """Missing metadata report response."""
    success: bool = True
    total_blocked: int
    skus: List[MissingMetadataItem]
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class UnknownIngredientItem(BaseModel):
    """Unknown ingredient with affected SKUs."""
    ingredient: str
    count: int
    affected_skus: List[str]


class UnknownIngredientsResponse(BaseModel):
    """Unknown ingredients report response."""
    success: bool = True
    total_unknown: int
    ingredients: List[UnknownIngredientItem]
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class ValidationResultItem(BaseModel):
    """Validation result for a single SKU."""
    sku_id: str
    product_name: str
    status: str
    reason_codes: List[str]
    missing_fields: List[str]
    unknown_ingredients: List[str]


class ValidationResultsResponse(BaseModel):
    """Full validation results response."""
    success: bool = True
    run_id: str
    results_hash: str
    total_skus: int
    valid_skus: int
    blocked_skus: int
    results: List[ValidationResultItem]
    generated_at: datetime = Field(default_factory=datetime.utcnow)


# Cached validation run (refreshed on each request for now)
_cached_run = None


def get_validation_run():
    """Get or create validation run."""
    global _cached_run
    
    # For now, always recompute (can add caching later)
    mapper = CatalogMapper()
    results, coverage = validate_catalog_snapshot(mapper)
    _cached_run = create_validation_run(results, coverage)
    
    return _cached_run


# Endpoints

@router.get("/coverage", response_model=CoverageResponse)
async def get_coverage(
    admin_key: str = Depends(verify_admin_key)
):
    """
    Get catalog coverage report.
    
    Returns statistics on valid vs blocked SKUs.
    """
    try:
        run = get_validation_run()
        
        return CoverageResponse(
            success=True,
            coverage=run.coverage_report,
            run_id=run.run_id,
            results_hash=run.results_hash,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@router.get("/missing-metadata", response_model=MissingMetadataResponse)
async def get_missing_metadata(
    field: Optional[str] = None,
    admin_key: str = Depends(verify_admin_key)
):
    """
    Get SKUs with missing metadata.
    
    Args:
        field: Optional filter by specific missing field (e.g., 'ingredient_tags')
    """
    try:
        run = get_validation_run()
        blocked = get_blocked_skus(run.results)
        
        # Filter by field if specified
        if field:
            blocked = get_skus_missing_field(run.results, field)
        
        items = [
            MissingMetadataItem(
                sku_id=r.sku_id,
                product_name=r.product_name,
                missing_fields=r.missing_fields,
                reason_codes=r.reason_codes,
            )
            for r in blocked
        ]
        
        return MissingMetadataResponse(
            success=True,
            total_blocked=len(items),
            skus=items,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@router.get("/unknown-ingredients", response_model=UnknownIngredientsResponse)
async def get_unknown_ingredients(
    admin_key: str = Depends(verify_admin_key)
):
    """
    Get unknown ingredient strings and affected SKUs.
    
    These are ingredient names not found in the canonical dictionary.
    """
    try:
        run = get_validation_run()
        summary = get_unknown_ingredients_summary(run.results)
        
        items = [
            UnknownIngredientItem(
                ingredient=ingredient,
                count=len(sku_ids),
                affected_skus=sku_ids,
            )
            for ingredient, sku_ids in summary.items()
        ]
        
        return UnknownIngredientsResponse(
            success=True,
            total_unknown=len(items),
            ingredients=items,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@router.get("/validate", response_model=ValidationResultsResponse)
async def run_validation(
    admin_key: str = Depends(verify_admin_key)
):
    """
    Run full catalog validation and return all results.
    
    WARNING: This may return a large response.
    """
    try:
        run = get_validation_run()
        
        items = [
            ValidationResultItem(
                sku_id=r.sku_id,
                product_name=r.product_name,
                status=r.status.value,
                reason_codes=r.reason_codes,
                missing_fields=r.missing_fields,
                unknown_ingredients=r.unknown_ingredients,
            )
            for r in run.results
        ]
        
        return ValidationResultsResponse(
            success=True,
            run_id=run.run_id,
            results_hash=run.results_hash,
            total_skus=len(items),
            valid_skus=run.coverage_report.valid_skus,
            blocked_skus=run.coverage_report.auto_blocked_skus,
            results=items,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@router.get("/health")
async def catalog_health():
    """
    Health check for catalog governance module.
    
    Does not require admin key.
    """
    return {
        "status": "ok",
        "module": "catalog_governance",
        "version": "catalog_governance_v1",
        "timestamp": datetime.utcnow().isoformat(),
    }
