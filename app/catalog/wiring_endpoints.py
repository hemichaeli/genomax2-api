"""
GenoMAX² Catalog Wiring Endpoints (Issue #15)

API endpoints for catalog wiring management.

Routes:
- GET /api/v1/catalog/wiring/health - Health check
- GET /api/v1/catalog/wiring/load - Load/reload catalog
- GET /api/v1/catalog/wiring/stats - Catalog statistics
- GET /api/v1/catalog/wiring/check/{sku} - Check if SKU is purchasable
- POST /api/v1/catalog/wiring/filter - Filter SKU list
- GET /api/v1/catalog/wiring/all-skus - List all purchasable SKUs

Version: catalog_wiring_v1
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .wiring import (
    get_catalog,
    ensure_catalog_available,
    get_catalog_health,
    filter_to_catalog,
    CatalogWiringError,
    CATALOG_WIRING_VERSION,
)


router = APIRouter(prefix="/api/v1/catalog/wiring", tags=["Catalog Wiring"])


class FilterSkusRequest(BaseModel):
    """Request body for SKU filtering."""
    skus: List[str]


class FilterSkusResponse(BaseModel):
    """Response for SKU filtering."""
    input_count: int
    output_count: int
    filtered_skus: List[str]
    removed_skus: List[str]


@router.get("/health")
def catalog_wiring_health() -> Dict[str, Any]:
    """
    Health check for catalog wiring.
    
    Returns:
        Health status including load state and product counts
    """
    return get_catalog_health()


@router.get("/load")
def load_catalog(force: bool = False) -> Dict[str, Any]:
    """
    Load or reload the catalog.
    
    Args:
        force: If True, reload even if already loaded
        
    Returns:
        Catalog statistics
        
    Raises:
        503: If catalog cannot be loaded
    """
    try:
        catalog = get_catalog()
        stats = catalog.load(force_reload=force)
        return {
            "status": "loaded",
            "stats": stats,
            "version": CATALOG_WIRING_VERSION,
        }
    except CatalogWiringError as e:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "CATALOG_UNAVAILABLE",
                "message": str(e),
                "action": "Check catalog_products table and database connection",
            }
        )


@router.get("/stats")
def catalog_stats() -> Dict[str, Any]:
    """
    Get catalog statistics.
    
    Returns:
        Product counts by category, product line, and evidence tier
        
    Raises:
        503: If catalog not loaded
    """
    catalog = get_catalog()
    
    if not catalog.is_loaded:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "CATALOG_NOT_LOADED",
                "message": "Catalog must be loaded first",
                "action": "Call /api/v1/catalog/wiring/load",
            }
        )
    
    # Build category breakdown
    categories: Dict[str, int] = {}
    for sku in catalog.available_skus:
        product = catalog.get_product(sku)
        if product:
            cat = product.category or "unknown"
            categories[cat] = categories.get(cat, 0) + 1
    
    return {
        "status": "loaded",
        "total_products": catalog.product_count,
        "by_product_line": {
            "maximo": len(catalog.filter_by_product_line("MAXimo²")),
            "maxima": len(catalog.filter_by_product_line("MAXima²")),
            "universal": len(catalog.filter_by_product_line("universal")),
        },
        "by_evidence_tier": {
            "tier1": len(catalog.filter_by_evidence_tier("TIER_1")),
            "tier2": len(catalog.filter_by_evidence_tier("TIER_2")),
        },
        "by_category": categories,
        "loaded_at": catalog._loaded_at.isoformat() if catalog._loaded_at else None,
        "version": CATALOG_WIRING_VERSION,
    }


@router.get("/check/{sku}")
def check_sku(sku: str) -> Dict[str, Any]:
    """
    Check if a specific SKU is purchasable.
    
    Args:
        sku: The SKU to check
        
    Returns:
        Purchasability status and product details if found
        
    Raises:
        503: If catalog not loaded
    """
    catalog = get_catalog()
    
    if not catalog.is_loaded:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "CATALOG_NOT_LOADED",
                "message": "Catalog must be loaded first",
                "action": "Call /api/v1/catalog/wiring/load",
            }
        )
    
    is_purchasable = catalog.is_purchasable(sku)
    product = catalog.get_product(sku)
    
    result = {
        "sku": sku,
        "is_purchasable": is_purchasable,
    }
    
    if product:
        result["product"] = {
            "name": product.name,
            "product_line": product.product_line,
            "category": product.category,
            "evidence_tier": product.evidence_tier,
            "price_usd": product.price_usd,
            "sex_target": product.sex_target,
            "governance_status": product.governance_status,
            "ingredient_tags": product.ingredient_tags,
        }
    else:
        result["reason"] = "SKU not found in catalog"
    
    return result


@router.post("/filter")
def filter_skus(request: FilterSkusRequest) -> FilterSkusResponse:
    """
    Filter a list of SKUs to only include purchasable ones.
    
    Args:
        request: List of SKUs to filter
        
    Returns:
        Filtered SKU list with statistics
        
    Raises:
        503: If catalog not loaded
    """
    try:
        filtered = filter_to_catalog(request.skus)
        removed = [sku for sku in request.skus if sku not in filtered]
        
        return FilterSkusResponse(
            input_count=len(request.skus),
            output_count=len(filtered),
            filtered_skus=filtered,
            removed_skus=removed,
        )
    except CatalogWiringError as e:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "CATALOG_NOT_LOADED",
                "message": str(e),
                "action": "Call /api/v1/catalog/wiring/load first",
            }
        )


@router.get("/all-skus")
def list_all_skus() -> Dict[str, Any]:
    """
    List all purchasable SKUs.
    
    Returns:
        List of all available SKUs grouped by product line and tier
        
    Raises:
        503: If catalog not loaded
    """
    catalog = get_catalog()
    
    if not catalog.is_loaded:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "CATALOG_NOT_LOADED",
                "message": "Catalog must be loaded first",
                "action": "Call /api/v1/catalog/wiring/load",
            }
        )
    
    # Group by product line
    maximo_skus = sorted(catalog.filter_by_product_line("MAXimo²"))
    maxima_skus = sorted(catalog.filter_by_product_line("MAXima²"))
    universal_skus = sorted(catalog.filter_by_product_line("universal"))
    
    # Group by tier
    tier1_skus = sorted(catalog.filter_by_evidence_tier("TIER_1"))
    tier2_skus = sorted(catalog.filter_by_evidence_tier("TIER_2"))
    
    return {
        "total": catalog.product_count,
        "by_product_line": {
            "maximo": {
                "count": len(maximo_skus),
                "skus": maximo_skus,
            },
            "maxima": {
                "count": len(maxima_skus),
                "skus": maxima_skus,
            },
            "universal": {
                "count": len(universal_skus),
                "skus": universal_skus,
            },
        },
        "by_evidence_tier": {
            "tier1": {
                "count": len(tier1_skus),
                "skus": tier1_skus,
            },
            "tier2": {
                "count": len(tier2_skus),
                "skus": tier2_skus,
            },
        },
        "version": CATALOG_WIRING_VERSION,
    }


@router.get("/products")
def list_all_products() -> Dict[str, Any]:
    """
    List all purchasable products with full details.
    
    Returns:
        List of all available products with details
        
    Raises:
        503: If catalog not loaded
    """
    catalog = get_catalog()
    
    if not catalog.is_loaded:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "CATALOG_NOT_LOADED",
                "message": "Catalog must be loaded first",
                "action": "Call /api/v1/catalog/wiring/load",
            }
        )
    
    products = []
    for product in catalog.get_all_products():
        products.append({
            "sku": product.sku,
            "name": product.name,
            "product_line": product.product_line,
            "category": product.category,
            "evidence_tier": product.evidence_tier,
            "price_usd": product.price_usd,
            "sex_target": product.sex_target,
            "ingredient_tags": product.ingredient_tags,
        })
    
    # Sort by tier then name
    products.sort(key=lambda p: (p["evidence_tier"], p["name"]))
    
    return {
        "total": len(products),
        "products": products,
        "version": CATALOG_WIRING_VERSION,
    }
