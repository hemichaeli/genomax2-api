"""
GenoMAX² Catalog Wiring Endpoints (Issue #15)

API endpoints for catalog wiring management.

Routes:
- GET /api/v1/catalog/wiring/health - Health check
- GET /api/v1/catalog/wiring/load - Load/reload catalog
- GET /api/v1/catalog/wiring/stats - Catalog statistics
- GET /api/v1/catalog/wiring/check/{sku} - Check if SKU is purchasable
- POST /api/v1/catalog/wiring/filter - Filter SKU list

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
        Product counts by category and product line
        
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
        "maximo_products": len(catalog.filter_by_product_line("MAXimo²")),
        "maxima_products": len(catalog.filter_by_product_line("MAXima²")),
        "categories": categories,
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
            "price_usd": product.price_usd,
            "active": product.active,
            "governance_status": product.governance_status,
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
        List of all available SKUs
        
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
    
    return {
        "total": catalog.product_count,
        "maximo": {
            "count": len(maximo_skus),
            "skus": maximo_skus,
        },
        "maxima": {
            "count": len(maxima_skus),
            "skus": maxima_skus,
        },
        "version": CATALOG_WIRING_VERSION,
    }
