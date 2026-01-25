"""
GenoMAX² Catalog API Endpoints
==============================

REST API endpoints for Supliful catalog integration.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import List, Optional
from pydantic import BaseModel

from .supliful_catalog import (
    get_catalog_manager,
    ProductLine,
    ProductCategory,
    IngredientTier
)


class ProductRecommendationRequest(BaseModel):
    """Request model for product recommendations"""
    sex: str
    routing_flags: List[str]
    active_gates: List[str] = []
    max_products: int = 10


class ProductSafetyCheckRequest(BaseModel):
    """Request model for product safety check"""
    sku: str
    active_gates: List[str]


def register_catalog_endpoints(app: FastAPI):
    """Register catalog endpoints with the FastAPI app"""
    
    @app.get("/api/v1/catalog/status")
    async def catalog_status():
        """Get catalog status and statistics"""
        manager = get_catalog_manager()
        stats = manager.get_catalog_stats()
        
        return {
            "status": "operational",
            "version": "1.0",
            "catalog": stats,
            "supliful_integration": {
                "enabled": True,
                "api_version": "v1",
                "fulfillment_ready": True
            },
            "governance": {
                "mode": "append_only",
                "immutable_entries": True,
                "audit_logging": True
            }
        }
    
    @app.get("/api/v1/catalog/products")
    async def list_products(
        product_line: Optional[str] = Query(None, description="Filter by product line: MAXimo², MAXima², Universal"),
        category: Optional[str] = Query(None, description="Filter by category"),
        sex: Optional[str] = Query(None, description="Filter by sex: male, female"),
        active_only: bool = Query(True, description="Only return active products")
    ):
        """List all products in the catalog"""
        manager = get_catalog_manager()
        
        products = list(manager.products.values())
        
        # Apply filters
        if sex:
            products = manager.get_products_for_sex(sex)
        
        if product_line:
            try:
                line = ProductLine(product_line)
                products = [p for p in products if p.product_line == line]
            except ValueError:
                # Try matching by name
                products = [p for p in products if p.product_line.value.lower() == product_line.lower()]
        
        if category:
            try:
                cat = ProductCategory(category.lower())
                products = [p for p in products if p.category == cat]
            except ValueError:
                pass
        
        if active_only:
            products = [p for p in products if p.active]
        
        return {
            "count": len(products),
            "products": [
                {
                    "sku": p.sku,
                    "supliful_id": p.supliful_id,
                    "name": p.name,
                    "product_line": p.product_line.value,
                    "category": p.category.value,
                    "price_usd": p.price_usd,
                    "wholesale_price_usd": p.wholesale_price_usd,
                    "serving_size": p.serving_size,
                    "servings_per_container": p.servings_per_container,
                    "active": p.active
                }
                for p in products
            ]
        }
    
    @app.get("/api/v1/catalog/products/{sku}")
    async def get_product(sku: str):
        """Get detailed product information by SKU"""
        manager = get_catalog_manager()
        product = manager.get_product(sku)
        
        if not product:
            raise HTTPException(status_code=404, detail=f"Product not found: {sku}")
        
        # Get ingredient details
        ingredients_detail = []
        for ing in product.ingredients:
            ingredient_info = manager.ingredients.get(ing.ingredient_code)
            ingredients_detail.append({
                "code": ing.ingredient_code,
                "amount": ing.amount,
                "unit": ing.unit,
                "form": ing.form,
                "details": {
                    "name": ingredient_info.name if ingredient_info else ing.ingredient_code,
                    "tier": ingredient_info.tier.value if ingredient_info else "unknown",
                    "optimal_dose": ingredient_info.optimal_dose if ingredient_info else None,
                    "biomarkers_affected": ingredient_info.biomarkers_affected if ingredient_info else [],
                    "safety_notes": ingredient_info.safety_notes if ingredient_info else None
                } if ingredient_info else None
            })
        
        return {
            "sku": product.sku,
            "supliful_id": product.supliful_id,
            "name": product.name,
            "product_line": product.product_line.value,
            "category": product.category.value,
            "description": product.description,
            "price_usd": product.price_usd,
            "wholesale_price_usd": product.wholesale_price_usd,
            "serving_size": product.serving_size,
            "servings_per_container": product.servings_per_container,
            "ingredients": ingredients_detail,
            "safety_routing": {
                "blocked_by_gates": product.blocked_by_gates,
                "caution_with_gates": product.caution_with_gates,
                "requires_biomarkers": product.requires_biomarkers,
                "recommended_for_flags": product.recommended_for_flags
            },
            "metadata": {
                "created_at": product.created_at,
                "version": product.version,
                "checksum": product.checksum,
                "active": product.active
            }
        }
    
    @app.get("/api/v1/catalog/ingredients")
    async def list_ingredients(
        tier: Optional[str] = Query(None, description="Filter by tier: tier_1, tier_2, tier_3"),
        include_rejected: bool = Query(False, description="Include TIER_3 rejected ingredients")
    ):
        """List all ingredients in the database"""
        manager = get_catalog_manager()
        
        ingredients = list(manager.ingredients.values())
        
        if tier:
            try:
                tier_enum = IngredientTier(tier.lower())
                ingredients = [i for i in ingredients if i.tier == tier_enum]
            except ValueError:
                pass
        
        if not include_rejected:
            ingredients = [i for i in ingredients if i.tier != IngredientTier.TIER_3]
        
        return {
            "count": len(ingredients),
            "ingredients": [
                {
                    "code": i.code,
                    "name": i.name,
                    "tier": i.tier.value,
                    "canonical_unit": i.canonical_unit,
                    "dose_range": {
                        "min": i.min_dose,
                        "optimal": i.optimal_dose,
                        "max": i.max_dose
                    },
                    "biomarkers_affected": i.biomarkers_affected,
                    "contraindications": i.contraindications,
                    "drug_interactions": i.drug_interactions,
                    "safety_notes": i.safety_notes
                }
                for i in ingredients
            ]
        }
    
    @app.get("/api/v1/catalog/ingredients/{code}")
    async def get_ingredient(code: str):
        """Get detailed ingredient information"""
        manager = get_catalog_manager()
        ingredient = manager.ingredients.get(code)
        
        if not ingredient:
            raise HTTPException(status_code=404, detail=f"Ingredient not found: {code}")
        
        # Find products containing this ingredient
        products_with_ingredient = manager._by_ingredient.get(code, [])
        
        return {
            "code": ingredient.code,
            "name": ingredient.name,
            "tier": ingredient.tier.value,
            "canonical_unit": ingredient.canonical_unit,
            "dose_range": {
                "min": ingredient.min_dose,
                "optimal": ingredient.optimal_dose,
                "max": ingredient.max_dose
            },
            "biomarkers_affected": ingredient.biomarkers_affected,
            "contraindications": ingredient.contraindications,
            "drug_interactions": ingredient.drug_interactions,
            "safety_notes": ingredient.safety_notes,
            "products_containing": products_with_ingredient,
            "tier_description": {
                "tier_1": "Strong evidence: ≥20 RCTs, >2000 participants, validated biomarkers",
                "tier_2": "Moderate evidence: 5-19 RCTs, contextual use",
                "tier_3": "REJECTED: Insufficient evidence or safety concerns"
            }.get(ingredient.tier.value, "Unknown")
        }
    
    @app.post("/api/v1/catalog/recommend")
    async def recommend_products(request: ProductRecommendationRequest):
        """
        Get product recommendations based on routing flags and safety gates.
        
        This is the core recommendation engine that maps biomarker-derived
        routing constraints to appropriate products.
        """
        manager = get_catalog_manager()
        
        recommendations = manager.recommend_products(
            sex=request.sex,
            routing_flags=request.routing_flags,
            active_gates=request.active_gates,
            max_products=request.max_products
        )
        
        return {
            "sex": request.sex,
            "routing_flags_provided": request.routing_flags,
            "active_safety_gates": request.active_gates,
            "recommendation_count": len(recommendations),
            "recommendations": recommendations,
            "note": "Products are recommended based on biomarker-derived routing flags and filtered by safety gates"
        }
    
    @app.post("/api/v1/catalog/safety-check")
    async def check_product_safety(request: ProductSafetyCheckRequest):
        """
        Check if a specific product is safe given active safety gates.
        
        Returns blocked status and any caution flags.
        """
        manager = get_catalog_manager()
        
        result = manager.check_product_safety(
            sku=request.sku,
            active_gates=request.active_gates
        )
        
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        
        return result
    
    @app.get("/api/v1/catalog/product-lines")
    async def get_product_lines():
        """Get available product lines with descriptions"""
        return {
            "product_lines": [
                {
                    "code": ProductLine.MAXIMO2.value,
                    "name": "MAXimo²",
                    "target": "Male Biology",
                    "description": "Gender-optimized supplements for male physiology, hormone support, and metabolic optimization"
                },
                {
                    "code": ProductLine.MAXIMA2.value,
                    "name": "MAXima²",
                    "target": "Female Biology",
                    "description": "Gender-optimized supplements for female physiology, hormone balance, and metabolic health"
                },
                {
                    "code": ProductLine.UNIVERSAL.value,
                    "name": "Universal",
                    "target": "All Users",
                    "description": "Gender-neutral supplements suitable for all users"
                }
            ]
        }
    
    @app.get("/api/v1/catalog/categories")
    async def get_categories():
        """Get available product categories"""
        manager = get_catalog_manager()
        
        return {
            "categories": [
                {
                    "code": cat.value,
                    "name": cat.name.replace("_", " ").title(),
                    "product_count": len(manager._by_category.get(cat, []))
                }
                for cat in ProductCategory
            ]
        }
    
    @app.get("/api/v1/catalog/flags/{flag}")
    async def get_products_for_flag(flag: str):
        """Get all products recommended for a specific routing flag"""
        manager = get_catalog_manager()
        
        products = manager.get_products_for_flag(flag)
        
        return {
            "flag": flag,
            "product_count": len(products),
            "products": [
                {
                    "sku": p.sku,
                    "name": p.name,
                    "product_line": p.product_line.value,
                    "category": p.category.value,
                    "price_usd": p.price_usd
                }
                for p in products
            ]
        }
    
    @app.get("/api/v1/catalog/export")
    async def export_catalog():
        """Export full catalog as JSON (for backup/sync)"""
        manager = get_catalog_manager()
        return manager.to_dict()
    
    return [
        "/api/v1/catalog/status",
        "/api/v1/catalog/products",
        "/api/v1/catalog/products/{sku}",
        "/api/v1/catalog/ingredients",
        "/api/v1/catalog/ingredients/{code}",
        "/api/v1/catalog/recommend",
        "/api/v1/catalog/safety-check",
        "/api/v1/catalog/product-lines",
        "/api/v1/catalog/categories",
        "/api/v1/catalog/flags/{flag}",
        "/api/v1/catalog/export"
    ]
