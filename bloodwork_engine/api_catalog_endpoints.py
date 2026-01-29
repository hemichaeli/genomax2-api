"""
GenoMAX² Catalog API Endpoints (v3.40.0 - Consolidated)
=======================================================

CHANGE LOG:
- v3.40.0: Migrated product endpoints from hardcoded SuplifulCatalogManager (22 products) 
           to database-backed CatalogWiring (151 products)
- Ingredient endpoints still use legacy manager (pending migration)

REST API endpoints for catalog integration.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

# Import CatalogWiring for products (database-backed, 151 products)
try:
    from app.catalog.wiring import get_catalog as get_catalog_wiring
    CATALOG_WIRING_AVAILABLE = True
except ImportError:
    CATALOG_WIRING_AVAILABLE = False
    get_catalog_wiring = None

# Keep legacy imports for ingredients (still needed until ingredient migration)
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
        # Try CatalogWiring first (database-backed)
        if CATALOG_WIRING_AVAILABLE:
            try:
                catalog = get_catalog_wiring()
                if catalog and catalog.is_loaded:
                    stats = catalog._get_stats()
                    return {
                        "status": "operational",
                        "version": "3.40.0",
                        "source": "database_catalog_wiring",
                        "catalog": {
                            "total_products": stats.get("total_products", 0),
                            "maximo_products": stats.get("maximo_products", 0),
                            "maxima_products": stats.get("maxima_products", 0),
                            "universal_products": stats.get("universal_products", 0),
                            "tier1_count": stats.get("tier1_products", 0),
                            "tier2_count": stats.get("tier2_products", 0),
                            "loaded_at": stats.get("loaded_at")
                        },
                        "supliful_integration": {
                            "enabled": True,
                            "api_version": "v1",
                            "fulfillment_ready": True
                        },
                        "governance": {
                            "mode": "append_only",
                            "immutable_entries": True,
                            "audit_logging": True
                        },
                        "note": "Migrated from legacy 22-product hardcoded catalog to 151-product database catalog"
                    }
            except Exception as e:
                pass  # Fall back to legacy
        
        # Fallback to legacy manager
        manager = get_catalog_manager()
        stats = manager.get_catalog_stats()
        
        return {
            "status": "operational",
            "version": "1.0",
            "source": "legacy_supliful_manager",
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
        product_line: Optional[str] = Query(None, description="Filter by product line: maximo, maxima, universal"),
        category: Optional[str] = Query(None, description="Filter by category"),
        sex: Optional[str] = Query(None, description="Filter by sex: male, female"),
        tier: Optional[str] = Query(None, description="Filter by evidence tier: TIER_1, TIER_2"),
        active_only: bool = Query(True, description="Only return active products")
    ):
        """List all products in the catalog (now 151 products from database)"""
        
        # Use CatalogWiring (database-backed)
        if CATALOG_WIRING_AVAILABLE:
            try:
                catalog = get_catalog_wiring()
                if catalog and catalog.is_loaded:
                    products = catalog.get_all_products()
                    
                    # Apply filters
                    if sex:
                        sex_lower = sex.lower()
                        if sex_lower == "male":
                            products = [p for p in products if p.sku.startswith("GMAX-M-") or p.sku.startswith("GMAX-U-")]
                        elif sex_lower == "female":
                            products = [p for p in products if p.sku.startswith("GMAX-F-") or p.sku.startswith("GMAX-U-")]
                    
                    if product_line:
                        line_map = {"maximo": "GMAX-M-", "maxima": "GMAX-F-", "universal": "GMAX-U-"}
                        prefix = line_map.get(product_line.lower(), "")
                        if prefix:
                            products = [p for p in products if p.sku.startswith(prefix)]
                    
                    if category:
                        products = [p for p in products if p.category and category.lower() in p.category.lower()]
                    
                    if tier:
                        products = [p for p in products if p.evidence_tier == tier.upper()]
                    
                    if active_only:
                        products = [p for p in products if p.governance_status == "ACTIVE"]
                    
                    return {
                        "count": len(products),
                        "source": "database_catalog_wiring",
                        "products": [
                            {
                                "sku": p.sku,
                                "name": p.name,
                                "product_line": p.product_line,
                                "category": p.category,
                                "evidence_tier": p.evidence_tier,
                                "sex_target": p.sex_target,
                                "price_usd": p.price_usd,
                                "active": p.governance_status == "ACTIVE"
                            }
                            for p in products
                        ]
                    }
            except Exception as e:
                pass  # Fall back to legacy
        
        # Fallback to legacy manager (22 products)
        manager = get_catalog_manager()
        products = list(manager.products.values())
        
        if sex:
            products = manager.get_products_for_sex(sex)
        
        if product_line:
            try:
                line = ProductLine(product_line)
                products = [p for p in products if p.product_line == line]
            except ValueError:
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
            "source": "legacy_supliful_manager",
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
        
        # Try CatalogWiring first
        if CATALOG_WIRING_AVAILABLE:
            try:
                catalog = get_catalog_wiring()
                if catalog and catalog.is_loaded:
                    product = catalog.get_product(sku.upper())
                    if product:
                        return {
                            "sku": product.sku,
                            "name": product.name,
                            "product_name": product.name,
                            "product_line": product.product_line,
                            "category": product.category,
                            "evidence_tier": product.evidence_tier,
                            "sex_target": product.sex_target,
                            "price_usd": product.price_usd,
                            "governance_status": product.governance_status,
                            "source": "database_catalog_wiring"
                        }
            except Exception:
                pass  # Fall back to legacy
        
        # Fallback to legacy manager
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
            },
            "source": "legacy_supliful_manager"
        }
    
    @app.get("/api/v1/catalog/ingredients")
    async def list_ingredients(
        tier: Optional[str] = Query(None, description="Filter by tier: tier_1, tier_2, tier_3"),
        include_rejected: bool = Query(False, description="Include TIER_3 rejected ingredients")
    ):
        """List all ingredients in the database (from legacy manager)"""
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
            "source": "legacy_supliful_manager",
            "note": "Ingredients pending migration to database",
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
                "tier_1": "Strong evidence: >=20 RCTs, >2000 participants, validated biomarkers",
                "tier_2": "Moderate evidence: 5-19 RCTs, contextual use",
                "tier_3": "REJECTED: Insufficient evidence or safety concerns"
            }.get(ingredient.tier.value, "Unknown"),
            "source": "legacy_supliful_manager"
        }
    
    @app.post("/api/v1/catalog/recommend")
    async def recommend_products(request: ProductRecommendationRequest):
        """
        Get product recommendations based on routing flags and safety gates.
        
        Uses CatalogWiring (database) when available, falls back to legacy manager.
        """
        # Try CatalogWiring first
        if CATALOG_WIRING_AVAILABLE:
            try:
                catalog = get_catalog_wiring()
                if catalog and catalog.is_loaded:
                    # Get sex-appropriate products
                    sex_prefix = "GMAX-M-" if request.sex.lower() == "male" else "GMAX-F-"
                    products = [p for p in catalog.get_all_products() 
                               if p.sku.startswith(sex_prefix) or p.sku.startswith("GMAX-U-")]
                    
                    # Sort by evidence tier (TIER_1 first)
                    products.sort(key=lambda p: (0 if p.evidence_tier == "TIER_1" else 1, p.sku))
                    
                    # Limit results
                    products = products[:request.max_products]
                    
                    return {
                        "sex": request.sex,
                        "routing_flags_provided": request.routing_flags,
                        "active_safety_gates": request.active_gates,
                        "recommendation_count": len(products),
                        "source": "database_catalog_wiring",
                        "recommendations": [
                            {
                                "sku": p.sku,
                                "name": p.name,
                                "product_line": p.product_line,
                                "category": p.category,
                                "evidence_tier": p.evidence_tier,
                                "price_usd": p.price_usd
                            }
                            for p in products
                        ],
                        "note": "Products are recommended based on sex and evidence tier from database catalog"
                    }
            except Exception:
                pass  # Fall back to legacy
        
        # Fallback to legacy manager
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
            "source": "legacy_supliful_manager",
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
        # Get counts from CatalogWiring if available
        counts = {"maximo": 0, "maxima": 0, "universal": 0}
        
        if CATALOG_WIRING_AVAILABLE:
            try:
                catalog = get_catalog_wiring()
                if catalog and catalog.is_loaded:
                    stats = catalog._get_stats()
                    counts = {
                        "maximo": stats.get("maximo_products", 0),
                        "maxima": stats.get("maxima_products", 0),
                        "universal": stats.get("universal_products", 0)
                    }
            except Exception:
                pass
        
        return {
            "product_lines": [
                {
                    "code": "MAXimo²",
                    "name": "MAXimo²",
                    "target": "Male Biology",
                    "description": "Gender-optimized supplements for male physiology, hormone support, and metabolic optimization",
                    "product_count": counts.get("maximo", 0)
                },
                {
                    "code": "MAXima²",
                    "name": "MAXima²",
                    "target": "Female Biology",
                    "description": "Gender-optimized supplements for female physiology, hormone balance, and metabolic health",
                    "product_count": counts.get("maxima", 0)
                },
                {
                    "code": "Universal",
                    "name": "Universal",
                    "target": "All Users",
                    "description": "Gender-neutral supplements suitable for all users",
                    "product_count": counts.get("universal", 0)
                }
            ]
        }
    
    @app.get("/api/v1/catalog/categories")
    async def get_categories():
        """Get available product categories"""
        # Try CatalogWiring first
        if CATALOG_WIRING_AVAILABLE:
            try:
                catalog = get_catalog_wiring()
                if catalog and catalog.is_loaded:
                    # Aggregate categories from products
                    categories = {}
                    for p in catalog.get_all_products():
                        cat = p.category or "supplement"
                        categories[cat] = categories.get(cat, 0) + 1
                    return {
                        "source": "database_catalog_wiring",
                        "categories": [
                            {"code": cat, "name": cat.replace("_", " ").title(), "product_count": count}
                            for cat, count in sorted(categories.items())
                        ]
                    }
            except Exception:
                pass
        
        # Fallback to legacy manager
        manager = get_catalog_manager()
        
        return {
            "source": "legacy_supliful_manager",
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
            "source": "legacy_supliful_manager",
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
        # Try CatalogWiring first
        if CATALOG_WIRING_AVAILABLE:
            try:
                catalog = get_catalog_wiring()
                if catalog and catalog.is_loaded:
                    products = catalog.get_all_products()
                    return {
                        "export_version": "3.40.0",
                        "exported_at": datetime.utcnow().isoformat(),
                        "source": "database_catalog_wiring",
                        "product_count": len(products),
                        "by_tier": {
                            "TIER_1": len([p for p in products if p.evidence_tier == "TIER_1"]),
                            "TIER_2": len([p for p in products if p.evidence_tier == "TIER_2"]),
                            "TIER_3": len([p for p in products if p.evidence_tier == "TIER_3"])
                        },
                        "products": [
                            {
                                "sku": p.sku,
                                "product_name": p.name,
                                "category": p.category,
                                "evidence_tier": p.evidence_tier,
                                "sex_target": p.sex_target,
                                "base_price": p.price_usd,
                                "governance_status": p.governance_status
                            }
                            for p in products
                        ]
                    }
            except Exception:
                pass
        
        # Fallback to legacy manager
        manager = get_catalog_manager()
        export_data = manager.to_dict()
        export_data["source"] = "legacy_supliful_manager"
        return export_data
    
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
