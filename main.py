"""
GenoMAX2 API Server Entry Point v3.24.0
Adds Shopify Integration endpoints for product export.

Use this file for Railway deployment:
  uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from api_server import app
from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA

# ===== BLOODWORK ENGINE V1 =====
try:
    from bloodwork_engine.api import register_bloodwork_endpoints
    register_bloodwork_endpoints(app)
    print("✅ Bloodwork Engine v1 endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Bloodwork Engine: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== MIGRATION RUNNER =====
try:
    from app.migrations.runner import router as migrations_router
    app.include_router(migrations_router)
    print("✅ Migration Runner endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Migration Runner: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== SUPPLIER CATALOG ADMIN =====
try:
    from app.routers.supplier_catalog_admin import router as supplier_catalog_router
    app.include_router(supplier_catalog_router)
    print("✅ Supplier Catalog Admin endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Supplier Catalog Admin: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== QA ALLOWLIST MAPPING =====
try:
    from app.qa.allowlist import router as allowlist_router
    app.include_router(allowlist_router)
    print("✅ QA Allowlist Mapping endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading QA Allowlist Mapping: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== SHOPIFY INTEGRATION (v3.24.0) =====
try:
    from app.integrations.shopify_router import router as shopify_router
    app.include_router(shopify_router)
    print("✅ Shopify Integration endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Shopify Integration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()


# ===== DEBUG: LIST ALL ROUTES =====
@app.get("/debug/routes")
def debug_routes():
    """List all registered routes for debugging."""
    routes = []
    for route in app.routes:
        if hasattr(route, 'path'):
            routes.append({
                "path": route.path,
                "name": getattr(route, 'name', None),
                "methods": list(route.methods) if hasattr(route, 'methods') else None
            })
    
    # Filter for bloodwork routes
    bloodwork_routes = [r for r in routes if 'bloodwork' in r['path'].lower()]
    
    # Filter for shopify routes
    shopify_routes = [r for r in routes if 'shopify' in r['path'].lower()]
    
    return {
        "total_routes": len(routes),
        "bloodwork_routes": bloodwork_routes,
        "shopify_routes": shopify_routes,
        "all_api_routes": [r for r in routes if r['path'].startswith('/api/')]
    }


# ===== PAINPOINTS AND LIFESTYLE SCHEMA ENDPOINTS =====

@app.get("/api/v1/brain/painpoints")
def list_painpoints():
    """List available painpoints with their mappings to supplement intents."""
    return {
        "count": len(PAINPOINTS_DICTIONARY),
        "painpoints": [
            {
                "id": key,
                "label": val.get("label"),
                "mapped_intents": list(val.get("mapped_intents", {}).keys())
            }
            for key, val in PAINPOINTS_DICTIONARY.items()
        ]
    }


@app.get("/api/v1/brain/lifestyle-schema")
def get_lifestyle_schema():
    """Get the lifestyle questionnaire schema for frontend forms."""
    return LIFESTYLE_SCHEMA


# For local development
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
