"""
GenoMAX2 API Server Entry Point v3.28.0
Bloodwork Engine v2.0 with Auto-Migration

v3.28.0:
- Bloodwork Engine upgraded to v2.0 (40 markers, 31 safety gates)
- Auto-migration runner on startup
- OCR parser service for blood test uploads
- Lab adapter interface for API integrations
- Safety routing service for ingredient filtering
- Health check endpoints for deployment verification

v3.27.0:
- Add Launch v1 enforcement router
- GET /api/v1/qa/launch-v1/pairing - Environment pairing validation
- GET /api/v1/launch-v1/export/design - Excel export with LAUNCH_V1_SUMMARY
- GET /api/v1/launch-v1/products - List Launch v1 products
- Shopify endpoints now enforce is_launch_v1 = TRUE
- All external pipelines use HARD GUARDRAIL filter

Use this file for Railway deployment:
  uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===== RUN MIGRATIONS ON STARTUP =====
def run_startup_migrations():
    """Run pending database migrations on startup."""
    try:
        from scripts.run_migrations import run_pending_migrations
        logger.info("Running startup migrations...")
        result = run_pending_migrations()
        if result['success']:
            logger.info(f"Migrations complete: {result['executed']} executed, {result['skipped']} skipped")
        else:
            logger.error(f"Migration failed: {result['errors']}")
        return result
    except ImportError as e:
        logger.warning(f"Migration runner not available: {e}")
        return {'success': True, 'executed': 0, 'skipped': 0}
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return {'success': False, 'errors': [str(e)]}

# Run migrations before importing app (ensures schema is ready)
if os.environ.get('RUN_MIGRATIONS', 'true').lower() == 'true':
    run_startup_migrations()

from api_server import app, get_db, now_iso
from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA
import json

# ===== BLOODWORK ENGINE V2 =====
try:
    from bloodwork_engine.api import register_bloodwork_endpoints
    register_bloodwork_endpoints(app)
    from bloodwork_engine import __version__ as bw_version
    print(f"Bloodwork Engine v{bw_version} endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Bloodwork Engine: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== HEALTH CHECK ENDPOINTS (v3.28.0) =====
try:
    from app.health import router as health_router
    app.include_router(health_router)
    print("Health Check endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Health Check: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== MIGRATION RUNNER =====
try:
    from app.migrations.runner import router as migrations_router
    app.include_router(migrations_router)
    print("Migration Runner endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Migration Runner: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== SUPPLIER CATALOG ADMIN =====
try:
    from app.routers.supplier_catalog_admin import router as supplier_catalog_router
    app.include_router(supplier_catalog_router)
    print("Supplier Catalog Admin endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Supplier Catalog Admin: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== QA ALLOWLIST MAPPING =====
try:
    from app.qa.allowlist import router as allowlist_router
    app.include_router(allowlist_router)
    print("QA Allowlist Mapping endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading QA Allowlist Mapping: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== SHOPIFY INTEGRATION (v3.24.0, updated v3.27.0 with Launch v1 enforcement) =====
try:
    from app.integrations.shopify_router import router as shopify_router
    app.include_router(shopify_router)
    print("Shopify Integration endpoints registered successfully (Launch v1 enforced)")
except Exception as e:
    print(f"ERROR loading Shopify Integration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== COPY CLEANUP (v3.25.0) =====
try:
    from app.copy.router import router as copy_router
    app.include_router(copy_router)
    print("Copy Cleanup endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Copy Cleanup: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== LAUNCH V1 ENFORCEMENT (v3.27.0) =====
try:
    from app.launch.enforcement import router as launch_enforcement_router
    app.include_router(launch_enforcement_router)
    print("Launch v1 Enforcement endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Launch v1 Enforcement: {type(e).__name__}: {e}")
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
    
    # Filter for health routes
    health_routes = [r for r in routes if 'health' in r['path'].lower()]
    
    # Filter for shopify routes
    shopify_routes = [r for r in routes if 'shopify' in r['path'].lower()]
    
    # Filter for launch/tier routes
    launch_routes = [r for r in routes if 'launch' in r['path'].lower() or 'tier' in r['path'].lower()]
    
    return {
        "total_routes": len(routes),
        "bloodwork_routes": bloodwork_routes,
        "health_routes": health_routes,
        "shopify_routes": shopify_routes,
        "launch_routes": launch_routes,
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
