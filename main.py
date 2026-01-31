"""
GenoMAX2 API Server Entry Point v3.42.0
os_environment Normalization - Eliminates Universal

v3.42.0:
- NEW: os_environment Migration (app/migrations/os_environment_normalization.py)
- POST /api/v1/migrations/run/016-os-environment - Execute normalization
- GET /api/v1/migrations/status/016-os-environment - Check migration state
- GET /api/v1/migrations/preview/016-os-environment - Preview changes
- Eliminates Universal environment - only MAXimo² and MAXima²
- Splits unisex products into two rows with -M/-F suffixes

v3.41.0:
- NEW: Catalog Cleanup Admin (app/routers/catalog_cleanup_admin.py)
- GET /api/v1/admin/catalog-cleanup/preview - Preview cleanup changes
- POST /api/v1/admin/catalog-cleanup/execute?confirm=true - Execute cleanup
- GET /api/v1/admin/catalog-cleanup/health - Health check
- Deletes Universal products (not a valid product line)
- Strips MAXima²/MAXimo²/GenoMAX² prefixes from product names

v3.40.0:
- BREAKING: /api/v1/catalog/products now returns 151 products (was 22)
- NEW: Catalog Consolidation Migration (app/migrations/consolidate_catalog.py)
- POST /api/v1/migrations/run/consolidate-catalog - Add version_note, mark products
- GET /api/v1/migrations/status/consolidate-catalog - Check consolidation status
- Deprecates hardcoded SuplifulCatalogManager (22 products)
- All catalog endpoints now use CatalogWiring (151 products from database)
- Legacy products archived to /legacy/supliful_catalog_archive.py

v3.38.0:
- NEW: Gender-Specific Product Cleanup (app/migrations/cleanup_gender_specific.py)
- POST /api/v1/migrations/run/cleanup-gender-specific - Remove invalid cross-gender products
- GET /api/v1/migrations/status/gender-specific-cleanup - Check for issues
- Removes GMAX-F-SAW-PALM (Saw Palmetto is prostate health - male only)

v3.37.0:
- NEW: Full Gender Conversion Migration (app/migrations/convert_to_gender_specific.py)
- POST /api/v1/migrations/run/convert-to-gender-specific - Convert all products
- GET /api/v1/migrations/preview/convert-to-gender-specific - Preview conversion
- GET /api/v1/migrations/status/gender-catalog - Check gender catalog status
- Converts 65 GX-* products to 130 GMAX-M-*/GMAX-F-* pairs
- Every TIER 1/2 product now available as MAXimo² AND MAXima²
- Gender-specific descriptions for male/female optimization

v3.36.0:
- NEW: Gender-Specific Products Migration (app/migrations/add_gender_specific_products.py)
- Adds 18 new products (10 MAXimo², 8 MAXima², 2 Universal)

v3.35.0:
- NEW: Methylation Products Migration (app/migrations/add_methylation_products.py)

v3.34.0:
- NEW: Brain Orchestrator module (bloodwork_engine/brain_orchestrator.py)

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

# ===== BRAIN PIPELINE (v3.34.0 - Issue #16) =====
try:
    from bloodwork_engine.brain_routes import router as brain_router
    app.include_router(brain_router)
    from bloodwork_engine.brain_orchestrator import BRAIN_ORCHESTRATOR_VERSION
    print(f"Brain Pipeline {BRAIN_ORCHESTRATOR_VERSION} endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Brain Pipeline: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== WEBHOOK ENDPOINTS (v3.29.0 - Legacy) =====
try:
    from bloodwork_engine.api_webhook_endpoints import register_webhook_endpoints
    register_webhook_endpoints(app)
    print("Webhook endpoints (legacy) registered successfully")
except Exception as e:
    print(f"ERROR loading Webhook endpoints (legacy): {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== WEBHOOK INFRASTRUCTURE (v3.31.0 - New) =====
try:
    from app.webhooks import webhook_router
    app.include_router(webhook_router)
    print("Webhook Infrastructure (v3.31.0) registered: Junction + Lab Testing API")
except Exception as e:
    print(f"ERROR loading Webhook Infrastructure: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== CONSTRAINT TRANSLATOR (v3.32.0) =====
try:
    from app.brain.constraint_admin import router as constraint_router
    app.include_router(constraint_router)
    from app.brain.constraint_translator import __version__ as ct_version
    print(f"Constraint Translator v{ct_version} endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Constraint Translator: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== CATALOG ENDPOINTS (v3.30.0, updated v3.40.0 to use CatalogWiring) =====
try:
    from bloodwork_engine.api_catalog_endpoints import register_catalog_endpoints
    catalog_routes = register_catalog_endpoints(app)
    print(f"Catalog endpoints registered successfully ({len(catalog_routes)} routes)")
    print("  NOTE: v3.40.0 - Now using CatalogWiring (151 products from database)")
except Exception as e:
    print(f"ERROR loading Catalog endpoints: {type(e).__name__}: {e}")
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

# ===== CATALOG PRODUCTS MIGRATION (v3.31.1) =====
try:
    from app.migrations.catalog_products import router as catalog_migration_router
    app.include_router(catalog_migration_router)
    print("Catalog Products Migration endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Catalog Products Migration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== METHYLATION PRODUCTS MIGRATION (v3.35.0 - Issue #17) =====
try:
    from app.migrations.add_methylation_products import router as methylation_migration_router
    app.include_router(methylation_migration_router)
    print("Methylation Products Migration endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Methylation Products Migration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== GENDER-SPECIFIC PRODUCTS MIGRATION (v3.36.0) =====
try:
    from app.migrations.add_gender_specific_products import router as gender_products_router
    app.include_router(gender_products_router)
    print("Gender-Specific Products Migration endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Gender-Specific Products Migration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== FULL GENDER CONVERSION MIGRATION (v3.37.0) =====
try:
    from app.migrations.convert_to_gender_specific import router as gender_conversion_router
    app.include_router(gender_conversion_router)
    print("Full Gender Conversion Migration endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Gender Conversion Migration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== GENDER-SPECIFIC CLEANUP MIGRATION (v3.38.0) =====
try:
    from app.migrations.cleanup_gender_specific import router as gender_cleanup_router
    app.include_router(gender_cleanup_router)
    print("Gender-Specific Cleanup Migration endpoints registered successfully")
    print("  - POST /api/v1/migrations/run/cleanup-gender-specific")
    print("  - GET /api/v1/migrations/status/gender-specific-cleanup")
except Exception as e:
    print(f"ERROR loading Gender-Specific Cleanup Migration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== CATALOG CONSOLIDATION MIGRATION (v3.40.0) =====
try:
    from app.migrations.consolidate_catalog import router as consolidate_catalog_router
    app.include_router(consolidate_catalog_router)
    print("Catalog Consolidation Migration endpoints registered successfully")
    print("  - POST /api/v1/migrations/run/consolidate-catalog")
    print("  - GET /api/v1/migrations/status/consolidate-catalog")
except Exception as e:
    print(f"ERROR loading Catalog Consolidation Migration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== OS_ENVIRONMENT NORMALIZATION MIGRATION (v3.42.0) =====
try:
    from app.migrations.os_environment_normalization import router as os_env_migration_router
    app.include_router(os_env_migration_router)
    print("os_environment Normalization Migration endpoints registered successfully")
    print("  - POST /api/v1/migrations/run/016-os-environment")
    print("  - GET /api/v1/migrations/status/016-os-environment")
    print("  - GET /api/v1/migrations/preview/016-os-environment")
except Exception as e:
    print(f"ERROR loading os_environment Migration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== CATALOG WIRING (v3.33.0 - Issue #15) =====
try:
    from app.catalog.wiring_endpoints import router as catalog_wiring_router
    app.include_router(catalog_wiring_router)
    from app.catalog.wiring import CATALOG_WIRING_VERSION
    print(f"Catalog Wiring {CATALOG_WIRING_VERSION} endpoints registered successfully")
except Exception as e:
    print(f"ERROR loading Catalog Wiring: {type(e).__name__}: {e}")
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

# ===== CATALOG CLEANUP ADMIN (v3.41.0) =====
try:
    from app.routers.catalog_cleanup_admin import router as catalog_cleanup_router
    app.include_router(catalog_cleanup_router)
    print("Catalog Cleanup Admin endpoints registered successfully")
    print("  - GET /api/v1/admin/catalog-cleanup/preview")
    print("  - POST /api/v1/admin/catalog-cleanup/execute?confirm=true")
    print("  - GET /api/v1/admin/catalog-cleanup/health")
except Exception as e:
    print(f"ERROR loading Catalog Cleanup Admin: {type(e).__name__}: {e}")
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
    
    bloodwork_routes = [r for r in routes if 'bloodwork' in r['path'].lower()]
    brain_routes = [r for r in routes if 'brain' in r['path'].lower()]
    health_routes = [r for r in routes if 'health' in r['path'].lower()]
    shopify_routes = [r for r in routes if 'shopify' in r['path'].lower()]
    launch_routes = [r for r in routes if 'launch' in r['path'].lower() or 'tier' in r['path'].lower()]
    webhook_routes = [r for r in routes if 'webhook' in r['path'].lower()]
    catalog_routes = [r for r in routes if 'catalog' in r['path'].lower()]
    constraint_routes = [r for r in routes if 'constraint' in r['path'].lower()]
    wiring_routes = [r for r in routes if 'wiring' in r['path'].lower()]
    migration_routes = [r for r in routes if 'migration' in r['path'].lower()]
    
    return {
        "total_routes": len(routes),
        "bloodwork_routes": bloodwork_routes,
        "brain_routes": brain_routes,
        "webhook_routes": webhook_routes,
        "catalog_routes": catalog_routes,
        "wiring_routes": wiring_routes,
        "constraint_routes": constraint_routes,
        "migration_routes": migration_routes,
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
