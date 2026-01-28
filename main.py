"""
GenoMAX2 API Server Entry Point v3.34.0
Brain Pipeline Integration (Issue #16)

v3.34.0:
- NEW: Brain Orchestrator module (bloodwork_engine/brain_orchestrator.py)
- NEW: Brain Routes (/api/v1/brain/*) for full pipeline execution
- POST /api/v1/brain/run - Execute complete Brain pipeline
- GET /api/v1/brain/run/{run_id} - Get run status/results
- POST /api/v1/brain/evaluate - Quick deficiency evaluation
- POST /api/v1/brain/canonical-handoff - Complete bloodwork-to-brain integration
- 13 priority biomarkers with gender-specific thresholds
- Module scoring: evidence + biomarker match + goal alignment + lifecycle
- Safety enforcement: "Blood does not negotiate" principle
- Gender optimization: MAXimo²/MAXima² filtering
- Lifecycle awareness: pregnancy, breastfeeding, perimenopause, athletic
- Full audit trail in brain_runs table

v3.33.0:
- NEW: Catalog Wiring module (app/catalog/wiring.py)
- CatalogWiring singleton loads canonical SKU universe from DB
- Hard abort (503) if catalog unavailable - no mocks, no fallbacks
- Blocked SKUs (governance_status=BLOCKED) never enter pipeline
- /api/v1/catalog/wiring/* endpoints for health, load, filter

v3.32.0:
- NEW: Constraint Translator module (app/brain/constraint_translator.py)
- NEW: /api/v1/constraints/* admin endpoints for testing/inspection
- Translates bloodwork constraint codes (BLOCK_IRON, CAUTION_RENAL) into
  canonical enforcement semantics for routing/matching layers
- Pure + deterministic translation with SHA-256 audit hashes
- 24 constraint mappings covering iron, potassium, iodine, hepatic, renal,
  methylation, thyroid, cardiovascular, and more
- QA matrix endpoint for full scenario validation

v3.31.2:
- Fix migrations_router typo in app.include_router()
- Ensure catalog_products migration endpoint loads properly

v3.31.1:
- Add catalog_products migration endpoint for TIER 1/2 catalog import

v3.31.0:
- New app/webhooks module with Junction (Vital) and Lab Testing API integration
- HMAC-SHA256 signature verification for Junction webhooks
- API key verification for Lab Testing API webhooks
- Biomarker code normalization (36 mappings)
- Unit conversion (nmol/L, pmol/L, umol/L, mmol/L)
- Automatic orchestrate/v2 triggering on lab results

v3.30.1:
- Fix safety gate counting in API endpoints (31 gates: 14/6/11 by tier)
- Bloodwork Engine v2.0.1 with correct get_safety_gate_summary()

v3.30.0:
- Supliful catalog integration with 185+ products
- MAXimo² (male) and MAXima² (female) product lines
- Append-only governance for catalog entries
- Biomarker-to-product recommendation engine
- Safety gate validation for product selection

v3.29.0:
- Add webhook endpoints for lab result notifications
- POST /api/v1/webhooks/vital - Junction (Vital) webhook receiver
- POST /api/v1/webhooks/labtestingapi - Lab Testing API webhook receiver
- GET /api/v1/webhooks/status - Webhook configuration status
- POST /api/v1/webhooks/test - Test webhook processing

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

# ===== BRAIN PIPELINE (v3.34.0 - Issue #16) =====
try:
    from bloodwork_engine.brain_routes import router as brain_router
    app.include_router(brain_router)
    from bloodwork_engine.brain_orchestrator import BRAIN_ORCHESTRATOR_VERSION
    print(f"Brain Pipeline {BRAIN_ORCHESTRATOR_VERSION} endpoints registered successfully")
    print("  - POST /api/v1/brain/run - Execute full pipeline")
    print("  - GET /api/v1/brain/run/{run_id} - Get run status")
    print("  - POST /api/v1/brain/evaluate - Quick deficiency evaluation")
    print("  - POST /api/v1/brain/canonical-handoff - Complete integration")
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

# ===== CATALOG ENDPOINTS (v3.30.0) =====
try:
    from bloodwork_engine.api_catalog_endpoints import register_catalog_endpoints
    catalog_routes = register_catalog_endpoints(app)
    print(f"Catalog endpoints registered successfully ({len(catalog_routes)} routes)")
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
    
    # Filter for brain routes
    brain_routes = [r for r in routes if 'brain' in r['path'].lower()]
    
    # Filter for health routes
    health_routes = [r for r in routes if 'health' in r['path'].lower()]
    
    # Filter for shopify routes
    shopify_routes = [r for r in routes if 'shopify' in r['path'].lower()]
    
    # Filter for launch/tier routes
    launch_routes = [r for r in routes if 'launch' in r['path'].lower() or 'tier' in r['path'].lower()]
    
    # Filter for webhook routes
    webhook_routes = [r for r in routes if 'webhook' in r['path'].lower()]
    
    # Filter for catalog routes
    catalog_routes = [r for r in routes if 'catalog' in r['path'].lower()]
    
    # Filter for constraint routes
    constraint_routes = [r for r in routes if 'constraint' in r['path'].lower()]
    
    # Filter for wiring routes
    wiring_routes = [r for r in routes if 'wiring' in r['path'].lower()]
    
    return {
        "total_routes": len(routes),
        "bloodwork_routes": bloodwork_routes,
        "brain_routes": brain_routes,
        "webhook_routes": webhook_routes,
        "catalog_routes": catalog_routes,
        "wiring_routes": wiring_routes,
        "constraint_routes": constraint_routes,
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
