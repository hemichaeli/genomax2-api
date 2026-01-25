"""
GenoMAX2 API Server Entry Point v3.28.0
Bloodwork Engine v2.0 with Auto-Migration

v3.28.0:
- Bloodwork Engine upgraded to v2.0 (40 markers, 31 safety gates)
- Auto-migration runner on startup
- OCR parser service for blood test uploads
- Lab adapter interface for API integrations
- Safety routing service for ingredient filtering

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
        logger.info("🔄 Running startup migrations...")
        result = run_pending_migrations()
        if result['success']:
            logger.info(f"✅ Migrations complete: {result['executed']} executed, {result['skipped']} skipped")
        else:
            logger.error(f"❌ Migration failed: {result['errors']}")
        return result
    except ImportError as e:
        logger.warning(f"⚠️ Migration runner not available: {e}")
        return {'success': True, 'executed': 0, 'skipped': 0}
    except Exception as e:
        logger.error(f"❌ Migration error: {e}")
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
    print(f"✅ Bloodwork Engine v{bw_version} endpoints registered successfully")
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

# ===== SHOPIFY INTEGRATION (v3.24.0, updated v3.27.0 with Launch v1 enforcement) =====
try:
    from app.integrations.shopify_router import router as shopify_router
    app.include_router(shopify_router)
    print("✅ Shopify Integration endpoints registered successfully (Launch v1 enforced)")
except Exception as e:
    print(f"❌ ERROR loading Shopify Integration: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== COPY CLEANUP (v3.25.0) =====
try:
    from app.copy.router import router as copy_router
    app.include_router(copy_router)
    print("✅ Copy Cleanup endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Copy Cleanup: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# ===== LAUNCH V1 ENFORCEMENT (v3.27.0) =====
try:
    from app.launch.enforcement import router as launch_enforcement_router
    app.include_router(launch_enforcement_router)
    print("✅ Launch v1 Enforcement endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Launch v1 Enforcement: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()


# ===== LAUNCH V1 LOCK MIGRATION (v3.26.0) =====

@app.get("/migrate-add-tier-column")
def migrate_add_tier_column():
    """
    Migration 009a: Add tier column to os_modules_v3_1.
    
    Default tiering based on os_layer:
    - Core = TIER 1
    - Adaptive = TIER 2
    - All others = TIER 3
    
    Run this BEFORE /migrate-lock-launch-v1
    """
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Step 1: Add tier column if not exists
        cur.execute("""
            ALTER TABLE os_modules_v3_1
            ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'TIER 3'
        """)
        
        # Step 2: Populate based on os_layer
        # Core = TIER 1
        cur.execute("""
            UPDATE os_modules_v3_1
            SET tier = 'TIER 1'
            WHERE os_layer = 'Core'
        """)
        tier_1_count = cur.rowcount
        
        # Adaptive = TIER 2
        cur.execute("""
            UPDATE os_modules_v3_1
            SET tier = 'TIER 2'
            WHERE os_layer = 'Adaptive'
        """)
        tier_2_count = cur.rowcount
        
        # Everything else = TIER 3
        cur.execute("""
            UPDATE os_modules_v3_1
            SET tier = 'TIER 3'
            WHERE os_layer NOT IN ('Core', 'Adaptive') OR os_layer IS NULL
        """)
        tier_3_count = cur.rowcount
        
        # Create index
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_os_modules_tier 
            ON os_modules_v3_1 (tier)
        """)
        
        # Get distribution
        cur.execute("""
            SELECT tier, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY tier
            ORDER BY tier
        """)
        distribution = {row["tier"]: row["count"] for row in cur.fetchall()}
        
        # Audit log
        cur.execute("""
            INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
            VALUES ('migration', NULL, 'add_tier_column', %s, NOW())
        """, (json.dumps({
            "migration": "009a_add_tier_column",
            "tier_1_updated": tier_1_count,
            "tier_2_updated": tier_2_count,
            "tier_3_updated": tier_3_count,
            "distribution": distribution
        }),))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "009a_add_tier_column",
            "timestamp": now_iso(),
            "tier_distribution": distribution,
            "mapping_used": {
                "TIER 1": "os_layer = 'Core'",
                "TIER 2": "os_layer = 'Adaptive'",
                "TIER 3": "all others"
            },
            "next_step": "Run /migrate-lock-launch-v1 to lock Launch v1 scope"
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.get("/migrate-lock-launch-v1")
def migrate_lock_launch_v1():
    """
    Migration 009b: Lock Launch v1 scope to TIER 1 + TIER 2 only.
    
    Prerequisites: Run /migrate-add-tier-column first if tier column doesn't exist.
    
    - Adds is_launch_v1 BOOLEAN column
    - Sets TRUE for TIER 1 and TIER 2 products
    - Sets FALSE for TIER 3 (preserved but excluded from launch)
    - Creates partial index for launch queries
    
    TIER 3 products remain in DB but are excluded from all launch pipelines.
    """
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Check if tier column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' AND column_name = 'tier'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {
                "error": "tier column not found",
                "fix": "Run /migrate-add-tier-column first",
                "status": "BLOCKED"
            }
        
        # Step 1: Add is_launch_v1 column if not exists
        cur.execute("""
            ALTER TABLE os_modules_v3_1
            ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE
        """)
        
        # Step 2a: Include Tier 1 + Tier 2
        cur.execute("""
            UPDATE os_modules_v3_1
            SET is_launch_v1 = TRUE
            WHERE tier IN ('TIER 1', 'TIER 2')
        """)
        tier_1_2_count = cur.rowcount
        
        # Step 2b: Explicitly exclude Tier 3
        cur.execute("""
            UPDATE os_modules_v3_1
            SET is_launch_v1 = FALSE
            WHERE tier = 'TIER 3'
        """)
        tier_3_count = cur.rowcount
        
        # Step 2c: Handle NULL tiers
        cur.execute("""
            UPDATE os_modules_v3_1
            SET is_launch_v1 = FALSE
            WHERE tier IS NULL OR tier NOT IN ('TIER 1', 'TIER 2', 'TIER 3')
        """)
        null_tier_count = cur.rowcount
        
        # Step 3: Create partial index
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
            ON os_modules_v3_1 (is_launch_v1) 
            WHERE is_launch_v1 = TRUE
        """)
        
        # Validation: Get tier distribution for launch v1
        cur.execute("""
            SELECT tier, COUNT(*) as count
            FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE
            GROUP BY tier
            ORDER BY tier
        """)
        launch_distribution = {row["tier"]: row["count"] for row in cur.fetchall()}
        
        # Validation: Confirm Tier 3 preserved
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded,
                SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as included_error
            FROM os_modules_v3_1
            WHERE tier = 'TIER 3'
        """)
        tier_3_row = cur.fetchone()
        tier_3_validation = (tier_3_row["total"], tier_3_row["excluded"], tier_3_row["included_error"]) if tier_3_row else (0, 0, 0)
        
        # Validation: Total counts
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded,
                COUNT(*) as total
            FROM os_modules_v3_1
        """)
        totals_row = cur.fetchone()
        totals = (totals_row["launch_v1"], totals_row["excluded"], totals_row["total"]) if totals_row else (0, 0, 0)
        
        # Log to audit
        cur.execute("""
            INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
            VALUES ('migration', NULL, 'lock_launch_v1', %s, NOW())
        """, (json.dumps({
            "migration": "009b_lock_launch_v1",
            "launch_distribution": launch_distribution,
            "tier_3_preserved": tier_3_validation[0],
            "tier_3_excluded": tier_3_validation[1],
            "tier_3_included_error": tier_3_validation[2],
            "total_launch_v1": totals[0],
            "total_excluded": totals[1],
            "total_products": totals[2]
        }),))
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Validation checks
        passed = True
        errors = []
        
        if launch_distribution.get('TIER 3', 0) > 0:
            passed = False
            errors.append(f"CRITICAL: {launch_distribution.get('TIER 3', 0)} TIER 3 products in Launch v1")
        
        if tier_3_validation[2] > 0:
            passed = False
            errors.append(f"CRITICAL: {tier_3_validation[2]} TIER 3 products incorrectly included")
        
        if launch_distribution.get('TIER 1', 0) == 0:
            errors.append("WARNING: No TIER 1 products in Launch v1")
        
        if launch_distribution.get('TIER 2', 0) == 0:
            errors.append("WARNING: No TIER 2 products in Launch v1")
        
        return {
            "status": "success" if passed else "failed",
            "migration": "009b_lock_launch_v1",
            "timestamp": now_iso(),
            "passed": passed,
            "errors": errors,
            "launch_v1_distribution": launch_distribution,
            "tier_3_validation": {
                "total_preserved": tier_3_validation[0],
                "correctly_excluded": tier_3_validation[1],
                "incorrectly_included": tier_3_validation[2]
            },
            "totals": {
                "launch_v1_products": totals[0],
                "excluded_products": totals[1],
                "total_products": totals[2]
            },
            "pipeline_enforcement": {
                "design_export": "Filter by is_launch_v1 = TRUE",
                "shopify_publish": "Filter by is_launch_v1 = TRUE",
                "qa_gates": "Filter by is_launch_v1 = TRUE",
                "brain_logic": "NO FILTER - uses all tiers",
                "research_views": "NO FILTER - uses all tiers"
            }
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.get("/qa/launch-v1-scope")
def qa_launch_v1_scope():
    """
    QA Assertion: Verify Launch v1 contains only TIER 1 + TIER 2.
    
    PASS conditions:
    - TIER 1 count > 0
    - TIER 2 count > 0  
    - TIER 3 in launch = 0
    - TIER 3 preserved > 0
    """
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed", "passed": False}
    
    try:
        cur = conn.cursor()
        
        # Check if is_launch_v1 column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' AND column_name = 'is_launch_v1'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {
                "passed": False,
                "error": "is_launch_v1 column not found. Run /migrate-lock-launch-v1 first."
            }
        
        # Launch v1 tier distribution
        cur.execute("""
            SELECT tier, COUNT(*) as count
            FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE
            GROUP BY tier
        """)
        tier_counts = {row["tier"]: row["count"] for row in cur.fetchall()}
        
        # Tier 3 preserved check
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE tier = 'TIER 3' AND is_launch_v1 = FALSE
        """)
        tier_3_preserved = cur.fetchone()["count"]
        
        cur.close()
        conn.close()
        
        # Assertions
        errors = []
        tier_1_count = tier_counts.get('TIER 1', 0)
        tier_2_count = tier_counts.get('TIER 2', 0)
        tier_3_in_launch = tier_counts.get('TIER 3', 0)
        
        if tier_1_count == 0:
            errors.append("FAIL: No TIER 1 products in Launch v1")
        
        if tier_2_count == 0:
            errors.append("FAIL: No TIER 2 products in Launch v1")
        
        if tier_3_in_launch > 0:
            errors.append(f"CRITICAL: {tier_3_in_launch} TIER 3 products incorrectly in Launch v1")
        
        if tier_3_preserved == 0:
            errors.append("WARNING: No TIER 3 products found (may have been deleted)")
        
        return {
            "passed": len(errors) == 0,
            "timestamp": now_iso(),
            "tier_1_count": tier_1_count,
            "tier_2_count": tier_2_count,
            "tier_3_in_launch": tier_3_in_launch,
            "tier_3_preserved": tier_3_preserved,
            "total_launch_products": tier_1_count + tier_2_count,
            "errors": errors
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e), "passed": False}


@app.get("/api/v1/catalog/launch-v1")
def list_launch_v1_products():
    """
    List all products in Launch v1 scope.
    Use for Design Export, Shopify sync verification.
    """
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Check if column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' AND column_name = 'is_launch_v1'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {"error": "is_launch_v1 column not found. Run /migrate-lock-launch-v1 first."}
        
        cur.execute("""
            SELECT 
                module_code,
                product_name,
                os_environment,
                os_layer,
                tier,
                is_launch_v1,
                created_at
            FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE
            ORDER BY tier, module_code
        """)
        
        products = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "count": len(products),
            "timestamp": now_iso(),
            "products": products
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.get("/api/v1/catalog/tier-distribution")
def get_tier_distribution():
    """
    Get current tier distribution in os_modules_v3_1.
    """
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Check if tier column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' AND column_name = 'tier'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {
                "error": "tier column not found",
                "fix": "Run /migrate-add-tier-column first"
            }
        
        cur.execute("""
            SELECT 
                tier, 
                os_layer,
                COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY tier, os_layer
            ORDER BY tier, os_layer
        """)
        
        distribution = [dict(row) for row in cur.fetchall()]
        
        cur.execute("""
            SELECT tier, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY tier
            ORDER BY tier
        """)
        tier_totals = {row["tier"]: row["count"] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return {
            "tier_totals": tier_totals,
            "detailed_distribution": distribution,
            "timestamp": now_iso()
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


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
    
    # Filter for copy routes
    copy_routes = [r for r in routes if 'copy' in r['path'].lower()]
    
    # Filter for launch/tier routes
    launch_routes = [r for r in routes if 'launch' in r['path'].lower() or 'tier' in r['path'].lower()]
    
    return {
        "total_routes": len(routes),
        "bloodwork_routes": bloodwork_routes,
        "shopify_routes": shopify_routes,
        "copy_routes": copy_routes,
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
