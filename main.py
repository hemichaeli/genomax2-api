"""
GenoMAX2 API Server Entry Point v3.26.0
Adds Launch v1 lock migration and QA endpoints.

Use this file for Railway deployment:
  uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from api_server import app, get_db, now_iso
from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA
import json

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

# ===== COPY CLEANUP (v3.25.0) =====
try:
    from app.copy.router import router as copy_router
    app.include_router(copy_router)
    print("✅ Copy Cleanup endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Copy Cleanup: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()


# ===== LAUNCH V1 LOCK MIGRATION (v3.26.0) =====

@app.get("/migrate-lock-launch-v1")
def migrate_lock_launch_v1():
    """
    Migration 009: Lock Launch v1 scope to TIER 1 + TIER 2 only.
    
    - Adds is_launch_v1 BOOLEAN column
    - Sets TRUE for TIER 1 and TIER 2 products
    - Sets FALSE for TIER 3 (preserved but excluded from launch)
    - Creates partial index for launch queries
    
    TIER 3 products remain in DB but are excluded from all launch pipelines.
    This is a state-alignment task, not a refactor.
    """
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Step 1: Add column if not exists
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
            "migration": "009_lock_launch_v1",
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
            passed = False
            errors.append("WARNING: No TIER 1 products in Launch v1")
        
        if launch_distribution.get('TIER 2', 0) == 0:
            passed = False
            errors.append("WARNING: No TIER 2 products in Launch v1")
        
        return {
            "status": "success" if passed else "failed",
            "migration": "009_lock_launch_v1",
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
        
        # Check if column exists
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
                id,
                module_code,
                product_name,
                os_environment,
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
    
    # Filter for launch routes
    launch_routes = [r for r in routes if 'launch' in r['path'].lower()]
    
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
