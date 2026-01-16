"""
Admin endpoints for GenoMAX² API
Module status management and migrations
"""

import json
from api_server import app, get_db, now_iso


@app.get("/migrate-dig-natura-inactive")
def migrate_dig_natura_inactive():
    """
    One-time migration: Suspend DIG-NATURA-M-087 and DIG-NATURA-F-087.
    
    Reason: NO_ACTIVE_SUPLIFUL_PRODUCT
    - Historical Supliful product removed from catalog (404)
    - No Supplement Facts or regulatory data exists
    
    Decision is FINAL and LOCKED.
    """
    MODULES_TO_SUSPEND = [
        "DIG-NATURA-M-087",
        "DIG-NATURA-F-087"
    ]
    
    REASON = "NO_ACTIVE_SUPLIFUL_PRODUCT: Historical Supliful product removed from catalog (404). No Supplement Facts or regulatory data exists. Decision final and locked."
    
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Check if supplier_status column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' AND column_name = 'supplier_status'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {"error": "supplier_status column not found. Run /migrate-supplier-status first."}
        
        # Execute the update
        cur.execute("""
            UPDATE os_modules_v3_1
            SET 
                supplier_status = 'INACTIVE',
                supplier_status_details = %s,
                supplier_last_checked_at = NOW()
            WHERE module_code = ANY(%s)
            RETURNING module_code, product_name, os_environment, supplier_status
        """, (REASON, MODULES_TO_SUSPEND))
        
        updated = [dict(row) for row in cur.fetchall()]
        
        # Log to audit
        for module in updated:
            cur.execute("""
                INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
                VALUES ('os_module', NULL, 'supplier_status_update', %s, NOW())
            """, (json.dumps({
                "module_code": module["module_code"],
                "new_status": "INACTIVE",
                "reason": REASON,
                "migration": "suspend_dig_natura"
            }),))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "suspend_dig_natura",
            "timestamp": now_iso(),
            "updated_count": len(updated),
            "updated_modules": updated,
            "reason": REASON,
            "effects": {
                "routing": "EXCLUDED - supplier_status IN ('ACTIVE','UNKNOWN') filter excludes INACTIVE",
                "frontend": "EXCLUDED - should filter by supplier_status != 'INACTIVE'",
                "shopify_sync": "EXCLUDED - no active product to sync",
                "label_generation": "EXCLUDED - no Supplement Facts data",
                "audit_reference": "PRESERVED - module remains in catalog for historical reference"
            }
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


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
        launch_distribution = {row[0]: row[1] for row in cur.fetchall()}
        
        # Validation: Confirm Tier 3 preserved
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded,
                SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as included_error
            FROM os_modules_v3_1
            WHERE tier = 'TIER 3'
        """)
        tier_3_validation = cur.fetchone()
        
        # Validation: Total counts
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded,
                COUNT(*) as total
            FROM os_modules_v3_1
        """)
        totals = cur.fetchone()
        
        # Log to audit
        cur.execute("""
            INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
            VALUES ('migration', NULL, 'lock_launch_v1', %s, NOW())
        """, (json.dumps({
            "migration": "009_lock_launch_v1",
            "launch_distribution": launch_distribution,
            "tier_3_preserved": tier_3_validation[0] if tier_3_validation else 0,
            "tier_3_excluded": tier_3_validation[1] if tier_3_validation else 0,
            "tier_3_included_error": tier_3_validation[2] if tier_3_validation else 0,
            "total_launch_v1": totals[0] if totals else 0,
            "total_excluded": totals[1] if totals else 0,
            "total_products": totals[2] if totals else 0
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
        
        if tier_3_validation and tier_3_validation[2] > 0:
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
                "total_preserved": tier_3_validation[0] if tier_3_validation else 0,
                "correctly_excluded": tier_3_validation[1] if tier_3_validation else 0,
                "incorrectly_included": tier_3_validation[2] if tier_3_validation else 0
            },
            "totals": {
                "launch_v1_products": totals[0] if totals else 0,
                "excluded_products": totals[1] if totals else 0,
                "total_products": totals[2] if totals else 0
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
        tier_counts = {row[0]: row[1] for row in cur.fetchall()}
        
        # Tier 3 preserved check
        cur.execute("""
            SELECT COUNT(*)
            FROM os_modules_v3_1
            WHERE tier = 'TIER 3' AND is_launch_v1 = FALSE
        """)
        tier_3_preserved = cur.fetchone()[0]
        
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


@app.post("/admin/module-status")
def update_module_status_generic(module_codes: list, status: str, reason: str):
    """
    Update supplier_status for one or more modules.
    
    Valid statuses: ACTIVE, UNKNOWN, DISCONTINUING_SOON, INACTIVE
    """
    from fastapi import HTTPException
    
    valid_statuses = ["ACTIVE", "UNKNOWN", "DISCONTINUING_SOON", "INACTIVE"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE os_modules_v3_1
            SET 
                supplier_status = %s,
                supplier_status_details = %s,
                supplier_last_checked_at = NOW()
            WHERE module_code = ANY(%s)
            RETURNING module_code, product_name, os_environment, supplier_status
        """, (status, reason, module_codes))
        
        updated = [dict(row) for row in cur.fetchall()]
        
        for module in updated:
            cur.execute("""
                INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
                VALUES ('os_module', NULL, 'supplier_status_update', %s, NOW())
            """, (json.dumps({
                "module_code": module["module_code"],
                "new_status": status,
                "reason": reason
            }),))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "updated_count": len(updated),
            "updated_modules": updated,
            "timestamp": now_iso()
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        raise HTTPException(status_code=500, detail=str(e))
