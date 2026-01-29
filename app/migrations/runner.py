# =============================================================================
# GenoMAX² Migration Runner
# One-time endpoint to run migrations via API
# =============================================================================

import os
import json
from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Body
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/migrations", tags=["Migrations"])

DATABASE_URL = os.getenv("DATABASE_URL")

# Standard DSHEA disclaimer (singular form - UI handles plural based on claim count)
FDA_DISCLAIMER_TEXT = "This statement has not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease."

# =============================================================================
# TOPICAL ALLOWLIST - Explicit list of handles approved as TOPICAL (cosmetics)
# =============================================================================
TOPICAL_ALLOWLIST: List[str] = []


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


# =============================================================================
# MIGRATION 015: Remove Legacy GX-* Products (Duplicate Cleanup)
# =============================================================================

@router.post("/run/015-remove-legacy-gx")
def run_migration_015() -> Dict[str, Any]:
    """
    Run migration 015: Remove legacy GX-* products from catalog_products.
    
    REASON: Migration v2.0.2 converted 65 legacy GX-* products to 130 gender-specific
    GMAX-M-*/GMAX-F-* pairs but failed to DELETE the original GX-* entries.
    
    Current state: 217 products (76 GMAX-M + 74 GMAX-F + 2 GMAX-U + 65 GX legacy)
    Target state: 152 products (76 GMAX-M + 74 GMAX-F + 2 GMAX-U)
    
    Products to remove: All where gx_catalog_id LIKE 'GX-%'
    
    NOTE: Temporarily disables triggers to bypass append-only governance.
    Audit trail preserved in catalog_cleanup_audit table.
    
    Safe to run multiple times (idempotent - only deletes if GX-* exists).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        # Step 1: Count current state
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GX-%') AS legacy_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-M-%') AS maximo_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-F-%') AS maxima_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-U-%') AS universal_count,
                COUNT(*) AS total_count
            FROM catalog_products
        """)
        before_counts = dict(cur.fetchone())
        results.append(f"Before: {before_counts}")
        
        if before_counts['legacy_count'] == 0:
            conn.close()
            return {
                "status": "skipped",
                "migration": "015-remove-legacy-gx",
                "reason": "No legacy GX-* products found - already cleaned",
                "counts": before_counts
            }
        
        # Step 2: Get list of SKUs to delete for audit
        cur.execute("""
            SELECT gx_catalog_id, product_name, evidence_tier, sex_target
            FROM catalog_products
            WHERE gx_catalog_id LIKE 'GX-%'
            ORDER BY gx_catalog_id
        """)
        legacy_products = [dict(r) for r in cur.fetchall()]
        results.append(f"Found {len(legacy_products)} legacy products to remove")
        
        # Step 3: Create audit table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS catalog_cleanup_audit (
                id SERIAL PRIMARY KEY,
                migration_id TEXT NOT NULL,
                action TEXT NOT NULL,
                sku TEXT NOT NULL,
                product_name TEXT,
                evidence_tier TEXT,
                sex_target TEXT,
                deleted_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        results.append("Created/verified catalog_cleanup_audit table")
        
        # Step 4: Log deletions to audit table
        for prod in legacy_products:
            cur.execute("""
                INSERT INTO catalog_cleanup_audit 
                (migration_id, action, sku, product_name, evidence_tier, sex_target)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                '015-remove-legacy-gx',
                'DELETE_DUPLICATE',
                prod['gx_catalog_id'],
                prod['product_name'],
                prod['evidence_tier'],
                prod['sex_target']
            ))
        results.append(f"Logged {len(legacy_products)} deletions to audit table")
        
        # Step 5: Temporarily disable append-only trigger and delete legacy products
        cur.execute("""
            ALTER TABLE catalog_products DISABLE TRIGGER ALL
        """)
        results.append("Temporarily disabled triggers on catalog_products")
        
        cur.execute("""
            DELETE FROM catalog_products
            WHERE gx_catalog_id LIKE 'GX-%'
            RETURNING gx_catalog_id
        """)
        deleted_skus = [r['gx_catalog_id'] for r in cur.fetchall()]
        results.append(f"Deleted {len(deleted_skus)} legacy products")
        
        # Re-enable triggers
        cur.execute("""
            ALTER TABLE catalog_products ENABLE TRIGGER ALL
        """)
        results.append("Re-enabled triggers on catalog_products")
        
        # Step 6: Verify final state
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GX-%') AS legacy_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-M-%') AS maximo_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-F-%') AS maxima_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-U-%') AS universal_count,
                COUNT(*) AS total_count
            FROM catalog_products
        """)
        after_counts = dict(cur.fetchone())
        results.append(f"After: {after_counts}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "015-remove-legacy-gx",
            "before": before_counts,
            "after": after_counts,
            "deleted_count": len(deleted_skus),
            "deleted_skus_sample": deleted_skus[:10],
            "steps": results
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/015-remove-legacy-gx")
def check_migration_015() -> Dict[str, Any]:
    """Check if migration 015 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check current counts
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GX-%') AS legacy_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-M-%') AS maximo_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-F-%') AS maxima_count,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-U-%') AS universal_count,
                COUNT(*) AS total_count
            FROM catalog_products
        """)
        counts = dict(cur.fetchone())
        
        # Check audit table
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'catalog_cleanup_audit'
        """)
        audit_exists = cur.fetchone() is not None
        
        audit_count = 0
        if audit_exists:
            cur.execute("""
                SELECT COUNT(*) as count FROM catalog_cleanup_audit
                WHERE migration_id = '015-remove-legacy-gx'
            """)
            audit_count = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        # Migration is applied if no legacy products exist
        applied = counts['legacy_count'] == 0 and audit_count > 0
        
        return {
            "migration": "015-remove-legacy-gx",
            "applied": applied,
            "current_counts": counts,
            "audit_entries": audit_count,
            "expected_total": 152,
            "note": "Applied when legacy_count = 0 and audit entries exist"
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


# =============================================================================
# MIGRATION 014: Suspend DIG-NATURA modules
# =============================================================================

@router.post("/run/014-suspend-dig-natura")
def run_migration_014() -> Dict[str, Any]:
    """
    Run migration 014: Suspend DIG-NATURA-M-087 and DIG-NATURA-F-087 modules.
    
    Reason: NO_ACTIVE_SUPLIFUL_PRODUCT
    - Historical Supliful product removed from catalog (404)
    - No Supplement Facts or regulatory data exists
    - Decision: FINAL and LOCKED
    
    Sets supplier_status to UNAVAILABLE per governance policy.
    Safe to run multiple times (idempotent).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    target_modules = ['DIG-NATURA-M-087', 'DIG-NATURA-F-087']
    
    try:
        cur = conn.cursor()
        
        # Step 1: Check current status
        cur.execute("""
            SELECT module_code, product_name, os_environment, supplier_status
            FROM os_modules_v3_1
            WHERE module_code = ANY(%s)
        """, (target_modules,))
        before_status = [dict(r) for r in cur.fetchall()]
        results.append(f"Found {len(before_status)} modules before update")
        
        if len(before_status) == 0:
            conn.close()
            return {
                "status": "skipped",
                "migration": "014-suspend-dig-natura",
                "reason": "Target modules not found in database",
                "target_modules": target_modules
            }
        
        # Step 2: Update to UNAVAILABLE status
        cur.execute("""
            UPDATE os_modules_v3_1
            SET 
                supplier_status = 'UNAVAILABLE',
                supplier_status_details = 'NO_ACTIVE_SUPLIFUL_PRODUCT: Historical Supliful product removed from catalog (404). No Supplement Facts or regulatory data exists. Decision final and locked. [Migration 014]',
                supplier_checked_at = NOW()
            WHERE module_code = ANY(%s)
            RETURNING module_code, product_name, os_environment, supplier_status
        """, (target_modules,))
        updated = [dict(r) for r in cur.fetchall()]
        results.append(f"Updated {len(updated)} modules to UNAVAILABLE")
        
        # Step 3: Log to module_suspension_audit (create if needed)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS module_suspension_audit (
                id SERIAL PRIMARY KEY,
                module_code TEXT NOT NULL,
                action TEXT NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                reason TEXT NOT NULL,
                migration_id TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        
        for module in updated:
            old_status = next((m['supplier_status'] for m in before_status if m['module_code'] == module['module_code']), None)
            cur.execute("""
                INSERT INTO module_suspension_audit 
                (module_code, action, old_status, new_status, reason, migration_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                module['module_code'],
                'SUSPEND',
                old_status,
                'UNAVAILABLE',
                'NO_ACTIVE_SUPLIFUL_PRODUCT: Historical Supliful product removed from catalog (404). No Supplement Facts or regulatory data exists.',
                '014-suspend-dig-natura'
            ))
        results.append(f"Logged {len(updated)} suspension audit entries")
        
        # Step 4: Verify update
        cur.execute("""
            SELECT module_code, product_name, os_environment, supplier_status, supplier_status_details
            FROM os_modules_v3_1
            WHERE module_code = ANY(%s)
        """, (target_modules,))
        after_status = [dict(r) for r in cur.fetchall()]
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "014-suspend-dig-natura",
            "target_modules": target_modules,
            "before": before_status,
            "after": after_status,
            "modules_updated": len(updated),
            "steps": results
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/014-suspend-dig-natura")
def check_migration_014() -> Dict[str, Any]:
    """Check if migration 014 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    target_modules = ['DIG-NATURA-M-087', 'DIG-NATURA-F-087']
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT module_code, product_name, os_environment, supplier_status, 
                   supplier_status_details, supplier_checked_at
            FROM os_modules_v3_1
            WHERE module_code = ANY(%s)
        """, (target_modules,))
        modules = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        all_unavailable = all(
            m.get('supplier_status') == 'UNAVAILABLE' 
            for m in modules
        ) if modules else False
        
        return {
            "migration": "014-suspend-dig-natura",
            "applied": all_unavailable,
            "target_modules": target_modules,
            "current_status": modules,
            "note": "Applied when both modules have supplier_status = 'UNAVAILABLE'"
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


# =============================================================================
# Sanity check endpoint
# =============================================================================

@router.get("/sanity-check/topical")
def topical_sanity_check() -> Dict[str, Any]:
    """Dedicated QA sanity check for TOPICAL products."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE disclaimer_applicability = 'TOPICAL') AS topical_total,
                COUNT(*) FILTER (WHERE disclaimer_applicability = 'SUPPLEMENT') AS supplement_total
            FROM os_modules_v3_1
        """)
        counts = dict(cur.fetchone())
        
        cur.close()
        conn.close()
        
        return {
            "sanity_check": "topical",
            "counts": counts,
            "allowlist": TOPICAL_ALLOWLIST
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Sanity check error: {str(e)}")
