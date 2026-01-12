# =============================================================================
# GenoMAX² Migration Runner
# One-time endpoint to run migrations via API
# =============================================================================

import os
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


@router.post("/run/003-disclaimer-columns")
def run_migration_003() -> Dict[str, Any]:
    """
    Run migration 003: Add disclaimer_symbol and disclaimer_applicability columns.
    Safe to run multiple times (idempotent).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        # 1) Add disclaimer_symbol
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS disclaimer_symbol VARCHAR(8)
        """)
        results.append("Added disclaimer_symbol column")
        
        # 2) Add disclaimer_applicability
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS disclaimer_applicability VARCHAR(16)
        """)
        results.append("Added disclaimer_applicability column")
        
        # 3) Set defaults
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ALTER COLUMN disclaimer_symbol SET DEFAULT '*'
        """)
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ALTER COLUMN disclaimer_applicability SET DEFAULT 'SUPPLEMENT'
        """)
        results.append("Set defaults (* and SUPPLEMENT)")
        
        # 4) Backfill
        cur.execute("""
            UPDATE public.os_modules_v3_1
            SET disclaimer_symbol = '*'
            WHERE disclaimer_symbol IS NULL OR BTRIM(disclaimer_symbol) = ''
        """)
        symbol_count = cur.rowcount
        
        cur.execute("""
            UPDATE public.os_modules_v3_1
            SET disclaimer_applicability = 'SUPPLEMENT'
            WHERE disclaimer_applicability IS NULL OR BTRIM(disclaimer_applicability) = ''
        """)
        applicability_count = cur.rowcount
        results.append(f"Backfilled {symbol_count} rows for symbol, {applicability_count} for applicability")
        
        # 5) CHECK constraint (if not exists)
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'chk_os_modules_v3_1_disclaimer_applicability'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE public.os_modules_v3_1
                ADD CONSTRAINT chk_os_modules_v3_1_disclaimer_applicability
                CHECK (disclaimer_applicability IN ('SUPPLEMENT', 'TOPICAL'))
            """)
            results.append("Added CHECK constraint")
        else:
            results.append("CHECK constraint already exists")
        
        # 6) Index (if not exists)
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'os_modules_v3_1'
              AND indexname = 'idx_os_modules_v3_1_disclaimer_applicability'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_os_modules_v3_1_disclaimer_applicability
                ON public.os_modules_v3_1 (disclaimer_applicability)
            """)
            results.append("Created index")
        else:
            results.append("Index already exists")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "003-disclaimer-columns",
            "steps": results
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/003-disclaimer-columns")
def check_migration_003() -> Dict[str, Any]:
    """Check if migration 003 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check columns exist
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'os_modules_v3_1'
              AND column_name IN ('disclaimer_symbol', 'disclaimer_applicability')
        """)
        columns = [r['column_name'] for r in cur.fetchall()]
        
        # Check constraint
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'chk_os_modules_v3_1_disclaimer_applicability'
        """)
        has_constraint = cur.fetchone() is not None
        
        # Check index
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'idx_os_modules_v3_1_disclaimer_applicability'
        """)
        has_index = cur.fetchone() is not None
        
        # Sample data
        cur.execute("""
            SELECT disclaimer_symbol, disclaimer_applicability, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY disclaimer_symbol, disclaimer_applicability
        """)
        data_sample = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        applied = (
            'disclaimer_symbol' in columns and 
            'disclaimer_applicability' in columns and 
            has_constraint
        )
        
        return {
            "migration": "003-disclaimer-columns",
            "applied": applied,
            "columns_found": columns,
            "has_constraint": has_constraint,
            "has_index": has_index,
            "data_distribution": data_sample
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


@router.post("/run/004-fill-fda-disclaimer")
def run_migration_004() -> Dict[str, Any]:
    """
    Run migration 004: Fill fda_disclaimer for all SUPPLEMENT modules.
    Safe to run multiple times (idempotent).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        # Count current state
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE disclaimer_applicability = 'SUPPLEMENT') AS supplement_count,
                COUNT(*) FILTER (WHERE disclaimer_applicability = 'TOPICAL') AS topical_count,
                COUNT(*) FILTER (
                    WHERE disclaimer_applicability = 'SUPPLEMENT'
                      AND (fda_disclaimer IS NULL OR BTRIM(fda_disclaimer) = '')
                ) AS supplement_missing_disclaimer,
                COUNT(*) FILTER (
                    WHERE disclaimer_applicability = 'SUPPLEMENT'
                      AND fda_disclaimer IS NOT NULL 
                      AND BTRIM(fda_disclaimer) <> ''
                ) AS supplement_has_disclaimer
            FROM os_modules_v3_1
        """)
        before_state = dict(cur.fetchone())
        results.append(f"Before: {before_state}")
        
        # Fill fda_disclaimer for SUPPLEMENT modules only
        cur.execute("""
            UPDATE os_modules_v3_1
            SET fda_disclaimer = %s,
                updated_at = NOW()
            WHERE disclaimer_applicability = 'SUPPLEMENT'
              AND (fda_disclaimer IS NULL OR BTRIM(fda_disclaimer) = '')
        """, (FDA_DISCLAIMER_TEXT,))
        updated_count = cur.rowcount
        results.append(f"Updated {updated_count} SUPPLEMENT modules with FDA disclaimer")
        
        # Verify
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (
                    WHERE disclaimer_applicability = 'SUPPLEMENT'
                      AND (fda_disclaimer IS NULL OR BTRIM(fda_disclaimer) = '')
                ) AS supplement_still_missing,
                COUNT(*) FILTER (
                    WHERE disclaimer_applicability = 'SUPPLEMENT'
                      AND fda_disclaimer IS NOT NULL 
                      AND BTRIM(fda_disclaimer) <> ''
                ) AS supplement_has_disclaimer
            FROM os_modules_v3_1
        """)
        after_state = dict(cur.fetchone())
        results.append(f"After: {after_state}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "004-fill-fda-disclaimer",
            "updated_count": updated_count,
            "disclaimer_text": FDA_DISCLAIMER_TEXT,
            "steps": results,
            "note": "UI layer handles singular/plural based on claim count"
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/004-fill-fda-disclaimer")
def check_migration_004() -> Dict[str, Any]:
    """Check FDA disclaimer fill status."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                disclaimer_applicability,
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE fda_disclaimer IS NOT NULL AND BTRIM(fda_disclaimer) <> ''
                ) AS has_disclaimer,
                COUNT(*) FILTER (
                    WHERE fda_disclaimer IS NULL OR BTRIM(fda_disclaimer) = ''
                ) AS missing_disclaimer
            FROM os_modules_v3_1
            GROUP BY disclaimer_applicability
        """)
        breakdown = [dict(r) for r in cur.fetchall()]
        
        # Sample disclaimers
        cur.execute("""
            SELECT module_code, shopify_handle, disclaimer_applicability, 
                   SUBSTRING(fda_disclaimer, 1, 80) AS disclaimer_preview
            FROM os_modules_v3_1
            WHERE fda_disclaimer IS NOT NULL AND BTRIM(fda_disclaimer) <> ''
            LIMIT 5
        """)
        samples = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        # Check if all SUPPLEMENT modules have disclaimer
        supplement_data = next((b for b in breakdown if b['disclaimer_applicability'] == 'SUPPLEMENT'), None)
        all_filled = supplement_data and supplement_data['missing_disclaimer'] == 0 if supplement_data else True
        
        return {
            "migration": "004-fill-fda-disclaimer",
            "applied": all_filled,
            "breakdown_by_applicability": breakdown,
            "samples": samples,
            "expected_disclaimer": FDA_DISCLAIMER_TEXT[:80] + "..."
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


# =============================================================================
# MIGRATION 005: Mark TOPICAL products from allowlist
# =============================================================================

@router.get("/topical/candidates")
def get_topical_candidates() -> Dict[str, Any]:
    """
    Step 1: Read-only scan for potential TOPICAL products.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT module_code, shopify_handle, product_name, 
                   os_layer, biological_domain, disclaimer_applicability
            FROM os_modules_v3_1
            WHERE LOWER(product_name) LIKE '%soap%'
               OR LOWER(product_name) LIKE '%lotion%'
               OR LOWER(product_name) LIKE '%balm%'
               OR LOWER(shopify_handle) LIKE '%soap%'
               OR LOWER(shopify_handle) LIKE '%lotion%'
            ORDER BY shopify_handle
        """)
        candidates = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        in_allowlist = [c for c in candidates if c['shopify_handle'] in TOPICAL_ALLOWLIST]
        not_in_allowlist = [c for c in candidates if c['shopify_handle'] not in TOPICAL_ALLOWLIST]
        
        return {
            "scan_type": "read_only",
            "total_candidates": len(candidates),
            "in_allowlist": len(in_allowlist),
            "not_in_allowlist": len(not_in_allowlist),
            "allowlist_entries": TOPICAL_ALLOWLIST,
            "candidates_in_allowlist": in_allowlist,
            "candidates_not_in_allowlist": not_in_allowlist
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Scan error: {str(e)}")


@router.post("/run/005-mark-topical")
def run_migration_005() -> Dict[str, Any]:
    """Run migration 005: Mark products in TOPICAL_ALLOWLIST as TOPICAL."""
    if not TOPICAL_ALLOWLIST:
        return {
            "status": "skipped",
            "migration": "005-mark-topical",
            "reason": "TOPICAL_ALLOWLIST is empty",
            "updated_count": 0
        }
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT disclaimer_applicability, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY disclaimer_applicability
        """)
        before_state = {r['disclaimer_applicability']: r['count'] for r in cur.fetchall()}
        results.append(f"Before: {before_state}")
        
        cur.execute("""
            SELECT shopify_handle FROM os_modules_v3_1
            WHERE shopify_handle = ANY(%s)
        """, (TOPICAL_ALLOWLIST,))
        found_handles = [r['shopify_handle'] for r in cur.fetchall()]
        not_found = [h for h in TOPICAL_ALLOWLIST if h not in found_handles]
        
        if found_handles:
            cur.execute("""
                UPDATE os_modules_v3_1
                SET disclaimer_applicability = 'TOPICAL',
                    updated_at = NOW()
                WHERE shopify_handle = ANY(%s)
                  AND disclaimer_applicability != 'TOPICAL'
            """, (found_handles,))
            updated_count = cur.rowcount
        else:
            updated_count = 0
        
        cur.execute("""
            SELECT disclaimer_applicability, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY disclaimer_applicability
        """)
        after_state = {r['disclaimer_applicability']: r['count'] for r in cur.fetchall()}
        results.append(f"After: {after_state}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "005-mark-topical",
            "allowlist": TOPICAL_ALLOWLIST,
            "found_in_db": found_handles,
            "not_found_in_db": not_found,
            "updated_count": updated_count,
            "steps": results
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/005-mark-topical")
def check_migration_005() -> Dict[str, Any]:
    """Check TOPICAL marking status and distribution."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT disclaimer_applicability, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY disclaimer_applicability
            ORDER BY count DESC
        """)
        distribution = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        topical_count = next((d['count'] for d in distribution if d['disclaimer_applicability'] == 'TOPICAL'), 0)
        
        return {
            "migration": "005-mark-topical",
            "distribution": distribution,
            "topical_count": topical_count,
            "allowlist": TOPICAL_ALLOWLIST
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Status error: {str(e)}")


# =============================================================================
# MIGRATION 006: Add supplier_status columns
# =============================================================================

@router.post("/run/006-supplier-status-columns")
def run_migration_006() -> Dict[str, Any]:
    """Run migration 006: Add supplier_status tracking columns with data normalization."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS supplier_status VARCHAR(20)
        """)
        results.append("Added/verified supplier_status column")
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS supplier_http_status INTEGER
        """)
        results.append("Added/verified supplier_http_status column")
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS supplier_status_details TEXT
        """)
        results.append("Added/verified supplier_status_details column")
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS supplier_checked_at TIMESTAMPTZ
        """)
        results.append("Added/verified supplier_checked_at column")
        
        cur.execute("""
            SELECT supplier_status, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY supplier_status
            ORDER BY count DESC
        """)
        before_dist = {r['supplier_status']: r['count'] for r in cur.fetchall()}
        results.append(f"Before normalization: {before_dist}")
        
        cur.execute("""
            UPDATE public.os_modules_v3_1
            SET supplier_status = 'ACTIVE'
            WHERE supplier_status IS NULL 
               OR UPPER(BTRIM(supplier_status)) = 'UNKNOWN'
               OR BTRIM(supplier_status) = ''
        """)
        unknown_to_active = cur.rowcount
        results.append(f"Normalized {unknown_to_active} NULL/UNKNOWN/empty to ACTIVE")
        
        cur.execute("""
            UPDATE public.os_modules_v3_1
            SET supplier_status = 'DISCONTINUED'
            WHERE UPPER(BTRIM(supplier_status)) IN ('INACTIVE', 'DUPLICATE_INACTIVE')
        """)
        inactive_to_discontinued = cur.rowcount
        results.append(f"Normalized {inactive_to_discontinued} INACTIVE/DUPLICATE_INACTIVE to DISCONTINUED")
        
        cur.execute("""
            SELECT supplier_status, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY supplier_status
            ORDER BY count DESC
        """)
        after_dist = {r['supplier_status']: r['count'] for r in cur.fetchall()}
        results.append(f"After normalization: {after_dist}")
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ALTER COLUMN supplier_status SET DEFAULT 'ACTIVE'
        """)
        results.append("Set default ACTIVE for supplier_status")
        
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'chk_os_modules_v3_1_supplier_status'
        """)
        if cur.fetchone():
            cur.execute("""
                ALTER TABLE public.os_modules_v3_1
                DROP CONSTRAINT chk_os_modules_v3_1_supplier_status
            """)
            results.append("Dropped existing CHECK constraint for recreation")
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD CONSTRAINT chk_os_modules_v3_1_supplier_status
            CHECK (supplier_status IN ('ACTIVE', 'DISCONTINUED', 'UNAVAILABLE'))
        """)
        results.append("Added CHECK constraint for supplier_status")
        
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'os_modules_v3_1'
              AND indexname = 'idx_os_modules_v3_1_supplier_status'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_os_modules_v3_1_supplier_status
                ON public.os_modules_v3_1 (supplier_status)
            """)
            results.append("Created index on supplier_status")
        else:
            results.append("Index already exists")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "006-supplier-status-columns",
            "normalization": {
                "before": before_dist,
                "after": after_dist,
                "unknown_to_active": unknown_to_active,
                "inactive_to_discontinued": inactive_to_discontinued
            },
            "steps": results
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/006-supplier-status-columns")
def check_migration_006() -> Dict[str, Any]:
    """Check if migration 006 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'os_modules_v3_1'
              AND column_name IN ('supplier_status', 'supplier_http_status', 
                                  'supplier_status_details', 'supplier_checked_at')
        """)
        columns = [r['column_name'] for r in cur.fetchall()]
        
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'chk_os_modules_v3_1_supplier_status'
        """)
        has_constraint = cur.fetchone() is not None
        
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'idx_os_modules_v3_1_supplier_status'
        """)
        has_index = cur.fetchone() is not None
        
        if 'supplier_status' in columns:
            cur.execute("""
                SELECT supplier_status, COUNT(*) as count
                FROM os_modules_v3_1
                GROUP BY supplier_status
                ORDER BY count DESC
            """)
            distribution = [dict(r) for r in cur.fetchall()]
        else:
            distribution = []
        
        cur.close()
        conn.close()
        
        expected_columns = ['supplier_status', 'supplier_http_status', 
                          'supplier_status_details', 'supplier_checked_at']
        applied = all(c in columns for c in expected_columns) and has_constraint
        
        return {
            "migration": "006-supplier-status-columns",
            "applied": applied,
            "columns_expected": expected_columns,
            "columns_found": columns,
            "has_constraint": has_constraint,
            "has_index": has_index,
            "status_distribution": distribution
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


# =============================================================================
# MIGRATION 007: Create supplier_catalog_snapshot_v1 table
# =============================================================================

@router.post("/run/007-supplier-catalog-snapshot")
def run_migration_007() -> Dict[str, Any]:
    """
    Run migration 007: Create supplier_catalog_snapshot_v1 table.
    
    This table stores ProductURLs from the Supliful catalog Excel file.
    Used for deterministic mapping of shopify_handle -> supliful_handle.
    
    Columns:
    - supliful_handle: TEXT PRIMARY KEY (e.g., 'creatine-monohydrate-powder')
    - supplier_url: TEXT NOT NULL (full URL)
    - updated_at: TIMESTAMPTZ NOT NULL DEFAULT NOW()
    
    Safe to run multiple times (idempotent).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS supplier_catalog_snapshot_v1 (
                supliful_handle TEXT PRIMARY KEY,
                supplier_url TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        results.append("Created/verified supplier_catalog_snapshot_v1 table")
        
        # Check row count
        cur.execute("SELECT COUNT(*) as count FROM supplier_catalog_snapshot_v1")
        row_count = cur.fetchone()['count']
        results.append(f"Current row count: {row_count}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "007-supplier-catalog-snapshot",
            "table": "supplier_catalog_snapshot_v1",
            "row_count": row_count,
            "steps": results
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/007-supplier-catalog-snapshot")
def check_migration_007() -> Dict[str, Any]:
    """Check if migration 007 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check table exists
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'supplier_catalog_snapshot_v1'
        """)
        table_exists = cur.fetchone() is not None
        
        if table_exists:
            # Get row count and sample
            cur.execute("SELECT COUNT(*) as count FROM supplier_catalog_snapshot_v1")
            row_count = cur.fetchone()['count']
            
            cur.execute("""
                SELECT supliful_handle, supplier_url, updated_at
                FROM supplier_catalog_snapshot_v1
                ORDER BY supliful_handle
                LIMIT 10
            """)
            samples = [dict(r) for r in cur.fetchall()]
        else:
            row_count = 0
            samples = []
        
        cur.close()
        conn.close()
        
        return {
            "migration": "007-supplier-catalog-snapshot",
            "applied": table_exists,
            "table_exists": table_exists,
            "row_count": row_count,
            "samples": samples
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


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
