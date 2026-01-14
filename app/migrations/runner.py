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


# =============================================================================
# MIGRATION 008: Create allowlist mapping tables
# =============================================================================

@router.post("/run/008-allowlist-mapping")
def run_migration_008() -> Dict[str, Any]:
    """
    Run migration 008: Create allowlist mapping tables.
    
    Creates:
    - catalog_handle_map_allowlist_v1: Human-approved handle mappings
    - catalog_handle_map_allowlist_audit_v1: Audit trail for apply operations
    
    Safe to run multiple times (idempotent).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        # Create allowlist table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS catalog_handle_map_allowlist_v1 (
                allowlist_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                shopify_base_handle TEXT NOT NULL,
                supliful_handle TEXT NOT NULL,
                supplier_url TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'MANUAL_ALLOWLIST',
                notes TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        results.append("Created/verified catalog_handle_map_allowlist_v1 table")
        
        # Add unique constraints if not exist
        cur.execute("""
            SELECT 1 FROM pg_constraint WHERE conname = 'uq_allowlist_shopify_base_handle'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE catalog_handle_map_allowlist_v1
                ADD CONSTRAINT uq_allowlist_shopify_base_handle UNIQUE (shopify_base_handle)
            """)
            results.append("Added unique constraint on shopify_base_handle")
        else:
            results.append("Unique constraint on shopify_base_handle already exists")
        
        cur.execute("""
            SELECT 1 FROM pg_constraint WHERE conname = 'uq_allowlist_supliful_handle'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE catalog_handle_map_allowlist_v1
                ADD CONSTRAINT uq_allowlist_supliful_handle UNIQUE (supliful_handle)
            """)
            results.append("Added unique constraint on supliful_handle")
        else:
            results.append("Unique constraint on supliful_handle already exists")
        
        # Add CHECK constraint for URL format
        cur.execute("""
            SELECT 1 FROM pg_constraint WHERE conname = 'chk_supplier_url_format'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE catalog_handle_map_allowlist_v1
                ADD CONSTRAINT chk_supplier_url_format 
                CHECK (supplier_url LIKE 'https://supliful.com/catalog/%')
            """)
            results.append("Added CHECK constraint for supplier_url format")
        else:
            results.append("CHECK constraint for supplier_url already exists")
        
        # Create indexes
        cur.execute("""
            SELECT 1 FROM pg_indexes WHERE indexname = 'idx_allowlist_shopify_base_handle'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_allowlist_shopify_base_handle 
                ON catalog_handle_map_allowlist_v1 (shopify_base_handle)
            """)
            results.append("Created index on shopify_base_handle")
        
        cur.execute("""
            SELECT 1 FROM pg_indexes WHERE indexname = 'idx_allowlist_supliful_handle'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_allowlist_supliful_handle 
                ON catalog_handle_map_allowlist_v1 (supliful_handle)
            """)
            results.append("Created index on supliful_handle")
        
        # Create audit table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS catalog_handle_map_allowlist_audit_v1 (
                audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                batch_id UUID NOT NULL,
                module_code TEXT NOT NULL,
                shopify_handle TEXT NOT NULL,
                os_environment TEXT NOT NULL,
                old_supliful_handle TEXT NULL,
                new_supliful_handle TEXT NULL,
                old_supplier_page_url TEXT NULL,
                new_supplier_page_url TEXT NULL,
                rule_used TEXT NOT NULL DEFAULT 'MANUAL_ALLOWLIST',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        results.append("Created/verified catalog_handle_map_allowlist_audit_v1 table")
        
        # Create audit indexes
        cur.execute("""
            SELECT 1 FROM pg_indexes WHERE indexname = 'idx_allowlist_audit_batch_id'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_allowlist_audit_batch_id 
                ON catalog_handle_map_allowlist_audit_v1 (batch_id)
            """)
            results.append("Created index on batch_id")
        
        cur.execute("""
            SELECT 1 FROM pg_indexes WHERE indexname = 'idx_allowlist_audit_module_code'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_allowlist_audit_module_code 
                ON catalog_handle_map_allowlist_audit_v1 (module_code)
            """)
            results.append("Created index on module_code")
        
        cur.execute("""
            SELECT 1 FROM pg_indexes WHERE indexname = 'idx_allowlist_audit_created_at'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_allowlist_audit_created_at 
                ON catalog_handle_map_allowlist_audit_v1 (created_at DESC)
            """)
            results.append("Created index on created_at")
        
        # Get current counts
        cur.execute("SELECT COUNT(*) as count FROM catalog_handle_map_allowlist_v1")
        allowlist_count = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM catalog_handle_map_allowlist_audit_v1")
        audit_count = cur.fetchone()['count']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "008-allowlist-mapping",
            "tables": {
                "catalog_handle_map_allowlist_v1": allowlist_count,
                "catalog_handle_map_allowlist_audit_v1": audit_count
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


@router.get("/status/008-allowlist-mapping")
def check_migration_008() -> Dict[str, Any]:
    """Check if migration 008 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check allowlist table exists
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'catalog_handle_map_allowlist_v1'
        """)
        allowlist_exists = cur.fetchone() is not None
        
        # Check audit table exists
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'catalog_handle_map_allowlist_audit_v1'
        """)
        audit_exists = cur.fetchone() is not None
        
        allowlist_count = 0
        audit_count = 0
        samples = []
        
        if allowlist_exists:
            cur.execute("SELECT COUNT(*) as count FROM catalog_handle_map_allowlist_v1")
            allowlist_count = cur.fetchone()['count']
            
            cur.execute("""
                SELECT shopify_base_handle, supliful_handle, supplier_url
                FROM catalog_handle_map_allowlist_v1
                ORDER BY shopify_base_handle
                LIMIT 5
            """)
            samples = [dict(r) for r in cur.fetchall()]
        
        if audit_exists:
            cur.execute("SELECT COUNT(*) as count FROM catalog_handle_map_allowlist_audit_v1")
            audit_count = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return {
            "migration": "008-allowlist-mapping",
            "applied": allowlist_exists and audit_exists,
            "tables": {
                "catalog_handle_map_allowlist_v1": {
                    "exists": allowlist_exists,
                    "row_count": allowlist_count
                },
                "catalog_handle_map_allowlist_audit_v1": {
                    "exists": audit_exists,
                    "row_count": audit_count
                }
            },
            "samples": samples
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


# =============================================================================
# Earlier migrations (003-007) - kept for reference
# =============================================================================

@router.post("/run/003-disclaimer-columns")
def run_migration_003() -> Dict[str, Any]:
    """Run migration 003: Add disclaimer_symbol and disclaimer_applicability columns."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS disclaimer_symbol VARCHAR(8)
        """)
        results.append("Added disclaimer_symbol column")
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD COLUMN IF NOT EXISTS disclaimer_applicability VARCHAR(16)
        """)
        results.append("Added disclaimer_applicability column")
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ALTER COLUMN disclaimer_symbol SET DEFAULT '*'
        """)
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ALTER COLUMN disclaimer_applicability SET DEFAULT 'SUPPLEMENT'
        """)
        results.append("Set defaults (* and SUPPLEMENT)")
        
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
        
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'os_modules_v3_1'
              AND column_name IN ('disclaimer_symbol', 'disclaimer_applicability')
        """)
        columns = [r['column_name'] for r in cur.fetchall()]
        
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'chk_os_modules_v3_1_disclaimer_applicability'
        """)
        has_constraint = cur.fetchone() is not None
        
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'idx_os_modules_v3_1_disclaimer_applicability'
        """)
        has_index = cur.fetchone() is not None
        
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


@router.post("/run/006-supplier-status-columns")
def run_migration_006() -> Dict[str, Any]:
    """Run migration 006: Add supplier_status tracking columns."""
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
            UPDATE public.os_modules_v3_1
            SET supplier_status = 'ACTIVE'
            WHERE supplier_status IS NULL 
               OR UPPER(BTRIM(supplier_status)) = 'UNKNOWN'
               OR BTRIM(supplier_status) = ''
        """)
        unknown_to_active = cur.rowcount
        results.append(f"Normalized {unknown_to_active} NULL/UNKNOWN/empty to ACTIVE")
        
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
        
        cur.execute("""
            ALTER TABLE public.os_modules_v3_1
            ADD CONSTRAINT chk_os_modules_v3_1_supplier_status
            CHECK (supplier_status IN ('ACTIVE', 'DISCONTINUED', 'UNAVAILABLE'))
        """)
        results.append("Added CHECK constraint for supplier_status")
        
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE indexname = 'idx_os_modules_v3_1_supplier_status'
        """)
        if not cur.fetchone():
            cur.execute("""
                CREATE INDEX idx_os_modules_v3_1_supplier_status
                ON public.os_modules_v3_1 (supplier_status)
            """)
            results.append("Created index on supplier_status")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "006-supplier-status-columns",
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
            "columns_found": columns,
            "has_constraint": has_constraint,
            "status_distribution": distribution
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


@router.post("/run/007-supplier-catalog-snapshot")
def run_migration_007() -> Dict[str, Any]:
    """Run migration 007: Create supplier_catalog_snapshot_v1 table."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS supplier_catalog_snapshot_v1 (
                supliful_handle TEXT PRIMARY KEY,
                supplier_url TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        results.append("Created/verified supplier_catalog_snapshot_v1 table")
        
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
        
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'supplier_catalog_snapshot_v1'
        """)
        table_exists = cur.fetchone() is not None
        
        if table_exists:
            cur.execute("SELECT COUNT(*) as count FROM supplier_catalog_snapshot_v1")
            row_count = cur.fetchone()['count']
        else:
            row_count = 0
        
        cur.close()
        conn.close()
        
        return {
            "migration": "007-supplier-catalog-snapshot",
            "applied": table_exists,
            "table_exists": table_exists,
            "row_count": row_count
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
