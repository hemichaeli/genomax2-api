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
        # Note: CHECK constraint allows: ('ACTIVE', 'DISCONTINUED', 'UNAVAILABLE')
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
        
        # Check if all target modules are UNAVAILABLE
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


# =============================================================================
# MIGRATION 013: Bloodwork Engine v2.0 Schema
# =============================================================================

@router.post("/run/013-bloodwork-v2-schema")
def run_migration_013() -> Dict[str, Any]:
    """
    Run migration 013: Bloodwork Engine v2.0 Schema.
    
    Creates:
    - routing_constraint_blocks: Maps safety gates to ingredient blocks (seeded with 23 rows)
    - bloodwork_uploads: Stores OCR/API bloodwork uploads  
    - bloodwork_results: Stores processed results with routing constraints
    - bloodwork_markers: Individual marker results
    
    Safe to run multiple times (idempotent via IF NOT EXISTS and ON CONFLICT).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        # 1. Create routing_constraint_blocks table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS routing_constraint_blocks (
                id SERIAL PRIMARY KEY,
                gate_id VARCHAR(50) NOT NULL,
                routing_constraint VARCHAR(100) NOT NULL,
                gate_tier INTEGER NOT NULL DEFAULT 1,
                gate_action VARCHAR(20) NOT NULL DEFAULT 'BLOCK',
                ingredient_canonical_name VARCHAR(255) NOT NULL,
                ingredient_pattern VARCHAR(255),
                block_reason TEXT NOT NULL,
                exception_condition TEXT,
                exception_note TEXT,
                effective_from TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                effective_until TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                created_by VARCHAR(100) DEFAULT 'system',
                CONSTRAINT uk_gate_ingredient UNIQUE (gate_id, ingredient_canonical_name)
            )
        """)
        results.append("Created/verified routing_constraint_blocks table")
        
        # Create indexes
        for idx_name, idx_col in [
            ('idx_rcb_routing_constraint', 'routing_constraint'),
            ('idx_rcb_ingredient', 'ingredient_canonical_name'),
            ('idx_rcb_gate_tier', 'gate_tier'),
        ]:
            cur.execute(f"SELECT 1 FROM pg_indexes WHERE indexname = %s", (idx_name,))
            if not cur.fetchone():
                cur.execute(f"CREATE INDEX {idx_name} ON routing_constraint_blocks({idx_col})")
                results.append(f"Created index {idx_name}")
        
        # 2. Create bloodwork_uploads table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bloodwork_uploads (
                id SERIAL PRIMARY KEY,
                upload_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
                user_id VARCHAR(255),
                session_id VARCHAR(255),
                source_type VARCHAR(50) NOT NULL,
                source_provider VARCHAR(100),
                original_filename VARCHAR(255),
                file_mime_type VARCHAR(100),
                file_size_bytes INTEGER,
                raw_ocr_text TEXT,
                raw_api_response JSONB,
                parsed_markers JSONB NOT NULL,
                lab_name VARCHAR(255),
                lab_report_date DATE,
                patient_name VARCHAR(255),
                patient_dob DATE,
                status VARCHAR(50) DEFAULT 'pending',
                error_message TEXT,
                processing_started_at TIMESTAMP WITH TIME ZONE,
                processing_completed_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        results.append("Created/verified bloodwork_uploads table")
        
        # 3. Create bloodwork_results table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bloodwork_results (
                id SERIAL PRIMARY KEY,
                result_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
                upload_id UUID REFERENCES bloodwork_uploads(upload_id),
                user_id VARCHAR(255),
                sex VARCHAR(10),
                age INTEGER,
                lab_profile VARCHAR(50) NOT NULL,
                engine_version VARCHAR(20) NOT NULL,
                ruleset_version VARCHAR(100) NOT NULL,
                processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                total_markers INTEGER NOT NULL,
                valid_markers INTEGER NOT NULL,
                unknown_markers INTEGER NOT NULL,
                optimal_markers INTEGER NOT NULL,
                total_gates_triggered INTEGER DEFAULT 0,
                tier1_blocks INTEGER DEFAULT 0,
                tier1_cautions INTEGER DEFAULT 0,
                tier1_flags INTEGER DEFAULT 0,
                tier2_blocks INTEGER DEFAULT 0,
                tier2_cautions INTEGER DEFAULT 0,
                tier2_flags INTEGER DEFAULT 0,
                tier3_blocks INTEGER DEFAULT 0,
                tier3_cautions INTEGER DEFAULT 0,
                tier3_flags INTEGER DEFAULT 0,
                routing_constraints TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
                computed_markers JSONB,
                full_result JSONB NOT NULL,
                input_hash VARCHAR(64) NOT NULL,
                output_hash VARCHAR(64) NOT NULL,
                require_review BOOLEAN DEFAULT false,
                reviewed_at TIMESTAMP WITH TIME ZONE,
                reviewed_by VARCHAR(100),
                review_notes TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        results.append("Created/verified bloodwork_results table")
        
        # 4. Create bloodwork_markers table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bloodwork_markers (
                id SERIAL PRIMARY KEY,
                result_id UUID REFERENCES bloodwork_results(result_id) ON DELETE CASCADE,
                original_code VARCHAR(100) NOT NULL,
                canonical_code VARCHAR(100),
                original_value NUMERIC,
                canonical_value NUMERIC,
                original_unit VARCHAR(50) NOT NULL,
                canonical_unit VARCHAR(50),
                status VARCHAR(50) NOT NULL,
                range_status VARCHAR(50) NOT NULL,
                lab_profile_used VARCHAR(50),
                fallback_used BOOLEAN DEFAULT false,
                reference_low NUMERIC,
                reference_high NUMERIC,
                genomax_optimal_low NUMERIC,
                genomax_optimal_high NUMERIC,
                conversion_applied BOOLEAN DEFAULT false,
                conversion_multiplier NUMERIC,
                flags TEXT[] DEFAULT ARRAY[]::TEXT[],
                log_entries TEXT[] DEFAULT ARRAY[]::TEXT[],
                is_genetic BOOLEAN DEFAULT false,
                genetic_value VARCHAR(50),
                genetic_interpretation TEXT
            )
        """)
        results.append("Created/verified bloodwork_markers table")
        
        # 5. Seed routing constraint blocks (23 rows)
        seed_sql = """
            INSERT INTO routing_constraint_blocks (gate_id, routing_constraint, gate_tier, gate_action, ingredient_canonical_name, block_reason, exception_condition, exception_note)
            VALUES
                ('GATE_001', 'BLOCK_IRON', 1, 'BLOCK', 'iron', 'Ferritin >300(M)/200(F) indicates iron overload risk', 'hs_crp > 3.0', 'Acute inflammation may artificially elevate ferritin'),
                ('GATE_001', 'BLOCK_IRON', 1, 'BLOCK', 'iron_bisglycinate', 'Ferritin >300(M)/200(F) indicates iron overload risk', 'hs_crp > 3.0', 'Acute inflammation may artificially elevate ferritin'),
                ('GATE_001', 'BLOCK_IRON', 1, 'BLOCK', 'ferrous_sulfate', 'Ferritin >300(M)/200(F) indicates iron overload risk', 'hs_crp > 3.0', 'Acute inflammation may artificially elevate ferritin'),
                ('GATE_002', 'CAUTION_VITAMIN_D', 1, 'CAUTION', 'vitamin_d3', 'Calcium >10.5 mg/dL - vitamin D may increase calcium absorption', NULL, NULL),
                ('GATE_002', 'CAUTION_VITAMIN_D', 1, 'CAUTION', 'vitamin_d2', 'Calcium >10.5 mg/dL - vitamin D may increase calcium absorption', NULL, NULL),
                ('GATE_003', 'CAUTION_HEPATOTOXIC', 1, 'CAUTION', 'kava', 'ALT/AST >50(M)/40(F) - hepatotoxic risk', NULL, NULL),
                ('GATE_003', 'BLOCK_ASHWAGANDHA', 1, 'BLOCK', 'ashwagandha', 'ALT/AST >50(M)/40(F) - documented hepatotoxicity risk', NULL, NULL),
                ('GATE_003', 'CAUTION_HEPATOTOXIC', 1, 'CAUTION', 'green_tea_extract', 'ALT/AST >50(M)/40(F) - high-dose EGCG hepatotoxicity', NULL, NULL),
                ('GATE_003', 'CAUTION_HEPATOTOXIC', 1, 'CAUTION', 'niacin', 'ALT/AST >50(M)/40(F) - high-dose niacin hepatotoxicity', NULL, NULL),
                ('GATE_004', 'CAUTION_RENAL', 1, 'CAUTION', 'creatine', 'eGFR <60 or Creatinine elevated - renal-cleared supplement', NULL, NULL),
                ('GATE_004', 'CAUTION_RENAL', 1, 'CAUTION', 'magnesium', 'eGFR <60 or Creatinine elevated - renal excretion required', NULL, NULL),
                ('GATE_006', 'BLOCK_POTASSIUM', 1, 'BLOCK', 'potassium', 'K+ >5.0 mEq/L - hyperkalemia risk', NULL, NULL),
                ('GATE_006', 'BLOCK_POTASSIUM', 1, 'BLOCK', 'potassium_citrate', 'K+ >5.0 mEq/L - hyperkalemia risk', NULL, NULL),
                ('GATE_008', 'BLOCK_IODINE', 1, 'BLOCK', 'iodine', 'TSH <0.4 - hyperthyroid state, iodine contraindicated', NULL, NULL),
                ('GATE_008', 'BLOCK_IODINE', 1, 'BLOCK', 'kelp', 'TSH <0.4 - hyperthyroid state, high-iodine source', NULL, NULL),
                ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'fish_oil', 'Platelets <100K - may potentiate bleeding', NULL, NULL),
                ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'vitamin_e', 'Platelets <100K - may potentiate bleeding', NULL, NULL),
                ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'ginkgo', 'Platelets <100K - may potentiate bleeding', NULL, NULL),
                ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'garlic_extract', 'Platelets <100K - may potentiate bleeding', NULL, NULL),
                ('GATE_017', 'CAUTION_ZINC_EXCESS', 2, 'CAUTION', 'zinc', 'Zn:Cu ratio >1.5 - may worsen copper deficiency', NULL, NULL),
                ('GATE_017', 'CAUTION_ZINC_EXCESS', 2, 'CAUTION', 'zinc_picolinate', 'Zn:Cu ratio >1.5 - may worsen copper deficiency', NULL, NULL),
                ('GATE_020', 'CAUTION_FISH_OIL_DOSE', 2, 'CAUTION', 'fish_oil_high', 'Triglycerides >500 - requires medical-grade fish oil dosing', NULL, NULL),
                ('GATE_021', 'BLOCK_FOLIC_ACID', 3, 'BLOCK', 'folic_acid', 'MTHFR TT or compound heterozygous - cannot metabolize folic acid', NULL, NULL)
            ON CONFLICT (gate_id, ingredient_canonical_name) DO UPDATE SET
                routing_constraint = EXCLUDED.routing_constraint,
                block_reason = EXCLUDED.block_reason,
                exception_condition = EXCLUDED.exception_condition,
                exception_note = EXCLUDED.exception_note
        """
        cur.execute(seed_sql)
        results.append(f"Seeded routing_constraint_blocks (upserted {cur.rowcount} rows)")
        
        # Create update trigger for bloodwork_uploads
        cur.execute("""
            CREATE OR REPLACE FUNCTION update_bloodwork_uploads_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """)
        
        cur.execute("DROP TRIGGER IF EXISTS trg_bloodwork_uploads_updated ON bloodwork_uploads")
        cur.execute("""
            CREATE TRIGGER trg_bloodwork_uploads_updated
                BEFORE UPDATE ON bloodwork_uploads
                FOR EACH ROW
                EXECUTE FUNCTION update_bloodwork_uploads_timestamp()
        """)
        results.append("Created update timestamp trigger")
        
        # Get counts
        cur.execute("SELECT COUNT(*) as count FROM routing_constraint_blocks")
        rcb_count = cur.fetchone()['count']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "013-bloodwork-v2-schema",
            "tables_created": [
                "routing_constraint_blocks",
                "bloodwork_uploads", 
                "bloodwork_results",
                "bloodwork_markers"
            ],
            "routing_constraint_blocks_seeded": rcb_count,
            "steps": results
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/013-bloodwork-v2-schema")
def check_migration_013() -> Dict[str, Any]:
    """Check if migration 013 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        tables = ['routing_constraint_blocks', 'bloodwork_uploads', 'bloodwork_results', 'bloodwork_markers']
        table_status = {}
        
        for table in tables:
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            """, (table,))
            exists = cur.fetchone() is not None
            
            row_count = 0
            if exists:
                cur.execute(f"SELECT COUNT(*) as count FROM {table}")
                row_count = cur.fetchone()['count']
            
            table_status[table] = {
                "exists": exists,
                "row_count": row_count
            }
        
        # Sample routing constraints
        samples = []
        if table_status['routing_constraint_blocks']['exists']:
            cur.execute("""
                SELECT gate_id, routing_constraint, ingredient_canonical_name, gate_tier, gate_action
                FROM routing_constraint_blocks
                ORDER BY gate_id, ingredient_canonical_name
                LIMIT 10
            """)
            samples = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        all_exist = all(t['exists'] for t in table_status.values())
        rcb_seeded = table_status['routing_constraint_blocks']['row_count'] >= 20
        
        return {
            "migration": "013-bloodwork-v2-schema",
            "applied": all_exist and rcb_seeded,
            "tables": table_status,
            "routing_constraint_samples": samples
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
