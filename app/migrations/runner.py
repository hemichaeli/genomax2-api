"""
GenoMAX² Migration Runner
One-time endpoint to run migrations via API
"""

import os
from typing import Dict, Any
from fastapi import APIRouter, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/migrations", tags=["Migrations"])

DATABASE_URL = os.getenv("DATABASE_URL")

# Standard DSHEA disclaimer (singular form - UI handles plural based on claim count)
FDA_DISCLAIMER_TEXT = "This statement has not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease."


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
    
    - Only fills modules where disclaimer_applicability = 'SUPPLEMENT'
    - Only fills if fda_disclaimer is NULL or empty
    - Uses standard DSHEA disclaimer text (singular form)
    - UI layer handles singular/plural based on claim count
    
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
