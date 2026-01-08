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
