"""
Launch Gate Router for GenoMAX²
===============================
Provides endpoints for Launch v1 scope management and validation.

Endpoints:
- GET  /api/v1/launch/v1/status - Get Launch v1 status and counts
- GET  /api/v1/launch/v1/modules - List Launch v1 modules
- GET  /api/v1/launch/v1/validate - Validate Launch v1 scope (QA assertion)
- POST /api/v1/launch/v1/migrate - Run is_launch_v1 migration

Launch v1 Definition (LOCKED):
- Includes: TIER 1 + TIER 2
- Excludes: TIER 3
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/launch", tags=["launch"])

DATABASE_URL = os.getenv("DATABASE_URL")


# ===== Database Helpers =====

def get_db():
    """Get database connection."""
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


# ===== Endpoints =====

@router.get("/v1/status")
def launch_v1_status():
    """
    Get Launch v1 status and module counts.
    
    Returns:
        Summary of Launch v1 scope with tier breakdown
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check if is_launch_v1 column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' 
              AND column_name = 'is_launch_v1'
        """)
        column_exists = cur.fetchone() is not None
        
        if not column_exists:
            cur.close()
            conn.close()
            return {
                "status": "NOT_MIGRATED",
                "message": "is_launch_v1 column not found. Run migration first.",
                "column_exists": False,
                "launch_v1_count": 0,
                "excluded_count": 0,
            }
        
        # Get counts by tier for Launch v1
        cur.execute("""
            SELECT 
                tier,
                COUNT(*) as count,
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as in_launch,
                COUNT(*) FILTER (WHERE supplier_status = 'ACTIVE') as active_count
            FROM os_modules_v3_1
            GROUP BY tier
            ORDER BY tier
        """)
        tier_breakdown = [dict(row) for row in cur.fetchall()]
        
        # Get summary counts
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE AND supplier_status = 'ACTIVE') as launch_v1_active,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded_count,
                COUNT(*) as total_count
            FROM os_modules_v3_1
        """)
        summary = dict(cur.fetchone())
        
        cur.close()
        conn.close()
        
        return {
            "status": "MIGRATED",
            "column_exists": True,
            "launch_v1_count": summary["launch_v1_count"],
            "launch_v1_active": summary["launch_v1_active"],
            "excluded_count": summary["excluded_count"],
            "total_count": summary["total_count"],
            "tier_breakdown": tier_breakdown,
            "definition": {
                "includes": ["TIER 1", "TIER 2"],
                "excludes": ["TIER 3"],
            },
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/v1/modules")
def launch_v1_modules(
    include_excluded: bool = Query(default=False, description="Include TIER 3 in response"),
    active_only: bool = Query(default=True, description="Filter to ACTIVE supplier_status"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """
    List modules in Launch v1 scope.
    
    By default, returns only is_launch_v1=TRUE and supplier_status=ACTIVE.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check if column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' 
              AND column_name = 'is_launch_v1'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=400,
                detail="is_launch_v1 column not found. Run migration first."
            )
        
        # Build query
        query = """
            SELECT 
                module_code,
                product_name,
                shopify_handle,
                os_environment,
                tier,
                is_launch_v1,
                supplier_status
            FROM os_modules_v3_1
            WHERE 1=1
        """
        params = []
        
        if not include_excluded:
            query += " AND is_launch_v1 = TRUE"
        
        if active_only:
            query += " AND supplier_status = 'ACTIVE'"
        
        query += " ORDER BY tier, os_environment, module_code LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        modules = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "count": len(modules),
            "filters": {
                "include_excluded": include_excluded,
                "active_only": active_only,
            },
            "modules": modules,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/v1/validate")
def launch_v1_validate():
    """
    QA assertion endpoint to validate Launch v1 scope.
    
    Checks:
    1. is_launch_v1 column exists
    2. TIER 1 products are in launch (count > 0)
    3. TIER 2 products are in launch (count > 0)
    4. TIER 3 products are NOT in launch (count = 0)
    5. No NULL tier values in launch
    
    Returns:
        Validation result with pass/fail and details
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        checks = []
        all_passed = True
        
        # Check 1: Column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' 
              AND column_name = 'is_launch_v1'
        """)
        column_exists = cur.fetchone() is not None
        checks.append({
            "check": "column_exists",
            "passed": column_exists,
            "expected": True,
            "actual": column_exists,
        })
        if not column_exists:
            all_passed = False
            cur.close()
            conn.close()
            return {
                "validation_passed": False,
                "checks": checks,
                "message": "Migration required: is_launch_v1 column does not exist",
            }
        
        # Check 2: TIER 1 in launch
        cur.execute("""
            SELECT COUNT(*) FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE 
              AND tier IN ('TIER 1', 'Tier 1', 'tier 1', 'T1', '1')
        """)
        tier1_count = cur.fetchone()[0]
        tier1_passed = tier1_count > 0
        checks.append({
            "check": "tier1_in_launch",
            "passed": tier1_passed,
            "expected": "> 0",
            "actual": tier1_count,
        })
        if not tier1_passed:
            all_passed = False
        
        # Check 3: TIER 2 in launch
        cur.execute("""
            SELECT COUNT(*) FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE 
              AND tier IN ('TIER 2', 'Tier 2', 'tier 2', 'T2', '2')
        """)
        tier2_count = cur.fetchone()[0]
        tier2_passed = tier2_count > 0
        checks.append({
            "check": "tier2_in_launch",
            "passed": tier2_passed,
            "expected": "> 0",
            "actual": tier2_count,
        })
        if not tier2_passed:
            all_passed = False
        
        # Check 4: TIER 3 NOT in launch
        cur.execute("""
            SELECT COUNT(*) FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE 
              AND tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3')
        """)
        tier3_in_launch = cur.fetchone()[0]
        tier3_passed = tier3_in_launch == 0
        checks.append({
            "check": "tier3_excluded",
            "passed": tier3_passed,
            "expected": 0,
            "actual": tier3_in_launch,
        })
        if not tier3_passed:
            all_passed = False
        
        # Check 5: No NULL tier in launch
        cur.execute("""
            SELECT COUNT(*) FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE AND tier IS NULL
        """)
        null_tier_in_launch = cur.fetchone()[0]
        null_tier_passed = null_tier_in_launch == 0
        checks.append({
            "check": "no_null_tier_in_launch",
            "passed": null_tier_passed,
            "expected": 0,
            "actual": null_tier_in_launch,
        })
        if not null_tier_passed:
            all_passed = False
        
        # Get summary for response
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded_count
            FROM os_modules_v3_1
        """)
        summary = dict(cur.fetchone())
        
        cur.close()
        conn.close()
        
        return {
            "validation_passed": all_passed,
            "checks": checks,
            "summary": {
                "launch_v1_count": summary["launch_v1_count"],
                "excluded_count": summary["excluded_count"],
                "tier1_count": tier1_count,
                "tier2_count": tier2_count,
                "tier3_excluded": tier3_in_launch == 0,
            },
            "message": "All Launch v1 scope checks passed" if all_passed else "One or more checks failed",
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/v1/migrate")
def launch_v1_migrate(
    confirm: bool = Query(default=False, description="Must be true to execute migration"),
):
    """
    Run is_launch_v1 migration.
    
    Adds is_launch_v1 column and populates based on tier:
    - TRUE for TIER 1, TIER 2
    - FALSE for TIER 3
    
    If confirm=false, returns preview only.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check current state
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' 
              AND column_name = 'is_launch_v1'
        """)
        column_exists = cur.fetchone() is not None
        
        # Get tier distribution
        cur.execute("""
            SELECT tier, COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY tier
            ORDER BY tier
        """)
        tier_distribution = {row["tier"]: row["count"] for row in cur.fetchall()}
        
        if not confirm:
            cur.close()
            conn.close()
            return {
                "mode": "DRY_RUN",
                "column_exists": column_exists,
                "tier_distribution": tier_distribution,
                "will_set_launch_v1_true": [
                    k for k in tier_distribution.keys() 
                    if k and k.upper() in ('TIER 1', 'TIER 2', 'T1', 'T2', '1', '2')
                ],
                "will_set_launch_v1_false": [
                    k for k in tier_distribution.keys() 
                    if k and k.upper() in ('TIER 3', 'T3', '3') or k is None
                ],
                "message": "Add confirm=true to execute migration",
            }
        
        # Execute migration
        cur.execute("""
            ALTER TABLE os_modules_v3_1
            ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
                ON os_modules_v3_1(is_launch_v1) 
                WHERE is_launch_v1 = TRUE
        """)
        
        # Set TRUE for Tier 1 + Tier 2
        cur.execute("""
            UPDATE os_modules_v3_1
            SET is_launch_v1 = TRUE
            WHERE UPPER(tier) IN ('TIER 1', 'TIER 2', 'T1', 'T2', '1', '2')
        """)
        tier12_updated = cur.rowcount
        
        # Set FALSE for Tier 3 and NULL
        cur.execute("""
            UPDATE os_modules_v3_1
            SET is_launch_v1 = FALSE
            WHERE UPPER(tier) IN ('TIER 3', 'T3', '3')
               OR tier IS NULL
               OR is_launch_v1 IS NULL
        """)
        tier3_updated = cur.rowcount
        
        conn.commit()
        
        # Validate
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count
            FROM os_modules_v3_1
        """)
        result = dict(cur.fetchone())
        
        cur.close()
        conn.close()
        
        return {
            "mode": "EXECUTED",
            "status": "SUCCESS",
            "tier12_rows_updated": tier12_updated,
            "tier3_rows_updated": tier3_updated,
            "launch_v1_count": result["launch_v1_count"],
            "excluded_count": result["excluded_count"],
            "message": "Launch v1 scope locked successfully",
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")
