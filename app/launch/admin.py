"""
Launch Admin Router for GenoMAX²
================================
Endpoints for managing Launch v1 scope and validation.

Launch v1 Definition (LOCKED):
- TIER 1 + TIER 2 = included
- TIER 3 = excluded

Endpoints:
- GET  /api/v1/launch/v1/status - Current launch scope status
- POST /api/v1/launch/v1/lock - Execute lock migration
- GET  /api/v1/launch/v1/validate - Validate launch scope
- GET  /api/v1/launch/v1/modules - List modules in launch scope
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)
router = APIRouter(tags=["launch-admin"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    """Get database connection."""
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


# ===== Launch v1 Lock SQL =====

LOCK_LAUNCH_V1_SQL = """
-- Step 1: Add the is_launch_v1 column if not present
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');

-- Step 3: Explicitly exclude Tier 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL
   OR is_launch_v1 IS NULL;

-- Step 4: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;
"""


# ===== Endpoints =====

@router.get("/api/v1/launch/v1/status")
def get_launch_status():
    """
    Get current Launch v1 scope status.
    
    Returns:
    - Whether is_launch_v1 column exists
    - Count by tier in launch scope
    - Total launch vs excluded counts
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
        column_exists = cur.fetchone() is not None
        
        if not column_exists:
            cur.close()
            conn.close()
            return {
                "status": "NOT_LOCKED",
                "column_exists": False,
                "message": "is_launch_v1 column does not exist. Run /api/v1/launch/v1/lock to apply.",
                "launch_v1_count": 0,
                "excluded_count": 0,
            }
        
        # Get tier distribution in launch scope
        cur.execute("""
            SELECT 
                tier,
                COUNT(*) as count,
                is_launch_v1
            FROM os_modules_v3_1
            GROUP BY tier, is_launch_v1
            ORDER BY tier, is_launch_v1
        """)
        tier_data = cur.fetchall()
        
        # Get totals
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded,
                COUNT(*) as total
            FROM os_modules_v3_1
        """)
        totals = cur.fetchone()
        
        # Get active + launch intersection
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE AND supplier_status = 'ACTIVE') as active_launch,
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE AND supplier_status != 'ACTIVE') as inactive_launch
            FROM os_modules_v3_1
        """)
        active_data = cur.fetchone()
        
        cur.close()
        conn.close()
        
        # Build tier breakdown
        launch_by_tier = {}
        excluded_by_tier = {}
        for row in tier_data:
            tier = row["tier"] or "NULL"
            if row["is_launch_v1"]:
                launch_by_tier[tier] = row["count"]
            else:
                excluded_by_tier[tier] = row["count"]
        
        # Check for TIER 3 in launch (should be 0)
        tier_3_in_launch = launch_by_tier.get("TIER 3", 0) + \
                          launch_by_tier.get("Tier 3", 0) + \
                          launch_by_tier.get("tier 3", 0)
        
        return {
            "status": "LOCKED" if tier_3_in_launch == 0 else "INVALID",
            "column_exists": True,
            "launch_v1_count": totals["launch_v1"],
            "excluded_count": totals["excluded"],
            "total_modules": totals["total"],
            "active_in_launch": active_data["active_launch"],
            "inactive_in_launch": active_data["inactive_launch"],
            "launch_by_tier": launch_by_tier,
            "excluded_by_tier": excluded_by_tier,
            "tier_3_in_launch": tier_3_in_launch,
            "validation": "PASS" if tier_3_in_launch == 0 else "FAIL - TIER 3 found in launch",
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/api/v1/launch/v1/lock")
def lock_launch_v1(
    confirm: bool = Query(default=False, description="Must be true to execute lock")
):
    """
    Execute the Lock Launch v1 migration.
    
    This will:
    1. Add is_launch_v1 column if not present
    2. Set is_launch_v1 = TRUE for TIER 1 + TIER 2
    3. Set is_launch_v1 = FALSE for TIER 3 and NULL tiers
    4. Create index for efficient filtering
    
    Requires confirm=true to execute.
    """
    if not confirm:
        return {
            "status": "DRY_RUN",
            "message": "Add ?confirm=true to execute the lock migration",
            "sql_preview": LOCK_LAUNCH_V1_SQL,
        }
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Execute lock migration
        cur.execute(LOCK_LAUNCH_V1_SQL)
        conn.commit()
        
        # Get results
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded,
                COUNT(*) as total
            FROM os_modules_v3_1
        """)
        totals = cur.fetchone()
        
        # Get tier breakdown
        cur.execute("""
            SELECT tier, COUNT(*) as count
            FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE
            GROUP BY tier
            ORDER BY tier
        """)
        launch_tiers = {row["tier"]: row["count"] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return {
            "status": "LOCKED",
            "message": "Launch v1 scope locked successfully",
            "launch_v1_count": totals["launch_v1"],
            "excluded_count": totals["excluded"],
            "total_modules": totals["total"],
            "launch_by_tier": launch_tiers,
            "executed_at": datetime.now(timezone.utc).isoformat(),
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")


@router.get("/api/v1/launch/v1/validate")
def validate_launch_scope():
    """
    Validate Launch v1 scope integrity.
    
    Checks:
    1. No TIER 3 products in launch scope
    2. All TIER 1 + TIER 2 products are in launch scope
    3. Column exists and is populated
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
            "message": "is_launch_v1 column exists" if column_exists else "Column missing - run lock migration"
        })
        if not column_exists:
            all_passed = False
            cur.close()
            conn.close()
            return {
                "status": "FAIL",
                "all_passed": False,
                "checks": checks,
                "message": "Launch v1 not locked - column missing"
            }
        
        # Check 2: No TIER 3 in launch
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE is_launch_v1 = TRUE
            AND tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
        """)
        tier_3_in_launch = cur.fetchone()["count"]
        check_2_passed = tier_3_in_launch == 0
        checks.append({
            "check": "no_tier_3_in_launch",
            "passed": check_2_passed,
            "count": tier_3_in_launch,
            "message": "No TIER 3 in launch scope" if check_2_passed else f"FAIL: {tier_3_in_launch} TIER 3 products in launch"
        })
        if not check_2_passed:
            all_passed = False
        
        # Check 3: All TIER 1 in launch
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as in_launch,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded,
                COUNT(*) as total
            FROM os_modules_v3_1
            WHERE tier IN ('TIER 1', 'Tier 1', 'tier 1', '1')
        """)
        tier_1 = cur.fetchone()
        check_3_passed = tier_1["excluded"] == 0
        checks.append({
            "check": "all_tier_1_in_launch",
            "passed": check_3_passed,
            "in_launch": tier_1["in_launch"],
            "excluded": tier_1["excluded"],
            "message": f"All {tier_1['total']} TIER 1 products in launch" if check_3_passed else f"FAIL: {tier_1['excluded']} TIER 1 products excluded"
        })
        if not check_3_passed:
            all_passed = False
        
        # Check 4: All TIER 2 in launch
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as in_launch,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded,
                COUNT(*) as total
            FROM os_modules_v3_1
            WHERE tier IN ('TIER 2', 'Tier 2', 'tier 2', '2')
        """)
        tier_2 = cur.fetchone()
        check_4_passed = tier_2["excluded"] == 0
        checks.append({
            "check": "all_tier_2_in_launch",
            "passed": check_4_passed,
            "in_launch": tier_2["in_launch"],
            "excluded": tier_2["excluded"],
            "message": f"All {tier_2['total']} TIER 2 products in launch" if check_4_passed else f"FAIL: {tier_2['excluded']} TIER 2 products excluded"
        })
        if not check_4_passed:
            all_passed = False
        
        # Check 5: TIER 3 preserved (not deleted)
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
        """)
        tier_3_total = cur.fetchone()["count"]
        checks.append({
            "check": "tier_3_preserved",
            "passed": True,  # Informational
            "count": tier_3_total,
            "message": f"{tier_3_total} TIER 3 products preserved in DB (excluded from launch)"
        })
        
        cur.close()
        conn.close()
        
        return {
            "status": "PASS" if all_passed else "FAIL",
            "all_passed": all_passed,
            "checks": checks,
            "summary": {
                "tier_1_in_launch": tier_1["in_launch"],
                "tier_2_in_launch": tier_2["in_launch"],
                "tier_3_excluded": tier_3_total,
                "tier_3_in_launch_violation": tier_3_in_launch,
            }
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/api/v1/launch/v1/modules")
def list_launch_modules(
    include_excluded: bool = Query(default=False, description="Include excluded modules"),
    tier: Optional[str] = Query(default=None, description="Filter by tier"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """
    List modules in Launch v1 scope.
    
    By default returns only modules where is_launch_v1 = TRUE.
    Use include_excluded=true to see all modules with their launch status.
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
                detail="Launch v1 not locked - run POST /api/v1/launch/v1/lock?confirm=true first"
            )
        
        # Build query
        query = """
            SELECT 
                module_code,
                shopify_handle,
                product_name,
                tier,
                os_environment,
                supplier_status,
                is_launch_v1
            FROM os_modules_v3_1
            WHERE 1=1
        """
        params = []
        
        if not include_excluded:
            query += " AND is_launch_v1 = TRUE"
        
        if tier:
            query += " AND tier ILIKE %s"
            params.append(f"%{tier}%")
        
        query += " ORDER BY tier, os_environment, module_code LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        modules = [dict(row) for row in cur.fetchall()]
        
        # Get counts
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1,
                COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded
            FROM os_modules_v3_1
        """)
        counts = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return {
            "launch_v1_total": counts["launch_v1"],
            "excluded_total": counts["excluded"],
            "returned_count": len(modules),
            "include_excluded": include_excluded,
            "tier_filter": tier,
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
