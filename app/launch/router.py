"""
Launch v1 Lock Router
=====================
Endpoints to manage Launch v1 scope in the database.

Endpoints:
- GET  /api/v1/launch/v1/status - Current launch scope status
- POST /api/v1/launch/v1/lock - Execute the lock migration
- GET  /api/v1/launch/v1/validate - Run validation queries
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

from app.migrations.launch_v1_lock import (
    MIGRATION_ID,
    MIGRATION_VERSION,
    run_migration,
    validate_migration,
    VALIDATION_QUERIES,
    ADD_COLUMN_SQL,
    ADD_INDEX_SQL,
    SET_TIER_1_2_SQL,
    SET_TIER_3_SQL,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["launch"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    """Get database connection."""
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


@router.get("/api/v1/launch/v1/status")
def get_launch_v1_status():
    """
    Get current Launch v1 scope status.
    
    Returns:
    - Whether is_launch_v1 column exists
    - Tier distribution with launch flags
    - Total counts for in-launch vs excluded
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
                "message": "Launch v1 has not been locked yet. Run POST /api/v1/launch/v1/lock to lock.",
                "migration_id": MIGRATION_ID,
                "migration_version": MIGRATION_VERSION,
            }
        
        # Get tier distribution with launch flag
        cur.execute("""
            SELECT 
                tier,
                is_launch_v1,
                supplier_status,
                COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY tier, is_launch_v1, supplier_status
            ORDER BY tier, is_launch_v1, supplier_status
        """)
        distribution = [dict(row) for row in cur.fetchall()]
        
        # Get summary counts
        cur.execute("""
            SELECT 
                SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as in_launch_v1,
                SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded,
                SUM(CASE WHEN is_launch_v1 IS NULL THEN 1 ELSE 0 END) as null_flag,
                COUNT(*) as total
            FROM os_modules_v3_1
        """)
        summary = dict(cur.fetchone())
        
        # Get ACTIVE-only counts for launch
        cur.execute("""
            SELECT 
                SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as active_in_launch,
                COUNT(*) as total_active
            FROM os_modules_v3_1
            WHERE supplier_status = 'ACTIVE'
        """)
        active_summary = dict(cur.fetchone())
        
        cur.close()
        conn.close()
        
        return {
            "status": "LOCKED",
            "column_exists": True,
            "migration_id": MIGRATION_ID,
            "migration_version": MIGRATION_VERSION,
            "summary": summary,
            "active_only": active_summary,
            "tier_distribution": distribution,
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/api/v1/launch/v1/lock")
def lock_launch_v1(
    confirm: bool = Query(default=False, description="Must be true to execute lock"),
    dry_run: bool = Query(default=True, description="If true, show what would happen without executing"),
):
    """
    Lock Launch v1 in the database.
    
    - Adds is_launch_v1 column
    - Sets TRUE for TIER 1 + TIER 2
    - Sets FALSE for TIER 3 and NULL tiers
    
    Use dry_run=true (default) to preview, confirm=true to execute.
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
        
        # Get tier distribution preview
        cur.execute("""
            SELECT 
                tier,
                supplier_status,
                COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY tier, supplier_status
            ORDER BY tier, supplier_status
        """)
        current_distribution = [dict(row) for row in cur.fetchall()]
        
        # Calculate what would happen
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2')
        """)
        would_be_launch_v1 = cur.fetchone()["count"]
        
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
               OR tier IS NULL
        """)
        would_be_excluded = cur.fetchone()["count"]
        
        preview = {
            "column_already_exists": column_exists,
            "current_tier_distribution": current_distribution,
            "would_set_launch_v1_true": would_be_launch_v1,
            "would_set_launch_v1_false": would_be_excluded,
        }
        
        if dry_run and not confirm:
            cur.close()
            conn.close()
            return {
                "mode": "DRY_RUN",
                "message": "Preview only. Use confirm=true&dry_run=false to execute.",
                "preview": preview,
                "migration_id": MIGRATION_ID,
            }
        
        if not confirm:
            cur.close()
            conn.close()
            return {
                "mode": "PREVIEW",
                "message": "Add confirm=true to execute the lock.",
                "preview": preview,
            }
        
        # Execute migration
        if column_exists:
            # Column exists, just update values
            cur.execute(SET_TIER_1_2_SQL)
            tier_1_2_updated = cur.rowcount
            
            cur.execute(SET_TIER_3_SQL)
            tier_3_updated = cur.rowcount
            
            conn.commit()
            
            result = {
                "mode": "EXECUTED",
                "action": "UPDATE_ONLY",
                "message": "Column already existed. Updated launch flags.",
                "tier_1_2_updated": tier_1_2_updated,
                "tier_3_updated": tier_3_updated,
            }
        else:
            # Full migration
            migration_result = run_migration(cur)
            conn.commit()
            
            result = {
                "mode": "EXECUTED",
                "action": "FULL_MIGRATION",
                "message": "Launch v1 locked successfully.",
                "migration_result": migration_result,
            }
        
        # Run validation
        validation = validate_migration(cur)
        result["validation"] = validation
        
        cur.close()
        conn.close()
        
        return result
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/api/v1/launch/v1/validate")
def validate_launch_v1():
    """
    Run validation queries to confirm Launch v1 is properly locked.
    
    Checks:
    - Tier 1 + Tier 2 have is_launch_v1 = TRUE
    - Tier 3 has is_launch_v1 = FALSE
    - No NULL flags remain
    - Tier 3 products are preserved (not deleted)
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' 
            AND column_name = 'is_launch_v1'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {
                "valid": False,
                "error": "is_launch_v1 column does not exist",
                "action_required": "Run POST /api/v1/launch/v1/lock?confirm=true&dry_run=false"
            }
        
        # Run all validation queries
        validation = validate_migration(cur)
        
        # Check for violations
        violations = []
        
        # Check: No Tier 3 in launch
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
            AND is_launch_v1 = TRUE
        """)
        tier_3_in_launch = cur.fetchone()["count"]
        if tier_3_in_launch > 0:
            violations.append(f"VIOLATION: {tier_3_in_launch} Tier 3 products have is_launch_v1=TRUE")
        
        # Check: Tier 1 + 2 should be in launch
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', '1', '2')
            AND (is_launch_v1 = FALSE OR is_launch_v1 IS NULL)
        """)
        tier_1_2_excluded = cur.fetchone()["count"]
        if tier_1_2_excluded > 0:
            violations.append(f"WARNING: {tier_1_2_excluded} Tier 1/2 products have is_launch_v1=FALSE")
        
        # Check: No NULL flags
        cur.execute("""
            SELECT COUNT(*) as count
            FROM os_modules_v3_1
            WHERE is_launch_v1 IS NULL
        """)
        null_flags = cur.fetchone()["count"]
        if null_flags > 0:
            violations.append(f"WARNING: {null_flags} products have is_launch_v1=NULL")
        
        cur.close()
        conn.close()
        
        return {
            "valid": len(violations) == 0,
            "violations": violations,
            "validation_queries": validation,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")


@router.get("/api/v1/launch/v1/modules")
def list_launch_v1_modules(
    in_launch: bool = Query(default=True, description="Filter by is_launch_v1 flag"),
    active_only: bool = Query(default=True, description="Only ACTIVE supplier status"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    List modules in or out of Launch v1.
    
    Useful for verifying which products are included/excluded.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check column exists
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
                detail="Launch v1 not locked yet. Run POST /api/v1/launch/v1/lock first."
            )
        
        # Build query
        conditions = ["is_launch_v1 = %s"]
        params = [in_launch]
        
        if active_only:
            conditions.append("supplier_status = 'ACTIVE'")
        
        cur.execute(f"""
            SELECT 
                module_code,
                product_name,
                shopify_handle,
                tier,
                os_environment,
                supplier_status,
                is_launch_v1
            FROM os_modules_v3_1
            WHERE {' AND '.join(conditions)}
            ORDER BY tier, os_environment, module_code
            LIMIT %s
        """, params + [limit])
        
        modules = [dict(row) for row in cur.fetchall()]
        
        # Get total count
        cur.execute(f"""
            SELECT COUNT(*) as total
            FROM os_modules_v3_1
            WHERE {' AND '.join(conditions)}
        """, params)
        total = cur.fetchone()["total"]
        
        cur.close()
        conn.close()
        
        return {
            "filter": {
                "in_launch": in_launch,
                "active_only": active_only,
            },
            "total": total,
            "returned": len(modules),
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
