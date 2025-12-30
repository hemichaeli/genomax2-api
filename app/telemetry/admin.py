"""
GenoMAX² Telemetry Admin Endpoints (Issue #9)
Secured admin-only endpoints for observability dashboard.

Auth: X-Admin-API-Key header required
"""

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import APIRouter, HTTPException, Header, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

from .models import (
    TelemetrySummary,
    TelemetryHealthResponse,
    TelemetryDailyRollup,
)


router = APIRouter(prefix="/api/v1/admin/telemetry", tags=["telemetry-admin"])


def get_db():
    """Get database connection."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return None
    try:
        return psycopg2.connect(db_url, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"[Telemetry Admin] DB connection failed: {e}")
        return None


def verify_admin_key(x_admin_api_key: Optional[str] = Header(None)):
    """Verify admin API key."""
    expected_key = os.getenv("ADMIN_API_KEY")
    if not expected_key:
        # If no key configured, block all access
        raise HTTPException(status_code=403, detail="Admin access not configured")
    if x_admin_api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid admin API key")
    return True


# ===== HEALTH CHECK =====

@router.get("/health", response_model=TelemetryHealthResponse)
async def telemetry_health(x_admin_api_key: Optional[str] = Header(None)):
    """Check telemetry system health."""
    verify_admin_key(x_admin_api_key)
    
    conn = get_db()
    if not conn:
        return TelemetryHealthResponse(
            status="unhealthy",
            telemetry_enabled=os.getenv("TELEMETRY_ENABLED", "true").lower() == "true",
            tables_exist=False,
        )
    
    try:
        cur = conn.cursor()
        
        # Check tables exist
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'telemetry_runs'
            ) as runs_exist,
            EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'telemetry_events'
            ) as events_exist
        """)
        row = cur.fetchone()
        tables_exist = row["runs_exist"] and row["events_exist"]
        
        if not tables_exist:
            cur.close()
            conn.close()
            return TelemetryHealthResponse(
                status="degraded",
                telemetry_enabled=True,
                tables_exist=False,
            )
        
        # Get last event
        cur.execute("""
            SELECT MAX(created_at) as last_event 
            FROM telemetry_events
        """)
        last_row = cur.fetchone()
        last_event = last_row["last_event"] if last_row else None
        
        # Get 24h counts
        cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM telemetry_runs 
                 WHERE created_at > NOW() - INTERVAL '24 hours') as runs_24h,
                (SELECT COUNT(*) FROM telemetry_events 
                 WHERE created_at > NOW() - INTERVAL '24 hours') as events_24h
        """)
        counts = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return TelemetryHealthResponse(
            status="healthy",
            telemetry_enabled=True,
            tables_exist=True,
            last_event_at=last_event,
            total_runs_24h=counts["runs_24h"] or 0,
            total_events_24h=counts["events_24h"] or 0,
        )
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return TelemetryHealthResponse(
            status="error",
            telemetry_enabled=True,
            tables_exist=False,
        )


# ===== SUMMARY =====

@router.get("/summary", response_model=TelemetrySummary)
async def telemetry_summary(
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    x_admin_api_key: Optional[str] = Header(None),
):
    """Get telemetry summary for date range."""
    verify_admin_key(x_admin_api_key)
    
    # Default to last 7 days
    now = datetime.now(timezone.utc)
    if to_date:
        period_end = datetime.fromisoformat(to_date).replace(tzinfo=timezone.utc)
    else:
        period_end = now
    
    if from_date:
        period_start = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
    else:
        period_start = period_end - timedelta(days=7)
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Basic counts
        cur.execute("""
            SELECT 
                COUNT(*) as total_runs,
                SUM(CASE WHEN has_bloodwork THEN 1 ELSE 0 END) as runs_with_bloodwork,
                SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) as runs_low_confidence,
                SUM(blocked_skus_count) as total_blocks,
                SUM(unmatched_intents_count) as total_unmatched,
                SUM(caution_flags_count) as total_cautions
            FROM telemetry_runs
            WHERE created_at BETWEEN %s AND %s
        """, (period_start, period_end))
        counts = cur.fetchone()
        
        # By sex
        cur.execute("""
            SELECT sex, COUNT(*) as count
            FROM telemetry_runs
            WHERE created_at BETWEEN %s AND %s
            AND sex IS NOT NULL
            GROUP BY sex
        """, (period_start, period_end))
        by_sex = {row["sex"]: row["count"] for row in cur.fetchall()}
        
        # By age bucket
        cur.execute("""
            SELECT age_bucket, COUNT(*) as count
            FROM telemetry_runs
            WHERE created_at BETWEEN %s AND %s
            AND age_bucket IS NOT NULL
            GROUP BY age_bucket
        """, (period_start, period_end))
        by_age = {row["age_bucket"]: row["count"] for row in cur.fetchall()}
        
        # By confidence
        cur.execute("""
            SELECT confidence_level, COUNT(*) as count
            FROM telemetry_runs
            WHERE created_at BETWEEN %s AND %s
            AND confidence_level IS NOT NULL
            GROUP BY confidence_level
        """, (period_start, period_end))
        by_confidence = {row["confidence_level"]: row["count"] for row in cur.fetchall()}
        
        # Top block reasons
        cur.execute("""
            SELECT code, SUM(count) as total
            FROM telemetry_events
            WHERE created_at BETWEEN %s AND %s
            AND event_type = 'ROUTING_BLOCK'
            GROUP BY code
            ORDER BY total DESC
            LIMIT 10
        """, (period_start, period_end))
        top_blocks = [{"code": row["code"], "count": row["total"]} for row in cur.fetchall()]
        
        # Top unmatched intents
        cur.execute("""
            SELECT code, SUM(count) as total
            FROM telemetry_events
            WHERE created_at BETWEEN %s AND %s
            AND event_type = 'MATCHING_UNMATCHED_INTENT'
            GROUP BY code
            ORDER BY total DESC
            LIMIT 10
        """, (period_start, period_end))
        top_unmatched = [{"intent": row["code"], "count": row["total"]} for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return TelemetrySummary(
            period_start=period_start,
            period_end=period_end,
            total_runs=counts["total_runs"] or 0,
            runs_with_bloodwork=counts["runs_with_bloodwork"] or 0,
            runs_low_confidence=counts["runs_low_confidence"] or 0,
            total_blocks=counts["total_blocks"] or 0,
            total_unmatched_intents=counts["total_unmatched"] or 0,
            total_caution_flags=counts["total_cautions"] or 0,
            top_block_reasons=top_blocks,
            top_unmatched_intents=top_unmatched,
            by_sex=by_sex,
            by_age_bucket=by_age,
            by_confidence=by_confidence,
        )
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


# ===== TOP ISSUES =====

@router.get("/top-issues")
async def top_issues(
    limit: int = Query(10, ge=1, le=50),
    x_admin_api_key: Optional[str] = Header(None),
):
    """Get top issues across all event types."""
    verify_admin_key(x_admin_api_key)
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Top issues by event type
        cur.execute("""
            SELECT 
                event_type,
                code,
                SUM(count) as total,
                MAX(created_at) as last_seen
            FROM telemetry_events
            WHERE created_at > NOW() - INTERVAL '30 days'
            GROUP BY event_type, code
            ORDER BY total DESC
            LIMIT %s
        """, (limit,))
        
        issues = []
        for row in cur.fetchall():
            issues.append({
                "event_type": row["event_type"],
                "code": row["code"],
                "total_count": row["total"],
                "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None,
            })
        
        # Coverage gaps (from matching)
        cur.execute("""
            SELECT code, SUM(count) as total
            FROM telemetry_events
            WHERE event_type = 'MATCHING_UNMATCHED_INTENT'
            AND created_at > NOW() - INTERVAL '30 days'
            GROUP BY code
            ORDER BY total DESC
            LIMIT %s
        """, (limit,))
        coverage_gaps = [{"intent": row["code"], "count": row["total"]} for row in cur.fetchall()]
        
        # Blocked most often
        cur.execute("""
            SELECT code, SUM(count) as total
            FROM telemetry_events
            WHERE event_type IN ('ROUTING_BLOCK', 'CATALOG_AUTO_BLOCK')
            AND created_at > NOW() - INTERVAL '30 days'
            GROUP BY code
            ORDER BY total DESC
            LIMIT %s
        """, (limit,))
        top_blocks = [{"reason": row["code"], "count": row["total"]} for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "period": "last_30_days",
            "all_issues": issues,
            "coverage_gaps": coverage_gaps,
            "top_blocks": top_blocks,
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


# ===== SINGLE RUN DETAIL =====

@router.get("/run/{run_id}")
async def get_run_telemetry(
    run_id: str,
    x_admin_api_key: Optional[str] = Header(None),
):
    """Get telemetry for a specific run."""
    verify_admin_key(x_admin_api_key)
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get run data
        cur.execute("""
            SELECT * FROM telemetry_runs WHERE run_id = %s
        """, (run_id,))
        run_row = cur.fetchone()
        
        if not run_row:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"No telemetry for run: {run_id}")
        
        # Get events for this run
        cur.execute("""
            SELECT event_type, code, count, metadata, created_at
            FROM telemetry_events
            WHERE run_id = %s
            ORDER BY created_at
        """, (run_id,))
        events = []
        for row in cur.fetchall():
            events.append({
                "event_type": row["event_type"],
                "code": row["code"],
                "count": row["count"],
                "metadata": row["metadata"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            })
        
        cur.close()
        conn.close()
        
        return {
            "run_id": run_id,
            "telemetry": dict(run_row),
            "events": events,
            "event_count": len(events),
        }
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


# ===== ROLLUP TRIGGER =====

@router.post("/rollup/run")
async def run_daily_rollup(
    x_admin_api_key: Optional[str] = Header(None),
):
    """Trigger daily rollup calculation."""
    verify_admin_key(x_admin_api_key)
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Calculate rollup for yesterday
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        
        # Get aggregates
        cur.execute("""
            SELECT 
                COUNT(*) as total_runs,
                AVG(CASE WHEN has_bloodwork THEN 100.0 ELSE 0 END) as pct_bloodwork,
                AVG(CASE WHEN confidence_level = 'low' THEN 100.0 ELSE 0 END) as pct_low_conf,
                AVG(unmatched_intents_count) as avg_unmatched,
                AVG(blocked_skus_count) as avg_blocked
            FROM telemetry_runs
            WHERE DATE(created_at) = %s
        """, (yesterday,))
        agg = cur.fetchone()
        
        # Top block reasons
        cur.execute("""
            SELECT code, SUM(count) as total
            FROM telemetry_events
            WHERE DATE(created_at) = %s
            AND event_type IN ('ROUTING_BLOCK', 'CATALOG_AUTO_BLOCK')
            GROUP BY code
            ORDER BY total DESC
            LIMIT 10
        """, (yesterday,))
        top_blocks = [{"code": row["code"], "count": row["total"]} for row in cur.fetchall()]
        
        # Top missing fields / ingredients
        cur.execute("""
            SELECT code, SUM(count) as total
            FROM telemetry_events
            WHERE DATE(created_at) = %s
            AND event_type = 'UNKNOWN_INGREDIENT'
            GROUP BY code
            ORDER BY total DESC
            LIMIT 10
        """, (yesterday,))
        top_unknown = [{"ingredient": row["code"], "count": row["total"]} for row in cur.fetchall()]
        
        # Upsert rollup
        cur.execute("""
            INSERT INTO telemetry_daily_rollups 
            (day, total_runs, pct_has_bloodwork, pct_low_confidence, 
             avg_unmatched_intents, avg_blocked_skus, 
             top_block_reasons, top_unknown_ingredients, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (day) DO UPDATE SET
                total_runs = EXCLUDED.total_runs,
                pct_has_bloodwork = EXCLUDED.pct_has_bloodwork,
                pct_low_confidence = EXCLUDED.pct_low_confidence,
                avg_unmatched_intents = EXCLUDED.avg_unmatched_intents,
                avg_blocked_skus = EXCLUDED.avg_blocked_skus,
                top_block_reasons = EXCLUDED.top_block_reasons,
                top_unknown_ingredients = EXCLUDED.top_unknown_ingredients,
                updated_at = NOW()
        """, (
            yesterday,
            agg["total_runs"] or 0,
            agg["pct_bloodwork"] or 0,
            agg["pct_low_conf"] or 0,
            agg["avg_unmatched"] or 0,
            agg["avg_blocked"] or 0,
            json.dumps(top_blocks),
            json.dumps(top_unknown),
        ))
        conn.commit()
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "day": yesterday.isoformat(),
            "total_runs": agg["total_runs"] or 0,
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Rollup failed: {str(e)}")


# ===== TRENDS =====

@router.get("/trends")
async def get_trends(
    days: int = Query(7, ge=1, le=90),
    x_admin_api_key: Optional[str] = Header(None),
):
    """Get trend data for the last N days."""
    verify_admin_key(x_admin_api_key)
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Daily run counts
        cur.execute("""
            SELECT 
                DATE(created_at) as day,
                COUNT(*) as total_runs,
                SUM(CASE WHEN has_bloodwork THEN 1 ELSE 0 END) as with_bloodwork,
                SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) as low_confidence,
                AVG(unmatched_intents_count) as avg_unmatched,
                AVG(blocked_skus_count) as avg_blocked
            FROM telemetry_runs
            WHERE created_at > NOW() - INTERVAL '%s days'
            GROUP BY DATE(created_at)
            ORDER BY day DESC
        """ % days)
        
        daily_data = []
        for row in cur.fetchall():
            daily_data.append({
                "day": row["day"].isoformat(),
                "total_runs": row["total_runs"],
                "with_bloodwork": row["with_bloodwork"] or 0,
                "low_confidence": row["low_confidence"] or 0,
                "avg_unmatched": float(row["avg_unmatched"] or 0),
                "avg_blocked": float(row["avg_blocked"] or 0),
            })
        
        cur.close()
        conn.close()
        
        return {
            "period_days": days,
            "daily": daily_data,
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


# ===== SETUP TABLES =====

@router.post("/setup")
async def setup_telemetry_tables(
    x_admin_api_key: Optional[str] = Header(None),
):
    """Create telemetry tables if they don't exist."""
    verify_admin_key(x_admin_api_key)
    
    from .emitter import TelemetryEmitter
    
    emitter = TelemetryEmitter.get_instance()
    conn = emitter._get_conn()
    
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    success = emitter._ensure_tables(conn)
    
    try:
        conn.close()
    except:
        pass
    
    if success:
        return {"status": "success", "message": "Telemetry tables created/verified"}
    else:
        raise HTTPException(status_code=500, detail="Table creation failed")
