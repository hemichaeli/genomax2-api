"""
Copy Cleanup Router for GenoMAX²
================================
API endpoints for placeholder analysis and cleanup.

Endpoints:
- GET  /api/v1/qa/copy/placeholders/list - List all modules with placeholders
- POST /api/v1/copy/cleanup/dry-run - Preview cleanup without changes
- POST /api/v1/copy/cleanup/execute - Execute cleanup with audit logging
- GET  /api/v1/qa/copy/clean/count - Count placeholder-free modules
"""

import os
import re
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor

from app.copy.renderer import (
    render_front_label,
    render_back_label,
    render_shopify_body,
    render_all,
    analyze_module_placeholders,
    contains_placeholder,
    find_placeholders,
    strip_placeholders,
    PLACEHOLDER_PATTERN,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["copy-cleanup"])

DATABASE_URL = os.getenv("DATABASE_URL")


# ===== Models =====

class CleanupRequest(BaseModel):
    """Request body for cleanup endpoints."""
    scope: str = Field(
        default="ACTIVE_ONLY",
        description="Scope of cleanup: ACTIVE_ONLY or ALL"
    )


class CleanupScope:
    ACTIVE_ONLY = "ACTIVE_ONLY"
    ALL = "ALL"


# ===== Database Helpers =====

def get_db():
    """Get database connection."""
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


def ensure_audit_table():
    """Create copy_cleanup_audit_v1 table if it doesn't exist."""
    conn = get_db()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS copy_cleanup_audit_v1 (
                audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                batch_id UUID NOT NULL,
                module_code TEXT NOT NULL,
                shopify_handle TEXT,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_copy_audit_batch_id 
                ON copy_cleanup_audit_v1(batch_id);
            CREATE INDEX IF NOT EXISTS idx_copy_audit_module_code 
                ON copy_cleanup_audit_v1(module_code);
            CREATE INDEX IF NOT EXISTS idx_copy_audit_created_at 
                ON copy_cleanup_audit_v1(created_at DESC);
        """)
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to create audit table: {e}")
        try:
            conn.close()
        except:
            pass
        return False


def log_cleanup_audit(
    batch_id: str,
    module_code: str,
    shopify_handle: Optional[str],
    field_name: str,
    old_value: Optional[str],
    new_value: Optional[str],
):
    """Log a cleanup change to the audit table."""
    conn = get_db()
    if not conn:
        logger.error("Cannot log audit: database connection failed")
        return
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO copy_cleanup_audit_v1 
            (batch_id, module_code, shopify_handle, field_name, old_value, new_value)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            batch_id,
            module_code,
            shopify_handle,
            field_name,
            old_value[:10000] if old_value else None,  # Truncate if too long
            new_value[:10000] if new_value else None,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to log audit: {e}")
        try:
            conn.close()
        except:
            pass


def fetch_modules_for_cleanup(scope: str = CleanupScope.ACTIVE_ONLY) -> List[Dict[str, Any]]:
    """
    Fetch modules from os_modules_v3_1 for cleanup analysis.
    
    Returns all fields needed for rendering and analysis.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        query = """
            SELECT 
                module_code,
                shopify_handle,
                product_name,
                os_environment,
                os_layer,
                biological_domain,
                net_quantity,
                front_label_text,
                back_label_text,
                shopify_body,
                suggested_use_full,
                safety_notes,
                contraindications,
                ingredients_raw_text,
                fda_disclaimer,
                supplier_status
            FROM os_modules_v3_1
        """
        
        if scope == CleanupScope.ACTIVE_ONLY:
            query += " WHERE supplier_status = 'ACTIVE'"
        
        query += " ORDER BY os_environment, os_layer, module_code"
        
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def update_module_copy(
    module_code: str,
    front_label_text: Optional[str] = None,
    back_label_text: Optional[str] = None,
    shopify_body: Optional[str] = None,
) -> bool:
    """Update copy fields for a module."""
    conn = get_db()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        # Build dynamic update
        updates = []
        values = []
        
        if front_label_text is not None:
            updates.append("front_label_text = %s")
            values.append(front_label_text)
        
        if back_label_text is not None:
            updates.append("back_label_text = %s")
            values.append(back_label_text)
        
        if shopify_body is not None:
            updates.append("shopify_body = %s")
            values.append(shopify_body)
        
        if not updates:
            conn.close()
            return True
        
        values.append(module_code)
        
        cur.execute(f"""
            UPDATE os_modules_v3_1
            SET {', '.join(updates)}
            WHERE module_code = %s
        """, values)
        
        conn.commit()
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to update module {module_code}: {e}")
        try:
            conn.close()
        except:
            pass
        return False


# ===== Endpoints =====

@router.get("/api/v1/qa/copy/placeholders/list")
def list_placeholders():
    """
    List all modules with placeholder tokens.
    
    Returns:
    - total_placeholder_modules
    - per-token counts
    - per-field counts (front vs back)
    - list of module_code + shopify_handle + which field(s) + which token(s)
    """
    modules = fetch_modules_for_cleanup(CleanupScope.ACTIVE_ONLY)
    
    # Analyze each module
    modules_with_placeholders = []
    token_counts = {"TBD": 0, "MISSING": 0, "REVIEW": 0, "PLACEHOLDER": 0}
    field_counts = {
        "front_label_text": 0,
        "back_label_text": 0,
        "shopify_body": 0,
        "product_name": 0,
    }
    
    for module in modules:
        analysis = analyze_module_placeholders(module)
        
        if analysis["has_placeholders"]:
            modules_with_placeholders.append(analysis)
            
            # Count by token
            for field, data in analysis["fields_with_placeholders"].items():
                field_counts[field] = field_counts.get(field, 0) + 1
                
                for token in data["tokens"]:
                    token_upper = token.upper()
                    if token_upper in token_counts:
                        token_counts[token_upper] += 1
    
    return {
        "total_active_modules": len(modules),
        "total_placeholder_modules": len(modules_with_placeholders),
        "placeholder_free_modules": len(modules) - len(modules_with_placeholders),
        "token_counts": token_counts,
        "field_counts": field_counts,
        "modules": modules_with_placeholders,
    }


@router.get("/api/v1/qa/copy/clean/count")
def count_clean_modules():
    """
    Count modules that are placeholder-free.
    
    Returns counts for ACTIVE modules only.
    """
    modules = fetch_modules_for_cleanup(CleanupScope.ACTIVE_ONLY)
    
    clean_count = 0
    placeholder_count = 0
    missing_product_name = 0
    missing_body_content = 0
    
    for module in modules:
        analysis = analyze_module_placeholders(module)
        
        if analysis["has_placeholders"]:
            placeholder_count += 1
        else:
            clean_count += 1
        
        # Check for missing required fields
        if not module.get("product_name"):
            missing_product_name += 1
        
        if not module.get("shopify_body") and not module.get("back_label_text"):
            missing_body_content += 1
    
    return {
        "total_active": len(modules),
        "placeholder_free": clean_count,
        "with_placeholders": placeholder_count,
        "clean_percentage": round(clean_count / len(modules) * 100, 1) if modules else 0,
        "missing_product_name": missing_product_name,
        "missing_body_content": missing_body_content,
    }


@router.post("/api/v1/copy/cleanup/dry-run")
def cleanup_dry_run(request: CleanupRequest):
    """
    Preview cleanup without making changes.
    
    Shows which modules will be updated and sample before/after diffs.
    """
    modules = fetch_modules_for_cleanup(request.scope)
    
    modules_scanned = len(modules)
    modules_with_placeholders = 0
    modules_will_be_updated = 0
    modules_cannot_fix = []  # Missing product_name
    field_update_counts = {
        "front_label_text": 0,
        "back_label_text": 0,
        "shopify_body": 0,
    }
    sample_diffs = []
    
    for module in modules:
        module_code = module.get("module_code")
        analysis = analyze_module_placeholders(module)
        
        # Check if product_name is missing (cannot fix)
        if not module.get("product_name"):
            modules_cannot_fix.append({
                "module_code": module_code,
                "reason": "missing_product_name",
            })
            continue
        
        if not analysis["has_placeholders"]:
            # Check if shopify_body needs to be generated
            if not module.get("shopify_body") and module.get("back_label_text"):
                body_result = render_shopify_body(module)
                if body_result.success:
                    modules_will_be_updated += 1
                    field_update_counts["shopify_body"] += 1
                    
                    if len(sample_diffs) < 10:
                        sample_diffs.append({
                            "module_code": module_code,
                            "field": "shopify_body",
                            "action": "generate",
                            "before": None,
                            "after_preview": body_result.content[:500] + "..." if len(body_result.content) > 500 else body_result.content,
                        })
            continue
        
        modules_with_placeholders += 1
        
        # Render new content
        render_results = render_all(module)
        
        fields_to_update = []
        
        # Check front_label_text
        if contains_placeholder(module.get("front_label_text")):
            front_result = render_results["front_label_text"]
            if front_result.success:
                fields_to_update.append("front_label_text")
                field_update_counts["front_label_text"] += 1
                
                if len(sample_diffs) < 10:
                    sample_diffs.append({
                        "module_code": module_code,
                        "field": "front_label_text",
                        "action": "replace",
                        "before_preview": (module.get("front_label_text") or "")[:200],
                        "after_preview": front_result.content[:200],
                        "placeholders_removed": find_placeholders(module.get("front_label_text")),
                    })
        
        # Check back_label_text
        if contains_placeholder(module.get("back_label_text")):
            back_result = render_results["back_label_text"]
            if back_result.success:
                fields_to_update.append("back_label_text")
                field_update_counts["back_label_text"] += 1
                
                if len(sample_diffs) < 10:
                    sample_diffs.append({
                        "module_code": module_code,
                        "field": "back_label_text",
                        "action": "replace",
                        "before_preview": (module.get("back_label_text") or "")[:200],
                        "after_preview": back_result.content[:200],
                        "placeholders_removed": find_placeholders(module.get("back_label_text")),
                    })
        
        # Check shopify_body (generate if missing or has placeholders)
        existing_body = module.get("shopify_body")
        if not existing_body or contains_placeholder(existing_body):
            body_result = render_results["shopify_body"]
            if body_result.success:
                fields_to_update.append("shopify_body")
                field_update_counts["shopify_body"] += 1
                
                if len(sample_diffs) < 10:
                    sample_diffs.append({
                        "module_code": module_code,
                        "field": "shopify_body",
                        "action": "replace" if existing_body else "generate",
                        "before_preview": (existing_body or "")[:200] if existing_body else None,
                        "after_preview": body_result.content[:300],
                    })
        
        if fields_to_update:
            modules_will_be_updated += 1
    
    return {
        "scope": request.scope,
        "modules_scanned": modules_scanned,
        "modules_with_placeholders": modules_with_placeholders,
        "modules_will_be_updated": modules_will_be_updated,
        "modules_cannot_fix": len(modules_cannot_fix),
        "modules_cannot_fix_list": modules_cannot_fix,
        "field_update_counts": field_update_counts,
        "sample_diffs": sample_diffs,
    }


@router.post("/api/v1/copy/cleanup/execute")
def cleanup_execute(
    request: CleanupRequest,
    confirm: bool = Query(default=False, description="Must be true to execute cleanup"),
):
    """
    Execute copy cleanup with audit logging.
    
    Only updates fields that contain placeholders OR are missing but required.
    Never writes placeholder tokens.
    
    STOP CONDITIONS:
    - If any generated text still contains placeholders -> abort
    - If product_name missing -> skip and report
    """
    # If not confirmed, return dry-run
    if not confirm:
        return cleanup_dry_run(request)
    
    # Ensure audit table exists
    ensure_audit_table()
    
    # Generate batch ID
    batch_id = str(uuid.uuid4())
    
    modules = fetch_modules_for_cleanup(request.scope)
    
    results = {
        "batch_id": batch_id,
        "scope": request.scope,
        "modules_processed": 0,
        "modules_updated": 0,
        "modules_skipped": 0,
        "modules_failed": 0,
        "field_updates": {
            "front_label_text": 0,
            "back_label_text": 0,
            "shopify_body": 0,
        },
        "skipped_reasons": [],
        "failed_modules": [],
        "abort_triggered": False,
    }
    
    for module in modules:
        module_code = module.get("module_code")
        shopify_handle = module.get("shopify_handle")
        results["modules_processed"] += 1
        
        # Skip if product_name missing
        if not module.get("product_name"):
            results["modules_skipped"] += 1
            results["skipped_reasons"].append({
                "module_code": module_code,
                "reason": "missing_product_name",
            })
            continue
        
        analysis = analyze_module_placeholders(module)
        
        # Determine what needs updating
        needs_front = contains_placeholder(module.get("front_label_text"))
        needs_back = contains_placeholder(module.get("back_label_text"))
        needs_body = (
            not module.get("shopify_body") or 
            contains_placeholder(module.get("shopify_body"))
        )
        
        if not (needs_front or needs_back or needs_body):
            # Nothing to update
            continue
        
        # Render new content
        render_results = render_all(module)
        
        # Prepare updates
        new_front = None
        new_back = None
        new_body = None
        
        if needs_front:
            front_result = render_results["front_label_text"]
            if front_result.success and not front_result.has_placeholders:
                new_front = front_result.content
            elif front_result.has_placeholders:
                # ABORT: Generated content still has placeholders
                results["abort_triggered"] = True
                results["failed_modules"].append({
                    "module_code": module_code,
                    "field": "front_label_text",
                    "reason": "generated_content_has_placeholders",
                    "placeholders": find_placeholders(front_result.content),
                })
                continue
        
        if needs_back:
            back_result = render_results["back_label_text"]
            if back_result.success and not back_result.has_placeholders:
                new_back = back_result.content
            elif back_result.has_placeholders:
                results["abort_triggered"] = True
                results["failed_modules"].append({
                    "module_code": module_code,
                    "field": "back_label_text",
                    "reason": "generated_content_has_placeholders",
                    "placeholders": find_placeholders(back_result.content),
                })
                continue
        
        if needs_body:
            body_result = render_results["shopify_body"]
            if body_result.success and not body_result.has_placeholders:
                new_body = body_result.content
            elif body_result.has_placeholders:
                results["abort_triggered"] = True
                results["failed_modules"].append({
                    "module_code": module_code,
                    "field": "shopify_body",
                    "reason": "generated_content_has_placeholders",
                    "placeholders": find_placeholders(body_result.content),
                })
                continue
        
        # Execute update
        success = update_module_copy(
            module_code=module_code,
            front_label_text=new_front,
            back_label_text=new_back,
            shopify_body=new_body,
        )
        
        if success:
            results["modules_updated"] += 1
            
            # Log audit for each field
            if new_front is not None:
                log_cleanup_audit(
                    batch_id=batch_id,
                    module_code=module_code,
                    shopify_handle=shopify_handle,
                    field_name="front_label_text",
                    old_value=module.get("front_label_text"),
                    new_value=new_front,
                )
                results["field_updates"]["front_label_text"] += 1
            
            if new_back is not None:
                log_cleanup_audit(
                    batch_id=batch_id,
                    module_code=module_code,
                    shopify_handle=shopify_handle,
                    field_name="back_label_text",
                    old_value=module.get("back_label_text"),
                    new_value=new_back,
                )
                results["field_updates"]["back_label_text"] += 1
            
            if new_body is not None:
                log_cleanup_audit(
                    batch_id=batch_id,
                    module_code=module_code,
                    shopify_handle=shopify_handle,
                    field_name="shopify_body",
                    old_value=module.get("shopify_body"),
                    new_value=new_body,
                )
                results["field_updates"]["shopify_body"] += 1
        else:
            results["modules_failed"] += 1
            results["failed_modules"].append({
                "module_code": module_code,
                "reason": "database_update_failed",
            })
    
    return results


@router.get("/api/v1/copy/audit/logs/{batch_id}")
def get_cleanup_audit_logs(
    batch_id: str,
    limit: int = Query(default=100, ge=1, le=500),
):
    """
    Retrieve audit logs for a specific cleanup batch.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                audit_id,
                batch_id,
                module_code,
                shopify_handle,
                field_name,
                LENGTH(old_value) as old_value_length,
                LENGTH(new_value) as new_value_length,
                created_at
            FROM copy_cleanup_audit_v1
            WHERE batch_id = %s
            ORDER BY created_at
            LIMIT %s
        """, (batch_id, limit))
        
        logs = [dict(row) for row in cur.fetchall()]
        
        # Get summary counts
        cur.execute("""
            SELECT field_name, COUNT(*) as count
            FROM copy_cleanup_audit_v1
            WHERE batch_id = %s
            GROUP BY field_name
        """, (batch_id,))
        
        summary = {row["field_name"]: row["count"] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return {
            "batch_id": batch_id,
            "total_changes": len(logs),
            "field_summary": summary,
            "logs": logs,
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
