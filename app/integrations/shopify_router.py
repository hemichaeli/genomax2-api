"""
Shopify Integration Router for GenoMAX²
=======================================
Provides endpoints for syncing os_modules to Shopify products.

LAUNCH v1 ENFORCEMENT (v3.27.0):
- ALL export endpoints now filter by is_launch_v1 = TRUE
- Hard guardrail: supplier_status = 'ACTIVE' AND is_launch_v1 = TRUE
- No fuzzy matching, no heuristic inclusion

Endpoints:
- GET  /api/v1/shopify/health - Verify Shopify API connectivity
- GET  /api/v1/shopify/readiness/summary - Check which LAUNCH v1 modules are ready
- POST /api/v1/shopify/export/dry-run - Preview export (Launch v1 only)
- POST /api/v1/shopify/export/publish - Execute export to Shopify (Launch v1 only)

Environment Variables Required:
- SHOPIFY_ADMIN_BASE_URL
- SHOPIFY_ACCESS_TOKEN
"""

import os
import re
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor

from app.integrations.shopify_client import (
    get_shopify_client,
    ShopifyClient,
    ShopifyError,
    ShopifyAuthError,
    ShopifyValidationError,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/shopify", tags=["shopify"])

DATABASE_URL = os.getenv("DATABASE_URL")

# ===== Constants =====

FIELD_MAP_VERSION = "v2"  # Updated for Launch v1 enforcement
METAFIELD_NAMESPACE = "genomax"

# Placeholder patterns to block
PLACEHOLDER_PATTERNS = re.compile(
    r"\b(TBD|MISSING|REVIEW|PLACEHOLDER)\b",
    re.IGNORECASE
)

# HARD GUARDRAIL: Launch v1 scope filter
# This filter MUST be applied to ALL external pipelines
LAUNCH_V1_SCOPE_FILTER = """
    is_launch_v1 = TRUE 
    AND supplier_status = 'ACTIVE'
"""


# ===== Models =====

class ExportRequest(BaseModel):
    """Request body for export endpoints."""
    limit: int = Field(default=50, ge=1, le=250, description="Max modules to process")
    only_ready: bool = Field(default=True, description="Only export READY_FOR_SHOPIFY modules")
    # Note: is_launch_v1 filter is ALWAYS applied, not optional


class BlockedReason(str, Enum):
    """Reasons a module might be blocked from export."""
    NOT_LAUNCH_V1 = "not_in_launch_v1"  # NEW: Launch v1 guardrail
    NOT_ACTIVE = "supplier_status_not_active"
    MISSING_PRODUCT_NAME = "missing_product_name"
    MISSING_HANDLE = "missing_shopify_handle"
    MISSING_NET_QUANTITY = "missing_net_quantity"
    MISSING_FDA_DISCLAIMER = "missing_fda_disclaimer"
    MISSING_BODY = "missing_body_content"
    PLACEHOLDER_IN_LABELS = "placeholder_in_labels"


class PublishAction(str, Enum):
    """Possible actions during publish."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    SKIP = "SKIP"
    ERROR = "ERROR"


# ===== Database Helpers =====

def get_db():
    """Get database connection."""
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


def fetch_modules_for_export(limit: int = 50, only_active: bool = True) -> List[Dict[str, Any]]:
    """
    Fetch modules from os_modules_v3_1 for export consideration.
    
    HARD GUARDRAIL: Always filters by is_launch_v1 = TRUE AND supplier_status = 'ACTIVE'
    
    Returns all required fields for Shopify export.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Build query with LAUNCH v1 HARD GUARDRAIL
        # Note: is_launch_v1 filter is ALWAYS applied
        query = f"""
            SELECT 
                module_code,
                product_name,
                shopify_handle,
                os_environment,
                os_layer,
                tier,
                biological_domain,
                net_quantity,
                fda_disclaimer,
                shopify_body,
                front_label_text,
                back_label_text,
                suggested_use_full,
                safety_notes,
                contraindications,
                dosing_protocol,
                supplier_page_url,
                supliful_handle,
                supplier_status,
                is_launch_v1
            FROM os_modules_v3_1
            WHERE {LAUNCH_V1_SCOPE_FILTER}
            ORDER BY tier, os_environment, os_layer, module_code 
            LIMIT %s
        """
        
        cur.execute(query, (limit,))
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


# ===== Readiness Gate =====

def check_module_readiness(module: Dict[str, Any]) -> Tuple[bool, List[BlockedReason]]:
    """
    Check if a module passes the READY_FOR_SHOPIFY gate.
    
    LAUNCH v1 ENFORCEMENT: is_launch_v1 = TRUE is a hard requirement.
    
    Returns:
        Tuple of (is_ready: bool, blocked_reasons: List[BlockedReason])
    """
    reasons = []
    
    # NEW: Required: is_launch_v1 = TRUE (HARD GUARDRAIL)
    if not module.get("is_launch_v1"):
        reasons.append(BlockedReason.NOT_LAUNCH_V1)
    
    # Required: supplier_status = 'ACTIVE'
    if module.get("supplier_status") != "ACTIVE":
        reasons.append(BlockedReason.NOT_ACTIVE)
    
    # Required: product_name not null/empty
    if not module.get("product_name"):
        reasons.append(BlockedReason.MISSING_PRODUCT_NAME)
    
    # Required: shopify_handle not null/empty
    if not module.get("shopify_handle"):
        reasons.append(BlockedReason.MISSING_HANDLE)
    
    # Required: net_quantity not null/empty
    if not module.get("net_quantity"):
        reasons.append(BlockedReason.MISSING_NET_QUANTITY)
    
    # Required: fda_disclaimer not null/empty
    if not module.get("fda_disclaimer"):
        reasons.append(BlockedReason.MISSING_FDA_DISCLAIMER)
    
    # Required: shopify_body OR back_label_text not null/empty
    if not module.get("shopify_body") and not module.get("back_label_text"):
        reasons.append(BlockedReason.MISSING_BODY)
    
    # Required: No placeholders in front_label_text or back_label_text
    front_label = module.get("front_label_text") or ""
    back_label = module.get("back_label_text") or ""
    
    if PLACEHOLDER_PATTERNS.search(front_label) or PLACEHOLDER_PATTERNS.search(back_label):
        reasons.append(BlockedReason.PLACEHOLDER_IN_LABELS)
    
    return (len(reasons) == 0, reasons)


# ===== Payload Builder =====

def build_shopify_product_payload(module: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build Shopify product payload from GenoMAX² module.
    
    Maps fields according to spec:
    - title: product_name
    - handle: shopify_handle
    - body_html: shopify_body (fallback: back_label_text)
    - status: active
    - tags: os_environment, os_layer, biological_domain, tier
    """
    # Build body HTML
    body_html = module.get("shopify_body")
    if not body_html and module.get("back_label_text"):
        # Format back_label_text as basic HTML
        back_text = module["back_label_text"]
        body_html = f"<p>{back_text.replace(chr(10), '</p><p>')}</p>"
    
    # Build tags (now includes tier)
    tags = []
    if module.get("os_environment"):
        tags.append(module["os_environment"])
    if module.get("os_layer"):
        tags.append(module["os_layer"])
    if module.get("biological_domain"):
        tags.append(module["biological_domain"])
    if module.get("tier"):
        tags.append(module["tier"])
    
    return {
        "title": module.get("product_name"),
        "handle": module.get("shopify_handle"),
        "body_html": body_html,
        "status": "active",
        "tags": ", ".join(tags) if tags else None,
        "vendor": "GenoMAX²",
    }


def build_metafields_payload(module: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build metafields payload from GenoMAX² module.
    
    All metafields use namespace: genomax
    """
    metafields = []
    
    # Map of field -> metafield key
    field_map = {
        "os_environment": "os_environment",
        "os_layer": "os_layer",
        "tier": "tier",  # NEW: Include tier
        "biological_domain": "biological_domain",
        "net_quantity": "net_quantity",
        "fda_disclaimer": "fda_disclaimer_block",
        "suggested_use_full": "suggested_use",
        "safety_notes": "safety_notes",
        "contraindications": "contraindications",
        "dosing_protocol": "dosing_protocol",
        "supplier_page_url": "supplier_page_url",
        "supliful_handle": "supliful_handle",
        "supplier_status": "supplier_status",
    }
    
    for db_field, mf_key in field_map.items():
        value = module.get(db_field)
        if value is not None and value != "":
            metafields.append({
                "namespace": METAFIELD_NAMESPACE,
                "key": mf_key,
                "value": str(value),
                "type": "single_line_text_field" if len(str(value)) <= 255 else "multi_line_text_field",
            })
    
    return metafields


# ===== Audit Table Creation =====

def ensure_audit_table():
    """Create shopify_publish_audit_v1 table if it doesn't exist."""
    conn = get_db()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shopify_publish_audit_v1 (
                audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                batch_id UUID NOT NULL,
                module_code TEXT NOT NULL,
                shopify_handle TEXT NOT NULL,
                action TEXT NOT NULL,
                shopify_product_id TEXT,
                request_payload JSONB,
                response_status INT,
                response_body JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_shopify_audit_batch_id 
                ON shopify_publish_audit_v1(batch_id);
            CREATE INDEX IF NOT EXISTS idx_shopify_audit_module_code 
                ON shopify_publish_audit_v1(module_code);
            CREATE INDEX IF NOT EXISTS idx_shopify_audit_created_at 
                ON shopify_publish_audit_v1(created_at DESC);
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


def log_publish_audit(
    batch_id: str,
    module_code: str,
    shopify_handle: str,
    action: PublishAction,
    shopify_product_id: Optional[str] = None,
    request_payload: Optional[Dict] = None,
    response_status: Optional[int] = None,
    response_body: Optional[Dict] = None,
):
    """Log a publish attempt to the audit table."""
    conn = get_db()
    if not conn:
        logger.error("Cannot log audit: database connection failed")
        return
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shopify_publish_audit_v1 
            (batch_id, module_code, shopify_handle, action, shopify_product_id, 
             request_payload, response_status, response_body)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            batch_id,
            module_code,
            shopify_handle,
            action.value,
            shopify_product_id,
            json.dumps(request_payload) if request_payload else None,
            response_status,
            json.dumps(response_body) if response_body else None,
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


# ===== Endpoints =====

@router.get("/health")
def shopify_health():
    """
    Verify Shopify API connectivity and authentication.
    
    Returns:
        Health status with shop info and rate limit
    """
    client = get_shopify_client()
    return client.health_check()


@router.get("/readiness/summary")
def readiness_summary():
    """
    Check which LAUNCH v1 modules are ready for Shopify export.
    
    LAUNCH v1 ENFORCEMENT: Only checks modules where is_launch_v1 = TRUE
    
    Returns summary of Launch v1 modules, ready modules, blocked modules,
    and breakdown of blocking reasons.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get all LAUNCH v1 ACTIVE modules with relevant fields
        # HARD GUARDRAIL: is_launch_v1 = TRUE filter applied
        cur.execute(f"""
            SELECT 
                module_code,
                product_name,
                shopify_handle,
                net_quantity,
                fda_disclaimer,
                shopify_body,
                front_label_text,
                back_label_text,
                supplier_status,
                is_launch_v1,
                tier
            FROM os_modules_v3_1
            WHERE {LAUNCH_V1_SCOPE_FILTER}
            ORDER BY tier, module_code
        """)
        
        launch_v1_modules = [dict(row) for row in cur.fetchall()]
        
        # Count total Launch v1
        cur.execute(f"""
            SELECT COUNT(*) as total FROM os_modules_v3_1 
            WHERE {LAUNCH_V1_SCOPE_FILTER}
        """)
        launch_v1_count = cur.fetchone()["total"]
        
        # Count by tier
        cur.execute(f"""
            SELECT tier, COUNT(*) as count 
            FROM os_modules_v3_1 
            WHERE {LAUNCH_V1_SCOPE_FILTER}
            GROUP BY tier
            ORDER BY tier
        """)
        tier_distribution = {row["tier"]: row["count"] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        # Check readiness for each
        ready_modules = []
        blocked_modules = []
        blocked_breakdown = {}
        
        for module in launch_v1_modules:
            is_ready, reasons = check_module_readiness(module)
            
            if is_ready:
                ready_modules.append(module["module_code"])
            else:
                blocked_modules.append({
                    "module_code": module["module_code"],
                    "tier": module.get("tier"),
                    "reasons": [r.value for r in reasons]
                })
                
                # Count reasons
                for reason in reasons:
                    blocked_breakdown[reason.value] = blocked_breakdown.get(reason.value, 0) + 1
        
        # Get top blocked examples (first 10)
        top_blocked = blocked_modules[:10]
        
        return {
            "scope": {
                "is_launch_v1": True,
                "supplier_status": "ACTIVE",
            },
            "launch_v1_modules": launch_v1_count,
            "tier_distribution": tier_distribution,
            "ready_for_shopify": len(ready_modules),
            "blocked": len(blocked_modules),
            "blocked_breakdown": blocked_breakdown,
            "top_blocked_examples": top_blocked,
            "field_map_version": FIELD_MAP_VERSION,
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/export/dry-run")
def export_dry_run(request: ExportRequest):
    """
    Preview Shopify export without making changes.
    
    LAUNCH v1 ENFORCEMENT: Only exports modules where is_launch_v1 = TRUE
    
    Builds payloads for candidate modules and returns summary
    without actually calling Shopify API.
    """
    # Fetch modules (LAUNCH v1 filter always applied)
    modules = fetch_modules_for_export(
        limit=request.limit,
        only_active=request.only_ready
    )
    
    # Analyze each module
    ready_modules = []
    blocked_modules = []
    blocked_reasons = {}
    sample_payloads = []
    
    for module in modules:
        is_ready, reasons = check_module_readiness(module)
        
        if is_ready:
            ready_modules.append(module)
            
            # Build payload sample (first 5)
            if len(sample_payloads) < 5:
                payload = build_shopify_product_payload(module)
                metafields = build_metafields_payload(module)
                sample_payloads.append({
                    "module_code": module["module_code"],
                    "tier": module.get("tier"),
                    "product": payload,
                    "metafields_count": len(metafields),
                })
        else:
            blocked_modules.append({
                "module_code": module["module_code"],
                "tier": module.get("tier"),
                "reasons": [r.value for r in reasons]
            })
            
            for reason in reasons:
                blocked_reasons[reason.value] = blocked_reasons.get(reason.value, 0) + 1
    
    return {
        "scope": {
            "is_launch_v1": True,
            "supplier_status": "ACTIVE",
        },
        "candidate_count": len(modules),
        "ready_count": len(ready_modules),
        "blocked_count": len(blocked_modules),
        "blocked_reasons": blocked_reasons,
        "sample_payloads": sample_payloads,
        "field_map_version": FIELD_MAP_VERSION,
    }


@router.post("/export/publish")
def export_publish(
    request: ExportRequest,
    confirm: bool = Query(default=False, description="Must be true to execute publish"),
):
    """
    Execute Shopify product export.
    
    LAUNCH v1 ENFORCEMENT: Only exports modules where is_launch_v1 = TRUE
    
    If confirm=false, behaves like dry-run.
    If confirm=true, creates/updates products in Shopify.
    
    Idempotent: Uses upsert by handle.
    """
    # If not confirmed, return dry-run
    if not confirm:
        return export_dry_run(request)
    
    # Ensure audit table exists
    ensure_audit_table()
    
    # Generate batch ID
    batch_id = str(uuid.uuid4())
    
    # Get Shopify client
    client = get_shopify_client()
    
    # Check connectivity first
    health = client.health_check()
    if not health.get("ok"):
        raise HTTPException(
            status_code=503,
            detail={
                "error": "SHOPIFY_UNAVAILABLE",
                "message": health.get("error", "Cannot connect to Shopify"),
            }
        )
    
    # Fetch modules (LAUNCH v1 filter always applied)
    modules = fetch_modules_for_export(
        limit=request.limit,
        only_active=request.only_ready
    )
    
    # Process each module
    results = {
        "batch_id": batch_id,
        "created": [],
        "updated": [],
        "skipped": [],
        "errors": [],
    }
    
    for module in modules:
        module_code = module["module_code"]
        shopify_handle = module.get("shopify_handle", "")
        
        # Check readiness
        is_ready, reasons = check_module_readiness(module)
        
        if not is_ready:
            results["skipped"].append({
                "module_code": module_code,
                "tier": module.get("tier"),
                "reasons": [r.value for r in reasons]
            })
            log_publish_audit(
                batch_id=batch_id,
                module_code=module_code,
                shopify_handle=shopify_handle,
                action=PublishAction.SKIP,
            )
            continue
        
        # Build payload
        product_payload = build_shopify_product_payload(module)
        metafields = build_metafields_payload(module)
        
        try:
            # Upsert product
            product, action = client.upsert_product_by_handle(
                handle=shopify_handle,
                product_data=product_payload
            )
            
            product_id = str(product.get("id", ""))
            
            # Set metafields
            if product_id and metafields:
                try:
                    client.set_product_metafields_bulk(int(product_id), metafields)
                except Exception as mf_error:
                    logger.warning(f"Metafield error for {module_code}: {mf_error}")
            
            # Record result
            result_entry = {
                "module_code": module_code,
                "tier": module.get("tier"),
                "shopify_product_id": product_id,
                "shopify_handle": shopify_handle,
            }
            
            if action == "created":
                results["created"].append(result_entry)
                log_publish_audit(
                    batch_id=batch_id,
                    module_code=module_code,
                    shopify_handle=shopify_handle,
                    action=PublishAction.CREATE,
                    shopify_product_id=product_id,
                    request_payload=product_payload,
                    response_status=201,
                )
            else:
                results["updated"].append(result_entry)
                log_publish_audit(
                    batch_id=batch_id,
                    module_code=module_code,
                    shopify_handle=shopify_handle,
                    action=PublishAction.UPDATE,
                    shopify_product_id=product_id,
                    request_payload=product_payload,
                    response_status=200,
                )
                
        except ShopifyValidationError as e:
            results["errors"].append({
                "module_code": module_code,
                "error": str(e),
                "status_code": e.status_code,
            })
            log_publish_audit(
                batch_id=batch_id,
                module_code=module_code,
                shopify_handle=shopify_handle,
                action=PublishAction.ERROR,
                request_payload=product_payload,
                response_status=e.status_code,
                response_body=e.response_body,
            )
            
        except ShopifyError as e:
            results["errors"].append({
                "module_code": module_code,
                "error": str(e),
                "status_code": e.status_code,
            })
            log_publish_audit(
                batch_id=batch_id,
                module_code=module_code,
                shopify_handle=shopify_handle,
                action=PublishAction.ERROR,
                request_payload=product_payload,
                response_status=e.status_code,
                response_body=e.response_body,
            )
            
        except Exception as e:
            results["errors"].append({
                "module_code": module_code,
                "error": str(e),
            })
            log_publish_audit(
                batch_id=batch_id,
                module_code=module_code,
                shopify_handle=shopify_handle,
                action=PublishAction.ERROR,
                response_body={"exception": str(e)},
            )
    
    # Build summary
    return {
        "batch_id": batch_id,
        "scope": {
            "is_launch_v1": True,
            "supplier_status": "ACTIVE",
        },
        "summary": {
            "created": len(results["created"]),
            "updated": len(results["updated"]),
            "skipped": len(results["skipped"]),
            "errors": len(results["errors"]),
            "total_processed": len(modules),
        },
        "created": results["created"],
        "updated": results["updated"],
        "skipped": results["skipped"][:10],  # Limit skipped to 10
        "errors": results["errors"],
        "field_map_version": FIELD_MAP_VERSION,
    }


@router.get("/audit/logs/{batch_id}")
def get_audit_logs(batch_id: str, limit: int = Query(default=100, ge=1, le=500)):
    """
    Retrieve audit logs for a specific publish batch.
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
                action,
                shopify_product_id,
                response_status,
                created_at
            FROM shopify_publish_audit_v1
            WHERE batch_id = %s
            ORDER BY created_at
            LIMIT %s
        """, (batch_id, limit))
        
        logs = [dict(row) for row in cur.fetchall()]
        
        # Get summary counts
        cur.execute("""
            SELECT action, COUNT(*) as count
            FROM shopify_publish_audit_v1
            WHERE batch_id = %s
            GROUP BY action
        """, (batch_id,))
        
        summary = {row["action"]: row["count"] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return {
            "batch_id": batch_id,
            "total_logs": len(logs),
            "summary": summary,
            "logs": logs,
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
