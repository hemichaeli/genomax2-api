# =============================================================================
# GenoMAX² Supplier Catalog Admin Endpoints v2
# Handle Normalization with Audit Trail
# =============================================================================

import os
import re
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, Body
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/admin/supplier-catalog", tags=["Admin - Supplier Catalog"])

DATABASE_URL = os.getenv("DATABASE_URL")

# =============================================================================
# LOCKED SUFFIX LIST v1 - DO NOT MODIFY WITHOUT VERSIONING
# =============================================================================
NORMALIZATION_VERSION = "handle_normalization_v1"
LOCKED_SUFFIX_LIST = [
    "",              # EXACT match (no suffix)
    "-capsules",
    "-powder", 
    "-sachets",
    "-drops",
    "-gummies",
    "-strips",
    "-softgel-capsules",
    "-tablets",
    "-liquid",
    "-chewables",
    "-cream",
    "-serum",
    "-oil",
]


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def derive_base_handle(shopify_handle: str) -> str:
    """Strip -maximo/-maxima suffix from shopify_handle."""
    return re.sub(r'(-maximo|-maxima)$', '', shopify_handle)


def find_all_matches(base_handle: str, snapshot_lookup: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Find ALL matching candidates in snapshot using locked suffix list.
    Returns list of all matches (could be 0, 1, or multiple).
    """
    matches = []
    for suffix in LOCKED_SUFFIX_LIST:
        candidate = base_handle + suffix
        if candidate in snapshot_lookup:
            rule = "EXACT" if suffix == "" else f"APPEND_{suffix}"
            matches.append({
                "candidate_handle": candidate,
                "rule_used": rule,
                "suffix": suffix,
                "supplier_url": snapshot_lookup[candidate]
            })
    return matches


# =============================================================================
# Existing endpoints (kept for compatibility)
# =============================================================================

@router.post("/snapshot/upsert")
def upsert_snapshot_entry(
    supliful_handle: str = Query(..., description="Supliful handle"),
    supplier_url: str = Query(..., description="Full supplier URL")
) -> Dict[str, Any]:
    """Upsert a single entry into supplier_catalog_snapshot_v1."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO supplier_catalog_snapshot_v1 (supliful_handle, supplier_url, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (supliful_handle)
            DO UPDATE SET supplier_url = EXCLUDED.supplier_url, updated_at = NOW()
        """, (supliful_handle, supplier_url))
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "supliful_handle": supliful_handle}
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/snapshot/bulk-upsert")
def bulk_upsert_snapshot(
    data: List[Dict[str, str]] = Body(..., description="List of {supliful_handle, supplier_url}")
) -> Dict[str, Any]:
    """Bulk upsert entries into supplier_catalog_snapshot_v1."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    inserted = 0
    updated = 0
    errors = []
    
    try:
        cur = conn.cursor()
        for item in data:
            try:
                handle = item.get('supliful_handle', '').strip()
                url = item.get('supplier_url', '').strip()
                if not handle or not url:
                    errors.append({"item": item, "reason": "missing_handle_or_url"})
                    continue
                
                cur.execute("SELECT 1 FROM supplier_catalog_snapshot_v1 WHERE supliful_handle = %s", (handle,))
                exists = cur.fetchone() is not None
                
                cur.execute("""
                    INSERT INTO supplier_catalog_snapshot_v1 (supliful_handle, supplier_url, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (supliful_handle)
                    DO UPDATE SET supplier_url = EXCLUDED.supplier_url, updated_at = NOW()
                """, (handle, url))
                
                if exists:
                    updated += 1
                else:
                    inserted += 1
            except Exception as e:
                errors.append({"item": item, "reason": str(e)})
        
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "inserted": inserted, "updated": updated, "errors": len(errors)}
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/snapshot/stats")
def get_snapshot_stats() -> Dict[str, Any]:
    """Get stats on supplier_catalog_snapshot_v1 table."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'supplier_catalog_snapshot_v1'")
        if not cur.fetchone():
            return {"table_exists": False, "row_count": 0}
        
        cur.execute("SELECT COUNT(*) as count FROM supplier_catalog_snapshot_v1")
        row_count = cur.fetchone()['count']
        
        cur.execute("SELECT supliful_handle, supplier_url FROM supplier_catalog_snapshot_v1 ORDER BY supliful_handle LIMIT 10")
        samples = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        return {"table_exists": True, "row_count": row_count, "samples": samples}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# NORMALIZATION v1 - PREVIEW (DRY RUN)
# =============================================================================

@router.get("/normalize/preview")
def normalize_preview() -> Dict[str, Any]:
    """
    PREVIEW ONLY - No DB writes.
    Analyzes all modules missing supplier sources and computes candidate mappings.
    
    Returns detailed report with:
    - MATCHED: exactly one suffix produces a valid match
    - AMBIGUOUS: multiple suffixes match (DO NOT AUTO-UPDATE)
    - NO_MATCH: no suffix produces a match under current locked rules
    - ALREADY_MAPPED: module already has supplier data (skip)
    - NO_SUFFIX: shopify_handle doesn't end with -maximo/-maxima
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Load snapshot into memory for fast lookups
        cur.execute("SELECT supliful_handle, supplier_url FROM supplier_catalog_snapshot_v1")
        snapshot_lookup = {r['supliful_handle']: r['supplier_url'] for r in cur.fetchall()}
        
        # Get ALL modules
        cur.execute("""
            SELECT module_code, shopify_handle, os_environment, product_name,
                   url, supplier_page_url, supliful_handle
            FROM os_modules_v3_1
            ORDER BY shopify_handle
        """)
        all_modules = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        # Analyze each module
        results = {
            "MATCHED": [],
            "AMBIGUOUS": [],
            "NO_MATCH": [],
            "ALREADY_MAPPED": [],
            "NO_SUFFIX": []
        }
        
        for m in all_modules:
            handle = m['shopify_handle'] or ''
            
            # Check if already has supplier data
            has_url = bool(m['url'] and m['url'].strip())
            has_supplier_page = bool(m['supplier_page_url'] and m['supplier_page_url'].strip())
            has_supliful = bool(m['supliful_handle'] and m['supliful_handle'].strip())
            
            if has_url or has_supplier_page or has_supliful:
                results["ALREADY_MAPPED"].append({
                    "module_code": m['module_code'],
                    "shopify_handle": handle,
                    "os_environment": m['os_environment'],
                    "existing_url": m['url'],
                    "existing_supplier_page_url": m['supplier_page_url'],
                    "existing_supliful_handle": m['supliful_handle']
                })
                continue
            
            # Check for -maximo/-maxima suffix
            if not re.search(r'(-maximo|-maxima)$', handle):
                results["NO_SUFFIX"].append({
                    "module_code": m['module_code'],
                    "shopify_handle": handle,
                    "os_environment": m['os_environment'],
                    "reason": "shopify_handle does not end with -maximo or -maxima"
                })
                continue
            
            # Derive base handle and find all matches
            base_handle = derive_base_handle(handle)
            all_matches = find_all_matches(base_handle, snapshot_lookup)
            
            entry = {
                "module_code": m['module_code'],
                "shopify_handle": handle,
                "os_environment": m['os_environment'],
                "product_name": m['product_name'],
                "base_handle": base_handle,
                "all_matches": all_matches,
                "match_count": len(all_matches)
            }
            
            if len(all_matches) == 0:
                entry["status"] = "NO_MATCH"
                entry["reason"] = f"No suffix from locked list produces a match in snapshot"
                results["NO_MATCH"].append(entry)
            elif len(all_matches) == 1:
                entry["status"] = "MATCHED"
                entry["matched_handle"] = all_matches[0]["candidate_handle"]
                entry["rule_used"] = all_matches[0]["rule_used"]
                entry["supplier_url"] = all_matches[0]["supplier_url"]
                results["MATCHED"].append(entry)
            else:
                entry["status"] = "AMBIGUOUS"
                entry["reason"] = f"Multiple suffixes match: {[m['candidate_handle'] for m in all_matches]}"
                results["AMBIGUOUS"].append(entry)
        
        # Summary
        summary = {
            "normalization_version": NORMALIZATION_VERSION,
            "locked_suffix_list": LOCKED_SUFFIX_LIST,
            "snapshot_size": len(snapshot_lookup),
            "total_modules": len(all_modules),
            "MATCHED": len(results["MATCHED"]),
            "AMBIGUOUS": len(results["AMBIGUOUS"]),
            "NO_MATCH": len(results["NO_MATCH"]),
            "ALREADY_MAPPED": len(results["ALREADY_MAPPED"]),
            "NO_SUFFIX": len(results["NO_SUFFIX"]),
            "safe_to_update": len(results["MATCHED"]),
            "will_skip": len(results["AMBIGUOUS"]) + len(results["NO_MATCH"]) + len(results["ALREADY_MAPPED"]) + len(results["NO_SUFFIX"])
        }
        
        # Rule usage breakdown
        rule_usage = {}
        for m in results["MATCHED"]:
            rule = m["rule_used"]
            rule_usage[rule] = rule_usage.get(rule, 0) + 1
        
        return {
            "summary": summary,
            "rule_usage_breakdown": rule_usage,
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview error: {str(e)}")


# =============================================================================
# NORMALIZATION v1 - APPLY (DB WRITE WITH AUDIT)
# =============================================================================

@router.post("/normalize/apply")
def normalize_apply() -> Dict[str, Any]:
    """
    APPLY mappings for MATCHED modules only.
    
    Guardrails:
    - Only updates modules where supliful_handle, supplier_page_url, AND url are ALL empty
    - Skips AMBIGUOUS (multiple matches)
    - Skips NO_MATCH
    - Creates audit trail in handle_mapping_audit_v1 table
    
    Returns detailed results including batch_id for rollback reference.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Generate batch ID
        batch_id = str(uuid.uuid4())[:8]
        batch_timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Create audit table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS handle_mapping_audit_v1 (
                id SERIAL PRIMARY KEY,
                batch_id VARCHAR(20) NOT NULL,
                module_code VARCHAR(50) NOT NULL,
                old_supliful_handle TEXT,
                new_supliful_handle TEXT NOT NULL,
                old_supplier_page_url TEXT,
                new_supplier_page_url TEXT NOT NULL,
                rule_used VARCHAR(50) NOT NULL,
                normalization_version VARCHAR(50) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        
        # Load snapshot
        cur.execute("SELECT supliful_handle, supplier_url FROM supplier_catalog_snapshot_v1")
        snapshot_lookup = {r['supliful_handle']: r['supplier_url'] for r in cur.fetchall()}
        
        # Get modules that are candidates for update
        cur.execute("""
            SELECT module_code, shopify_handle, os_environment,
                   url, supplier_page_url, supliful_handle
            FROM os_modules_v3_1
            WHERE (url IS NULL OR btrim(url) = '')
              AND (supplier_page_url IS NULL OR btrim(supplier_page_url) = '')
              AND (supliful_handle IS NULL OR btrim(supliful_handle) = '')
              AND (shopify_handle LIKE '%-maximo' OR shopify_handle LIKE '%-maxima')
            ORDER BY shopify_handle
        """)
        candidates = [dict(r) for r in cur.fetchall()]
        
        updated = []
        skipped_ambiguous = []
        skipped_no_match = []
        errors = []
        
        for m in candidates:
            handle = m['shopify_handle']
            base_handle = derive_base_handle(handle)
            all_matches = find_all_matches(base_handle, snapshot_lookup)
            
            if len(all_matches) == 0:
                skipped_no_match.append({
                    "module_code": m['module_code'],
                    "base_handle": base_handle,
                    "reason": "NO_MATCH"
                })
                continue
            
            if len(all_matches) > 1:
                skipped_ambiguous.append({
                    "module_code": m['module_code'],
                    "base_handle": base_handle,
                    "matches": [x["candidate_handle"] for x in all_matches],
                    "reason": "AMBIGUOUS"
                })
                continue
            
            # Exactly one match - safe to update
            match = all_matches[0]
            new_handle = match["candidate_handle"]
            new_url = match["supplier_url"]
            rule_used = match["rule_used"]
            
            try:
                # Insert audit record FIRST
                cur.execute("""
                    INSERT INTO handle_mapping_audit_v1 
                    (batch_id, module_code, old_supliful_handle, new_supliful_handle,
                     old_supplier_page_url, new_supplier_page_url, rule_used, normalization_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    batch_id,
                    m['module_code'],
                    m['supliful_handle'],
                    new_handle,
                    m['supplier_page_url'],
                    new_url,
                    rule_used,
                    NORMALIZATION_VERSION
                ))
                
                # Update the module
                cur.execute("""
                    UPDATE os_modules_v3_1
                    SET supliful_handle = %s,
                        supplier_page_url = %s,
                        updated_at = NOW()
                    WHERE module_code = %s
                      AND (url IS NULL OR btrim(url) = '')
                      AND (supplier_page_url IS NULL OR btrim(supplier_page_url) = '')
                      AND (supliful_handle IS NULL OR btrim(supliful_handle) = '')
                """, (new_handle, new_url, m['module_code']))
                
                if cur.rowcount > 0:
                    updated.append({
                        "module_code": m['module_code'],
                        "base_handle": base_handle,
                        "new_supliful_handle": new_handle,
                        "new_supplier_page_url": new_url,
                        "rule_used": rule_used
                    })
                else:
                    errors.append({
                        "module_code": m['module_code'],
                        "reason": "UPDATE returned 0 rows (guardrail blocked or already updated)"
                    })
                    
            except Exception as e:
                errors.append({
                    "module_code": m['module_code'],
                    "reason": str(e)
                })
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Rule usage breakdown
        rule_usage = {}
        for u in updated:
            rule = u["rule_used"]
            rule_usage[rule] = rule_usage.get(rule, 0) + 1
        
        return {
            "status": "success",
            "batch_id": batch_id,
            "batch_timestamp": batch_timestamp,
            "normalization_version": NORMALIZATION_VERSION,
            "summary": {
                "candidates_analyzed": len(candidates),
                "updated": len(updated),
                "skipped_ambiguous": len(skipped_ambiguous),
                "skipped_no_match": len(skipped_no_match),
                "errors": len(errors)
            },
            "rule_usage_breakdown": rule_usage,
            "updated_modules": updated[:20],  # First 20 for brevity
            "skipped_ambiguous": skipped_ambiguous,
            "skipped_no_match": skipped_no_match[:20],  # First 20
            "errors": errors
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Apply error: {str(e)}")


# =============================================================================
# AUDIT TRAIL ENDPOINTS
# =============================================================================

@router.get("/normalize/audit")
def get_audit_trail(
    batch_id: Optional[str] = Query(None, description="Filter by batch ID"),
    limit: int = Query(100, description="Max records to return")
) -> Dict[str, Any]:
    """Get audit trail of applied mappings."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check table exists
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'handle_mapping_audit_v1'")
        if not cur.fetchone():
            return {"table_exists": False, "records": []}
        
        if batch_id:
            cur.execute("""
                SELECT * FROM handle_mapping_audit_v1
                WHERE batch_id = %s
                ORDER BY applied_at DESC
                LIMIT %s
            """, (batch_id, limit))
        else:
            cur.execute("""
                SELECT * FROM handle_mapping_audit_v1
                ORDER BY applied_at DESC
                LIMIT %s
            """, (limit,))
        
        records = [dict(r) for r in cur.fetchall()]
        
        # Get batch summary
        cur.execute("""
            SELECT batch_id, COUNT(*) as count, MIN(applied_at) as applied_at
            FROM handle_mapping_audit_v1
            GROUP BY batch_id
            ORDER BY MIN(applied_at) DESC
        """)
        batches = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "table_exists": True,
            "total_records": len(records),
            "batches": batches,
            "records": records
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# LEGACY ENDPOINTS (kept for backward compatibility)
# =============================================================================

@router.get("/mapping-preview")
def preview_mapping() -> Dict[str, Any]:
    """Legacy endpoint - redirects to normalize/preview."""
    return normalize_preview()


@router.post("/backfill-supplier-urls")
def backfill_supplier_urls() -> Dict[str, Any]:
    """
    Legacy deterministic backfill (EXACT match only).
    Kept for backward compatibility. For suffix matching, use normalize/apply.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1")
        total_modules = cur.fetchone()['count']
        
        cur.execute("""
            SELECT COUNT(*) as count FROM os_modules_v3_1
            WHERE (url IS NULL OR btrim(url) = '')
              AND (supplier_page_url IS NULL OR btrim(supplier_page_url) = '')
              AND (supliful_handle IS NULL OR btrim(supliful_handle) = '')
        """)
        missing_before = cur.fetchone()['count']
        
        # Run EXACT match update only
        cur.execute("""
            UPDATE os_modules_v3_1 m
            SET
              supliful_handle = s.supliful_handle,
              supplier_page_url = s.supplier_url,
              updated_at = NOW()
            FROM supplier_catalog_snapshot_v1 s
            WHERE
              (m.url IS NULL OR btrim(m.url) = '')
              AND (m.supplier_page_url IS NULL OR btrim(m.supplier_page_url) = '')
              AND (m.supliful_handle IS NULL OR btrim(m.supliful_handle) = '')
              AND (m.shopify_handle LIKE '%-maximo' OR m.shopify_handle LIKE '%-maxima')
              AND s.supliful_handle = regexp_replace(m.shopify_handle, '(-maximo|-maxima)$', '')
        """)
        updated_count = cur.rowcount
        
        cur.execute("""
            SELECT COUNT(*) as count FROM os_modules_v3_1
            WHERE (url IS NULL OR btrim(url) = '')
              AND (supplier_page_url IS NULL OR btrim(supplier_page_url) = '')
              AND (supliful_handle IS NULL OR btrim(supliful_handle) = '')
        """)
        missing_after = cur.fetchone()['count']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "method": "EXACT_MATCH_ONLY",
            "total_modules": total_modules,
            "missing_supplier_sources_before": missing_before,
            "updated_supplier_links": updated_count,
            "still_missing_supplier_sources_after": missing_after
        }
        
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
