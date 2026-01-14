"""
GenoMAX² Allowlist Mapping Module v1.0
Endpoints for managing NO_MATCH handle mappings via explicit allowlist.

GET  /api/v1/qa/allowlist/no-match        - Export 44 NO_MATCH modules
POST /api/v1/qa/allowlist/import          - Import allowlist from JSON/CSV
POST /api/v1/qa/allowlist/apply           - Apply allowlist mappings to modules
GET  /api/v1/qa/allowlist/list            - List current allowlist entries
GET  /api/v1/qa/allowlist/audit           - View audit trail

RULES:
- No fuzzy matching
- Only explicit allowlist mappings applied
- Full audit trail with batch_id
"""

import os
import uuid
import csv
import io
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/qa/allowlist", tags=["QA Allowlist Mapping"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


class AllowlistEntry(BaseModel):
    shopify_base_handle: str
    supliful_handle: str
    supplier_url: str
    notes: Optional[str] = None


class AllowlistImportRequest(BaseModel):
    entries: List[AllowlistEntry]


# ============================================================
# GET /no-match - Export NO_MATCH modules for allowlist creation
# ============================================================

@router.get("/no-match")
def get_no_match_modules() -> Dict[str, Any]:
    """
    Export ACTIVE modules with NO_MATCH status.
    
    NO_MATCH definition:
    - net_quantity is NULL/empty
    - supplier_status = 'ACTIVE' (or NULL)
    - supplier_page_url/url/supliful_handle all empty
    - shopify_handle ends with -maximo/-maxima
    - base_handle not in supplier_catalog_snapshot_v1
    
    Used to build the manual allowlist file.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Find NO_MATCH modules: ACTIVE, missing net_quantity, no supplier URL
        cur.execute("""
            WITH base_handles AS (
                SELECT 
                    module_code,
                    shopify_handle,
                    os_environment,
                    product_name,
                    -- Extract base handle by removing -maximo/-maxima suffix
                    CASE 
                        WHEN shopify_handle LIKE '%-maximo' 
                            THEN SUBSTRING(shopify_handle FROM 1 FOR LENGTH(shopify_handle) - 7)
                        WHEN shopify_handle LIKE '%-maxima'
                            THEN SUBSTRING(shopify_handle FROM 1 FOR LENGTH(shopify_handle) - 7)
                        ELSE shopify_handle
                    END AS shopify_base_handle,
                    supliful_handle,
                    supplier_page_url,
                    url,
                    supplier_status,
                    net_quantity
                FROM os_modules_v3_1
                WHERE (supplier_status = 'ACTIVE' OR supplier_status IS NULL)
                  AND (net_quantity IS NULL OR BTRIM(net_quantity) = '')
                  AND (shopify_handle LIKE '%-maximo' OR shopify_handle LIKE '%-maxima')
            )
            SELECT 
                bh.module_code,
                bh.shopify_handle,
                bh.os_environment,
                bh.shopify_base_handle,
                bh.product_name,
                bh.supliful_handle,
                bh.supplier_page_url,
                bh.url
            FROM base_handles bh
            WHERE (bh.supliful_handle IS NULL OR BTRIM(bh.supliful_handle) = '')
              AND (bh.supplier_page_url IS NULL OR BTRIM(bh.supplier_page_url) = '')
              AND (bh.url IS NULL OR BTRIM(bh.url) = '')
            ORDER BY bh.shopify_base_handle, bh.os_environment
        """)
        
        rows = cur.fetchall()
        
        # Group by base handle to show unique handles needed
        modules = []
        base_handles_seen = set()
        
        for row in rows:
            modules.append({
                "module_code": row["module_code"],
                "shopify_handle": row["shopify_handle"],
                "os_environment": row["os_environment"],
                "shopify_base_handle": row["shopify_base_handle"],
                "product_name": row["product_name"],
                "reason": "NO_MATCH_UNDER_LOCKED_RULES"
            })
            base_handles_seen.add(row["shopify_base_handle"])
        
        cur.close()
        conn.close()
        
        return {
            "count": len(modules),
            "unique_base_handles": len(base_handles_seen),
            "base_handles": sorted(list(base_handles_seen)),
            "modules": modules,
            "note": "Use base_handles list to create ALLOWLIST_44.csv with shopify_base_handle,supliful_handle,supplier_url columns"
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# ============================================================
# GET /list - List current allowlist entries
# ============================================================

@router.get("/list")
def list_allowlist() -> Dict[str, Any]:
    """
    List all current allowlist entries.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'catalog_handle_map_allowlist_v1'
            )
        """)
        if not cur.fetchone()["exists"]:
            cur.close()
            conn.close()
            return {
                "status": "table_not_found",
                "message": "Run migration 008 first",
                "entries": [],
                "count": 0
            }
        
        cur.execute("""
            SELECT 
                allowlist_id,
                shopify_base_handle,
                supliful_handle,
                supplier_url,
                source,
                notes,
                created_at,
                updated_at
            FROM catalog_handle_map_allowlist_v1
            ORDER BY shopify_base_handle
        """)
        
        entries = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "count": len(entries),
            "entries": entries
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# ============================================================
# POST /import - Import allowlist from JSON
# ============================================================

@router.post("/import")
def import_allowlist(request: AllowlistImportRequest) -> Dict[str, Any]:
    """
    Import allowlist entries from JSON.
    
    Validates:
    - shopify_base_handle non-empty
    - supliful_handle non-empty
    - supplier_url starts with https://supliful.com/catalog/
    - supliful_handle matches path segment in supplier_url
    - Unique shopify_base_handle within import
    - Unique supliful_handle within import
    
    Upsert behavior:
    - Insert new rows
    - Update existing rows by shopify_base_handle
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Ensure table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'catalog_handle_map_allowlist_v1'
            )
        """)
        if not cur.fetchone()["exists"]:
            cur.close()
            conn.close()
            raise HTTPException(status_code=400, detail="Run migration 008 first to create allowlist tables")
        
        # Validation
        rejected = []
        valid_entries = []
        seen_base_handles = set()
        seen_supliful_handles = set()
        
        for i, entry in enumerate(request.entries):
            errors = []
            
            # Non-empty checks
            if not entry.shopify_base_handle or not entry.shopify_base_handle.strip():
                errors.append("shopify_base_handle is empty")
            if not entry.supliful_handle or not entry.supliful_handle.strip():
                errors.append("supliful_handle is empty")
            if not entry.supplier_url or not entry.supplier_url.strip():
                errors.append("supplier_url is empty")
            
            # URL format check
            if entry.supplier_url and not entry.supplier_url.startswith("https://supliful.com/catalog/"):
                errors.append("supplier_url must start with https://supliful.com/catalog/")
            
            # Handle matches URL path
            if entry.supplier_url and entry.supliful_handle:
                expected_url = f"https://supliful.com/catalog/{entry.supliful_handle}"
                if entry.supplier_url.rstrip('/') != expected_url:
                    errors.append(f"supliful_handle '{entry.supliful_handle}' does not match supplier_url path")
            
            # Uniqueness within import
            base_handle = entry.shopify_base_handle.strip() if entry.shopify_base_handle else ""
            supliful_handle = entry.supliful_handle.strip() if entry.supliful_handle else ""
            
            if base_handle in seen_base_handles:
                errors.append(f"Duplicate shopify_base_handle: {base_handle}")
            if supliful_handle in seen_supliful_handles:
                errors.append(f"Duplicate supliful_handle: {supliful_handle}")
            
            if errors:
                rejected.append({
                    "row": i + 1,
                    "shopify_base_handle": entry.shopify_base_handle,
                    "errors": errors
                })
            else:
                seen_base_handles.add(base_handle)
                seen_supliful_handles.add(supliful_handle)
                valid_entries.append(entry)
        
        # Upsert valid entries
        inserted = 0
        updated = 0
        
        for entry in valid_entries:
            cur.execute("""
                INSERT INTO catalog_handle_map_allowlist_v1 
                    (shopify_base_handle, supliful_handle, supplier_url, source, notes)
                VALUES (%s, %s, %s, 'MANUAL_ALLOWLIST', %s)
                ON CONFLICT (shopify_base_handle) DO UPDATE SET
                    supliful_handle = EXCLUDED.supliful_handle,
                    supplier_url = EXCLUDED.supplier_url,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
            """, (
                entry.shopify_base_handle.strip(),
                entry.supliful_handle.strip(),
                entry.supplier_url.strip(),
                entry.notes.strip() if entry.notes else None
            ))
            
            result = cur.fetchone()
            if result["inserted"]:
                inserted += 1
            else:
                updated += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "rows_read": len(request.entries),
            "inserted": inserted,
            "updated": updated,
            "rejected": len(rejected),
            "rejected_examples": rejected[:10] if rejected else []
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Import error: {str(e)}")


# ============================================================
# POST /import-csv - Import allowlist from CSV file upload
# ============================================================

@router.post("/import-csv")
async def import_allowlist_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    Import allowlist from CSV file.
    
    Required columns: shopify_base_handle, supliful_handle, supplier_url
    Optional columns: notes
    """
    content = await file.read()
    text = content.decode('utf-8')
    
    # Parse CSV
    reader = csv.DictReader(io.StringIO(text))
    entries = []
    
    for row in reader:
        entries.append(AllowlistEntry(
            shopify_base_handle=row.get('shopify_base_handle', ''),
            supliful_handle=row.get('supliful_handle', ''),
            supplier_url=row.get('supplier_url', ''),
            notes=row.get('notes')
        ))
    
    # Reuse JSON import logic
    request = AllowlistImportRequest(entries=entries)
    return import_allowlist(request)


# ============================================================
# POST /apply - Apply allowlist mappings to modules
# ============================================================

@router.post("/apply")
def apply_allowlist(
    confirm: bool = Query(False, description="Set to true to actually apply changes"),
    batch_note: Optional[str] = Query(None, description="Optional note for this batch")
) -> Dict[str, Any]:
    """
    Apply allowlist mappings to os_modules_v3_1.
    
    For each allowlist row:
    - Find modules where shopify_handle = base_handle + '-maximo' OR '-maxima'
    - Only update if supliful_handle AND supplier_page_url AND url are ALL empty
    - Update supliful_handle and supplier_page_url
    - Audit each change
    
    Guardrails:
    - confirm=false: dry-run only
    - Skip modules with existing supplier data
    - Full audit trail with batch_id
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get all allowlist entries
        cur.execute("""
            SELECT 
                shopify_base_handle,
                supliful_handle,
                supplier_url
            FROM catalog_handle_map_allowlist_v1
            ORDER BY shopify_base_handle
        """)
        allowlist = cur.fetchall()
        
        if not allowlist:
            cur.close()
            conn.close()
            return {
                "status": "no_allowlist",
                "message": "No allowlist entries found. Import allowlist first.",
                "modules_updated": 0
            }
        
        batch_id = str(uuid.uuid4())
        
        # Process each allowlist entry
        modules_updated = 0
        skipped_existing = 0
        not_found = 0
        updates = []
        skipped_details = []
        
        for entry in allowlist:
            base_handle = entry["shopify_base_handle"]
            new_supliful_handle = entry["supliful_handle"]
            new_supplier_url = entry["supplier_url"]
            
            # Find matching modules (both -maximo and -maxima)
            cur.execute("""
                SELECT 
                    module_code,
                    shopify_handle,
                    os_environment,
                    supliful_handle,
                    supplier_page_url,
                    url
                FROM os_modules_v3_1
                WHERE shopify_handle = %s OR shopify_handle = %s
            """, (f"{base_handle}-maximo", f"{base_handle}-maxima"))
            
            modules = cur.fetchall()
            
            if not modules:
                not_found += 1
                continue
            
            for mod in modules:
                # Check guardrail: only update if ALL supplier fields empty
                has_existing = (
                    (mod["supliful_handle"] and mod["supliful_handle"].strip()) or
                    (mod["supplier_page_url"] and mod["supplier_page_url"].strip()) or
                    (mod["url"] and mod["url"].strip())
                )
                
                if has_existing:
                    skipped_existing += 1
                    skipped_details.append({
                        "module_code": mod["module_code"],
                        "reason": "existing_supplier_data",
                        "existing_supliful_handle": mod["supliful_handle"],
                        "existing_supplier_page_url": mod["supplier_page_url"],
                        "existing_url": mod["url"]
                    })
                    continue
                
                # Record update
                update_info = {
                    "module_code": mod["module_code"],
                    "shopify_handle": mod["shopify_handle"],
                    "os_environment": mod["os_environment"],
                    "old_supliful_handle": mod["supliful_handle"],
                    "new_supliful_handle": new_supliful_handle,
                    "old_supplier_page_url": mod["supplier_page_url"],
                    "new_supplier_page_url": new_supplier_url
                }
                updates.append(update_info)
                
                if confirm:
                    # Apply update
                    cur.execute("""
                        UPDATE os_modules_v3_1
                        SET supliful_handle = %s,
                            supplier_page_url = %s,
                            updated_at = NOW()
                        WHERE module_code = %s
                    """, (new_supliful_handle, new_supplier_url, mod["module_code"]))
                    
                    # Audit
                    cur.execute("""
                        INSERT INTO catalog_handle_map_allowlist_audit_v1
                            (batch_id, module_code, shopify_handle, os_environment,
                             old_supliful_handle, new_supliful_handle,
                             old_supplier_page_url, new_supplier_page_url,
                             rule_used)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'MANUAL_ALLOWLIST')
                    """, (
                        batch_id,
                        mod["module_code"],
                        mod["shopify_handle"],
                        mod["os_environment"],
                        mod["supliful_handle"],
                        new_supliful_handle,
                        mod["supplier_page_url"],
                        new_supplier_url
                    ))
                    
                    modules_updated += 1
        
        if confirm:
            conn.commit()
        
        cur.close()
        conn.close()
        
        return {
            "status": "applied" if confirm else "dry_run",
            "batch_id": batch_id if confirm else None,
            "batch_note": batch_note,
            "allowlist_rows": len(allowlist),
            "modules_would_update" if not confirm else "modules_updated": len(updates) if not confirm else modules_updated,
            "skipped_existing_supplier_data": skipped_existing,
            "not_found_modules": not_found,
            "errors": 0,
            "samples": updates[:10],
            "skipped_samples": skipped_details[:5] if skipped_details else []
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Apply error: {str(e)}")


# ============================================================
# GET /audit - View audit trail
# ============================================================

@router.get("/audit")
def get_audit_trail(
    batch_id: Optional[str] = Query(None, description="Filter by batch ID"),
    limit: int = Query(100, description="Max rows to return")
) -> Dict[str, Any]:
    """
    View audit trail for allowlist apply operations.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'catalog_handle_map_allowlist_audit_v1'
            )
        """)
        if not cur.fetchone()["exists"]:
            cur.close()
            conn.close()
            return {
                "status": "table_not_found",
                "message": "Run migration 008 first",
                "entries": [],
                "count": 0
            }
        
        if batch_id:
            cur.execute("""
                SELECT * FROM catalog_handle_map_allowlist_audit_v1
                WHERE batch_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (batch_id, limit))
        else:
            cur.execute("""
                SELECT * FROM catalog_handle_map_allowlist_audit_v1
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
        
        entries = [dict(r) for r in cur.fetchall()]
        
        # Get batch summary
        cur.execute("""
            SELECT 
                batch_id,
                COUNT(*) as modules_updated,
                MIN(created_at) as applied_at
            FROM catalog_handle_map_allowlist_audit_v1
            GROUP BY batch_id
            ORDER BY MIN(created_at) DESC
            LIMIT 10
        """)
        batches = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "count": len(entries),
            "entries": entries,
            "recent_batches": batches
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# ============================================================
# GET /modules-by-batch - Get modules updated in a batch (for scraper)
# ============================================================

@router.get("/modules-by-batch/{batch_id}")
def get_modules_by_batch(batch_id: str) -> Dict[str, Any]:
    """
    Get modules updated in a specific batch.
    Used to run targeted scraper on allowlist-updated modules.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                a.module_code,
                a.shopify_handle,
                a.os_environment,
                a.new_supplier_page_url as supplier_url,
                m.net_quantity
            FROM catalog_handle_map_allowlist_audit_v1 a
            JOIN os_modules_v3_1 m ON a.module_code = m.module_code
            WHERE a.batch_id = %s
            ORDER BY a.shopify_handle
        """, (batch_id,))
        
        modules = [dict(r) for r in cur.fetchall()]
        
        # Count how many still need scraping
        need_scraping = [m for m in modules if not m.get("net_quantity")]
        
        cur.close()
        conn.close()
        
        return {
            "batch_id": batch_id,
            "total_modules": len(modules),
            "need_scraping": len(need_scraping),
            "already_have_net_qty": len(modules) - len(need_scraping),
            "modules": modules
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
