"""
GenoMAX² QA Net Quantity Module v1.2
Endpoint for identifying modules missing net_quantity field and managing supplier status.

GET /api/v1/qa/net-qty/missing
- Returns all modules where net_quantity IS NULL or empty
- Includes deterministic supplier URL for scraping
- Supports one-time backfill workflow

POST /api/v1/qa/net-qty/update
- Update net_quantity for a single module

POST /api/v1/qa/net-qty/mark-discontinued
- Mark modules as DISCONTINUED with 404 status

POST /api/v1/qa/net-qty/reactivate
- Reactivate modules incorrectly marked as discontinued

GET /api/v1/qa/net-qty/discontinued
- List all discontinued modules

v1.0 - Initial implementation
v1.1 - Added supplier status endpoints
v1.2 - Added reactivate endpoint
"""

import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/qa/net-qty", tags=["QA Net Quantity"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


class MissingNetQuantityModule(BaseModel):
    module_code: str
    shopify_handle: Optional[str]
    os_environment: Optional[str]
    product_name: Optional[str]
    supliful_handle: Optional[str]
    supplier_url: Optional[str]
    reason: str


class MissingNetQuantityResponse(BaseModel):
    total_missing: int
    missing_with_url: int
    missing_without_url: int
    modules: List[MissingNetQuantityModule]
    note: str


class DiscontinuedModule(BaseModel):
    module_code: str
    supplier_url: str
    http_status: int = 404
    details: Optional[str] = None


class MarkDiscontinuedRequest(BaseModel):
    modules: List[DiscontinuedModule]


@router.get("/missing", response_model=MissingNetQuantityResponse)
def get_missing_net_quantity() -> Dict[str, Any]:
    """
    Get all modules missing net_quantity with their supplier URLs.
    
    Returns modules where net_quantity IS NULL or empty string.
    For each module, provides a deterministic supplier URL for scraping:
    1. supplier_page_url (preferred)
    2. url (fallback)
    3. Build from supliful_handle: https://supliful.com/catalog/{handle}
    4. null if none available
    
    Used by one-time backfill script to populate net_quantity.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Query all modules missing net_quantity with deterministic URL selection
        # Priority: supplier_page_url > url > supliful_handle-based URL
        cur.execute("""
            SELECT
                module_code,
                shopify_handle,
                os_environment,
                product_name,
                supliful_handle,
                supplier_status,
                CASE
                    WHEN supplier_page_url IS NOT NULL AND BTRIM(supplier_page_url) <> '' 
                        THEN supplier_page_url
                    WHEN url IS NOT NULL AND BTRIM(url) <> '' 
                        THEN url
                    WHEN supliful_handle IS NOT NULL AND BTRIM(supliful_handle) <> ''
                        THEN 'https://supliful.com/catalog/' || supliful_handle
                    ELSE NULL
                END AS supplier_url,
                CASE
                    WHEN (supplier_page_url IS NOT NULL AND BTRIM(supplier_page_url) <> '')
                        OR (url IS NOT NULL AND BTRIM(url) <> '')
                        OR (supliful_handle IS NOT NULL AND BTRIM(supliful_handle) <> '')
                        THEN 'OK'
                    ELSE 'MISSING_SUPPLIER_URL'
                END AS reason
            FROM os_modules_v3_1
            WHERE net_quantity IS NULL OR BTRIM(net_quantity) = ''
            ORDER BY shopify_handle, os_environment
        """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # Build response
        modules = []
        missing_with_url = 0
        missing_without_url = 0
        
        for row in rows:
            module = {
                "module_code": row["module_code"],
                "shopify_handle": row["shopify_handle"],
                "os_environment": row["os_environment"],
                "product_name": row["product_name"],
                "supliful_handle": row.get("supliful_handle"),
                "supplier_url": row["supplier_url"],
                "supplier_status": row.get("supplier_status"),
                "reason": row["reason"]
            }
            modules.append(module)
            
            if row["supplier_url"]:
                missing_with_url += 1
            else:
                missing_without_url += 1
        
        return {
            "total_missing": len(modules),
            "missing_with_url": missing_with_url,
            "missing_without_url": missing_without_url,
            "modules": modules,
            "note": "Use supplier_url to scrape Product Amount from Supliful catalog pages"
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@router.post("/update")
def update_net_quantity(
    module_code: str = Query(..., description="Module code to update"),
    net_quantity: str = Query(..., description="Net quantity value (full raw string)")
) -> Dict[str, Any]:
    """
    Update net_quantity for a single module.
    
    Only updates if net_quantity is currently NULL or empty.
    Stores the FULL raw Product Amount string including slashes and multiple units.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Only update if currently missing
        cur.execute("""
            UPDATE os_modules_v3_1
            SET net_quantity = %s, updated_at = NOW()
            WHERE module_code = %s
              AND (net_quantity IS NULL OR BTRIM(net_quantity) = '')
        """, (net_quantity.strip(), module_code))
        
        updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        
        if updated == 0:
            return {
                "status": "no_change",
                "module_code": module_code,
                "reason": "Module not found or net_quantity already populated"
            }
        
        return {
            "status": "success",
            "module_code": module_code,
            "net_quantity": net_quantity.strip()
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Update error: {str(e)}")


@router.post("/mark-discontinued")
def mark_discontinued(
    module_codes: List[str] = Query(..., description="List of module codes to mark as discontinued"),
    http_status: int = Query(404, description="HTTP status code (default 404)"),
    details: Optional[str] = Query(None, description="Optional details/notes")
) -> Dict[str, Any]:
    """
    Mark modules as DISCONTINUED.
    
    Used after scraping to mark products that returned 404.
    Sets:
    - supplier_status = 'DISCONTINUED'
    - supplier_http_status = http_status
    - supplier_status_details = details
    - supplier_checked_at = NOW()
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not module_codes:
        return {"status": "error", "detail": "No module codes provided"}
    
    try:
        cur = conn.cursor()
        
        # Build details string
        status_details = details or f"404 on Supliful catalog - checked {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        
        # Update modules
        cur.execute("""
            UPDATE os_modules_v3_1
            SET supplier_status = 'DISCONTINUED',
                supplier_http_status = %s,
                supplier_status_details = %s,
                supplier_checked_at = NOW(),
                updated_at = NOW()
            WHERE module_code = ANY(%s)
        """, (http_status, status_details, module_codes))
        
        updated = cur.rowcount
        conn.commit()
        
        # Get updated modules
        cur.execute("""
            SELECT module_code, shopify_handle, supplier_status, supplier_http_status
            FROM os_modules_v3_1
            WHERE module_code = ANY(%s)
        """, (module_codes,))
        updated_modules = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        not_found = [m for m in module_codes if m not in [u['module_code'] for u in updated_modules]]
        
        return {
            "status": "success",
            "updated_count": updated,
            "http_status": http_status,
            "details": status_details,
            "updated_modules": updated_modules,
            "not_found": not_found
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Update error: {str(e)}")


@router.post("/reactivate")
def reactivate_modules(
    module_codes: List[str] = Query(..., description="List of module codes to reactivate"),
    details: Optional[str] = Query(None, description="Optional details/notes")
) -> Dict[str, Any]:
    """
    Reactivate modules that were incorrectly marked as DISCONTINUED.
    
    Sets:
    - supplier_status = 'ACTIVE'
    - supplier_http_status = 200
    - supplier_status_details = details
    - supplier_checked_at = NOW()
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not module_codes:
        return {"status": "error", "detail": "No module codes provided"}
    
    try:
        cur = conn.cursor()
        
        status_details = details or f"Reactivated - verified product exists {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        
        cur.execute("""
            UPDATE os_modules_v3_1
            SET supplier_status = 'ACTIVE',
                supplier_http_status = 200,
                supplier_status_details = %s,
                supplier_checked_at = NOW(),
                updated_at = NOW()
            WHERE module_code = ANY(%s)
        """, (status_details, module_codes))
        
        updated = cur.rowcount
        conn.commit()
        
        cur.execute("""
            SELECT module_code, shopify_handle, supplier_status, supplier_http_status, net_quantity
            FROM os_modules_v3_1
            WHERE module_code = ANY(%s)
        """, (module_codes,))
        updated_modules = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "updated_count": updated,
            "updated_modules": updated_modules
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Update error: {str(e)}")


@router.get("/discontinued")
def get_discontinued() -> Dict[str, Any]:
    """
    List all modules marked as DISCONTINUED.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                module_code,
                shopify_handle,
                os_environment,
                product_name,
                supplier_status,
                supplier_http_status,
                supplier_status_details,
                supplier_checked_at,
                supplier_page_url,
                url
            FROM os_modules_v3_1
            WHERE supplier_status = 'DISCONTINUED'
            ORDER BY shopify_handle, os_environment
        """)
        
        modules = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        
        return {
            "total_discontinued": len(modules),
            "modules": modules
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@router.get("/stats")
def get_net_quantity_stats() -> Dict[str, Any]:
    """
    Quick stats on net_quantity field coverage.
    
    Returns counts of:
    - Total modules
    - Modules with net_quantity populated
    - Modules missing net_quantity
    - Coverage percentage
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE net_quantity IS NOT NULL AND BTRIM(net_quantity) <> ''
                ) AS populated,
                COUNT(*) FILTER (
                    WHERE net_quantity IS NULL OR BTRIM(net_quantity) = ''
                ) AS missing
            FROM os_modules_v3_1
        """)
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        total = row["total"]
        populated = row["populated"]
        missing = row["missing"]
        
        return {
            "table": "os_modules_v3_1",
            "field": "net_quantity",
            "total_modules": total,
            "populated": populated,
            "missing": missing,
            "coverage_pct": round(100 * populated / total, 1) if total > 0 else 0,
            "missing_pct": round(100 * missing / total, 1) if total > 0 else 0
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")


@router.get("/supplier-stats")
def get_supplier_stats() -> Dict[str, Any]:
    """
    Stats on supplier_status field distribution.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check if column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'os_modules_v3_1'
              AND column_name = 'supplier_status'
        """)
        
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {
                "status": "column_not_found",
                "message": "supplier_status column does not exist. Run migration 006 first.",
                "migration_endpoint": "POST /api/v1/migrations/run/006-supplier-status-columns"
            }
        
        cur.execute("""
            SELECT 
                supplier_status,
                COUNT(*) as count
            FROM os_modules_v3_1
            GROUP BY supplier_status
            ORDER BY count DESC
        """)
        
        distribution = [dict(r) for r in cur.fetchall()]
        
        cur.execute("""
            SELECT COUNT(*) as total FROM os_modules_v3_1
        """)
        total = cur.fetchone()["total"]
        
        cur.close()
        conn.close()
        
        return {
            "table": "os_modules_v3_1",
            "field": "supplier_status",
            "total_modules": total,
            "distribution": distribution
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")
