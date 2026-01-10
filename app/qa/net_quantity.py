"""
GenoMAX² QA Net Quantity Module v1.0
Endpoint for identifying modules missing net_quantity field.

GET /api/v1/qa/net-qty/missing
- Returns all modules where net_quantity IS NULL or empty
- Includes deterministic supplier URL for scraping
- Supports one-time backfill workflow

v1.0 - Initial implementation
"""

import os
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException
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
    supplier_url: Optional[str]
    reason: str


class MissingNetQuantityResponse(BaseModel):
    total_missing: int
    missing_with_url: int
    missing_without_url: int
    modules: List[MissingNetQuantityModule]


@router.get("/missing", response_model=MissingNetQuantityResponse)
def get_missing_net_quantity() -> Dict[str, Any]:
    """
    Get all modules missing net_quantity with their supplier URLs.
    
    Returns modules where net_quantity IS NULL or empty string.
    For each module, provides a deterministic supplier URL for scraping:
    - supplier_page_url (preferred)
    - url (fallback)
    - null if neither available
    
    Used by one-time backfill script to populate net_quantity.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Query all modules missing net_quantity with deterministic URL selection
        cur.execute("""
            SELECT
                module_code,
                shopify_handle,
                os_environment,
                product_name,
                CASE
                    WHEN supplier_page_url IS NOT NULL AND BTRIM(supplier_page_url) <> '' 
                        THEN supplier_page_url
                    WHEN url IS NOT NULL AND BTRIM(url) <> '' 
                        THEN url
                    ELSE NULL
                END AS supplier_url,
                CASE
                    WHEN (supplier_page_url IS NOT NULL AND BTRIM(supplier_page_url) <> '')
                        OR (url IS NOT NULL AND BTRIM(url) <> '') 
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
                "supplier_url": row["supplier_url"],
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
