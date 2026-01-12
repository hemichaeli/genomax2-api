# =============================================================================
# GenoMAX² Supplier Catalog Admin Endpoints
# Handles import from Excel snapshot and backfill operations
# =============================================================================

import os
import re
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query, Body
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/admin/supplier-catalog", tags=["Admin - Supplier Catalog"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


@router.post("/snapshot/upsert")
def upsert_snapshot_entry(
    supliful_handle: str = Query(..., description="Supliful handle (e.g., 'creatine-monohydrate-powder')"),
    supplier_url: str = Query(..., description="Full supplier URL")
) -> Dict[str, Any]:
    """
    Upsert a single entry into supplier_catalog_snapshot_v1.
    """
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
        
        return {
            "status": "success",
            "supliful_handle": supliful_handle,
            "supplier_url": supplier_url
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Upsert error: {str(e)}")


@router.post("/snapshot/bulk-upsert")
def bulk_upsert_snapshot(
    data: List[Dict[str, str]] = Body(..., description="List of {supliful_handle, supplier_url}")
) -> Dict[str, Any]:
    """
    Bulk upsert entries into supplier_catalog_snapshot_v1.
    
    Expected format: [{"supliful_handle": "...", "supplier_url": "..."}, ...]
    """
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
                
                # Check if exists
                cur.execute("""
                    SELECT 1 FROM supplier_catalog_snapshot_v1
                    WHERE supliful_handle = %s
                """, (handle,))
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
        
        return {
            "status": "success",
            "inserted": inserted,
            "updated": updated,
            "total_processed": inserted + updated,
            "errors": len(errors),
            "error_examples": errors[:10]
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Bulk upsert error: {str(e)}")


@router.get("/snapshot/stats")
def get_snapshot_stats() -> Dict[str, Any]:
    """Get stats on supplier_catalog_snapshot_v1 table."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check table exists
        cur.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'supplier_catalog_snapshot_v1'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {
                "table_exists": False,
                "row_count": 0,
                "message": "Run migration 007 first"
            }
        
        cur.execute("SELECT COUNT(*) as count FROM supplier_catalog_snapshot_v1")
        row_count = cur.fetchone()['count']
        
        cur.execute("""
            SELECT supliful_handle, supplier_url
            FROM supplier_catalog_snapshot_v1
            ORDER BY supliful_handle
            LIMIT 10
        """)
        samples = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "table_exists": True,
            "row_count": row_count,
            "samples": samples
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")


@router.post("/backfill-supplier-urls")
def backfill_supplier_urls() -> Dict[str, Any]:
    """
    Deterministic backfill: Update supliful_handle + supplier_page_url in os_modules_v3_1.
    
    Rules:
    - Only updates modules where ALL supplier sources are missing (url, supplier_page_url, supliful_handle)
    - Derives base_handle by stripping -maximo/-maxima suffix from shopify_handle
    - Only matches if base_handle exactly exists in supplier_catalog_snapshot_v1
    - No fuzzy matching
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get initial counts
        cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1")
        total_modules = cur.fetchone()['count']
        
        cur.execute("""
            SELECT COUNT(*) as count FROM os_modules_v3_1
            WHERE (url IS NULL OR btrim(url) = '')
              AND (supplier_page_url IS NULL OR btrim(supplier_page_url) = '')
              AND (supliful_handle IS NULL OR btrim(supliful_handle) = '')
        """)
        missing_before = cur.fetchone()['count']
        
        # Count modules without -maximo/-maxima suffix (unmappable)
        cur.execute("""
            SELECT COUNT(*) as count FROM os_modules_v3_1
            WHERE (url IS NULL OR btrim(url) = '')
              AND (supplier_page_url IS NULL OR btrim(supplier_page_url) = '')
              AND (supliful_handle IS NULL OR btrim(supliful_handle) = '')
              AND shopify_handle NOT LIKE '%-maximo'
              AND shopify_handle NOT LIKE '%-maxima'
        """)
        unmappable_suffix = cur.fetchone()['count']
        
        # Find modules that WOULD map but base_handle not in snapshot
        cur.execute("""
            SELECT m.module_code, m.shopify_handle, 
                   regexp_replace(m.shopify_handle, '(-maximo|-maxima)$', '') as base_handle
            FROM os_modules_v3_1 m
            LEFT JOIN supplier_catalog_snapshot_v1 s 
                ON s.supliful_handle = regexp_replace(m.shopify_handle, '(-maximo|-maxima)$', '')
            WHERE (m.url IS NULL OR btrim(m.url) = '')
              AND (m.supplier_page_url IS NULL OR btrim(m.supplier_page_url) = '')
              AND (m.supliful_handle IS NULL OR btrim(m.supliful_handle) = '')
              AND (m.shopify_handle LIKE '%-maximo' OR m.shopify_handle LIKE '%-maxima')
              AND s.supliful_handle IS NULL
        """)
        not_in_snapshot = [dict(r) for r in cur.fetchall()]
        
        # Run the deterministic update
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
        
        # Get after counts
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
            "total_modules": total_modules,
            "missing_supplier_sources_before": missing_before,
            "updated_supplier_links": updated_count,
            "still_missing_supplier_sources_after": missing_after,
            "unmappable_suffix": unmappable_suffix,
            "not_in_snapshot": len(not_in_snapshot),
            "examples_not_in_snapshot": not_in_snapshot[:10]
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Backfill error: {str(e)}")


@router.get("/mapping-preview")
def preview_mapping() -> Dict[str, Any]:
    """
    Preview what the backfill would do without executing.
    Shows shopify_handle -> base_handle -> would_map status.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get all modules missing supplier sources
        cur.execute("""
            SELECT m.module_code, m.shopify_handle, m.os_environment,
                   regexp_replace(m.shopify_handle, '(-maximo|-maxima)$', '') as base_handle,
                   CASE 
                       WHEN m.shopify_handle NOT LIKE '%-maximo' 
                            AND m.shopify_handle NOT LIKE '%-maxima' 
                       THEN 'NO_SUFFIX'
                       WHEN s.supliful_handle IS NOT NULL THEN 'WILL_MAP'
                       ELSE 'NOT_IN_SNAPSHOT'
                   END as mapping_status,
                   s.supplier_url
            FROM os_modules_v3_1 m
            LEFT JOIN supplier_catalog_snapshot_v1 s 
                ON s.supliful_handle = regexp_replace(m.shopify_handle, '(-maximo|-maxima)$', '')
            WHERE (m.url IS NULL OR btrim(m.url) = '')
              AND (m.supplier_page_url IS NULL OR btrim(m.supplier_page_url) = '')
              AND (m.supliful_handle IS NULL OR btrim(m.supliful_handle) = '')
            ORDER BY m.shopify_handle
        """)
        results = [dict(r) for r in cur.fetchall()]
        
        # Summary
        will_map = [r for r in results if r['mapping_status'] == 'WILL_MAP']
        not_in_snapshot = [r for r in results if r['mapping_status'] == 'NOT_IN_SNAPSHOT']
        no_suffix = [r for r in results if r['mapping_status'] == 'NO_SUFFIX']
        
        cur.close()
        conn.close()
        
        return {
            "total_missing_sources": len(results),
            "will_map": len(will_map),
            "not_in_snapshot": len(not_in_snapshot),
            "no_suffix_unmappable": len(no_suffix),
            "samples_will_map": will_map[:10],
            "samples_not_in_snapshot": not_in_snapshot[:10],
            "samples_no_suffix": no_suffix[:10]
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Preview error: {str(e)}")
