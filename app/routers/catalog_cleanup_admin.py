# =============================================================================
# GenoMAX² Catalog Cleanup Admin Endpoints
# Delete Universal products + Strip brand prefixes from product names
# =============================================================================

import os
from datetime import datetime
from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/admin/catalog-cleanup", tags=["Admin - Catalog Cleanup"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


# =============================================================================
# PREVIEW: Show what will be deleted/updated
# =============================================================================

@router.get("/preview")
def preview_cleanup() -> Dict[str, Any]:
    """
    Preview cleanup operations (NO changes made):
    1. Universal products to delete
    2. Product names with brand prefixes to strip
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # 1. Find Universal products
        cur.execute("""
            SELECT sku, product_name, product_line, os_environment
            FROM os_modules_v3_1
            WHERE product_line = 'Universal'
            ORDER BY sku
        """)
        universal_products = [dict(r) for r in cur.fetchall()]
        
        # 2. Find products with brand prefixes
        cur.execute("""
            SELECT sku, product_name, product_line,
                   CASE 
                       WHEN product_name LIKE 'MAXima² %' THEN SUBSTRING(product_name FROM 9)
                       WHEN product_name LIKE 'MAXimo² %' THEN SUBSTRING(product_name FROM 9)
                       WHEN product_name LIKE 'GenoMAX² %' THEN SUBSTRING(product_name FROM 10)
                       ELSE product_name
                   END as cleaned_name
            FROM os_modules_v3_1
            WHERE product_name LIKE 'MAXima² %'
               OR product_name LIKE 'MAXimo² %'
               OR product_name LIKE 'GenoMAX² %'
            ORDER BY sku
        """)
        prefixed_products = [dict(r) for r in cur.fetchall()]
        
        # Get total counts
        cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1")
        total_products = cur.fetchone()['count']
        
        cur.close()
        conn.close()
        
        return {
            "status": "preview",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "total_products": total_products,
            "universal_products_to_delete": {
                "count": len(universal_products),
                "products": universal_products
            },
            "prefixed_products_to_clean": {
                "count": len(prefixed_products),
                "products": prefixed_products[:20],  # First 20 for brevity
                "sample_transformations": [
                    {"before": p["product_name"], "after": p["cleaned_name"]}
                    for p in prefixed_products[:5]
                ]
            }
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# EXECUTE: Delete Universal + Strip Prefixes
# =============================================================================

@router.post("/execute")
def execute_cleanup(
    confirm: bool = Query(False, description="Must be true to execute")
) -> Dict[str, Any]:
    """
    Execute cleanup operations:
    1. Delete all Universal products (not a valid product_line)
    2. Strip brand prefixes from all product names
    
    Requires confirm=true query parameter.
    """
    if not confirm:
        raise HTTPException(
            status_code=400, 
            detail="Must pass confirm=true to execute. Use /preview first to review changes."
        )
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get before counts
        cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1")
        total_before = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1 WHERE product_line = 'Universal'")
        universal_count = cur.fetchone()['count']
        
        cur.execute("""
            SELECT COUNT(*) as count FROM os_modules_v3_1
            WHERE product_name LIKE 'MAXima² %'
               OR product_name LIKE 'MAXimo² %'
               OR product_name LIKE 'GenoMAX² %'
        """)
        prefixed_count = cur.fetchone()['count']
        
        # Store deleted products for audit
        cur.execute("""
            SELECT sku, product_name, product_line
            FROM os_modules_v3_1
            WHERE product_line = 'Universal'
        """)
        deleted_products = [dict(r) for r in cur.fetchall()]
        
        # 1. DELETE Universal products
        cur.execute("DELETE FROM os_modules_v3_1 WHERE product_line = 'Universal'")
        deleted_count = cur.rowcount
        
        # 2. STRIP brand prefixes
        cur.execute("""
            UPDATE os_modules_v3_1
            SET product_name = CASE 
                    WHEN product_name LIKE 'MAXima² %' THEN SUBSTRING(product_name FROM 9)
                    WHEN product_name LIKE 'MAXimo² %' THEN SUBSTRING(product_name FROM 9)
                    WHEN product_name LIKE 'GenoMAX² %' THEN SUBSTRING(product_name FROM 10)
                    ELSE product_name
                END,
                updated_at = NOW()
            WHERE product_name LIKE 'MAXima² %'
               OR product_name LIKE 'MAXimo² %'
               OR product_name LIKE 'GenoMAX² %'
        """)
        updated_count = cur.rowcount
        
        # Get after counts
        cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1")
        total_after = cur.fetchone()['count']
        
        # Verify no prefixes remain
        cur.execute("""
            SELECT COUNT(*) as count FROM os_modules_v3_1
            WHERE product_name LIKE 'MAXima² %'
               OR product_name LIKE 'MAXimo² %'
               OR product_name LIKE 'GenoMAX² %'
        """)
        remaining_prefixes = cur.fetchone()['count']
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "operations": {
                "universal_deletion": {
                    "products_deleted": deleted_count,
                    "deleted_skus": [p["sku"] for p in deleted_products]
                },
                "prefix_stripping": {
                    "products_updated": updated_count,
                    "remaining_prefixes": remaining_prefixes
                }
            },
            "counts": {
                "total_before": total_before,
                "total_after": total_after,
                "universal_deleted": deleted_count,
                "prefixes_stripped": updated_count
            }
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# HEALTH CHECK
# =============================================================================

@router.get("/health")
def cleanup_health():
    """Health check for catalog cleanup module."""
    return {
        "status": "ok",
        "module": "catalog_cleanup_admin",
        "version": "v1",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
