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
    1. Universal products to delete (sex_target = 'universal' if any exist)
    2. Product names with brand prefixes to strip
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # 1. Find Universal products (sex_target column in catalog_products)
        # Note: Valid values are 'male', 'female', 'unisex' - 'universal' would be invalid
        cur.execute("""
            SELECT gx_catalog_id, product_name, sex_target, evidence_tier, governance_status
            FROM catalog_products
            WHERE LOWER(sex_target) = 'universal'
            ORDER BY gx_catalog_id
        """)
        universal_products = [dict(r) for r in cur.fetchall()]
        
        # 2. Find products with brand prefixes
        cur.execute("""
            SELECT gx_catalog_id, product_name, sex_target, evidence_tier,
                   CASE 
                       WHEN product_name LIKE 'MAXima² %' THEN SUBSTRING(product_name FROM 9)
                       WHEN product_name LIKE 'MAXimo² %' THEN SUBSTRING(product_name FROM 9)
                       WHEN product_name LIKE 'GenoMAX² %' THEN SUBSTRING(product_name FROM 10)
                       ELSE product_name
                   END as cleaned_name
            FROM catalog_products
            WHERE product_name LIKE 'MAXima² %'
               OR product_name LIKE 'MAXimo² %'
               OR product_name LIKE 'GenoMAX² %'
            ORDER BY gx_catalog_id
        """)
        prefixed_products = [dict(r) for r in cur.fetchall()]
        
        # Get total counts
        cur.execute("SELECT COUNT(*) as count FROM catalog_products")
        total_products = cur.fetchone()['count']
        
        # Get breakdown by sex_target
        cur.execute("""
            SELECT sex_target, COUNT(*) as count 
            FROM catalog_products 
            GROUP BY sex_target 
            ORDER BY sex_target
        """)
        sex_target_breakdown = {r['sex_target']: r['count'] for r in cur.fetchall()}
        
        # Get breakdown by evidence_tier
        cur.execute("""
            SELECT evidence_tier, COUNT(*) as count 
            FROM catalog_products 
            GROUP BY evidence_tier 
            ORDER BY evidence_tier
        """)
        tier_breakdown = {r['evidence_tier']: r['count'] for r in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return {
            "status": "preview",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "table": "catalog_products",
            "total_products": total_products,
            "breakdowns": {
                "by_sex_target": sex_target_breakdown,
                "by_evidence_tier": tier_breakdown
            },
            "universal_products_to_delete": {
                "count": len(universal_products),
                "products": universal_products,
                "note": "Valid sex_target values are 'male', 'female', 'unisex'"
            },
            "prefixed_products_to_clean": {
                "count": len(prefixed_products),
                "products": prefixed_products[:20],  # First 20 for brevity
                "sample_transformations": [
                    {"before": p["product_name"], "after": p["cleaned_name"]}
                    for p in prefixed_products[:5]
                ] if prefixed_products else []
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
    1. Delete all Universal products (if any exist with invalid sex_target)
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
        cur.execute("SELECT COUNT(*) as count FROM catalog_products")
        total_before = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM catalog_products WHERE LOWER(sex_target) = 'universal'")
        universal_count = cur.fetchone()['count']
        
        cur.execute("""
            SELECT COUNT(*) as count FROM catalog_products
            WHERE product_name LIKE 'MAXima² %'
               OR product_name LIKE 'MAXimo² %'
               OR product_name LIKE 'GenoMAX² %'
        """)
        prefixed_count = cur.fetchone()['count']
        
        # Store deleted products for audit
        cur.execute("""
            SELECT gx_catalog_id, product_name, sex_target, evidence_tier
            FROM catalog_products
            WHERE LOWER(sex_target) = 'universal'
        """)
        deleted_products = [dict(r) for r in cur.fetchall()]
        
        # 1. DELETE Universal products (if any)
        cur.execute("DELETE FROM catalog_products WHERE LOWER(sex_target) = 'universal'")
        deleted_count = cur.rowcount
        
        # 2. STRIP brand prefixes
        cur.execute("""
            UPDATE catalog_products
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
        cur.execute("SELECT COUNT(*) as count FROM catalog_products")
        total_after = cur.fetchone()['count']
        
        # Verify no prefixes remain
        cur.execute("""
            SELECT COUNT(*) as count FROM catalog_products
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
            "table": "catalog_products",
            "operations": {
                "universal_deletion": {
                    "products_deleted": deleted_count,
                    "deleted_ids": [p["gx_catalog_id"] for p in deleted_products]
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
        "version": "v1.1",
        "table": "catalog_products",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
