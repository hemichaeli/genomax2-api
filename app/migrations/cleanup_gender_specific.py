"""
GenoMAX² Gender-Specific Product Cleanup
Removes/blocks products that should only exist for one gender

Version: 1.0.0
- GMAX-F-SAW-PALM: Saw Palmetto is for prostate health (male anatomy only)
"""

from fastapi import APIRouter
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/migrations", tags=["Migrations"])

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Products that should be MALE-ONLY (remove female versions)
MALE_ONLY_PRODUCTS = [
    'GMAX-F-SAW-PALM',  # Saw Palmetto - prostate health, male anatomy only
]

# Products that should be FEMALE-ONLY (remove male versions)
FEMALE_ONLY_PRODUCTS = [
    # GMAX-M-HORMONE doesn't exist (correctly)
    # Add any future female-only products here
]


@router.post("/run/cleanup-gender-specific")
def run_gender_cleanup():
    """Remove products that shouldn't exist for a specific gender."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        removed = []
        errors = []
        
        # Remove incorrect female versions of male-only products
        for sku in MALE_ONLY_PRODUCTS:
            cur.execute("""
                DELETE FROM catalog_products 
                WHERE gx_catalog_id = %s
                RETURNING gx_catalog_id, product_name
            """, (sku,))
            result = cur.fetchone()
            if result:
                removed.append({
                    "sku": result['gx_catalog_id'],
                    "name": result['product_name'],
                    "reason": "Male-only product (prostate/testosterone)"
                })
        
        # Remove incorrect male versions of female-only products
        for sku in FEMALE_ONLY_PRODUCTS:
            cur.execute("""
                DELETE FROM catalog_products 
                WHERE gx_catalog_id = %s
                RETURNING gx_catalog_id, product_name
            """, (sku,))
            result = cur.fetchone()
            if result:
                removed.append({
                    "sku": result['gx_catalog_id'],
                    "name": result['product_name'],
                    "reason": "Female-only product (hormone/menstrual)"
                })
        
        # Get updated counts
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-M-%') as maximo,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-F-%') as maxima,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-U-%') as universal
            FROM catalog_products
            WHERE governance_status = 'ACTIVE'
        """)
        stats = cur.fetchone()
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": "Gender-specific cleanup completed",
            "removed": removed,
            "removed_count": len(removed),
            "catalog_totals": {
                "maximo_products": stats['maximo'],
                "maxima_products": stats['maxima'],
                "universal_products": stats['universal']
            },
            "executed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }


@router.get("/status/gender-specific-cleanup")
def check_gender_cleanup():
    """Check for products that should be gender-specific only."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        issues = []
        
        # Check if any male-only products have female versions
        for sku in MALE_ONLY_PRODUCTS:
            cur.execute("SELECT gx_catalog_id, product_name FROM catalog_products WHERE gx_catalog_id = %s", (sku,))
            result = cur.fetchone()
            if result:
                issues.append({
                    "sku": sku,
                    "name": result['product_name'],
                    "issue": "Female version exists for male-only product"
                })
        
        # Check if any female-only products have male versions
        for sku in FEMALE_ONLY_PRODUCTS:
            cur.execute("SELECT gx_catalog_id, product_name FROM catalog_products WHERE gx_catalog_id = %s", (sku,))
            result = cur.fetchone()
            if result:
                issues.append({
                    "sku": sku,
                    "name": result['product_name'],
                    "issue": "Male version exists for female-only product"
                })
        
        conn.close()
        
        return {
            "status": "clean" if not issues else "needs_cleanup",
            "issues": issues,
            "issue_count": len(issues),
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}
