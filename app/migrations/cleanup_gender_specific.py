"""
GenoMAX² Gender-Specific Product Cleanup
Blocks products that should only exist for one gender (append-only compliant)

Version: 1.0.1
- Uses governance_status = 'BLOCKED' instead of DELETE (append-only compliance)
- GMAX-F-SAW-PALM: Saw Palmetto is for prostate health (male anatomy only)
"""

from fastapi import APIRouter
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/migrations", tags=["Migrations"])

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Products that should be MALE-ONLY (block female versions)
MALE_ONLY_PRODUCTS = [
    'GMAX-F-SAW-PALM',  # Saw Palmetto - prostate health, male anatomy only
]

# Products that should be FEMALE-ONLY (block male versions)
FEMALE_ONLY_PRODUCTS = [
    # GMAX-M-HORMONE doesn't exist (correctly)
    # Add any future female-only products here
]


@router.post("/run/cleanup-gender-specific")
def run_gender_cleanup():
    """Block products that shouldn't exist for a specific gender (append-only compliant)."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        blocked = []
        errors = []
        
        # Block incorrect female versions of male-only products
        for sku in MALE_ONLY_PRODUCTS:
            cur.execute("""
                UPDATE catalog_products 
                SET governance_status = 'BLOCKED',
                    updated_at = NOW()
                WHERE gx_catalog_id = %s
                  AND governance_status != 'BLOCKED'
                RETURNING gx_catalog_id, product_name
            """, (sku,))
            result = cur.fetchone()
            if result:
                blocked.append({
                    "sku": result['gx_catalog_id'],
                    "name": result['product_name'],
                    "reason": "Male-only product (prostate/testosterone) - female version blocked"
                })
        
        # Block incorrect male versions of female-only products
        for sku in FEMALE_ONLY_PRODUCTS:
            cur.execute("""
                UPDATE catalog_products 
                SET governance_status = 'BLOCKED',
                    updated_at = NOW()
                WHERE gx_catalog_id = %s
                  AND governance_status != 'BLOCKED'
                RETURNING gx_catalog_id, product_name
            """, (sku,))
            result = cur.fetchone()
            if result:
                blocked.append({
                    "sku": result['gx_catalog_id'],
                    "name": result['product_name'],
                    "reason": "Female-only product (hormone/menstrual) - male version blocked"
                })
        
        # Get updated counts (only ACTIVE products)
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-M-%' AND governance_status = 'ACTIVE') as maximo,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-F-%' AND governance_status = 'ACTIVE') as maxima,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-U-%' AND governance_status = 'ACTIVE') as universal,
                COUNT(*) FILTER (WHERE governance_status = 'BLOCKED') as total_blocked
            FROM catalog_products
        """)
        stats = cur.fetchone()
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": "Gender-specific cleanup completed (append-only compliant)",
            "blocked": blocked,
            "blocked_count": len(blocked),
            "catalog_totals": {
                "maximo_products": stats['maximo'],
                "maxima_products": stats['maxima'],
                "universal_products": stats['universal'],
                "total_blocked_products": stats['total_blocked']
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
        
        # Check if any male-only products have ACTIVE female versions
        for sku in MALE_ONLY_PRODUCTS:
            cur.execute("""
                SELECT gx_catalog_id, product_name, governance_status 
                FROM catalog_products 
                WHERE gx_catalog_id = %s AND governance_status = 'ACTIVE'
            """, (sku,))
            result = cur.fetchone()
            if result:
                issues.append({
                    "sku": sku,
                    "name": result['product_name'],
                    "issue": "Female version exists for male-only product"
                })
        
        # Check if any female-only products have ACTIVE male versions
        for sku in FEMALE_ONLY_PRODUCTS:
            cur.execute("""
                SELECT gx_catalog_id, product_name, governance_status 
                FROM catalog_products 
                WHERE gx_catalog_id = %s AND governance_status = 'ACTIVE'
            """, (sku,))
            result = cur.fetchone()
            if result:
                issues.append({
                    "sku": sku,
                    "name": result['product_name'],
                    "issue": "Male version exists for female-only product"
                })
        
        # Get already blocked gender-specific products
        cur.execute("""
            SELECT gx_catalog_id, product_name 
            FROM catalog_products 
            WHERE governance_status = 'BLOCKED'
              AND (gx_catalog_id = ANY(%s) OR gx_catalog_id = ANY(%s))
        """, (MALE_ONLY_PRODUCTS, FEMALE_ONLY_PRODUCTS))
        already_blocked = [{"sku": r['gx_catalog_id'], "name": r['product_name']} for r in cur.fetchall()]
        
        conn.close()
        
        return {
            "status": "clean" if not issues else "needs_cleanup",
            "issues": issues,
            "issue_count": len(issues),
            "already_blocked": already_blocked,
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}
