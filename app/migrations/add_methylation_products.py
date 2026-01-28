"""
GenoMAX² Methylation Products Migration
Adds MAXimo² and MAXima² Methylation Support products

Version: 1.1.0
"""

from fastapi import APIRouter, HTTPException
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/migrations", tags=["Migrations"])

DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_db_connection():
    """Get database connection."""
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
    return psycopg2.connect(DATABASE_URL)


@router.post("/run/add-methylation-products")
def run_methylation_migration():
    """Add methylation support products to catalog_products table."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Methylation products with gender-specific targeting
        methylation_products = [
            ('GMAX-M-METH-B', 'MAXimo² Methylation Support', 
             'https://supliful.com/catalog/methylation-b-complex', 
             'Methylation & B-Vitamins', 
             'Active B vitamins for male methylation: methylcobalamin 5000mcg, methylfolate 800mcg, P5P 50mg', 
             29.99, 'TIER_1', 
             '["methylcobalamin", "methylfolate", "b6_p5p"]',
             'male'),
            ('GMAX-F-METH-B', 'MAXima² Methylation Support', 
             'https://supliful.com/catalog/methylation-b-complex', 
             'Methylation & B-Vitamins', 
             'Active B vitamins for female methylation: methylcobalamin 5000mcg, methylfolate 800mcg, P5P 50mg', 
             29.99, 'TIER_1', 
             '["methylcobalamin", "methylfolate", "b6_p5p"]',
             'female'),
        ]
        
        inserted = 0
        updated = 0
        
        for p in methylation_products:
            cur.execute("""
                INSERT INTO catalog_products (
                    gx_catalog_id, product_name, product_url, category, 
                    short_description, base_price, evidence_tier, 
                    governance_status, ingredient_tags, sex_target
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s::jsonb, %s)
                ON CONFLICT (gx_catalog_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_url = EXCLUDED.product_url,
                    category = EXCLUDED.category,
                    short_description = EXCLUDED.short_description,
                    base_price = EXCLUDED.base_price,
                    evidence_tier = EXCLUDED.evidence_tier,
                    ingredient_tags = EXCLUDED.ingredient_tags,
                    sex_target = EXCLUDED.sex_target,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
            """, p)
            result = cur.fetchone()
            if result['inserted']:
                inserted += 1
            else:
                updated += 1
        
        # Get final counts
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2,
                COUNT(*) FILTER (WHERE governance_status = 'ACTIVE') as active,
                COUNT(*) FILTER (WHERE sex_target = 'male') as male,
                COUNT(*) FILTER (WHERE sex_target = 'female') as female,
                COUNT(*) FILTER (WHERE sex_target = 'unisex') as unisex
            FROM catalog_products
        """)
        stats = cur.fetchone()
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": "Methylation products migration completed",
            "changes": {
                "inserted": inserted,
                "updated": updated
            },
            "counts": {
                "total": stats['total'],
                "tier1": stats['tier1'],
                "tier2": stats['tier2'],
                "active": stats['active'],
                "by_sex_target": {
                    "male": stats['male'],
                    "female": stats['female'],
                    "unisex": stats['unisex']
                }
            },
            "executed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@router.get("/status/methylation-products")
def check_methylation_products():
    """Check if methylation products exist in catalog."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT gx_catalog_id, product_name, sex_target, evidence_tier, 
                   governance_status, ingredient_tags
            FROM catalog_products
            WHERE gx_catalog_id IN ('GMAX-M-METH-B', 'GMAX-F-METH-B')
        """)
        products = cur.fetchall()
        
        conn.close()
        
        return {
            "found": len(products),
            "products": products,
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }
