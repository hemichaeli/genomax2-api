"""
GenoMAX² Gender-Specific Products Migration
Adds MAXimo², MAXima², and Universal GenoMAX² products to catalog_products

Version: 1.0.0
Products Added: 20 (10 MAXimo², 8 MAXima², 2 Universal)
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


# === PRODUCT DEFINITIONS ===
# Format: (sku, name, url, category, description, price, tier, ingredient_tags, sex_target)

MAXIMO_PRODUCTS = [
    ('GMAX-M-VD5K', 'MAXimo² Vitamin D3 5000 IU',
     'https://genomax2.ai/products/maximo-vitamin-d3',
     'Vitamins & Minerals',
     'Male-optimized Vitamin D3 5000 IU for bone health, immune function, and testosterone support',
     24.99, 'TIER_1',
     '["vitamin_d3", "cholecalciferol"]',
     'male'),
    
    ('GMAX-M-O3-2000', 'MAXimo² Omega-3 2000mg EPA/DHA',
     'https://genomax2.ai/products/maximo-omega3',
     'Fish Oil & Omega Fatty Acids',
     'Male-optimized Omega-3 with 1200mg EPA + 800mg DHA for cardiovascular and cognitive health',
     34.99, 'TIER_1',
     '["omega_3", "epa", "dha", "fish_oil"]',
     'male'),
    
    ('GMAX-M-MG400', 'MAXimo² Magnesium Glycinate 400mg',
     'https://genomax2.ai/products/maximo-magnesium',
     'Vitamins & Minerals',
     'Male-optimized highly absorbable magnesium glycinate for muscle recovery, sleep, and stress',
     22.99, 'TIER_1',
     '["magnesium", "magnesium_glycinate"]',
     'male'),
    
    ('GMAX-M-TEST', 'MAXimo² Testosterone Support',
     'https://genomax2.ai/products/maximo-testosterone',
     'Sexual & Reproductive Wellness',
     'Male hormone optimization with zinc, D-aspartic acid, fenugreek, and ashwagandha-free formula',
     39.99, 'TIER_2',
     '["zinc", "fenugreek", "d_aspartic_acid", "tongkat_ali"]',
     'male'),
    
    ('GMAX-M-METAB', 'MAXimo² Metabolic Support',
     'https://genomax2.ai/products/maximo-metabolic',
     'Weight Management',
     'Male metabolic optimization with berberine, chromium, and alpha-lipoic acid',
     44.99, 'TIER_1',
     '["berberine", "chromium", "alpha_lipoic_acid"]',
     'male'),
    
    ('GMAX-M-CARDIO', 'MAXimo² Cardiovascular Support',
     'https://genomax2.ai/products/maximo-cardiovascular',
     'Specialty Supplements',
     'Male cardiovascular support with CoQ10 ubiquinol, omega-3, and hawthorn berry',
     49.99, 'TIER_1',
     '["coq10", "ubiquinol", "omega_3", "hawthorn"]',
     'male'),
    
    ('GMAX-M-IRON', 'MAXimo² Iron Complex',
     'https://genomax2.ai/products/maximo-iron',
     'Vitamins & Minerals',
     'Male-dosed iron bisglycinate with vitamin C for enhanced absorption - lower dose for male needs',
     19.99, 'TIER_1',
     '["iron", "vitamin_c"]',
     'male'),
    
    ('GMAX-M-THYROID', 'MAXimo² Thyroid Support',
     'https://genomax2.ai/products/maximo-thyroid',
     'Specialty Supplements',
     'Male thyroid optimization with selenium, iodine, zinc, and L-tyrosine',
     24.99, 'TIER_2',
     '["selenium", "iodine", "zinc", "l_tyrosine"]',
     'male'),
    
    ('GMAX-M-LIVER', 'MAXimo² Liver Detox',
     'https://genomax2.ai/products/maximo-liver',
     'Specialty Supplements',
     'Male liver support with milk thistle, NAC, and alpha-lipoic acid',
     34.99, 'TIER_2',
     '["milk_thistle", "silymarin", "nac", "alpha_lipoic_acid"]',
     'male'),
    
    ('GMAX-M-ADRENAL', 'MAXimo² Adrenal Support',
     'https://genomax2.ai/products/maximo-adrenal',
     'Specialty Supplements',
     'Male stress adaptation with rhodiola, eleuthero, and pantothenic acid - ashwagandha-free',
     39.99, 'TIER_1',
     '["rhodiola", "rhodiola_rosea", "eleuthero", "pantothenic_acid"]',
     'male'),
]

MAXIMA_PRODUCTS = [
    ('GMAX-F-VD5K', 'MAXima² Vitamin D3 5000 IU',
     'https://genomax2.ai/products/maxima-vitamin-d3',
     'Vitamins & Minerals',
     'Female-optimized Vitamin D3 5000 IU for bone health, immune function, and hormonal balance',
     24.99, 'TIER_1',
     '["vitamin_d3", "cholecalciferol"]',
     'female'),
    
    ('GMAX-F-O3-2000', 'MAXima² Omega-3 2000mg EPA/DHA',
     'https://genomax2.ai/products/maxima-omega3',
     'Fish Oil & Omega Fatty Acids',
     'Female-optimized Omega-3 with 1200mg EPA + 800mg DHA for cardiovascular, brain, and skin health',
     34.99, 'TIER_1',
     '["omega_3", "epa", "dha", "fish_oil"]',
     'female'),
    
    ('GMAX-F-MG400', 'MAXima² Magnesium Glycinate 400mg',
     'https://genomax2.ai/products/maxima-magnesium',
     'Vitamins & Minerals',
     'Female-optimized magnesium glycinate for PMS support, sleep, and stress management',
     22.99, 'TIER_1',
     '["magnesium", "magnesium_glycinate"]',
     'female'),
    
    ('GMAX-F-HORMONE', 'MAXima² Hormone Balance',
     'https://genomax2.ai/products/maxima-hormone',
     'Sexual & Reproductive Wellness',
     'Female hormone optimization with DIM, vitex, maca, and B6 for cycle support',
     39.99, 'TIER_2',
     '["dim", "vitex", "maca", "vitamin_b6"]',
     'female'),
    
    ('GMAX-F-IRON', 'MAXima² Iron Complex',
     'https://genomax2.ai/products/maxima-iron',
     'Vitamins & Minerals',
     'Female-dosed iron bisglycinate with vitamin C and folate - optimized for menstruating women',
     19.99, 'TIER_1',
     '["iron", "vitamin_c", "folate"]',
     'female'),
    
    ('GMAX-F-METAB', 'MAXima² Metabolic Support',
     'https://genomax2.ai/products/maxima-metabolic',
     'Weight Management',
     'Female metabolic optimization with berberine, inositol, and chromium for PCOS support',
     44.99, 'TIER_1',
     '["berberine", "inositol", "chromium"]',
     'female'),
    
    ('GMAX-F-THYROID', 'MAXima² Thyroid Support',
     'https://genomax2.ai/products/maxima-thyroid',
     'Specialty Supplements',
     'Female thyroid optimization with selenium, iodine, zinc, and ashwagandha-free formula',
     24.99, 'TIER_2',
     '["selenium", "iodine", "zinc", "l_tyrosine"]',
     'female'),
    
    ('GMAX-F-ADRENAL', 'MAXima² Adrenal Support',
     'https://genomax2.ai/products/maxima-adrenal',
     'Specialty Supplements',
     'Female stress adaptation with rhodiola, holy basil, and B-vitamins - ashwagandha-free',
     39.99, 'TIER_1',
     '["rhodiola", "rhodiola_rosea", "holy_basil", "vitamin_b5"]',
     'female'),
]

UNIVERSAL_PRODUCTS = [
    ('GMAX-U-PROBIOTIC', 'GenoMAX² Probiotic 50B',
     'https://genomax2.ai/products/genomax-probiotic',
     'Digestive Support',
     'High-potency 50 billion CFU multi-strain probiotic for gut health and immune function',
     34.99, 'TIER_1',
     '["probiotics", "lactobacillus", "bifidobacterium"]',
     'unisex'),
    
    ('GMAX-U-COQ10', 'GenoMAX² CoQ10 Ubiquinol 200mg',
     'https://genomax2.ai/products/genomax-coq10',
     'Specialty Supplements',
     'Premium ubiquinol 200mg - the active form of CoQ10 for cellular energy and heart health',
     44.99, 'TIER_1',
     '["coq10", "ubiquinol"]',
     'unisex'),
]

ALL_PRODUCTS = MAXIMO_PRODUCTS + MAXIMA_PRODUCTS + UNIVERSAL_PRODUCTS


@router.post("/run/add-gender-specific-products")
def run_gender_products_migration():
    """Add all MAXimo², MAXima², and Universal GenoMAX² products to catalog_products."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        inserted = 0
        updated = 0
        skipped = []
        
        for p in ALL_PRODUCTS:
            # Skip methylation products (already exist)
            if p[0] in ('GMAX-M-METH-B', 'GMAX-F-METH-B'):
                skipped.append(p[0])
                continue
                
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
                COUNT(*) FILTER (WHERE sex_target = 'unisex') as unisex,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-%') as genomax_branded
            FROM catalog_products
        """)
        stats = cur.fetchone()
        
        # Get product line breakdown
        cur.execute("""
            SELECT 
                CASE 
                    WHEN gx_catalog_id LIKE 'GMAX-M-%' THEN 'MAXimo²'
                    WHEN gx_catalog_id LIKE 'GMAX-F-%' THEN 'MAXima²'
                    WHEN gx_catalog_id LIKE 'GMAX-U-%' THEN 'Universal GenoMAX²'
                    ELSE 'Universal'
                END as product_line,
                COUNT(*) as count
            FROM catalog_products
            GROUP BY 1
            ORDER BY 1
        """)
        product_lines = {r['product_line']: r['count'] for r in cur.fetchall()}
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": "Gender-specific products migration completed",
            "migration_version": "1.0.0",
            "changes": {
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped
            },
            "catalog_totals": {
                "total_products": stats['total'],
                "tier1": stats['tier1'],
                "tier2": stats['tier2'],
                "active": stats['active'],
                "genomax_branded": stats['genomax_branded']
            },
            "by_sex_target": {
                "male": stats['male'],
                "female": stats['female'],
                "unisex": stats['unisex']
            },
            "by_product_line": product_lines,
            "executed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@router.get("/status/gender-specific-products")
def check_gender_products():
    """Check status of all GMAX-* products in catalog."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT gx_catalog_id, product_name, sex_target, evidence_tier, 
                   category, governance_status, base_price
            FROM catalog_products
            WHERE gx_catalog_id LIKE 'GMAX-%'
            ORDER BY 
                CASE 
                    WHEN gx_catalog_id LIKE 'GMAX-M-%' THEN 1
                    WHEN gx_catalog_id LIKE 'GMAX-F-%' THEN 2
                    ELSE 3
                END,
                gx_catalog_id
        """)
        products = cur.fetchall()
        
        # Group by product line
        maximo = [p for p in products if p['gx_catalog_id'].startswith('GMAX-M-')]
        maxima = [p for p in products if p['gx_catalog_id'].startswith('GMAX-F-')]
        universal = [p for p in products if p['gx_catalog_id'].startswith('GMAX-U-')]
        
        conn.close()
        
        return {
            "total_genomax_products": len(products),
            "by_product_line": {
                "MAXimo²": {
                    "count": len(maximo),
                    "products": [p['gx_catalog_id'] for p in maximo]
                },
                "MAXima²": {
                    "count": len(maxima),
                    "products": [p['gx_catalog_id'] for p in maxima]
                },
                "Universal": {
                    "count": len(universal),
                    "products": [p['gx_catalog_id'] for p in universal]
                }
            },
            "full_details": products,
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@router.get("/preview/gender-specific-products")
def preview_migration():
    """Preview what products will be added without executing."""
    return {
        "migration": "add-gender-specific-products",
        "version": "1.0.0",
        "will_add": {
            "MAXimo²": [p[0] for p in MAXIMO_PRODUCTS],
            "MAXima²": [p[0] for p in MAXIMA_PRODUCTS],
            "Universal": [p[0] for p in UNIVERSAL_PRODUCTS]
        },
        "total_products": len(ALL_PRODUCTS),
        "note": "GMAX-M-METH-B and GMAX-F-METH-B already exist - will skip",
        "products": [
            {
                "sku": p[0],
                "name": p[1],
                "category": p[3],
                "tier": p[6],
                "sex_target": p[8],
                "ingredient_tags": p[7]
            }
            for p in ALL_PRODUCTS
        ]
    }
