"""
GenoMAX² Full Catalog Gender Conversion Migration
Converts all GX-* products to GMAX-M-*/GMAX-F-* gender-specific pairs

Version: 2.0.2
Fixed: Properly handle psycopg2 JSONB returns (they come as lists)
Converts: 65 GX-* products -> 130 GMAX-* products (M+F pairs)
Result: Every product available as MAXimo² and MAXima²
"""

from fastapi import APIRouter, HTTPException
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import json

router = APIRouter(prefix="/api/v1/migrations", tags=["Migrations"])

DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_db_connection():
    """Get database connection."""
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
    return psycopg2.connect(DATABASE_URL)


# === SKU MAPPING: GX-* -> Meaningful GMAX code ===
# Format: old_sku -> (new_code, male_description, female_description)

SKU_MAPPING = {
    # TIER 1 Products (17)
    'GX-T1-001': ('COLLAGEN', 'Male-optimized collagen for skin, joints, and muscle recovery', 'Female-optimized collagen for skin elasticity, hair, and nail health'),
    'GX-T1-002': ('CREATINE', 'Male-optimized creatine for muscle strength and athletic performance', 'Female-optimized creatine for lean muscle and energy'),
    'GX-T1-003': ('DENTAL', 'Male-optimized oral health support', 'Female-optimized oral health support'),
    'GX-T1-004': ('MG-GLY', 'Male-optimized magnesium glycinate for muscle recovery and sleep', 'Female-optimized magnesium glycinate for PMS, sleep, and stress'),
    'GX-T1-005': ('PROB-META', 'Male-optimized probiotic strips for metabolism', 'Female-optimized probiotic strips for metabolism'),
    'GX-T1-006': ('VIT-C-SER', 'Male-optimized vitamin C serum for skin health', 'Female-optimized vitamin C serum for brightening and anti-aging'),
    'GX-T1-007': ('BERBERINE', 'Male-optimized berberine for blood sugar and metabolic health', 'Female-optimized berberine for blood sugar, PCOS, and metabolic support'),
    'GX-T1-008': ('PROB-20B', 'Male-optimized 20 billion CFU probiotic for gut health', 'Female-optimized 20 billion CFU probiotic for gut and vaginal health'),
    'GX-T1-009': ('IRON-STR', 'Male-dosed iron strips for lower male iron needs', 'Female-dosed iron strips optimized for menstrual support'),
    'GX-T1-010': ('MULTI', 'Male-optimized complete multivitamin with prostate support', 'Female-optimized complete multivitamin with iron and folate'),
    'GX-T1-011': ('OMEGA3', 'Male-optimized omega-3 for cardiovascular and brain health', 'Female-optimized omega-3 for heart, brain, and skin'),
    'GX-T1-012': ('VD3K2', 'Male-optimized vitamin D3+K2 for testosterone and bone health', 'Female-optimized vitamin D3+K2 for bone density and mood'),
    'GX-T1-013': ('COQ10', 'Male-optimized CoQ10 ubiquinol for heart and energy', 'Female-optimized CoQ10 ubiquinol for cellular energy and fertility'),
    'GX-T1-014': ('ZINC', 'Male-optimized zinc for testosterone and immune function', 'Female-optimized zinc for immune and skin health'),
    'GX-T1-015': ('MELATONIN', 'Male-optimized melatonin for sleep and recovery', 'Female-optimized melatonin for sleep and hormonal balance'),
    'GX-T1-016': ('CURCUMIN', 'Male-optimized curcumin for inflammation and joint health', 'Female-optimized curcumin for inflammation and menstrual comfort'),
    'GX-T1-017': ('RHODIOLA', 'Male-optimized rhodiola for stress and endurance', 'Female-optimized rhodiola for stress and energy'),
    
    # TIER 2 Products (48)
    'GX-T2-001': ('DIGEST-ENZ', 'Male-optimized digestive enzyme blend', 'Female-optimized digestive enzyme blend'),
    'GX-T2-002': ('MUSHROOM', 'Male-optimized mushroom extract complex for immunity', 'Female-optimized mushroom extract complex for immunity'),
    'GX-T2-003': ('LIONS-MANE', 'Male-optimized lions mane for cognitive focus', 'Female-optimized lions mane for cognitive clarity'),
    'GX-T2-004': ('REISHI', 'Male-optimized reishi for calm and immune support', 'Female-optimized reishi for stress and immune balance'),
    'GX-T2-005': ('CORDYCEPS', 'Male-optimized cordyceps for energy and athletic performance', 'Female-optimized cordyceps for energy and stamina'),
    'GX-T2-006': ('COLLAGEN-PEP', 'Male-optimized collagen peptides for joints and recovery', 'Female-optimized collagen peptides for skin and hair'),
    'GX-T2-007': ('GLUC-CHON', 'Male-optimized glucosamine chondroitin for joints', 'Female-optimized glucosamine chondroitin for joint flexibility'),
    'GX-T2-008': ('MILK-THISTLE', 'Male-optimized milk thistle for liver support', 'Female-optimized milk thistle for liver and hormonal detox'),
    'GX-T2-009': ('BACOPA', 'Male-optimized bacopa for memory and focus', 'Female-optimized bacopa for memory and mental clarity'),
    'GX-T2-010': ('MACA', 'Male-optimized maca for energy and libido', 'Female-optimized maca for hormone balance and energy'),
    'GX-T2-011': ('SAW-PALM', 'Male-optimized saw palmetto for prostate health', 'Female-optimized saw palmetto for hormonal balance'),
    'GX-T2-012': ('VALERIAN', 'Male-optimized valerian for sleep support', 'Female-optimized valerian for sleep and anxiety'),
    'GX-T2-013': ('ELDERBERRY', 'Male-optimized elderberry for immune defense', 'Female-optimized elderberry for immune support'),
    'GX-T2-014': ('ECHINACEA', 'Male-optimized echinacea for immune response', 'Female-optimized echinacea for immune health'),
    'GX-T2-015': ('GARLIC', 'Male-optimized garlic extract for cardiovascular health', 'Female-optimized garlic extract for heart and immunity'),
    'GX-T2-016': ('RESVERATROL', 'Male-optimized resveratrol for longevity and heart', 'Female-optimized resveratrol for anti-aging and heart'),
    'GX-T2-017': ('QUERCETIN', 'Male-optimized quercetin for inflammation and allergies', 'Female-optimized quercetin for inflammation and histamine'),
    'GX-T2-018': ('GREEN-TEA', 'Male-optimized green tea extract for metabolism', 'Female-optimized green tea extract for metabolism and antioxidants'),
    'GX-T2-019': ('LUTEIN', 'Male-optimized lutein zeaxanthin for eye health', 'Female-optimized lutein zeaxanthin for vision'),
    'GX-T2-020': ('L-CARNITINE', 'Male-optimized L-carnitine for fat burning and energy', 'Female-optimized L-carnitine for metabolism and energy'),
    'GX-T2-021': ('TAURINE', 'Male-optimized taurine for heart and athletic performance', 'Female-optimized taurine for heart and stress'),
    'GX-T2-022': ('GLYCINE', 'Male-optimized glycine for sleep and collagen', 'Female-optimized glycine for sleep and skin'),
    'GX-T2-023': ('L-GLUTAMINE', 'Male-optimized L-glutamine for gut and muscle recovery', 'Female-optimized L-glutamine for gut health'),
    'GX-T2-024': ('BETA-GLUCAN', 'Male-optimized beta glucan for immune function', 'Female-optimized beta glucan for immunity'),
    'GX-T2-025': ('SPIRULINA', 'Male-optimized spirulina for energy and nutrition', 'Female-optimized spirulina for iron and nutrition'),
    'GX-T2-026': ('CHLORELLA', 'Male-optimized chlorella for detox and nutrition', 'Female-optimized chlorella for detox and iron'),
    'GX-T2-027': ('INOSITOL', 'Male-optimized inositol for mood and metabolism', 'Female-optimized inositol for PCOS and mood'),
    'GX-T2-028': ('CHOLINE', 'Male-optimized choline for brain and liver', 'Female-optimized choline for brain and prenatal support'),
    'GX-T2-029': ('ALPHA-GPC', 'Male-optimized alpha GPC for cognitive performance', 'Female-optimized alpha GPC for memory and focus'),
    'GX-T2-030': ('NMN', 'Male-optimized NMN for cellular energy and longevity', 'Female-optimized NMN for anti-aging and energy'),
    'GX-T2-031': ('SULFORAPHANE', 'Male-optimized sulforaphane for detox and cellular health', 'Female-optimized sulforaphane for detox and hormones'),
    'GX-T2-032': ('DIM', 'Male-optimized DIM for estrogen metabolism', 'Female-optimized DIM for hormone balance'),
    'GX-T2-033': ('PRE-WORK', 'Male-optimized pre-workout for strength and power', 'Female-optimized pre-workout for energy without jitters'),
    'GX-T2-034': ('BCAA', 'Male-optimized BCAA for muscle recovery', 'Female-optimized BCAA for lean muscle and recovery'),
    'GX-T2-035': ('ELECTRO', 'Male-optimized electrolyte mix for hydration', 'Female-optimized electrolyte mix for hydration'),
    'GX-T2-036': ('GREENS', 'Male-optimized greens superfood for nutrition', 'Female-optimized greens superfood for nutrition and beauty'),
    'GX-T2-037': ('MUSH-COFFEE', 'Male-optimized mushroom coffee for focus', 'Female-optimized mushroom coffee for calm focus'),
    'GX-T2-038': ('MATCHA', 'Male-optimized matcha for energy and antioxidants', 'Female-optimized matcha for metabolism and calm energy'),
    'GX-T2-039': ('HA-SERUM', 'Male-optimized hyaluronic acid for skin hydration', 'Female-optimized hyaluronic acid for plump hydrated skin'),
    'GX-T2-040': ('BIOTIN', 'Male-optimized biotin for hair and nail health', 'Female-optimized biotin for hair growth and nail strength'),
    'GX-T2-041': ('FENUGREEK', 'Male-optimized fenugreek for testosterone support', 'Female-optimized fenugreek for lactation and hormones'),
    'GX-T2-042': ('GINSENG', 'Male-optimized panax ginseng for energy and vitality', 'Female-optimized panax ginseng for energy and balance'),
    'GX-T2-043': ('HOLY-BASIL', 'Male-optimized holy basil for stress adaptation', 'Female-optimized holy basil for stress and hormones'),
    'GX-T2-044': ('PASSION', 'Male-optimized passionflower for relaxation', 'Female-optimized passionflower for anxiety and sleep'),
    'GX-T2-045': ('LEMON-BALM', 'Male-optimized lemon balm for calm and focus', 'Female-optimized lemon balm for mood and relaxation'),
    'GX-T2-046': ('PRE-PUMP', 'Male-optimized pre-workout pump formula', 'Female-optimized pre-workout for performance'),
    'GX-T2-047': ('POST-PROTEIN', 'Male-optimized post-workout protein for muscle', 'Female-optimized post-workout protein for recovery'),
    'GX-T2-048': ('BEE-POLLEN', 'Male-optimized bee pollen for energy and immunity', 'Female-optimized bee pollen for energy and hormones'),
}


def get_product_data(conn, old_sku):
    """Get existing product data from catalog_products."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM catalog_products WHERE gx_catalog_id = %s
    """, (old_sku,))
    return cur.fetchone()


def get_tier_from_sku(old_sku):
    """Extract tier from old SKU."""
    if '-T1-' in old_sku:
        return 'TIER_1'
    elif '-T2-' in old_sku:
        return 'TIER_2'
    return 'TIER_2'


@router.post("/run/convert-to-gender-specific")
def run_gender_conversion():
    """Convert all GX-* products to GMAX-M-*/GMAX-F-* gender-specific pairs."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        inserted_male = 0
        inserted_female = 0
        updated = 0
        errors = []
        
        for old_sku, (new_code, male_desc, female_desc) in SKU_MAPPING.items():
            # Get original product data
            original = get_product_data(conn, old_sku)
            if not original:
                errors.append(f"Source product not found: {old_sku}")
                continue
            
            tier = get_tier_from_sku(old_sku)
            base_name = original['product_name']
            base_price = original.get('base_price') or 29.99
            category = original.get('category') or 'Specialty Supplements'
            
            # Handle ingredient_tags - psycopg2 returns JSONB as Python list
            raw_tags = original.get('ingredient_tags')
            if raw_tags is None:
                ingredient_tags = []
            elif isinstance(raw_tags, list):
                ingredient_tags = raw_tags
            else:
                ingredient_tags = []
            
            # Create MAXimo² (male) version using psycopg2.extras.Json for proper JSONB handling
            male_sku = f"GMAX-M-{new_code}"
            male_name = f"MAXimo² {base_name}"
            
            cur.execute("""
                INSERT INTO catalog_products (
                    gx_catalog_id, product_name, product_url, category,
                    short_description, base_price, evidence_tier,
                    governance_status, ingredient_tags, sex_target
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s, 'male')
                ON CONFLICT (gx_catalog_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    short_description = EXCLUDED.short_description,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
            """, (
                male_sku,
                male_name,
                f"https://genomax2.ai/products/maximo-{new_code.lower().replace('_', '-')}",
                category,
                male_desc,
                base_price,
                tier,
                Json(ingredient_tags)  # Use psycopg2.extras.Json for proper JSONB
            ))
            result = cur.fetchone()
            if result and result['inserted']:
                inserted_male += 1
            else:
                updated += 1
            
            # Create MAXima² (female) version
            female_sku = f"GMAX-F-{new_code}"
            female_name = f"MAXima² {base_name}"
            
            cur.execute("""
                INSERT INTO catalog_products (
                    gx_catalog_id, product_name, product_url, category,
                    short_description, base_price, evidence_tier,
                    governance_status, ingredient_tags, sex_target
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s, 'female')
                ON CONFLICT (gx_catalog_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    short_description = EXCLUDED.short_description,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
            """, (
                female_sku,
                female_name,
                f"https://genomax2.ai/products/maxima-{new_code.lower().replace('_', '-')}",
                category,
                female_desc,
                base_price,
                tier,
                Json(ingredient_tags)  # Use psycopg2.extras.Json for proper JSONB
            ))
            result = cur.fetchone()
            if result and result['inserted']:
                inserted_female += 1
            else:
                updated += 1
        
        # Get final counts
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-M-%') as maximo,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-F-%') as maxima,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GMAX-U-%') as universal,
                COUNT(*) FILTER (WHERE gx_catalog_id LIKE 'GX-%' AND gx_catalog_id NOT LIKE 'GMAX-%') as legacy,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2
            FROM catalog_products
            WHERE governance_status = 'ACTIVE'
        """)
        stats = cur.fetchone()
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": "Gender-specific catalog conversion completed",
            "migration_version": "2.0.2",
            "changes": {
                "maximo_inserted": inserted_male,
                "maxima_inserted": inserted_female,
                "updated": updated,
                "errors": errors
            },
            "catalog_totals": {
                "total_active": stats['total'],
                "maximo_products": stats['maximo'],
                "maxima_products": stats['maxima'],
                "universal_products": stats['universal'],
                "legacy_gx_products": stats['legacy'],
                "tier1": stats['tier1'],
                "tier2": stats['tier2']
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


@router.get("/preview/convert-to-gender-specific")
def preview_gender_conversion():
    """Preview the gender conversion without executing."""
    products = []
    for old_sku, (new_code, male_desc, female_desc) in SKU_MAPPING.items():
        tier = get_tier_from_sku(old_sku)
        products.append({
            "old_sku": old_sku,
            "maximo_sku": f"GMAX-M-{new_code}",
            "maxima_sku": f"GMAX-F-{new_code}",
            "tier": tier,
            "male_description": male_desc[:60] + "..." if len(male_desc) > 60 else male_desc,
            "female_description": female_desc[:60] + "..." if len(female_desc) > 60 else female_desc
        })
    
    tier1_count = len([p for p in products if p['tier'] == 'TIER_1'])
    tier2_count = len([p for p in products if p['tier'] == 'TIER_2'])
    
    return {
        "migration": "convert-to-gender-specific",
        "version": "2.0.2",
        "summary": {
            "source_products": len(SKU_MAPPING),
            "new_maximo_products": len(SKU_MAPPING),
            "new_maxima_products": len(SKU_MAPPING),
            "total_new_products": len(SKU_MAPPING) * 2,
            "tier1_pairs": tier1_count,
            "tier2_pairs": tier2_count
        },
        "conversions": products
    }


@router.get("/status/gender-catalog")
def check_gender_catalog():
    """Check the full gender-specific catalog status."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all GMAX products
        cur.execute("""
            SELECT gx_catalog_id, product_name, sex_target, evidence_tier, category
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
        gmax_products = cur.fetchall()
        
        # Get legacy GX products
        cur.execute("""
            SELECT gx_catalog_id, product_name, evidence_tier
            FROM catalog_products
            WHERE gx_catalog_id LIKE 'GX-%' 
            AND gx_catalog_id NOT LIKE 'GMAX-%'
            ORDER BY gx_catalog_id
        """)
        legacy_products = cur.fetchall()
        
        # Group GMAX by line
        maximo = [p for p in gmax_products if p['gx_catalog_id'].startswith('GMAX-M-')]
        maxima = [p for p in gmax_products if p['gx_catalog_id'].startswith('GMAX-F-')]
        universal = [p for p in gmax_products if p['gx_catalog_id'].startswith('GMAX-U-')]
        
        conn.close()
        
        return {
            "total_gmax_products": len(gmax_products),
            "by_product_line": {
                "MAXimo²": {
                    "count": len(maximo),
                    "tier1": len([p for p in maximo if p['evidence_tier'] == 'TIER_1']),
                    "tier2": len([p for p in maximo if p['evidence_tier'] == 'TIER_2']),
                    "products": [p['gx_catalog_id'] for p in maximo]
                },
                "MAXima²": {
                    "count": len(maxima),
                    "tier1": len([p for p in maxima if p['evidence_tier'] == 'TIER_1']),
                    "tier2": len([p for p in maxima if p['evidence_tier'] == 'TIER_2']),
                    "products": [p['gx_catalog_id'] for p in maxima]
                },
                "Universal": {
                    "count": len(universal),
                    "products": [p['gx_catalog_id'] for p in universal]
                }
            },
            "legacy_products": {
                "count": len(legacy_products),
                "note": "These GX-* products should be deprecated after full conversion",
                "products": [p['gx_catalog_id'] for p in legacy_products]
            },
            "conversion_coverage": f"{len(maximo)}/{len(SKU_MAPPING)} MAXimo², {len(maxima)}/{len(SKU_MAPPING)} MAXima²",
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }
