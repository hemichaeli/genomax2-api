"""
GenoMAX² Catalog Import Migration Runner
Executes the TIER 1/2 evidence-based catalog migration

Version: 1.0.0
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


@router.get("/status/catalog-products")
def check_catalog_migration():
    """Check status of catalog_products migration."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'catalog_products'
            ) as table_exists
        """)
        table_exists = cur.fetchone()['table_exists']
        
        if not table_exists:
            return {
                "status": "not_applied",
                "table_exists": False,
                "message": "catalog_products table does not exist"
            }
        
        # Get counts
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2,
                COUNT(*) FILTER (WHERE governance_status = 'ACTIVE') as active
            FROM catalog_products
        """)
        stats = cur.fetchone()
        
        conn.close()
        
        return {
            "status": "applied" if stats['total'] > 0 else "empty",
            "table_exists": True,
            "counts": {
                "total": stats['total'],
                "tier1": stats['tier1'],
                "tier2": stats['tier2'],
                "active": stats['active']
            },
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@router.post("/run/catalog-products")
def run_catalog_migration():
    """Execute the catalog_products migration with TIER 1/2 products."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS catalog_products (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                gx_catalog_id VARCHAR(50) UNIQUE NOT NULL,
                product_name VARCHAR(255) NOT NULL,
                product_url VARCHAR(500),
                category VARCHAR(100),
                sub_category VARCHAR(100),
                short_description TEXT,
                serving_info TEXT,
                base_price DECIMAL(10,2),
                currency VARCHAR(10) DEFAULT 'USD',
                evidence_tier VARCHAR(20) NOT NULL CHECK (evidence_tier IN ('TIER_1', 'TIER_2', 'TIER_3')),
                governance_status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (governance_status IN ('ACTIVE', 'BLOCKED', 'PENDING', 'SUSPENDED')),
                block_reason TEXT,
                ingredient_tags JSONB DEFAULT '[]'::jsonb,
                category_tags JSONB DEFAULT '[]'::jsonb,
                sex_target VARCHAR(20) DEFAULT 'unisex' CHECK (sex_target IN ('male', 'female', 'unisex')),
                source_version VARCHAR(50) DEFAULT 'catalog_import_v1.0',
                shopify_enabled BOOLEAN DEFAULT TRUE,
                amazon_enabled BOOLEAN DEFAULT FALSE,
                tiktok_enabled BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        
        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_catalog_evidence_tier ON catalog_products(evidence_tier)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_catalog_governance_status ON catalog_products(governance_status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_catalog_category ON catalog_products(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_catalog_ingredient_tags ON catalog_products USING GIN(ingredient_tags)")
        
        # Create audit trigger
        cur.execute("""
            CREATE OR REPLACE FUNCTION catalog_products_audit()
            RETURNS TRIGGER AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'DELETE not allowed on catalog_products (append-only governance)';
                END IF;
                IF TG_OP = 'UPDATE' THEN
                    NEW.updated_at = NOW();
                    IF OLD.gx_catalog_id != NEW.gx_catalog_id THEN
                        RAISE EXCEPTION 'Cannot modify gx_catalog_id (immutable)';
                    END IF;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """)
        
        cur.execute("DROP TRIGGER IF EXISTS catalog_products_audit_trigger ON catalog_products")
        cur.execute("""
            CREATE TRIGGER catalog_products_audit_trigger
            BEFORE UPDATE OR DELETE ON catalog_products
            FOR EACH ROW EXECUTE FUNCTION catalog_products_audit()
        """)
        
        # Insert TIER 1 products (17)
        tier1_products = [
            ('GX-T1-001', 'Beauty + Collagen Strips', 'https://supliful.com/catalog/beauty-collagen-strips', 'Skin, Hair & Nail Health', 'Mango-flavored oral strip combining Collagen Peptides and Vitamin E', 8.03, 'TIER_1', '["collagen", "vitamin_e"]'),
            ('GX-T1-002', 'Creatine Monohydrate', 'https://supliful.com/catalog/creatine-monohydrate-powder', 'Muscle Builders', 'Creatine Monohydrate powder for muscle building and athletic performance', 12.65, 'TIER_1', '["creatine", "creatine_monohydrate"]'),
            ('GX-T1-003', 'Dental + Oral Health Chewables', 'https://supliful.com/catalog/dental-oral-health-chewables', 'Specialty Supplements', 'Probiotics, fruit powder, xylitol, and hydroxyapatite', 5.55, 'TIER_1', '["probiotics", "xylitol"]'),
            ('GX-T1-004', 'Magnesium Glycinate', 'https://supliful.com/catalog/magnesium-glycinate-capsules', 'Vitamins & Minerals', 'Magnesium Glycinate for muscle relaxation and sleep quality', 8.89, 'TIER_1', '["magnesium", "magnesium_glycinate"]'),
            ('GX-T1-005', 'Probiotic + Metabolism Strips', 'https://supliful.com/catalog/probiotic-metabolism-strips', 'Digestive Support', 'Probiotic strips for gut health and metabolism support', 9.12, 'TIER_1', '["probiotics", "lactobacillus", "bifidobacterium"]'),
            ('GX-T1-006', 'Vitamin C Serum', 'https://supliful.com/catalog/vitamin-c-serum', 'Facial Care', 'Vitamin C Serum for skin brightening and protection', 7.45, 'TIER_1', '["vitamin_c"]'),
            ('GX-T1-007', 'Berberine', 'https://supliful.com/catalog/berberine-capsules', 'Weight Management', 'Berberine supplement for metabolic health', 7.55, 'TIER_1', '["berberine"]'),
            ('GX-T1-008', 'Probiotic 20 Billion', 'https://supliful.com/catalog/probiotic-20-billion', 'Digestive Support', 'High-potency probiotic with 20 billion CFU', 6.95, 'TIER_1', '["probiotics", "lactobacillus", "bifidobacterium"]'),
            ('GX-T1-009', 'Iron Strips', 'https://supliful.com/catalog/iron-strips', 'Vitamins & Minerals', 'Iron supplement strips for anemia support', 8.72, 'TIER_1', '["iron"]'),
            ('GX-T1-010', 'Complete Multivitamin', 'https://supliful.com/catalog/complete-multivitamin', 'Vitamins & Minerals', 'Comprehensive multivitamin with essential nutrients', 7.89, 'TIER_1', '["vitamin_d3", "vitamin_b12", "folate", "zinc", "vitamin_c", "vitamin_e", "vitamin_a", "selenium"]'),
            ('GX-T1-011', 'Omega-3 Fish Oil', 'https://supliful.com/catalog/omega-3-fish-oil', 'Fish Oil & Omega Fatty Acids', 'High-quality fish oil with EPA and DHA', 9.50, 'TIER_1', '["omega_3", "epa", "dha", "fish_oil"]'),
            ('GX-T1-012', 'Vitamin D3 + K2', 'https://supliful.com/catalog/vitamin-d3-k2', 'Vitamins & Minerals', 'Vitamin D3 with K2 for bone and cardiovascular health', 8.25, 'TIER_1', '["vitamin_d3", "vitamin_k2"]'),
            ('GX-T1-013', 'CoQ10 Ubiquinol', 'https://supliful.com/catalog/coq10-ubiquinol', 'Specialty Supplements', 'Ubiquinol form of CoQ10 for heart health', 12.50, 'TIER_1', '["coq10", "ubiquinol"]'),
            ('GX-T1-014', 'Zinc Picolinate', 'https://supliful.com/catalog/zinc-picolinate', 'Vitamins & Minerals', 'Highly absorbable zinc for immune support', 6.75, 'TIER_1', '["zinc"]'),
            ('GX-T1-015', 'Melatonin Sleep Support', 'https://supliful.com/catalog/melatonin-sleep', 'Specialty Supplements', 'Melatonin for natural sleep support', 5.99, 'TIER_1', '["melatonin"]'),
            ('GX-T1-016', 'Curcumin + Black Pepper', 'https://supliful.com/catalog/curcumin-black-pepper', 'Specialty Supplements', 'Curcumin with piperine for enhanced absorption', 9.25, 'TIER_1', '["curcumin", "turmeric"]'),
            ('GX-T1-017', 'Rhodiola Rosea', 'https://supliful.com/catalog/rhodiola-rosea', 'Specialty Supplements', 'Rhodiola adaptogen for stress and fatigue', 8.50, 'TIER_1', '["rhodiola", "rhodiola_rosea"]'),
        ]
        
        for p in tier1_products:
            cur.execute("""
                INSERT INTO catalog_products (gx_catalog_id, product_name, product_url, category, short_description, base_price, evidence_tier, governance_status, ingredient_tags)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s::jsonb)
                ON CONFLICT (gx_catalog_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_url = EXCLUDED.product_url,
                    category = EXCLUDED.category,
                    short_description = EXCLUDED.short_description,
                    base_price = EXCLUDED.base_price,
                    evidence_tier = EXCLUDED.evidence_tier,
                    ingredient_tags = EXCLUDED.ingredient_tags,
                    updated_at = NOW()
            """, p)
        
        # Insert TIER 2 products (48)
        tier2_products = [
            ('GX-T2-001', 'Digestive Enzyme Pro Blend', 'https://supliful.com/catalog/digestive-enzyme-blend', 'Digestive Support', 'Comprehensive digestive enzyme blend', 6.95, 'TIER_2', '["digestive_enzymes", "bromelain", "papain"]'),
            ('GX-T2-002', 'Mushroom Extract Complex', 'https://supliful.com/catalog/mushroom-extract-complex', 'Mushroom Products', 'Blend of Lions Mane, Reishi, and Cordyceps', 9.15, 'TIER_2', '["lions_mane", "reishi", "cordyceps"]'),
            ('GX-T2-003', 'Lions Mane Focus', 'https://supliful.com/catalog/lions-mane-focus', 'Mushroom Products', 'Lions Mane for cognitive support', 8.50, 'TIER_2', '["lions_mane"]'),
            ('GX-T2-004', 'Reishi Calm', 'https://supliful.com/catalog/reishi-calm', 'Mushroom Products', 'Reishi mushroom for stress support', 8.25, 'TIER_2', '["reishi"]'),
            ('GX-T2-005', 'Cordyceps Energy', 'https://supliful.com/catalog/cordyceps-energy', 'Mushroom Products', 'Cordyceps for energy and endurance', 9.00, 'TIER_2', '["cordyceps"]'),
            ('GX-T2-006', 'Collagen Peptides', 'https://supliful.com/catalog/collagen-peptides', 'Skin, Hair & Nail Health', 'Hydrolyzed collagen peptides', 11.50, 'TIER_2', '["collagen"]'),
            ('GX-T2-007', 'Glucosamine Chondroitin', 'https://supliful.com/catalog/glucosamine-chondroitin', 'Specialty Supplements', 'Joint support formula', 10.25, 'TIER_2', '["glucosamine", "chondroitin", "msm"]'),
            ('GX-T2-008', 'Milk Thistle', 'https://supliful.com/catalog/milk-thistle', 'Specialty Supplements', 'Milk thistle for liver support', 7.50, 'TIER_2', '["milk_thistle", "silymarin"]'),
            ('GX-T2-009', 'Bacopa Memory', 'https://supliful.com/catalog/bacopa-memory', 'Brain & Cognitive', 'Bacopa monnieri for memory support', 8.00, 'TIER_2', '["bacopa"]'),
            ('GX-T2-010', 'Maca Root', 'https://supliful.com/catalog/maca-root', 'Sexual & Reproductive Wellness', 'Maca for energy and vitality', 7.25, 'TIER_2', '["maca"]'),
            ('GX-T2-011', 'Saw Palmetto', 'https://supliful.com/catalog/saw-palmetto', 'Sexual & Reproductive Wellness', 'Saw palmetto for prostate health', 7.50, 'TIER_2', '["saw_palmetto"]'),
            ('GX-T2-012', 'Valerian Root', 'https://supliful.com/catalog/valerian-root', 'Specialty Supplements', 'Valerian for sleep support', 6.50, 'TIER_2', '["valerian"]'),
            ('GX-T2-013', 'Elderberry Extract', 'https://supliful.com/catalog/elderberry-extract', 'Specialty Supplements', 'Elderberry for immune support', 8.00, 'TIER_2', '["elderberry"]'),
            ('GX-T2-014', 'Echinacea', 'https://supliful.com/catalog/echinacea', 'Specialty Supplements', 'Echinacea for cold prevention', 6.75, 'TIER_2', '["echinacea"]'),
            ('GX-T2-015', 'Garlic Extract', 'https://supliful.com/catalog/garlic-extract', 'Specialty Supplements', 'Odorless garlic for cardiovascular support', 6.25, 'TIER_2', '["garlic"]'),
            ('GX-T2-016', 'Resveratrol', 'https://supliful.com/catalog/resveratrol', 'Specialty Supplements', 'Resveratrol antioxidant', 11.00, 'TIER_2', '["resveratrol"]'),
            ('GX-T2-017', 'Quercetin', 'https://supliful.com/catalog/quercetin', 'Specialty Supplements', 'Quercetin flavonoid', 8.50, 'TIER_2', '["quercetin"]'),
            ('GX-T2-018', 'Green Tea Extract', 'https://supliful.com/catalog/green-tea-extract', 'Weight Management', 'EGCG green tea extract', 7.00, 'TIER_2', '["green_tea_extract", "egcg"]'),
            ('GX-T2-019', 'Lutein + Zeaxanthin', 'https://supliful.com/catalog/lutein-zeaxanthin', 'Specialty Supplements', 'Eye health formula', 9.50, 'TIER_2', '["lutein", "zeaxanthin"]'),
            ('GX-T2-020', 'L-Carnitine', 'https://supliful.com/catalog/l-carnitine', 'Weight Management', 'L-Carnitine for fat metabolism', 8.25, 'TIER_2', '["l_carnitine"]'),
            ('GX-T2-021', 'Taurine', 'https://supliful.com/catalog/taurine', 'Pre-Workout Supplements', 'Taurine amino acid', 6.50, 'TIER_2', '["taurine"]'),
            ('GX-T2-022', 'Glycine', 'https://supliful.com/catalog/glycine', 'Specialty Supplements', 'Glycine for sleep and collagen', 5.75, 'TIER_2', '["glycine"]'),
            ('GX-T2-023', 'L-Glutamine', 'https://supliful.com/catalog/l-glutamine', 'Post-Workout Recovery', 'L-Glutamine for gut and muscle recovery', 7.50, 'TIER_2', '["l_glutamine"]'),
            ('GX-T2-024', 'Beta Glucan', 'https://supliful.com/catalog/beta-glucan', 'Specialty Supplements', 'Beta glucan for immune support', 9.00, 'TIER_2', '["beta_glucan"]'),
            ('GX-T2-025', 'Spirulina', 'https://supliful.com/catalog/spirulina', 'Greens & Superfoods', 'Spirulina superfood', 8.00, 'TIER_2', '["spirulina"]'),
            ('GX-T2-026', 'Chlorella', 'https://supliful.com/catalog/chlorella', 'Greens & Superfoods', 'Chlorella detox superfood', 8.50, 'TIER_2', '["chlorella"]'),
            ('GX-T2-027', 'Inositol', 'https://supliful.com/catalog/inositol', 'Specialty Supplements', 'Inositol for mood and PCOS', 7.25, 'TIER_2', '["inositol"]'),
            ('GX-T2-028', 'Choline', 'https://supliful.com/catalog/choline', 'Brain & Cognitive', 'Choline for brain health', 7.00, 'TIER_2', '["choline"]'),
            ('GX-T2-029', 'Alpha GPC', 'https://supliful.com/catalog/alpha-gpc', 'Brain & Cognitive', 'Alpha GPC nootropic', 12.00, 'TIER_2', '["alpha_gpc"]'),
            ('GX-T2-030', 'NMN', 'https://supliful.com/catalog/nmn', 'Specialty Supplements', 'NMN for NAD+ support', 15.00, 'TIER_2', '["nmn"]'),
            ('GX-T2-031', 'Sulforaphane', 'https://supliful.com/catalog/sulforaphane', 'Specialty Supplements', 'Broccoli extract sulforaphane', 10.50, 'TIER_2', '["sulforaphane"]'),
            ('GX-T2-032', 'DIM', 'https://supliful.com/catalog/dim', 'Specialty Supplements', 'DIM for hormone balance', 9.25, 'TIER_2', '["dim"]'),
            ('GX-T2-033', 'Pre-Workout Energy', 'https://supliful.com/catalog/pre-workout-energy', 'Pre-Workout Supplements', 'Pre-workout formula with beta-alanine', 11.00, 'TIER_2', '["beta_alanine", "taurine", "l_citrulline"]'),
            ('GX-T2-034', 'BCAA Recovery', 'https://supliful.com/catalog/bcaa-recovery', 'Post-Workout Recovery', 'BCAA for muscle recovery', 10.50, 'TIER_2', '["l_glutamine", "taurine"]'),
            ('GX-T2-035', 'Electrolyte Mix', 'https://supliful.com/catalog/electrolyte-mix', 'Intra-Workout Supplements', 'Electrolyte hydration mix', 7.50, 'TIER_2', '["magnesium", "zinc"]'),
            ('GX-T2-036', 'Greens Superfood Powder', 'https://supliful.com/catalog/greens-superfood', 'Greens & Superfoods', 'Comprehensive greens blend', 12.00, 'TIER_2', '["spirulina", "chlorella"]'),
            ('GX-T2-037', 'Mushroom Coffee Medium', 'https://supliful.com/catalog/mushroom-coffee-medium', 'Coffee & Tea', 'Coffee with Lions Mane and Chaga', 13.99, 'TIER_2', '["lions_mane", "chaga"]'),
            ('GX-T2-038', 'Matcha Powder', 'https://supliful.com/catalog/matcha-powder', 'Coffee & Tea', 'Ceremonial grade matcha', 13.95, 'TIER_2', '["green_tea_extract", "l_theanine"]'),
            ('GX-T2-039', 'Hyaluronic Acid Serum', 'https://supliful.com/catalog/hyaluronic-acid-serum', 'Facial Care', 'Hyaluronic acid for skin hydration', 5.99, 'TIER_2', '["collagen"]'),
            ('GX-T2-040', 'Biotin Hair Growth', 'https://supliful.com/catalog/biotin-hair-growth', 'Skin, Hair & Nail Health', 'Biotin for hair and nails', 6.50, 'TIER_2', '["biotin"]'),
            ('GX-T2-041', 'Fenugreek', 'https://supliful.com/catalog/fenugreek', 'Sexual & Reproductive Wellness', 'Fenugreek for testosterone support', 6.75, 'TIER_2', '["fenugreek"]'),
            ('GX-T2-042', 'Panax Ginseng', 'https://supliful.com/catalog/panax-ginseng', 'Pre-Workout Supplements', 'Korean ginseng for energy', 8.50, 'TIER_2', '["panax_ginseng"]'),
            ('GX-T2-043', 'Holy Basil', 'https://supliful.com/catalog/holy-basil', 'Specialty Supplements', 'Tulsi for stress relief', 7.00, 'TIER_2', '["holy_basil"]'),
            ('GX-T2-044', 'Passionflower', 'https://supliful.com/catalog/passionflower', 'Specialty Supplements', 'Passionflower for anxiety', 6.50, 'TIER_2', '["passionflower"]'),
            ('GX-T2-045', 'Lemon Balm', 'https://supliful.com/catalog/lemon-balm', 'Specialty Supplements', 'Lemon balm for calm', 6.25, 'TIER_2', '["lemon_balm"]'),
            ('GX-T2-046', 'Pre-Workout Pump', 'https://supliful.com/catalog/pre-workout-pump', 'Pre-Workout Supplements', 'Nitric oxide booster', 10.50, 'TIER_2', '["l_citrulline", "beta_alanine"]'),
            ('GX-T2-047', 'Post-Workout Protein', 'https://supliful.com/catalog/post-workout-protein', 'Post-Workout Recovery', 'Whey protein recovery', 14.00, 'TIER_2', '["l_glutamine"]'),
            ('GX-T2-048', 'Bee Pollen', 'https://supliful.com/catalog/bee-pollen', 'Bee Products', 'Bee pollen superfood', 9.00, 'TIER_2', '["probiotics"]'),
        ]
        
        for p in tier2_products:
            cur.execute("""
                INSERT INTO catalog_products (gx_catalog_id, product_name, product_url, category, short_description, base_price, evidence_tier, governance_status, ingredient_tags)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s::jsonb)
                ON CONFLICT (gx_catalog_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_url = EXCLUDED.product_url,
                    category = EXCLUDED.category,
                    short_description = EXCLUDED.short_description,
                    base_price = EXCLUDED.base_price,
                    evidence_tier = EXCLUDED.evidence_tier,
                    ingredient_tags = EXCLUDED.ingredient_tags,
                    updated_at = NOW()
            """, p)
        
        # Create view
        cur.execute("""
            CREATE OR REPLACE VIEW v_active_catalog AS
            SELECT 
                gx_catalog_id,
                product_name,
                category,
                evidence_tier,
                ingredient_tags,
                category_tags,
                sex_target,
                base_price
            FROM catalog_products
            WHERE governance_status = 'ACTIVE'
            ORDER BY 
                CASE evidence_tier 
                    WHEN 'TIER_1' THEN 1 
                    WHEN 'TIER_2' THEN 2 
                    ELSE 3 
                END,
                product_name
        """)
        
        # Get final counts
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2,
                COUNT(*) FILTER (WHERE governance_status = 'ACTIVE') as active
            FROM catalog_products
        """)
        stats = cur.fetchone()
        
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "message": "Catalog migration completed",
            "counts": {
                "total": stats['total'],
                "tier1": stats['tier1'],
                "tier2": stats['tier2'],
                "active": stats['active']
            },
            "executed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }
