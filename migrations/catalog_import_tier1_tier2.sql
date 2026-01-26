-- GenoMAX² Catalog Import Migration
-- Version: 1.0.0
-- Date: 2026-01-26
-- 
-- Imports TIER 1 and TIER 2 evidence-backed products only
-- Total: 65 products (17 TIER 1 + 48 TIER 2)
--
-- Governance: Append-only pattern, no deletes allowed

-- ============================================
-- 1. CREATE CATALOG TABLE (IF NOT EXISTS)
-- ============================================

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
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_catalog_evidence_tier ON catalog_products(evidence_tier);
CREATE INDEX IF NOT EXISTS idx_catalog_governance_status ON catalog_products(governance_status);
CREATE INDEX IF NOT EXISTS idx_catalog_category ON catalog_products(category);
CREATE INDEX IF NOT EXISTS idx_catalog_ingredient_tags ON catalog_products USING GIN(ingredient_tags);

-- ============================================
-- 2. AUDIT TRIGGER (Append-only enforcement)
-- ============================================

CREATE OR REPLACE FUNCTION catalog_products_audit()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'DELETE not allowed on catalog_products (append-only governance)';
    END IF;
    
    -- Allow updates but track them
    IF TG_OP = 'UPDATE' THEN
        NEW.updated_at = NOW();
        -- Immutable fields check
        IF OLD.gx_catalog_id != NEW.gx_catalog_id THEN
            RAISE EXCEPTION 'Cannot modify gx_catalog_id (immutable)';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS catalog_products_audit_trigger ON catalog_products;
CREATE TRIGGER catalog_products_audit_trigger
BEFORE UPDATE OR DELETE ON catalog_products
FOR EACH ROW EXECUTE FUNCTION catalog_products_audit();

-- ============================================
-- 3. INSERT TIER 1 PRODUCTS (17 products)
-- ============================================

INSERT INTO catalog_products (gx_catalog_id, product_name, product_url, category, short_description, base_price, evidence_tier, governance_status, ingredient_tags)
VALUES
-- Strong meta-analytic evidence, validated biomarkers
('GX-T1-001', 'Beauty + Collagen Strips', 'https://supliful.com/catalog/beauty-collagen-strips', 'Skin, Hair & Nail Health', 'Mango-flavored oral strip combining Collagen Peptides and Vitamin E', 8.03, 'TIER_1', 'ACTIVE', '["collagen", "vitamin_e"]'::jsonb),
('GX-T1-002', 'Creatine Monohydrate', 'https://supliful.com/catalog/creatine-monohydrate-powder', 'Muscle Builders', 'Creatine Monohydrate powder for muscle building and athletic performance', 12.65, 'TIER_1', 'ACTIVE', '["creatine", "creatine_monohydrate"]'::jsonb),
('GX-T1-003', 'Dental + Oral Health Chewables', 'https://supliful.com/catalog/dental-oral-health-chewables', 'Specialty Supplements', 'Probiotics, fruit powder, xylitol, and hydroxyapatite', 5.55, 'TIER_1', 'ACTIVE', '["probiotics", "xylitol"]'::jsonb),
('GX-T1-004', 'Magnesium Glycinate', 'https://supliful.com/catalog/magnesium-glycinate-capsules', 'Vitamins & Minerals', 'Magnesium Glycinate for muscle relaxation and sleep quality', 8.89, 'TIER_1', 'ACTIVE', '["magnesium", "magnesium_glycinate"]'::jsonb),
('GX-T1-005', 'Probiotic + Metabolism Strips', 'https://supliful.com/catalog/probiotic-metabolism-strips', 'Digestive Support', 'Probiotic strips for gut health and metabolism support', 9.12, 'TIER_1', 'ACTIVE', '["probiotics", "lactobacillus", "bifidobacterium"]'::jsonb),
('GX-T1-006', 'Vitamin C Serum', 'https://supliful.com/catalog/vitamin-c-serum', 'Facial Care', 'Vitamin C Serum for skin brightening and protection', 7.45, 'TIER_1', 'ACTIVE', '["vitamin_c"]'::jsonb),
('GX-T1-007', 'Berberine', 'https://supliful.com/catalog/berberine-capsules', 'Weight Management', 'Berberine supplement for metabolic health', 7.55, 'TIER_1', 'ACTIVE', '["berberine"]'::jsonb),
('GX-T1-008', 'Probiotic 20 Billion', 'https://supliful.com/catalog/probiotic-20-billion', 'Digestive Support', 'High-potency probiotic with 20 billion CFU', 6.95, 'TIER_1', 'ACTIVE', '["probiotics", "lactobacillus", "bifidobacterium"]'::jsonb),
('GX-T1-009', 'Iron Strips', 'https://supliful.com/catalog/iron-strips', 'Vitamins & Minerals', 'Iron supplement strips for anemia support', 8.72, 'TIER_1', 'ACTIVE', '["iron"]'::jsonb),
('GX-T1-010', 'Complete Multivitamin', 'https://supliful.com/catalog/complete-multivitamin', 'Vitamins & Minerals', 'Comprehensive multivitamin with essential nutrients', 7.89, 'TIER_1', 'ACTIVE', '["vitamin_d3", "vitamin_b12", "folate", "zinc", "vitamin_c", "vitamin_e", "vitamin_a", "selenium"]'::jsonb),
('GX-T1-011', 'Omega-3 Fish Oil', 'https://supliful.com/catalog/omega-3-fish-oil', 'Fish Oil & Omega Fatty Acids', 'High-quality fish oil with EPA and DHA', 9.50, 'TIER_1', 'ACTIVE', '["omega_3", "epa", "dha", "fish_oil"]'::jsonb),
('GX-T1-012', 'Vitamin D3 + K2', 'https://supliful.com/catalog/vitamin-d3-k2', 'Vitamins & Minerals', 'Vitamin D3 with K2 for bone and cardiovascular health', 8.25, 'TIER_1', 'ACTIVE', '["vitamin_d3", "vitamin_k2"]'::jsonb),
('GX-T1-013', 'CoQ10 Ubiquinol', 'https://supliful.com/catalog/coq10-ubiquinol', 'Specialty Supplements', 'Ubiquinol form of CoQ10 for heart health', 12.50, 'TIER_1', 'ACTIVE', '["coq10", "ubiquinol"]'::jsonb),
('GX-T1-014', 'Zinc Picolinate', 'https://supliful.com/catalog/zinc-picolinate', 'Vitamins & Minerals', 'Highly absorbable zinc for immune support', 6.75, 'TIER_1', 'ACTIVE', '["zinc"]'::jsonb),
('GX-T1-015', 'Melatonin Sleep Support', 'https://supliful.com/catalog/melatonin-sleep', 'Specialty Supplements', 'Melatonin for natural sleep support', 5.99, 'TIER_1', 'ACTIVE', '["melatonin"]'::jsonb),
('GX-T1-016', 'Curcumin + Black Pepper', 'https://supliful.com/catalog/curcumin-black-pepper', 'Specialty Supplements', 'Curcumin with piperine for enhanced absorption', 9.25, 'TIER_1', 'ACTIVE', '["curcumin", "turmeric"]'::jsonb),
('GX-T1-017', 'Rhodiola Rosea', 'https://supliful.com/catalog/rhodiola-rosea', 'Specialty Supplements', 'Rhodiola adaptogen for stress and fatigue', 8.50, 'TIER_1', 'ACTIVE', '["rhodiola", "rhodiola_rosea"]'::jsonb)

ON CONFLICT (gx_catalog_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    product_url = EXCLUDED.product_url,
    category = EXCLUDED.category,
    short_description = EXCLUDED.short_description,
    base_price = EXCLUDED.base_price,
    evidence_tier = EXCLUDED.evidence_tier,
    governance_status = EXCLUDED.governance_status,
    ingredient_tags = EXCLUDED.ingredient_tags,
    updated_at = NOW();

-- ============================================
-- 4. INSERT TIER 2 PRODUCTS (48 products)
-- ============================================

INSERT INTO catalog_products (gx_catalog_id, product_name, product_url, category, short_description, base_price, evidence_tier, governance_status, ingredient_tags)
VALUES
-- Contextual evidence, smaller effect sizes, or limited participant pools
('GX-T2-001', 'Digestive Enzyme Pro Blend', 'https://supliful.com/catalog/digestive-enzyme-blend', 'Digestive Support', 'Comprehensive digestive enzyme blend', 6.95, 'TIER_2', 'ACTIVE', '["digestive_enzymes", "bromelain", "papain"]'::jsonb),
('GX-T2-002', 'Mushroom Extract Complex', 'https://supliful.com/catalog/mushroom-extract-complex', 'Mushroom Products', 'Blend of Lions Mane, Reishi, and Cordyceps', 9.15, 'TIER_2', 'ACTIVE', '["lions_mane", "reishi", "cordyceps"]'::jsonb),
('GX-T2-003', 'Lions Mane Focus', 'https://supliful.com/catalog/lions-mane-focus', 'Mushroom Products', 'Lions Mane for cognitive support', 8.50, 'TIER_2', 'ACTIVE', '["lions_mane"]'::jsonb),
('GX-T2-004', 'Reishi Calm', 'https://supliful.com/catalog/reishi-calm', 'Mushroom Products', 'Reishi mushroom for stress support', 8.25, 'TIER_2', 'ACTIVE', '["reishi"]'::jsonb),
('GX-T2-005', 'Cordyceps Energy', 'https://supliful.com/catalog/cordyceps-energy', 'Mushroom Products', 'Cordyceps for energy and endurance', 9.00, 'TIER_2', 'ACTIVE', '["cordyceps"]'::jsonb),
('GX-T2-006', 'Collagen Peptides', 'https://supliful.com/catalog/collagen-peptides', 'Skin, Hair & Nail Health', 'Hydrolyzed collagen peptides', 11.50, 'TIER_2', 'ACTIVE', '["collagen"]'::jsonb),
('GX-T2-007', 'Glucosamine Chondroitin', 'https://supliful.com/catalog/glucosamine-chondroitin', 'Specialty Supplements', 'Joint support formula', 10.25, 'TIER_2', 'ACTIVE', '["glucosamine", "chondroitin", "msm"]'::jsonb),
('GX-T2-008', 'Milk Thistle', 'https://supliful.com/catalog/milk-thistle', 'Specialty Supplements', 'Milk thistle for liver support', 7.50, 'TIER_2', 'ACTIVE', '["milk_thistle", "silymarin"]'::jsonb),
('GX-T2-009', 'Bacopa Memory', 'https://supliful.com/catalog/bacopa-memory', 'Brain & Cognitive', 'Bacopa monnieri for memory support', 8.00, 'TIER_2', 'ACTIVE', '["bacopa"]'::jsonb),
('GX-T2-010', 'Maca Root', 'https://supliful.com/catalog/maca-root', 'Sexual & Reproductive Wellness', 'Maca for energy and vitality', 7.25, 'TIER_2', 'ACTIVE', '["maca"]'::jsonb),
('GX-T2-011', 'Saw Palmetto', 'https://supliful.com/catalog/saw-palmetto', 'Sexual & Reproductive Wellness', 'Saw palmetto for prostate health', 7.50, 'TIER_2', 'ACTIVE', '["saw_palmetto"]'::jsonb),
('GX-T2-012', 'Valerian Root', 'https://supliful.com/catalog/valerian-root', 'Specialty Supplements', 'Valerian for sleep support', 6.50, 'TIER_2', 'ACTIVE', '["valerian"]'::jsonb),
('GX-T2-013', 'Elderberry Extract', 'https://supliful.com/catalog/elderberry-extract', 'Specialty Supplements', 'Elderberry for immune support', 8.00, 'TIER_2', 'ACTIVE', '["elderberry"]'::jsonb),
('GX-T2-014', 'Echinacea', 'https://supliful.com/catalog/echinacea', 'Specialty Supplements', 'Echinacea for cold prevention', 6.75, 'TIER_2', 'ACTIVE', '["echinacea"]'::jsonb),
('GX-T2-015', 'Garlic Extract', 'https://supliful.com/catalog/garlic-extract', 'Specialty Supplements', 'Odorless garlic for cardiovascular support', 6.25, 'TIER_2', 'ACTIVE', '["garlic"]'::jsonb),
('GX-T2-016', 'Resveratrol', 'https://supliful.com/catalog/resveratrol', 'Specialty Supplements', 'Resveratrol antioxidant', 11.00, 'TIER_2', 'ACTIVE', '["resveratrol"]'::jsonb),
('GX-T2-017', 'Quercetin', 'https://supliful.com/catalog/quercetin', 'Specialty Supplements', 'Quercetin flavonoid', 8.50, 'TIER_2', 'ACTIVE', '["quercetin"]'::jsonb),
('GX-T2-018', 'Green Tea Extract', 'https://supliful.com/catalog/green-tea-extract', 'Weight Management', 'EGCG green tea extract', 7.00, 'TIER_2', 'ACTIVE', '["green_tea_extract", "egcg"]'::jsonb),
('GX-T2-019', 'Lutein + Zeaxanthin', 'https://supliful.com/catalog/lutein-zeaxanthin', 'Specialty Supplements', 'Eye health formula', 9.50, 'TIER_2', 'ACTIVE', '["lutein", "zeaxanthin"]'::jsonb),
('GX-T2-020', 'L-Carnitine', 'https://supliful.com/catalog/l-carnitine', 'Weight Management', 'L-Carnitine for fat metabolism', 8.25, 'TIER_2', 'ACTIVE', '["l_carnitine"]'::jsonb),
('GX-T2-021', 'Taurine', 'https://supliful.com/catalog/taurine', 'Pre-Workout Supplements', 'Taurine amino acid', 6.50, 'TIER_2', 'ACTIVE', '["taurine"]'::jsonb),
('GX-T2-022', 'Glycine', 'https://supliful.com/catalog/glycine', 'Specialty Supplements', 'Glycine for sleep and collagen', 5.75, 'TIER_2', 'ACTIVE', '["glycine"]'::jsonb),
('GX-T2-023', 'L-Glutamine', 'https://supliful.com/catalog/l-glutamine', 'Post-Workout Recovery', 'L-Glutamine for gut and muscle recovery', 7.50, 'TIER_2', 'ACTIVE', '["l_glutamine"]'::jsonb),
('GX-T2-024', 'Beta Glucan', 'https://supliful.com/catalog/beta-glucan', 'Specialty Supplements', 'Beta glucan for immune support', 9.00, 'TIER_2', 'ACTIVE', '["beta_glucan"]'::jsonb),
('GX-T2-025', 'Spirulina', 'https://supliful.com/catalog/spirulina', 'Greens & Superfoods', 'Spirulina superfood', 8.00, 'TIER_2', 'ACTIVE', '["spirulina"]'::jsonb),
('GX-T2-026', 'Chlorella', 'https://supliful.com/catalog/chlorella', 'Greens & Superfoods', 'Chlorella detox superfood', 8.50, 'TIER_2', 'ACTIVE', '["chlorella"]'::jsonb),
('GX-T2-027', 'Inositol', 'https://supliful.com/catalog/inositol', 'Specialty Supplements', 'Inositol for mood and PCOS', 7.25, 'TIER_2', 'ACTIVE', '["inositol"]'::jsonb),
('GX-T2-028', 'Choline', 'https://supliful.com/catalog/choline', 'Brain & Cognitive', 'Choline for brain health', 7.00, 'TIER_2', 'ACTIVE', '["choline"]'::jsonb),
('GX-T2-029', 'Alpha GPC', 'https://supliful.com/catalog/alpha-gpc', 'Brain & Cognitive', 'Alpha GPC nootropic', 12.00, 'TIER_2', 'ACTIVE', '["alpha_gpc"]'::jsonb),
('GX-T2-030', 'NMN', 'https://supliful.com/catalog/nmn', 'Specialty Supplements', 'NMN for NAD+ support', 15.00, 'TIER_2', 'ACTIVE', '["nmn"]'::jsonb),
('GX-T2-031', 'Sulforaphane', 'https://supliful.com/catalog/sulforaphane', 'Specialty Supplements', 'Broccoli extract sulforaphane', 10.50, 'TIER_2', 'ACTIVE', '["sulforaphane"]'::jsonb),
('GX-T2-032', 'DIM', 'https://supliful.com/catalog/dim', 'Specialty Supplements', 'DIM for hormone balance', 9.25, 'TIER_2', 'ACTIVE', '["dim"]'::jsonb),
('GX-T2-033', 'Pre-Workout Energy', 'https://supliful.com/catalog/pre-workout-energy', 'Pre-Workout Supplements', 'Pre-workout formula with beta-alanine', 11.00, 'TIER_2', 'ACTIVE', '["beta_alanine", "taurine", "l_citrulline"]'::jsonb),
('GX-T2-034', 'BCAA Recovery', 'https://supliful.com/catalog/bcaa-recovery', 'Post-Workout Recovery', 'BCAA for muscle recovery', 10.50, 'TIER_2', 'ACTIVE', '["l_glutamine", "taurine"]'::jsonb),
('GX-T2-035', 'Electrolyte Mix', 'https://supliful.com/catalog/electrolyte-mix', 'Intra-Workout Supplements', 'Electrolyte hydration mix', 7.50, 'TIER_2', 'ACTIVE', '["magnesium", "zinc"]'::jsonb),
('GX-T2-036', 'Greens Superfood Powder', 'https://supliful.com/catalog/greens-superfood', 'Greens & Superfoods', 'Comprehensive greens blend', 12.00, 'TIER_2', 'ACTIVE', '["spirulina", "chlorella"]'::jsonb),
('GX-T2-037', 'Mushroom Coffee Medium', 'https://supliful.com/catalog/mushroom-coffee-medium', 'Coffee & Tea', 'Coffee with Lions Mane and Chaga', 13.99, 'TIER_2', 'ACTIVE', '["lions_mane", "chaga"]'::jsonb),
('GX-T2-038', 'Matcha Powder', 'https://supliful.com/catalog/matcha-powder', 'Coffee & Tea', 'Ceremonial grade matcha', 13.95, 'TIER_2', 'ACTIVE', '["green_tea_extract", "l_theanine"]'::jsonb),
('GX-T2-039', 'Hyaluronic Acid Serum', 'https://supliful.com/catalog/hyaluronic-acid-serum', 'Facial Care', 'Hyaluronic acid for skin hydration', 5.99, 'TIER_2', 'ACTIVE', '["collagen"]'::jsonb),
('GX-T2-040', 'Biotin Hair Growth', 'https://supliful.com/catalog/biotin-hair-growth', 'Skin, Hair & Nail Health', 'Biotin for hair and nails', 6.50, 'TIER_2', 'ACTIVE', '["biotin"]'::jsonb),
('GX-T2-041', 'Fenugreek', 'https://supliful.com/catalog/fenugreek', 'Sexual & Reproductive Wellness', 'Fenugreek for testosterone support', 6.75, 'TIER_2', 'ACTIVE', '["fenugreek"]'::jsonb),
('GX-T2-042', 'Panax Ginseng', 'https://supliful.com/catalog/panax-ginseng', 'Pre-Workout Supplements', 'Korean ginseng for energy', 8.50, 'TIER_2', 'ACTIVE', '["panax_ginseng"]'::jsonb),
('GX-T2-043', 'Holy Basil', 'https://supliful.com/catalog/holy-basil', 'Specialty Supplements', 'Tulsi for stress relief', 7.00, 'TIER_2', 'ACTIVE', '["holy_basil"]'::jsonb),
('GX-T2-044', 'Passionflower', 'https://supliful.com/catalog/passionflower', 'Specialty Supplements', 'Passionflower for anxiety', 6.50, 'TIER_2', 'ACTIVE', '["passionflower"]'::jsonb),
('GX-T2-045', 'Lemon Balm', 'https://supliful.com/catalog/lemon-balm', 'Specialty Supplements', 'Lemon balm for calm', 6.25, 'TIER_2', 'ACTIVE', '["lemon_balm"]'::jsonb),
('GX-T2-046', 'Pre-Workout Pump', 'https://supliful.com/catalog/pre-workout-pump', 'Pre-Workout Supplements', 'Nitric oxide booster', 10.50, 'TIER_2', 'ACTIVE', '["l_citrulline", "beta_alanine"]'::jsonb),
('GX-T2-047', 'Post-Workout Protein', 'https://supliful.com/catalog/post-workout-protein', 'Post-Workout Recovery', 'Whey protein recovery', 14.00, 'TIER_2', 'ACTIVE', '["l_glutamine"]'::jsonb),
('GX-T2-048', 'Bee Pollen', 'https://supliful.com/catalog/bee-pollen', 'Bee Products', 'Bee pollen superfood', 9.00, 'TIER_2', 'ACTIVE', '["probiotics"]'::jsonb)

ON CONFLICT (gx_catalog_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    product_url = EXCLUDED.product_url,
    category = EXCLUDED.category,
    short_description = EXCLUDED.short_description,
    base_price = EXCLUDED.base_price,
    evidence_tier = EXCLUDED.evidence_tier,
    governance_status = EXCLUDED.governance_status,
    ingredient_tags = EXCLUDED.ingredient_tags,
    updated_at = NOW();

-- ============================================
-- 5. CREATE CATALOG VIEW FOR BRAIN PIPELINE
-- ============================================

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
    product_name;

-- ============================================
-- 6. CATALOG GOVERNANCE STATS TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS catalog_governance_stats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_at TIMESTAMPTZ DEFAULT NOW(),
    total_products INTEGER,
    tier1_count INTEGER,
    tier2_count INTEGER,
    tier3_count INTEGER,
    active_count INTEGER,
    blocked_count INTEGER,
    pending_count INTEGER,
    version VARCHAR(50)
);

-- Insert initial stats
INSERT INTO catalog_governance_stats (total_products, tier1_count, tier2_count, tier3_count, active_count, blocked_count, pending_count, version)
SELECT 
    COUNT(*) as total_products,
    COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1_count,
    COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2_count,
    COUNT(*) FILTER (WHERE evidence_tier = 'TIER_3') as tier3_count,
    COUNT(*) FILTER (WHERE governance_status = 'ACTIVE') as active_count,
    COUNT(*) FILTER (WHERE governance_status = 'BLOCKED') as blocked_count,
    COUNT(*) FILTER (WHERE governance_status = 'PENDING') as pending_count,
    'catalog_import_v1.0' as version
FROM catalog_products;

-- ============================================
-- DONE
-- ============================================
-- Expected results:
-- - 17 TIER_1 products (strong evidence)
-- - 48 TIER_2 products (contextual evidence)
-- - 0 blocked products initially
-- - 65 total active products
