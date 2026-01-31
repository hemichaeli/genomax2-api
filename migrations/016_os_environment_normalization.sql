-- ============================================
-- Migration: 016_os_environment_normalization
-- Version: 3.41.0
-- Date: 2026-01-31
-- 
-- Purpose: Normalize catalog_products to os_environment execution model
-- Rule: No Universal - every product exists once per environment
-- 
-- Before: sex_target IN ('male', 'female', 'unisex')
-- After:  os_environment IN ('MAXimo²', 'MAXima²')
--
-- Transformation:
--   male   -> 1 row: MAXimo²
--   female -> 1 row: MAXima²  
--   unisex -> 2 rows: MAXimo² + MAXima² (duplicate with new gx_catalog_id)
-- ============================================

-- ============================================
-- STEP 0: PRE-MIGRATION AUDIT
-- ============================================

-- Count current state
SELECT 
    'PRE-MIGRATION' as stage,
    COUNT(*) as total_products,
    COUNT(*) FILTER (WHERE sex_target = 'male') as male_count,
    COUNT(*) FILTER (WHERE sex_target = 'female') as female_count,
    COUNT(*) FILTER (WHERE sex_target = 'unisex') as unisex_count
FROM catalog_products;

-- ============================================
-- STEP 1: ADD os_environment COLUMN
-- ============================================

ALTER TABLE catalog_products
ADD COLUMN IF NOT EXISTS os_environment VARCHAR(20);

-- Add CHECK constraint (NO UNIVERSAL allowed)
ALTER TABLE catalog_products
DROP CONSTRAINT IF EXISTS catalog_products_os_environment_check;

ALTER TABLE catalog_products
ADD CONSTRAINT catalog_products_os_environment_check 
CHECK (os_environment IN ('MAXimo²', 'MAXima²'));

-- ============================================
-- STEP 2: BACKFILL MALE -> MAXimo²
-- ============================================

UPDATE catalog_products
SET os_environment = 'MAXimo²'
WHERE sex_target = 'male'
  AND os_environment IS NULL;

-- ============================================
-- STEP 3: BACKFILL FEMALE -> MAXima²
-- ============================================

UPDATE catalog_products
SET os_environment = 'MAXima²'
WHERE sex_target = 'female'
  AND os_environment IS NULL;

-- ============================================
-- STEP 4: HANDLE UNISEX -> DUPLICATE INTO BOTH ENVIRONMENTS
-- ============================================

-- 4a. Update existing unisex rows to MAXimo² (keep original gx_catalog_id, add -M suffix)
UPDATE catalog_products
SET 
    os_environment = 'MAXimo²',
    gx_catalog_id = gx_catalog_id || '-M'
WHERE sex_target = 'unisex'
  AND os_environment IS NULL;

-- 4b. Insert MAXima² duplicates for former unisex products
INSERT INTO catalog_products (
    gx_catalog_id,
    product_name,
    product_url,
    category,
    sub_category,
    short_description,
    serving_info,
    base_price,
    currency,
    evidence_tier,
    governance_status,
    block_reason,
    ingredient_tags,
    category_tags,
    sex_target,
    os_environment,
    source_version,
    shopify_enabled,
    amazon_enabled,
    tiktok_enabled,
    created_at,
    updated_at
)
SELECT
    REPLACE(gx_catalog_id, '-M', '-F') as gx_catalog_id,  -- GX-T1-001-M -> GX-T1-001-F
    product_name,
    product_url,
    category,
    sub_category,
    short_description,
    serving_info,
    base_price,
    currency,
    evidence_tier,
    governance_status,
    block_reason,
    ingredient_tags,
    category_tags,
    sex_target,
    'MAXima²' as os_environment,
    source_version || '_dup_016',
    shopify_enabled,
    amazon_enabled,
    tiktok_enabled,
    created_at,
    NOW() as updated_at
FROM catalog_products
WHERE os_environment = 'MAXimo²'
  AND sex_target = 'unisex'
  AND gx_catalog_id LIKE '%-M'
ON CONFLICT (gx_catalog_id) DO NOTHING;

-- ============================================
-- STEP 5: MAKE os_environment NOT NULL
-- ============================================

-- Verify no NULLs remain
DO $$
DECLARE
    null_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO null_count
    FROM catalog_products
    WHERE os_environment IS NULL;
    
    IF null_count > 0 THEN
        RAISE EXCEPTION 'Migration failed: % rows still have NULL os_environment', null_count;
    END IF;
END $$;

-- Set NOT NULL constraint
ALTER TABLE catalog_products
ALTER COLUMN os_environment SET NOT NULL;

-- ============================================
-- STEP 6: CREATE COMPOSITE UNIQUE INDEX
-- ============================================

-- For routing: (base_catalog_id, os_environment) should be unique
-- But we now have gx_catalog_id with suffixes, so the existing unique works

-- Add index for os_environment queries
CREATE INDEX IF NOT EXISTS idx_catalog_os_environment 
ON catalog_products(os_environment);

-- Add compound index for routing
CREATE INDEX IF NOT EXISTS idx_catalog_env_tier 
ON catalog_products(os_environment, evidence_tier);

-- ============================================
-- STEP 7: UPDATE v_active_catalog VIEW
-- ============================================

CREATE OR REPLACE VIEW v_active_catalog AS
SELECT 
    gx_catalog_id,
    product_name,
    category,
    evidence_tier,
    ingredient_tags,
    category_tags,
    os_environment,
    sex_target,  -- Keep for backward compatibility
    base_price
FROM catalog_products
WHERE governance_status = 'ACTIVE'
ORDER BY 
    os_environment,
    CASE evidence_tier 
        WHEN 'TIER_1' THEN 1 
        WHEN 'TIER_2' THEN 2 
        ELSE 3 
    END,
    product_name;

-- ============================================
-- STEP 8: POST-MIGRATION AUDIT
-- ============================================

SELECT 
    'POST-MIGRATION' as stage,
    COUNT(*) as total_products,
    COUNT(*) FILTER (WHERE os_environment = 'MAXimo²') as maximo_count,
    COUNT(*) FILTER (WHERE os_environment = 'MAXima²') as maxima_count,
    COUNT(*) FILTER (WHERE sex_target = 'unisex') as former_unisex_now_split
FROM catalog_products;

-- Verify no UNIVERSAL exists
SELECT 
    CASE 
        WHEN COUNT(*) FILTER (WHERE os_environment NOT IN ('MAXimo²', 'MAXima²')) = 0 
        THEN 'PASS: Only MAXimo² and MAXima² exist'
        ELSE 'FAIL: Invalid os_environment values found'
    END as validation_result
FROM catalog_products;

-- ============================================
-- STEP 9: GOVERNANCE STATS UPDATE
-- ============================================

INSERT INTO catalog_governance_stats (
    total_products, 
    tier1_count, 
    tier2_count, 
    tier3_count, 
    active_count, 
    blocked_count, 
    pending_count, 
    version
)
SELECT 
    COUNT(*) as total_products,
    COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1_count,
    COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2_count,
    COUNT(*) FILTER (WHERE evidence_tier = 'TIER_3') as tier3_count,
    COUNT(*) FILTER (WHERE governance_status = 'ACTIVE') as active_count,
    COUNT(*) FILTER (WHERE governance_status = 'BLOCKED') as blocked_count,
    COUNT(*) FILTER (WHERE governance_status = 'PENDING') as pending_count,
    'os_environment_v3.41.0' as version
FROM catalog_products;

-- ============================================
-- ROLLBACK SCRIPT (if needed)
-- ============================================
/*
-- WARNING: This is destructive - only use if migration fails

-- Remove duplicated MAXima² rows from former unisex
DELETE FROM catalog_products
WHERE gx_catalog_id LIKE '%-F'
  AND sex_target = 'unisex';

-- Restore original gx_catalog_id for MAXimo² rows
UPDATE catalog_products
SET 
    gx_catalog_id = REPLACE(gx_catalog_id, '-M', ''),
    os_environment = NULL
WHERE gx_catalog_id LIKE '%-M'
  AND sex_target = 'unisex';

-- Clear os_environment for male/female
UPDATE catalog_products
SET os_environment = NULL
WHERE sex_target IN ('male', 'female');

-- Drop constraint and column
ALTER TABLE catalog_products DROP CONSTRAINT IF EXISTS catalog_products_os_environment_check;
ALTER TABLE catalog_products DROP COLUMN IF EXISTS os_environment;
*/

-- ============================================
-- DONE
-- ============================================
-- Expected post-migration state:
-- - All products have os_environment = 'MAXimo²' OR 'MAXima²'
-- - Former male products: 1 row each (MAXimo²)
-- - Former female products: 1 row each (MAXima²)
-- - Former unisex products: 2 rows each (MAXimo² with -M suffix, MAXima² with -F suffix)
-- - Total product count increased by (original unisex count)
-- - No NULL os_environment values
-- - No UNIVERSAL environment exists
