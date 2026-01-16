-- ============================================================
-- MIGRATION: 009_lock_launch_v1.sql
-- PURPOSE: Lock Launch v1 scope to TIER 1 + TIER 2 only
-- DATE: 2026-01-16
-- AUTHOR: GenoMAX² System
-- ============================================================

-- PRE-MIGRATION VALIDATION
-- Run this first to confirm current state:
/*
SELECT tier, COUNT(*) as count
FROM os_modules_v3_1
GROUP BY tier
ORDER BY tier;
*/

-- ============================================================
-- STEP 1: Add launch flag column (idempotent)
-- ============================================================
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- ============================================================
-- STEP 2: Populate launch flag deterministically
-- ============================================================

-- 2a. Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2')
  AND (is_launch_v1 IS NULL OR is_launch_v1 = FALSE);

-- 2b. Explicitly exclude Tier 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier = 'TIER 3';

-- 2c. Ensure any NULL tiers are excluded
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IS NULL OR tier NOT IN ('TIER 1', 'TIER 2', 'TIER 3');

-- ============================================================
-- STEP 3: Add index for launch queries (optional but recommended)
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
ON os_modules_v3_1 (is_launch_v1) 
WHERE is_launch_v1 = TRUE;

-- ============================================================
-- POST-MIGRATION VALIDATION (REQUIRED)
-- ============================================================

-- Validation Query 1: Launch v1 tier distribution
-- Expected: TIER 1 > 0, TIER 2 > 0, NO TIER 3
/*
SELECT 
    tier, 
    COUNT(*) as count,
    CASE 
        WHEN tier IN ('TIER 1', 'TIER 2') THEN 'INCLUDED'
        ELSE 'EXCLUDED'
    END as launch_status
FROM os_modules_v3_1
WHERE is_launch_v1 = TRUE
GROUP BY tier
ORDER BY tier;
*/

-- Validation Query 2: Confirm Tier 3 preserved but excluded
-- Expected: count > 0, all with is_launch_v1 = FALSE
/*
SELECT 
    COUNT(*) as tier_3_total,
    SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as tier_3_excluded,
    SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as tier_3_included_ERROR
FROM os_modules_v3_1
WHERE tier = 'TIER 3';
*/

-- Validation Query 3: Total launch scope
/*
SELECT 
    COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_products,
    COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_products,
    COUNT(*) as total_products
FROM os_modules_v3_1;
*/

-- ============================================================
-- ROLLBACK SCRIPT (if needed)
-- ============================================================
/*
-- To completely remove launch flag:
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
DROP INDEX IF EXISTS idx_os_modules_launch_v1;

-- To reset launch flag without removing column:
UPDATE os_modules_v3_1 SET is_launch_v1 = FALSE;
*/

-- ============================================================
-- MIGRATION COMPLETE
-- ============================================================
