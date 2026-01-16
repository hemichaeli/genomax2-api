"""
Migration: Add is_launch_v1 flag for Launch v1 enforcement
Version: 3.26.0
Date: 2026-01-15

Locks Launch v1 to TIER 1 + TIER 2 products only.
TIER 3 products remain in DB but are excluded from launch pipelines.

This is a state-alignment task, not a refactor.
"""

MIGRATION_SQL = """
-- ============================================
-- Launch v1 Lock Migration
-- ============================================
-- Launch v1 includes: TIER 1 and TIER 2
-- Launch v1 excludes: TIER 3
-- All products remain untouched otherwise
-- ============================================

-- Step 1: Add explicit launch flag
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
    ON os_modules_v3_1(is_launch_v1) 
    WHERE is_launch_v1 = TRUE;

-- Step 3: Populate launch flag deterministically
-- Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', 'T1', 'T2', '1', '2');

-- Explicitly exclude Tier 3 (safety: ensure FALSE)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3')
   OR tier IS NULL
   OR is_launch_v1 IS NULL;

-- Step 4: Add comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 flag - TRUE for TIER 1 + TIER 2 products, FALSE for TIER 3. Updated 2026-01-15.';
"""

VALIDATION_SQL = """
-- ============================================
-- Validation Queries (Run after migration)
-- ============================================

-- Sanity check: Launch v1 counts by tier
SELECT tier, COUNT(*) as count
FROM os_modules_v3_1
WHERE is_launch_v1 = TRUE
GROUP BY tier
ORDER BY tier;
-- Expected: TIER 1 > 0, TIER 2 > 0, TIER 3 = 0

-- Ensure Tier 3 still exists (preserved, just excluded)
SELECT COUNT(*) as tier3_count
FROM os_modules_v3_1
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3');
-- Expected: > 0 (Tier 3 preserved)

-- Verify no Tier 3 in launch
SELECT COUNT(*) as tier3_in_launch
FROM os_modules_v3_1
WHERE is_launch_v1 = TRUE 
  AND tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3');
-- Expected: 0

-- Summary
SELECT 
    COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
    COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded_count,
    COUNT(*) as total_count
FROM os_modules_v3_1;
"""

ROLLBACK_SQL = """
-- ============================================
-- Rollback (if needed)
-- ============================================
-- This removes the is_launch_v1 column entirely
-- Use only if migration needs to be reverted

ALTER TABLE os_modules_v3_1
DROP COLUMN IF EXISTS is_launch_v1;

DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
"""


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "column": "is_launch_v1", "action": "added_and_populated"}


def run_validation(cursor):
    """Run validation queries and return results."""
    results = {}
    
    # Launch v1 counts by tier
    cursor.execute("""
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier
    """)
    results["launch_v1_by_tier"] = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Tier 3 total count
    cursor.execute("""
        SELECT COUNT(*) FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3')
    """)
    results["tier3_total"] = cursor.fetchone()[0]
    
    # Tier 3 in launch (should be 0)
    cursor.execute("""
        SELECT COUNT(*) FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE 
          AND tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3')
    """)
    results["tier3_in_launch"] = cursor.fetchone()[0]
    
    # Summary
    cursor.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded_count,
            COUNT(*) as total_count
        FROM os_modules_v3_1
    """)
    row = cursor.fetchone()
    results["summary"] = {
        "launch_v1_count": row[0],
        "excluded_count": row[1],
        "total_count": row[2]
    }
    
    # Validation pass/fail
    results["validation_passed"] = (
        results["tier3_in_launch"] == 0 and
        results["summary"]["launch_v1_count"] > 0
    )
    
    return results


def run_rollback(cursor):
    """Execute rollback if needed."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back", "column": "is_launch_v1", "action": "removed"}


if __name__ == "__main__":
    print("Migration: is_launch_v1 (Launch v1 Lock)")
    print("\n=== MIGRATION SQL ===")
    print(MIGRATION_SQL)
    print("\n=== VALIDATION SQL ===")
    print(VALIDATION_SQL)
    print("\n=== ROLLBACK SQL ===")
    print(ROLLBACK_SQL)
