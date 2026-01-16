"""
Migration: Add is_launch_v1 flag to os_modules_v3_1
Version: 3.26.0
Date: 2025-01-15

Locks Launch v1 scope in database:
- TIER 1 + TIER 2 = included (is_launch_v1 = TRUE)
- TIER 3 = excluded (is_launch_v1 = FALSE)

This is a state-alignment task, not a refactor.
Products are NOT deleted or modified - only flagged.
"""

MIGRATION_SQL = """
-- =====================================================
-- GenoMAX² Launch v1 Lock Migration
-- Version: 3.26.0
-- =====================================================

-- Step 1: Add launch flag column (idempotent)
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for launch filtering (idempotent)
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
    ON os_modules_v3_1(is_launch_v1) 
    WHERE is_launch_v1 = TRUE;

-- Step 3: Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2')
  AND supplier_status = 'ACTIVE';

-- Step 4: Explicitly exclude Tier 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL
   OR supplier_status != 'ACTIVE';

-- Step 5: Add comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 inclusion flag. TRUE = Tier 1/2 ACTIVE products included in launch pipelines. FALSE = excluded from Shopify/Design/QA launch flows.';
"""

ROLLBACK_SQL = """
-- Rollback: Remove is_launch_v1 column
-- WARNING: This will lose launch lock state

ALTER TABLE os_modules_v3_1 
DROP COLUMN IF EXISTS is_launch_v1;

DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
"""

VALIDATION_SQL = """
-- Validation Query 1: Tier distribution in Launch v1
SELECT 
    tier,
    COUNT(*) as count,
    SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as in_launch_v1,
    SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded
FROM os_modules_v3_1
GROUP BY tier
ORDER BY tier;

-- Validation Query 2: Launch v1 summary
SELECT 
    is_launch_v1,
    supplier_status,
    COUNT(*) as count
FROM os_modules_v3_1
GROUP BY is_launch_v1, supplier_status
ORDER BY is_launch_v1 DESC, supplier_status;

-- Validation Query 3: Confirm no Tier 3 in launch
SELECT COUNT(*) as tier3_in_launch
FROM os_modules_v3_1
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
  AND is_launch_v1 = TRUE;
-- Expected: 0
"""


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "migration": "launch_v1_lock"}


def run_rollback(cursor):
    """Execute rollback (use with caution)."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back", "migration": "launch_v1_lock"}


def run_validation(cursor):
    """Run validation queries and return results."""
    results = {}
    
    # Tier distribution
    cursor.execute("""
        SELECT 
            tier,
            COUNT(*) as count,
            SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as in_launch_v1,
            SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded
        FROM os_modules_v3_1
        GROUP BY tier
        ORDER BY tier
    """)
    results["tier_distribution"] = [dict(row) for row in cursor.fetchall()]
    
    # Launch summary
    cursor.execute("""
        SELECT 
            is_launch_v1,
            supplier_status,
            COUNT(*) as count
        FROM os_modules_v3_1
        GROUP BY is_launch_v1, supplier_status
        ORDER BY is_launch_v1 DESC, supplier_status
    """)
    results["launch_summary"] = [dict(row) for row in cursor.fetchall()]
    
    # Tier 3 leak check
    cursor.execute("""
        SELECT COUNT(*) as tier3_in_launch
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
          AND is_launch_v1 = TRUE
    """)
    results["tier3_leak_check"] = cursor.fetchone()["tier3_in_launch"]
    
    return results


if __name__ == "__main__":
    print("=== Launch v1 Lock Migration ===")
    print("\nMigration SQL:")
    print(MIGRATION_SQL)
    print("\nRollback SQL:")
    print(ROLLBACK_SQL)
    print("\nValidation SQL:")
    print(VALIDATION_SQL)
