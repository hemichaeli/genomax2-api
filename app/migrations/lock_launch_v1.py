"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 boolean flag to os_modules_v3_1 table.
Launch v1 = TIER 1 + TIER 2 only.
TIER 3 products remain in DB but excluded from launch pipelines.

This is a state-alignment task, not a refactor.
"""

MIGRATION_SQL = """
-- ============================================
-- LAUNCH V1 LOCK MIGRATION
-- Version: 3.26.0
-- Date: 2025-01-15
-- ============================================

-- Step 1: Add explicit launch flag
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
    ON os_modules_v3_1(is_launch_v1) 
    WHERE is_launch_v1 = TRUE;

-- Step 3: Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');

-- Step 4: Explicitly exclude Tier 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL;

-- Step 5: Add comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 flag: TRUE = included in launch (Tier 1 + Tier 2), FALSE = excluded (Tier 3)';
"""

ROLLBACK_SQL = """
-- Rollback: Remove is_launch_v1 column
ALTER TABLE os_modules_v3_1
DROP COLUMN IF EXISTS is_launch_v1;

DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
"""

VALIDATION_SQL = """
-- Validation Query 1: Tier counts in Launch v1
SELECT tier, COUNT(*) as count
FROM os_modules_v3_1
WHERE is_launch_v1 = TRUE
GROUP BY tier
ORDER BY tier;

-- Validation Query 2: Tier 3 preserved but excluded
SELECT COUNT(*) as tier_3_total
FROM os_modules_v3_1
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');

-- Validation Query 3: Launch v1 summary
SELECT 
    COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
    COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
    COUNT(*) as total
FROM os_modules_v3_1;
"""


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "column": "is_launch_v1", "version": "3.26.0"}


def run_rollback(cursor):
    """Rollback the migration."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back", "column": "is_launch_v1"}


def run_validation(cursor):
    """Run validation queries."""
    results = {}
    
    # Query 1: Tier counts in launch
    cursor.execute("""
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier
    """)
    results["launch_v1_by_tier"] = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Query 2: Tier 3 preserved
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
    """)
    results["tier_3_total"] = cursor.fetchone()[0]
    
    # Query 3: Summary
    cursor.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
            COUNT(*) as total
        FROM os_modules_v3_1
    """)
    row = cursor.fetchone()
    results["summary"] = {
        "launch_v1_count": row[0],
        "excluded_count": row[1],
        "total": row[2]
    }
    
    return results


if __name__ == "__main__":
    print("Migration: Lock Launch v1")
    print("=" * 50)
    print(MIGRATION_SQL)
    print("\nRollback:")
    print(ROLLBACK_SQL)
    print("\nValidation:")
    print(VALIDATION_SQL)
