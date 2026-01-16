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
-- GenoMAX² v3.26.0
-- ============================================

-- Step 1: Add launch flag column
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for launch queries
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;

-- Step 3: Populate launch flag - Include TIER 1 + TIER 2
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');

-- Step 4: Explicitly exclude TIER 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL;

-- Step 5: Add comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
'Launch v1 flag: TRUE = included in launch (Tier 1 + Tier 2), FALSE = excluded (Tier 3). DO NOT modify tier values.';
"""

ROLLBACK_SQL = """
-- ROLLBACK: Remove launch flag (if needed)
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
DROP INDEX IF EXISTS idx_os_modules_launch_v1;
"""

VALIDATION_QUERIES = {
    "launch_tier_distribution": """
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier;
    """,
    "excluded_tier3_count": """
        SELECT COUNT(*) as tier3_excluded
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', '3')
          AND is_launch_v1 = FALSE;
    """,
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_total
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', '3');
    """,
    "launch_summary": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """
}


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "column": "is_launch_v1", "action": "added_and_populated"}


def run_rollback(cursor):
    """Rollback the migration."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back", "column": "is_launch_v1", "action": "removed"}


def run_validation(cursor):
    """Run validation queries and return results."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


if __name__ == "__main__":
    print("Migration: Lock Launch v1")
    print("=" * 50)
    print(MIGRATION_SQL)
    print("\nRollback SQL:")
    print(ROLLBACK_SQL)
