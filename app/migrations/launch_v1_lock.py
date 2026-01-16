"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 flag to os_modules_v3_1 table.
Launch v1 includes TIER 1 + TIER 2 only.
TIER 3 products remain in DB but excluded from launch pipelines.

This is a state-alignment task, not a refactor.
"""

# Migration SQL - Add column and populate deterministically
MIGRATION_SQL = """
-- ============================================================
-- LAUNCH V1 LOCK MIGRATION
-- ============================================================
-- Adds is_launch_v1 boolean flag to explicitly define launch scope
-- Launch v1 = TIER 1 + TIER 2 only
-- TIER 3 remains in DB but excluded from launch pipelines

-- Step 1: Add the launch flag column
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
    'Launch v1 scope flag. TRUE = included in launch (Tier 1+2). FALSE = excluded (Tier 3). DO NOT modify tier values.';
"""

# Validation SQL - Run after migration
VALIDATION_SQL = """
-- Sanity check: Launch v1 tier distribution
SELECT 
    tier,
    COUNT(*) as count,
    SUM(CASE WHEN is_launch_v1 THEN 1 ELSE 0 END) as in_launch_v1
FROM os_modules_v3_1
GROUP BY tier
ORDER BY tier;
"""

# Rollback SQL - If needed
ROLLBACK_SQL = """
-- Rollback: Remove is_launch_v1 column
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
"""


def run_migration(cursor):
    """Execute the launch v1 lock migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "migration": "launch_v1_lock"}


def validate_migration(cursor):
    """Validate launch v1 distribution."""
    cursor.execute(VALIDATION_SQL)
    rows = cursor.fetchall()
    return {
        "status": "success",
        "tier_distribution": [
            {"tier": row[0], "count": row[1], "in_launch_v1": row[2]}
            for row in rows
        ]
    }


def rollback_migration(cursor):
    """Rollback the migration if needed."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back"}


if __name__ == "__main__":
    print("Migration: Launch v1 Lock")
    print("=" * 60)
    print(MIGRATION_SQL)
    print("\nValidation:")
    print(VALIDATION_SQL)
