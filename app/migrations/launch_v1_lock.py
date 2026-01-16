"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 boolean flag to os_modules_v3_1.
Launch v1 includes: TIER 1 + TIER 2
Launch v1 excludes: TIER 3

This is a state-alignment task - products remain untouched otherwise.
"""

# Migration SQL with explicit steps
MIGRATION_SQL = """
-- =====================================================
-- LAUNCH V1 LOCK MIGRATION
-- =====================================================
-- Purpose: Formally lock Launch v1 scope in database
-- Scope: TIER 1 + TIER 2 = Launch v1
--        TIER 3 = Excluded from launch pipelines
-- =====================================================

-- Step 1: Add is_launch_v1 column if not exists
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for launch filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
    ON os_modules_v3_1(is_launch_v1) 
    WHERE is_launch_v1 = TRUE;

-- Step 3: Comment on column for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 flag: TRUE for TIER 1 + TIER 2, FALSE for TIER 3. Used to filter launch pipelines.';
"""

# Populate SQL - run separately after column exists
POPULATE_SQL = """
-- =====================================================
-- POPULATE LAUNCH V1 FLAGS
-- =====================================================

-- Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'T1', 'T2', '1', '2');

-- Explicitly exclude Tier 3 (and anything else)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'T3', '3')
   OR tier IS NULL
   OR is_launch_v1 IS NULL;
"""

# Validation queries
VALIDATION_QUERIES = {
    "launch_tier_counts": """
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier;
    """,
    "excluded_tier_counts": """
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = FALSE
        GROUP BY tier
        ORDER BY tier;
    """,
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_total
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'T3', '3');
    """,
    "launch_summary": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
            COUNT(*) FILTER (WHERE is_launch_v1 IS NULL) as null_count,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """
}

# Rollback SQL
ROLLBACK_SQL = """
-- =====================================================
-- ROLLBACK: Remove is_launch_v1 column
-- =====================================================
-- WARNING: This removes the launch lock entirely

DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
"""


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "step": "schema_added"}


def populate_flags(cursor):
    """Populate the launch flags based on tier."""
    cursor.execute(POPULATE_SQL)
    return {"status": "success", "step": "flags_populated"}


def validate(cursor):
    """Run validation queries."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


def rollback(cursor):
    """Rollback the migration."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back"}


if __name__ == "__main__":
    print("=== Launch v1 Lock Migration ===")
    print("\nSchema Migration:")
    print(MIGRATION_SQL)
    print("\nPopulate Flags:")
    print(POPULATE_SQL)
    print("\nRollback:")
    print(ROLLBACK_SQL)
