"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 flag to os_modules_v3_1 table.
Launch v1 = TIER 1 + TIER 2 only.
Tier 3 products remain in DB but excluded from launch pipelines.

This is a state-alignment task, not a refactor.
"""

# Migration SQL - deterministic, idempotent
MIGRATION_SQL = """
-- =====================================================
-- LAUNCH V1 LOCK MIGRATION
-- =====================================================
-- Objective: Formally lock Launch v1 scope in database
-- Launch v1 includes: TIER 1, TIER 2
-- Launch v1 excludes: TIER 3
-- =====================================================

-- Step 1: Add launch flag column (idempotent)
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
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
    'Launch v1 scope flag. TRUE = included in launch (Tier 1 + Tier 2). FALSE = excluded (Tier 3). Enforced in Shopify publish, Design export, and QA gates.';
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
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "launch_summary": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
            COUNT(*) as total_modules
        FROM os_modules_v3_1;
    """
}

# Rollback SQL (if needed)
ROLLBACK_SQL = """
-- Rollback: Remove launch flag (preserves all data)
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
DROP INDEX IF EXISTS idx_os_modules_launch_v1;
"""


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "column": "is_launch_v1", "action": "added_and_populated"}


def validate_migration(cursor) -> dict:
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
    print("\nValidation Queries:")
    for name, query in VALIDATION_QUERIES.items():
        print(f"\n-- {name}")
        print(query)
