"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2026-01-15

Adds is_launch_v1 boolean flag to os_modules_v3_1.
Launch v1 includes: TIER 1 + TIER 2
Launch v1 excludes: TIER 3

This is a state-alignment task - no data deletion or tier modification.
"""

MIGRATION_NAME = "lock_launch_v1"
MIGRATION_VERSION = "3.26.0"

# Forward migration SQL
MIGRATION_UP = """
-- ============================================
-- LAUNCH V1 LOCK MIGRATION
-- ============================================
-- Adds is_launch_v1 flag to enforce launch scope
-- TIER 1 + TIER 2 = Launch v1
-- TIER 3 = Excluded from launch pipelines
-- ============================================

-- Step 1: Add the launch flag column
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;

-- Step 3: Populate launch flag for Tier 1 + Tier 2
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
    'Launch v1 inclusion flag. TRUE = TIER 1 + TIER 2 products included in launch pipelines. FALSE = excluded (TIER 3 or untiered).';
"""

# Rollback migration SQL
MIGRATION_DOWN = """
-- Rollback: Remove is_launch_v1 column
DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
"""

# Validation queries
VALIDATION_QUERIES = {
    "tier_distribution": """
        SELECT 
            tier,
            is_launch_v1,
            COUNT(*) as count
        FROM os_modules_v3_1
        GROUP BY tier, is_launch_v1
        ORDER BY tier, is_launch_v1;
    """,
    "launch_v1_summary": """
        SELECT 
            is_launch_v1,
            COUNT(*) as count
        FROM os_modules_v3_1
        GROUP BY is_launch_v1;
    """,
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_count
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "launch_v1_by_tier": """
        SELECT 
            tier,
            COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier;
    """,
}


def run_migration(cursor):
    """Execute the forward migration."""
    cursor.execute(MIGRATION_UP)
    return {"status": "success", "migration": MIGRATION_NAME, "version": MIGRATION_VERSION}


def rollback_migration(cursor):
    """Execute the rollback migration."""
    cursor.execute(MIGRATION_DOWN)
    return {"status": "rolled_back", "migration": MIGRATION_NAME}


def validate_migration(cursor):
    """Run validation queries and return results."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n--- FORWARD MIGRATION ---")
    print(MIGRATION_UP)
    print("\n--- ROLLBACK MIGRATION ---")
    print(MIGRATION_DOWN)
