"""
Migration: Lock Launch v1 in DB
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 boolean flag to os_modules_v3_1 table.
Launch v1 includes: TIER 1 + TIER 2
Launch v1 excludes: TIER 3

This is a state-alignment task - products remain untouched otherwise.
"""

MIGRATION_NAME = "lock_launch_v1"
MIGRATION_VERSION = "3.26.0"

# Step 1: Add column
ADD_COLUMN_SQL = """
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;
"""

# Step 2: Create index for query performance
ADD_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;
"""

# Step 3: Populate launch flag - Include Tier 1 + Tier 2
SET_TIER_1_2_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');
"""

# Step 4: Explicitly exclude Tier 3 (defensive)
SET_TIER_3_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL
   OR tier = '';
"""

# Validation queries
VALIDATION_LAUNCH_COUNTS = """
SELECT 
    tier,
    COUNT(*) as total,
    SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as in_launch,
    SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded
FROM os_modules_v3_1
GROUP BY tier
ORDER BY tier;
"""

VALIDATION_TIER_3_EXISTS = """
SELECT COUNT(*) as tier_3_count
FROM os_modules_v3_1
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
"""

VALIDATION_LAUNCH_SUMMARY = """
SELECT 
    COUNT(*) as total_modules,
    SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as launch_v1_count,
    SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded_count,
    SUM(CASE WHEN is_launch_v1 = TRUE AND supplier_status = 'ACTIVE' THEN 1 ELSE 0 END) as launch_v1_active
FROM os_modules_v3_1;
"""

# Rollback (if needed)
ROLLBACK_SQL = """
-- Rollback: Remove is_launch_v1 column
-- ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
-- DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
"""

def get_migration_steps():
    """Return ordered list of migration steps."""
    return [
        ("add_column", ADD_COLUMN_SQL),
        ("add_index", ADD_INDEX_SQL),
        ("set_tier_1_2", SET_TIER_1_2_SQL),
        ("set_tier_3", SET_TIER_3_SQL),
    ]


def get_validation_queries():
    """Return validation queries."""
    return {
        "launch_counts": VALIDATION_LAUNCH_COUNTS,
        "tier_3_exists": VALIDATION_TIER_3_EXISTS,
        "launch_summary": VALIDATION_LAUNCH_SUMMARY,
    }


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n=== Migration Steps ===")
    for name, sql in get_migration_steps():
        print(f"\n-- Step: {name}")
        print(sql)
    print("\n=== Validation Queries ===")
    for name, sql in get_validation_queries().items():
        print(f"\n-- {name}")
        print(sql)
