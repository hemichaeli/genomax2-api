"""
Migration: Lock Launch v1 in Database
======================================
Version: 3.26.0
Date: 2025-01-15

Purpose:
- Add is_launch_v1 boolean flag to os_modules_v3_1
- Set TRUE for TIER 1 + TIER 2 products
- Set FALSE for TIER 3 products
- Enforce launch scope at DB level

Launch Definition (LOCKED):
- Launch v1 INCLUDES: TIER 1, TIER 2
- Launch v1 EXCLUDES: TIER 3

Constraints:
- NO product deletions
- NO tier value modifications
- NO copy/net_quantity/supplier changes
- Tier 3 preserved, just excluded from launch pipelines
"""

MIGRATION_ID = "launch_v1_lock_001"
MIGRATION_VERSION = "3.26.0"

# Forward migration
MIGRATION_UP = """
-- ============================================
-- MIGRATION: Lock Launch v1 in Database
-- Version: 3.26.0
-- Date: 2025-01-15
-- ============================================

-- Step 1: Add launch flag column
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for launch queries
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
'Launch v1 flag: TRUE = included in launch (Tier 1+2), FALSE = excluded (Tier 3). Added 2025-01-15.';
"""

# Rollback migration
MIGRATION_DOWN = """
-- ============================================
-- ROLLBACK: Remove Launch v1 lock
-- ============================================

-- Remove index
DROP INDEX IF EXISTS idx_os_modules_launch_v1;

-- Remove column
ALTER TABLE os_modules_v3_1
DROP COLUMN IF EXISTS is_launch_v1;
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
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_total
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "tier3_excluded": """
        SELECT COUNT(*) as tier3_in_launch
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
          AND is_launch_v1 = TRUE;
    """,
    "launch_summary": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as in_launch_v1,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """,
}


def run_migration(cursor):
    """Execute the forward migration."""
    cursor.execute(MIGRATION_UP)
    return {"status": "success", "migration_id": MIGRATION_ID, "version": MIGRATION_VERSION}


def run_rollback(cursor):
    """Execute the rollback migration."""
    cursor.execute(MIGRATION_DOWN)
    return {"status": "rolled_back", "migration_id": MIGRATION_ID}


def validate_migration(cursor):
    """Run validation queries and return results."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_ID}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n--- FORWARD MIGRATION ---")
    print(MIGRATION_UP)
    print("\n--- ROLLBACK ---")
    print(MIGRATION_DOWN)
