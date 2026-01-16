"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Purpose:
- Add is_launch_v1 boolean flag to os_modules_v3_1
- Set TRUE for TIER 1 + TIER 2 products
- Set FALSE for TIER 3 products
- Enforce launch scope at DB level

Launch v1 Definition (LOCKED):
- INCLUDES: TIER 1, TIER 2
- EXCLUDES: TIER 3

Constraints:
- Does NOT delete any products
- Does NOT modify tier values
- Does NOT change copy, net_quantity, or supplier data
- Tier 3 remains in DB, just excluded from launch pipelines
"""

MIGRATION_NAME = "lock_launch_v1"
MIGRATION_VERSION = "3.26.0"

# Forward migration SQL
MIGRATION_UP = """
-- =====================================================
-- MIGRATION: Lock Launch v1 in Database
-- Version: 3.26.0
-- =====================================================

-- Step 1: Add launch flag column (idempotent)
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
    ON os_modules_v3_1(is_launch_v1) 
    WHERE is_launch_v1 = TRUE;

-- Step 3: Set launch flag for Tier 1 + Tier 2 (Launch v1 scope)
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
    'Launch v1 scope flag. TRUE = included in launch (Tier 1+2). FALSE = excluded (Tier 3). Locked 2025-01-15.';
"""

# Rollback migration SQL
MIGRATION_DOWN = """
-- =====================================================
-- ROLLBACK: Remove Launch v1 flag
-- Version: 3.26.0
-- =====================================================

-- Remove index first
DROP INDEX IF EXISTS idx_os_modules_launch_v1;

-- Remove column
ALTER TABLE os_modules_v3_1
DROP COLUMN IF EXISTS is_launch_v1;
"""

# Validation queries
VALIDATION_QUERIES = {
    "launch_tier_distribution": """
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier;
    """,
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_count
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "launch_v1_summary": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
            COUNT(*) FILTER (WHERE is_launch_v1 IS NULL) as null_count,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """,
    "active_launch_ready": """
        SELECT 
            COUNT(*) FILTER (WHERE supplier_status = 'ACTIVE' AND is_launch_v1 = TRUE) as active_launch_count,
            COUNT(*) FILTER (WHERE supplier_status = 'ACTIVE' AND is_launch_v1 = FALSE) as active_excluded_count
        FROM os_modules_v3_1;
    """
}


def run_migration(cursor):
    """Execute the forward migration."""
    cursor.execute(MIGRATION_UP)
    return {"status": "success", "migration": MIGRATION_NAME, "version": MIGRATION_VERSION}


def run_rollback(cursor):
    """Execute the rollback migration."""
    cursor.execute(MIGRATION_DOWN)
    return {"status": "rolled_back", "migration": MIGRATION_NAME, "version": MIGRATION_VERSION}


def validate_migration(cursor):
    """Run validation queries and return results."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        rows = cursor.fetchall()
        results[name] = [dict(row) for row in rows] if rows else []
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n--- FORWARD MIGRATION ---")
    print(MIGRATION_UP)
    print("\n--- ROLLBACK MIGRATION ---")
    print(MIGRATION_DOWN)
