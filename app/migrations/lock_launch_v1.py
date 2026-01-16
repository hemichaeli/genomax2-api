"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 flag to os_modules_v3_1 table.
Only TIER 1 and TIER 2 products are included in Launch v1.
TIER 3 products remain in DB but are excluded from launch pipelines.

This is a state-alignment task, not a refactor.
"""

MIGRATION_ID = "lock_launch_v1"
MIGRATION_VERSION = "3.26.0"

# ===== MIGRATION SQL =====

MIGRATION_UP = """
-- Lock Launch v1: Add explicit launch flag
-- Only TIER 1 + TIER 2 are included in Launch v1

-- Step 1: Add the is_launch_v1 column if not present
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');

-- Step 3: Explicitly exclude Tier 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL;

-- Step 4: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;

-- Step 5: Add comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 flag: TRUE = TIER 1 + TIER 2 (launch scope), FALSE = TIER 3 or excluded';
"""

MIGRATION_DOWN = """
-- Rollback: Remove is_launch_v1 column
DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
"""

VALIDATION_QUERIES = """
-- Validation Query 1: Tier distribution in Launch v1
SELECT tier, COUNT(*) as count
FROM os_modules_v3_1
WHERE is_launch_v1 = TRUE
GROUP BY tier
ORDER BY tier;
-- Expected: TIER 1 → >0, TIER 2 → >0, TIER 3 → 0

-- Validation Query 2: Tier 3 still exists (just excluded)
SELECT COUNT(*) as tier_3_count
FROM os_modules_v3_1
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
-- Expected: >= 0 (preserved, just excluded)

-- Validation Query 3: Launch v1 total
SELECT 
    COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
    COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
    COUNT(*) as total
FROM os_modules_v3_1;
"""


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_UP)
    return {
        "status": "success",
        "migration_id": MIGRATION_ID,
        "version": MIGRATION_VERSION,
        "action": "Lock Launch v1 - TIER 1 + TIER 2 only"
    }


def rollback_migration(cursor):
    """Rollback the migration."""
    cursor.execute(MIGRATION_DOWN)
    return {
        "status": "rolled_back",
        "migration_id": MIGRATION_ID
    }


def validate_migration(cursor):
    """Validate the migration was applied correctly."""
    results = {}
    
    # Check 1: Launch v1 tier distribution
    cursor.execute("""
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier
    """)
    results["launch_v1_by_tier"] = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Check 2: Tier 3 preserved
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
    """)
    results["tier_3_preserved"] = cursor.fetchone()[0]
    
    # Check 3: Total counts
    cursor.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded,
            COUNT(*) as total
        FROM os_modules_v3_1
    """)
    row = cursor.fetchone()
    results["counts"] = {
        "launch_v1": row[0],
        "excluded": row[1],
        "total": row[2]
    }
    
    # Validation passes if no TIER 3 in launch
    tier_3_in_launch = results["launch_v1_by_tier"].get("TIER 3", 0) + \
                       results["launch_v1_by_tier"].get("Tier 3", 0) + \
                       results["launch_v1_by_tier"].get("tier 3", 0)
    
    results["validation_passed"] = tier_3_in_launch == 0
    
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_ID}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n--- UP ---")
    print(MIGRATION_UP)
    print("\n--- DOWN ---")
    print(MIGRATION_DOWN)
