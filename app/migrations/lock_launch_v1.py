"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 flag to os_modules_v3_1 table.
Launch v1 = TIER 1 + TIER 2 only.
Tier 3 products remain in DB but excluded from launch pipelines.

ROLLBACK:
  ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
"""

MIGRATION_NAME = "lock_launch_v1"
MIGRATION_VERSION = "3.26.0"

MIGRATION_SQL = """
-- ============================================================
-- GenoMAX² Launch v1 Lock Migration
-- Version: 3.26.0
-- Date: 2025-01-15
-- ============================================================

-- Step 1: Add launch flag column (idempotent)
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Create index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
    ON os_modules_v3_1(is_launch_v1) 
    WHERE is_launch_v1 = TRUE;

-- Step 3: Populate launch flag - Include Tier 1 + Tier 2
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2')
  AND supplier_status = 'ACTIVE';

-- Step 4: Explicitly exclude Tier 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL
   OR supplier_status != 'ACTIVE';

-- Step 5: Add comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 flag: TRUE for TIER 1 + TIER 2 ACTIVE products only. Tier 3 excluded from launch pipelines.';
"""

ROLLBACK_SQL = """
-- Rollback: Remove launch v1 flag
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
DROP INDEX IF EXISTS idx_os_modules_launch_v1;
"""

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
    "launch_v1_count": """
        SELECT COUNT(*) as launch_v1_count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE;
    """,
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_count
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "tier3_excluded": """
        SELECT COUNT(*) as tier3_in_launch
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
          AND is_launch_v1 = TRUE;
    """,
}


def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {
        "status": "success", 
        "migration": MIGRATION_NAME,
        "version": MIGRATION_VERSION,
    }


def run_rollback(cursor):
    """Rollback the migration."""
    cursor.execute(ROLLBACK_SQL)
    return {
        "status": "rolled_back",
        "migration": MIGRATION_NAME,
    }


def validate(cursor):
    """Run validation queries and return results."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n--- MIGRATION SQL ---")
    print(MIGRATION_SQL)
    print("\n--- ROLLBACK SQL ---")
    print(ROLLBACK_SQL)
