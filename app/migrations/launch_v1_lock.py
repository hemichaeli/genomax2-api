"""
Migration: Add is_launch_v1 flag to os_modules_v3_1
Version: 3.26.0
Date: 2026-01-15

Locks Launch v1 to TIER 1 + TIER 2 products only.
TIER 3 products remain in DB but are excluded from launch pipelines.

This is a state-alignment task, not a refactor.
"""

MIGRATION_NAME = "launch_v1_lock"
MIGRATION_VERSION = "3.26.0"

# Step 1: Add the column
ADD_COLUMN_SQL = """
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;
"""

# Step 2: Create index for efficient filtering
ADD_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;
"""

# Step 3: Populate launch flag deterministically
POPULATE_LAUNCH_FLAG_SQL = """
-- Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');

-- Explicitly exclude Tier 3 (defensive)
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL;
"""

# Step 4: Add comment for documentation
ADD_COMMENT_SQL = """
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
'Launch v1 scope flag. TRUE = included in launch (Tier 1+2). FALSE = excluded (Tier 3). Locked 2026-01-15.';
"""

# Combined migration
FULL_MIGRATION_SQL = f"""
-- =====================================================
-- Migration: {MIGRATION_NAME}
-- Version: {MIGRATION_VERSION}
-- Purpose: Lock Launch v1 to TIER 1 + TIER 2 only
-- =====================================================

BEGIN;

-- Step 1: Add is_launch_v1 column
{ADD_COLUMN_SQL}

-- Step 2: Add partial index for launch queries
{ADD_INDEX_SQL}

-- Step 3: Populate launch flag
{POPULATE_LAUNCH_FLAG_SQL}

-- Step 4: Add documentation comment
{ADD_COMMENT_SQL}

COMMIT;
"""

# Rollback SQL (if needed)
ROLLBACK_SQL = """
-- Rollback: Remove is_launch_v1 column
-- WARNING: This will lose launch scope data

BEGIN;

DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;

COMMIT;
"""

# Validation queries
VALIDATION_QUERIES = {
    "tier_distribution": """
        SELECT tier, is_launch_v1, COUNT(*) as count
        FROM os_modules_v3_1
        GROUP BY tier, is_launch_v1
        ORDER BY tier, is_launch_v1;
    """,
    "launch_v1_count": """
        SELECT COUNT(*) as launch_v1_count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE;
    """,
    "tier3_excluded": """
        SELECT COUNT(*) as tier3_in_launch
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', '3') 
          AND is_launch_v1 = TRUE;
    """,
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_total
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', '3');
    """,
}


def run_migration(cursor):
    """Execute the full migration."""
    cursor.execute(ADD_COLUMN_SQL)
    cursor.execute(ADD_INDEX_SQL)
    cursor.execute(POPULATE_LAUNCH_FLAG_SQL)
    cursor.execute(ADD_COMMENT_SQL)
    return {"status": "success", "migration": MIGRATION_NAME, "version": MIGRATION_VERSION}


def run_rollback(cursor):
    """Execute rollback (use with caution)."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back", "migration": MIGRATION_NAME}


def validate_migration(cursor) -> dict:
    """Run validation checks after migration."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n--- FULL MIGRATION SQL ---")
    print(FULL_MIGRATION_SQL)
    print("\n--- ROLLBACK SQL ---")
    print(ROLLBACK_SQL)
