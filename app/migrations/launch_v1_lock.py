"""
Migration: Add is_launch_v1 flag to os_modules_v3_1
Version: 3.26.0
Date: 2025-01-15

Locks Launch v1 scope in DB:
- TIER 1 + TIER 2 = is_launch_v1 = TRUE
- TIER 3 = is_launch_v1 = FALSE

This is a state-alignment task, not a refactor.
"""

MIGRATION_NAME = "launch_v1_lock"
MIGRATION_VERSION = "3.26.0"

# ===== FORWARD MIGRATION =====
MIGRATION_SQL = """
-- ============================================================
-- LAUNCH V1 LOCK MIGRATION
-- Adds is_launch_v1 boolean flag to os_modules_v3_1
-- Deterministically populates based on tier column
-- ============================================================

-- Step 1: Add the launch flag column (idempotent)
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;

-- Step 2: Add index for efficient filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_is_launch_v1 
    ON os_modules_v3_1(is_launch_v1) 
    WHERE is_launch_v1 = TRUE;

-- Step 3: Populate launch flag deterministically
-- Include Tier 1 + Tier 2 in Launch v1
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'TIER1', 'TIER2');

-- Explicitly exclude Tier 3
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'TIER3')
   OR tier IS NULL;

-- Step 4: Add comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 scope flag. TRUE = included in launch (Tier 1 + Tier 2). FALSE = excluded (Tier 3).';
"""

# ===== ROLLBACK MIGRATION =====
ROLLBACK_SQL = """
-- ============================================================
-- ROLLBACK: LAUNCH V1 LOCK MIGRATION
-- Removes is_launch_v1 column
-- ============================================================

DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
"""

# ===== VALIDATION QUERIES =====
VALIDATION_QUERIES = {
    "tier_distribution": """
        SELECT tier, is_launch_v1, COUNT(*) as count
        FROM os_modules_v3_1
        GROUP BY tier, is_launch_v1
        ORDER BY tier;
    """,
    "launch_v1_count": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as launch_v1_count,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded_count,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """,
    "tier3_preserved": """
        SELECT COUNT(*) as tier3_count
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'TIER3');
    """,
    "launch_by_tier": """
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier;
    """,
}


def run_migration(cursor):
    """Execute the forward migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "migration": MIGRATION_NAME, "version": MIGRATION_VERSION}


def run_rollback(cursor):
    """Execute the rollback migration."""
    cursor.execute(ROLLBACK_SQL)
    return {"status": "rolled_back", "migration": MIGRATION_NAME}


def validate_migration(cursor) -> dict:
    """Run validation queries and return results."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n=== FORWARD MIGRATION ===")
    print(MIGRATION_SQL)
    print("\n=== ROLLBACK ===")
    print(ROLLBACK_SQL)
