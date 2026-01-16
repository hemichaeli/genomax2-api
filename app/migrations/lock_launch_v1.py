"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2025-01-15

Adds is_launch_v1 flag to os_modules_v3_1 and populates it based on tier.

Launch v1 Definition:
- INCLUDES: TIER 1, TIER 2
- EXCLUDES: TIER 3

This is a state-alignment task. No product data is modified.
"""

# Step 1: Add column
ADD_COLUMN_SQL = """
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;
"""

# Step 2: Populate flag - Include Tier 1 + Tier 2
INCLUDE_TIERS_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', 'T1', 'T2', '1', '2');
"""

# Step 3: Explicitly exclude Tier 3 (safety)
EXCLUDE_TIER3_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3')
   OR tier IS NULL;
"""

# Step 4: Create index for pipeline filtering
CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;
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
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', 'T3', '3');
    """,
    "launch_summary": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as in_launch_v1,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE OR is_launch_v1 IS NULL) as excluded,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """,
    "tier_distribution_full": """
        SELECT 
            tier,
            is_launch_v1,
            supplier_status,
            COUNT(*) as count
        FROM os_modules_v3_1
        GROUP BY tier, is_launch_v1, supplier_status
        ORDER BY tier, is_launch_v1, supplier_status;
    """
}

# Rollback SQL (if needed)
ROLLBACK_SQL = """
-- Rollback: Remove launch flag column
-- ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
-- DROP INDEX IF EXISTS idx_os_modules_launch_v1;
"""

FULL_MIGRATION_SQL = f"""
-- =====================================================
-- MIGRATION: Lock Launch v1 in Database
-- Version: 3.26.0
-- Date: 2025-01-15
-- =====================================================

-- Step 1: Add is_launch_v1 column
{ADD_COLUMN_SQL}

-- Step 2: Include Tier 1 + Tier 2 in Launch v1
{INCLUDE_TIERS_SQL}

-- Step 3: Explicitly exclude Tier 3
{EXCLUDE_TIER3_SQL}

-- Step 4: Create index for efficient filtering
{CREATE_INDEX_SQL}

-- =====================================================
-- VALIDATION (run after migration)
-- =====================================================
-- SELECT tier, COUNT(*) FROM os_modules_v3_1 WHERE is_launch_v1 = TRUE GROUP BY tier;
-- Expected: TIER 1 → >0, TIER 2 → >0, TIER 3 → 0

-- SELECT COUNT(*) FROM os_modules_v3_1 WHERE tier = 'TIER 3';
-- Expected: >0 (preserved, just excluded)
"""


def run_migration(cursor):
    """Execute the migration steps."""
    results = {}
    
    # Step 1: Add column
    cursor.execute(ADD_COLUMN_SQL)
    results["add_column"] = "success"
    
    # Step 2: Include tiers
    cursor.execute(INCLUDE_TIERS_SQL)
    results["include_tiers"] = cursor.rowcount
    
    # Step 3: Exclude tier 3
    cursor.execute(EXCLUDE_TIER3_SQL)
    results["exclude_tier3"] = cursor.rowcount
    
    # Step 4: Create index
    cursor.execute(CREATE_INDEX_SQL)
    results["create_index"] = "success"
    
    return results


def validate_migration(cursor):
    """Run validation queries after migration."""
    results = {}
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        results[name] = cursor.fetchall()
    return results


if __name__ == "__main__":
    print("Migration: Lock Launch v1")
    print(FULL_MIGRATION_SQL)
