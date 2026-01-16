"""
Migration: Add is_launch_v1 column and lock Launch v1 scope
Version: 3.26.0
Date: 2026-01-16

Purpose:
- Add is_launch_v1 boolean flag to os_modules_v3_1
- Set TRUE for TIER 1 and TIER 2 products
- Set FALSE for TIER 3 products
- Enforces Launch v1 scope at DB level

Constraints:
- Does NOT delete any products
- Does NOT modify tier values
- Does NOT change copy, net_quantity, or supplier data
- Only adds/populates the is_launch_v1 flag
"""

MIGRATION_NAME = "launch_v1_lock"
MIGRATION_VERSION = "3.26.0"

# Step 1: Add column
ADD_COLUMN_SQL = """
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;
"""

# Step 2: Populate for Tier 1 + Tier 2
SET_LAUNCH_TRUE_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');
"""

# Step 3: Explicitly exclude Tier 3
SET_LAUNCH_FALSE_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL
   OR tier = '';
"""

# Step 4: Create index for query performance
CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
ON os_modules_v3_1(is_launch_v1) 
WHERE is_launch_v1 = TRUE;
"""

# Validation queries
VALIDATION_LAUNCH_COUNTS_SQL = """
SELECT 
    tier,
    COUNT(*) as total,
    SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as in_launch,
    SUM(CASE WHEN is_launch_v1 = FALSE THEN 1 ELSE 0 END) as excluded
FROM os_modules_v3_1
GROUP BY tier
ORDER BY tier;
"""

VALIDATION_TIER3_EXISTS_SQL = """
SELECT COUNT(*) as tier3_count
FROM os_modules_v3_1
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
"""

VALIDATION_LAUNCH_SUMMARY_SQL = """
SELECT 
    is_launch_v1,
    COUNT(*) as count,
    COUNT(DISTINCT tier) as distinct_tiers
FROM os_modules_v3_1
GROUP BY is_launch_v1;
"""

# Rollback (if needed)
ROLLBACK_SQL = """
ALTER TABLE os_modules_v3_1
DROP COLUMN IF EXISTS is_launch_v1;
"""


def get_full_migration_sql():
    """Return complete migration as single transaction."""
    return f"""
-- ============================================
-- Migration: Launch v1 Lock
-- Version: {MIGRATION_VERSION}
-- ============================================

BEGIN;

-- Step 1: Add is_launch_v1 column
{ADD_COLUMN_SQL}

-- Step 2: Include Tier 1 + Tier 2 in Launch v1
{SET_LAUNCH_TRUE_SQL}

-- Step 3: Explicitly exclude Tier 3 and NULL tiers
{SET_LAUNCH_FALSE_SQL}

-- Step 4: Create index for performance
{CREATE_INDEX_SQL}

COMMIT;

-- Validation (run after migration)
-- {VALIDATION_LAUNCH_COUNTS_SQL}
"""


def run_migration(cursor):
    """Execute the migration step by step."""
    results = {
        "migration": MIGRATION_NAME,
        "version": MIGRATION_VERSION,
        "steps": []
    }
    
    # Step 1: Add column
    cursor.execute(ADD_COLUMN_SQL)
    results["steps"].append({"step": 1, "action": "add_column", "status": "success"})
    
    # Step 2: Set TRUE for Tier 1 + 2
    cursor.execute(SET_LAUNCH_TRUE_SQL)
    tier12_count = cursor.rowcount
    results["steps"].append({
        "step": 2, 
        "action": "set_launch_true", 
        "rows_affected": tier12_count,
        "status": "success"
    })
    
    # Step 3: Set FALSE for Tier 3
    cursor.execute(SET_LAUNCH_FALSE_SQL)
    tier3_count = cursor.rowcount
    results["steps"].append({
        "step": 3, 
        "action": "set_launch_false", 
        "rows_affected": tier3_count,
        "status": "success"
    })
    
    # Step 4: Create index
    cursor.execute(CREATE_INDEX_SQL)
    results["steps"].append({"step": 4, "action": "create_index", "status": "success"})
    
    results["status"] = "success"
    results["launch_v1_count"] = tier12_count
    results["excluded_count"] = tier3_count
    
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print()
    print(get_full_migration_sql())
