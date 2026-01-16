"""
Migration: Add is_launch_v1 flag to os_modules_v3_1
Version: 3.26.0
Date: 2026-01-15

Purpose: Lock Launch v1 scope in DB
- TIER 1 + TIER 2 = Launch v1 (is_launch_v1 = TRUE)
- TIER 3 = Excluded from launch (is_launch_v1 = FALSE)

This is a state-alignment task, not a refactor.
Products remain unchanged except for the new flag.
"""

MIGRATION_ID = "launch_v1_lock_001"
MIGRATION_VERSION = "3.26.0"

# Step 1: Add the column
ADD_COLUMN_SQL = """
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS is_launch_v1 BOOLEAN DEFAULT FALSE;
"""

# Step 2: Create index for query performance
ADD_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_os_modules_launch_v1 
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
   OR is_launch_v1 IS NULL;
"""

# Step 5: Add comment for documentation
ADD_COMMENT_SQL = """
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
'Launch v1 inclusion flag. TRUE = TIER 1 + TIER 2 (included in launch). FALSE = TIER 3 or excluded. Locked 2026-01-15.';
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
    "tier_3_preserved": """
        SELECT COUNT(*) as tier_3_count
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "sanity_check": """
        SELECT 
            SUM(CASE WHEN is_launch_v1 = TRUE THEN 1 ELSE 0 END) as in_launch,
            SUM(CASE WHEN is_launch_v1 = FALSE OR is_launch_v1 IS NULL THEN 1 ELSE 0 END) as excluded,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """
}

# Rollback SQL (if needed)
ROLLBACK_SQL = """
-- Rollback: Remove is_launch_v1 column
-- Only use if migration needs to be reverted
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
DROP INDEX IF EXISTS idx_os_modules_launch_v1;
"""

def get_full_migration_sql():
    """Return complete migration SQL as single script."""
    return f"""
-- ============================================
-- Migration: Launch v1 Lock
-- ID: {MIGRATION_ID}
-- Version: {MIGRATION_VERSION}
-- Date: 2026-01-15
-- ============================================

-- Step 1: Add is_launch_v1 column
{ADD_COLUMN_SQL}

-- Step 2: Add index for performance
{ADD_INDEX_SQL}

-- Step 3: Set Tier 1 + Tier 2 as Launch v1
{SET_TIER_1_2_SQL}

-- Step 4: Explicitly exclude Tier 3
{SET_TIER_3_SQL}

-- Step 5: Add documentation comment
{ADD_COMMENT_SQL}

-- Migration complete
"""


def run_migration(cursor):
    """Execute the migration step by step."""
    results = {
        "migration_id": MIGRATION_ID,
        "version": MIGRATION_VERSION,
        "steps": []
    }
    
    # Step 1: Add column
    cursor.execute(ADD_COLUMN_SQL)
    results["steps"].append({"step": 1, "action": "add_column", "status": "success"})
    
    # Step 2: Add index
    cursor.execute(ADD_INDEX_SQL)
    results["steps"].append({"step": 2, "action": "add_index", "status": "success"})
    
    # Step 3: Set Tier 1 + 2
    cursor.execute(SET_TIER_1_2_SQL)
    tier_1_2_count = cursor.rowcount
    results["steps"].append({
        "step": 3, 
        "action": "set_tier_1_2", 
        "status": "success",
        "rows_affected": tier_1_2_count
    })
    
    # Step 4: Set Tier 3
    cursor.execute(SET_TIER_3_SQL)
    tier_3_count = cursor.rowcount
    results["steps"].append({
        "step": 4, 
        "action": "set_tier_3", 
        "status": "success",
        "rows_affected": tier_3_count
    })
    
    # Step 5: Add comment
    cursor.execute(ADD_COMMENT_SQL)
    results["steps"].append({"step": 5, "action": "add_comment", "status": "success"})
    
    return results


def validate_migration(cursor):
    """Run validation queries and return results."""
    validation = {}
    
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        rows = cursor.fetchall()
        validation[name] = [dict(row) for row in rows] if rows else []
    
    return validation


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_ID}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n" + "="*50)
    print(get_full_migration_sql())
