"""
Migration: Lock Launch v1 in Database
Version: 3.26.0
Date: 2026-01-15

Adds is_launch_v1 flag to os_modules_v3_1 table.
Launch v1 includes: TIER 1 + TIER 2
Launch v1 excludes: TIER 3

This is a state-alignment task - no data modification except the new flag.
"""

MIGRATION_VERSION = "3.26.0"
MIGRATION_NAME = "lock_launch_v1"

# Step 1: Add the column
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

# Step 3: Populate based on tier
# Include Tier 1 + Tier 2
POPULATE_LAUNCH_V1_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');
"""

# Explicitly exclude Tier 3 (defensive)
EXCLUDE_TIER_3_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL;
"""

# Step 4: Add comment for documentation
ADD_COMMENT_SQL = """
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
'Launch v1 flag: TRUE for TIER 1 + TIER 2 products only. TIER 3 excluded from launch pipelines.';
"""

# Validation queries
VALIDATION_QUERIES = {
    "tier_distribution_in_launch": """
        SELECT tier, COUNT(*) as count
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE
        GROUP BY tier
        ORDER BY tier;
    """,
    "tier_3_excluded": """
        SELECT COUNT(*) as tier_3_in_launch
        FROM os_modules_v3_1
        WHERE is_launch_v1 = TRUE 
          AND tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "tier_3_preserved": """
        SELECT COUNT(*) as tier_3_total
        FROM os_modules_v3_1
        WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3');
    """,
    "launch_v1_summary": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as in_launch_v1,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """
}

# Rollback SQL (if needed)
ROLLBACK_SQL = """
ALTER TABLE os_modules_v3_1
DROP COLUMN IF EXISTS is_launch_v1;
"""


def get_full_migration_sql():
    """Return the complete migration as a single SQL block."""
    return f"""
-- Migration: Lock Launch v1
-- Version: {MIGRATION_VERSION}
-- Date: 2026-01-15

-- Step 1: Add is_launch_v1 column
{ADD_COLUMN_SQL}

-- Step 2: Create partial index for launch queries
{ADD_INDEX_SQL}

-- Step 3: Populate launch flag for Tier 1 + Tier 2
{POPULATE_LAUNCH_V1_SQL}

-- Step 4: Explicitly exclude Tier 3 and NULL tiers
{EXCLUDE_TIER_3_SQL}

-- Step 5: Add documentation
{ADD_COMMENT_SQL}
"""


def run_migration(cursor):
    """Execute the migration step by step."""
    results = {
        "version": MIGRATION_VERSION,
        "name": MIGRATION_NAME,
        "steps": []
    }
    
    # Step 1: Add column
    cursor.execute(ADD_COLUMN_SQL)
    results["steps"].append({"step": "add_column", "status": "success"})
    
    # Step 2: Create index
    cursor.execute(ADD_INDEX_SQL)
    results["steps"].append({"step": "add_index", "status": "success"})
    
    # Step 3: Populate Tier 1 + Tier 2
    cursor.execute(POPULATE_LAUNCH_V1_SQL)
    tier_12_count = cursor.rowcount
    results["steps"].append({
        "step": "populate_tier_1_2", 
        "status": "success",
        "rows_updated": tier_12_count
    })
    
    # Step 4: Exclude Tier 3
    cursor.execute(EXCLUDE_TIER_3_SQL)
    tier_3_count = cursor.rowcount
    results["steps"].append({
        "step": "exclude_tier_3", 
        "status": "success",
        "rows_updated": tier_3_count
    })
    
    # Step 5: Add comment
    cursor.execute(ADD_COMMENT_SQL)
    results["steps"].append({"step": "add_comment", "status": "success"})
    
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
    print(f"Migration: {MIGRATION_NAME} v{MIGRATION_VERSION}")
    print("=" * 50)
    print(get_full_migration_sql())
