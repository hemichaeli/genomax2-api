"""
Migration: Add is_launch_v1 flag to os_modules_v3_1
Version: 3.26.0
Date: 2025-01-15

Locks Launch v1 to TIER 1 + TIER 2 products only.
Tier 3 products remain in DB but are excluded from launch pipelines.

This is a state-alignment task, not a refactor.
"""

# Migration version for tracking
MIGRATION_VERSION = "3.26.0"
MIGRATION_NAME = "launch_v1_lock"

# Step 1: Add the launch flag column
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

# Step 3: Populate launch flag - Include Tier 1 + Tier 2
POPULATE_TIER_1_2_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = TRUE
WHERE tier IN ('TIER 1', 'TIER 2', 'Tier 1', 'Tier 2', 'tier 1', 'tier 2', '1', '2');
"""

# Step 4: Explicitly exclude Tier 3 (defensive)
EXCLUDE_TIER_3_SQL = """
UPDATE os_modules_v3_1
SET is_launch_v1 = FALSE
WHERE tier IN ('TIER 3', 'Tier 3', 'tier 3', '3')
   OR tier IS NULL;
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
    "launch_v1_counts": """
        SELECT 
            COUNT(*) FILTER (WHERE is_launch_v1 = TRUE) as in_launch,
            COUNT(*) FILTER (WHERE is_launch_v1 = FALSE) as excluded,
            COUNT(*) as total
        FROM os_modules_v3_1;
    """,
}

# Rollback SQL (if needed)
ROLLBACK_SQL = """
-- Rollback: Remove is_launch_v1 column
-- WARNING: Only use if migration needs to be reverted
ALTER TABLE os_modules_v3_1 DROP COLUMN IF EXISTS is_launch_v1;
DROP INDEX IF EXISTS idx_os_modules_is_launch_v1;
"""

# Full migration script
FULL_MIGRATION_SQL = f"""
-- =====================================================
-- GenoMAX² Launch v1 Lock Migration
-- Version: {MIGRATION_VERSION}
-- Date: 2025-01-15
-- =====================================================

-- Step 1: Add is_launch_v1 column
{ADD_COLUMN_SQL}

-- Step 2: Create partial index for launch queries
{ADD_INDEX_SQL}

-- Step 3: Include Tier 1 + Tier 2 in Launch v1
{POPULATE_TIER_1_2_SQL}

-- Step 4: Explicitly exclude Tier 3 and NULL tiers
{EXCLUDE_TIER_3_SQL}

-- Add column comment for documentation
COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
    'Launch v1 flag: TRUE for TIER 1 + TIER 2 products only. Tier 3 excluded.';
"""


def run_migration(cursor):
    """Execute the full migration."""
    results = {
        "migration": MIGRATION_NAME,
        "version": MIGRATION_VERSION,
        "steps": []
    }
    
    # Step 1: Add column
    cursor.execute(ADD_COLUMN_SQL)
    results["steps"].append({"step": "add_column", "status": "success"})
    
    # Step 2: Add index
    cursor.execute(ADD_INDEX_SQL)
    results["steps"].append({"step": "add_index", "status": "success"})
    
    # Step 3: Populate Tier 1 + Tier 2
    cursor.execute(POPULATE_TIER_1_2_SQL)
    tier_1_2_count = cursor.rowcount
    results["steps"].append({
        "step": "populate_tier_1_2", 
        "status": "success",
        "rows_updated": tier_1_2_count
    })
    
    # Step 4: Exclude Tier 3
    cursor.execute(EXCLUDE_TIER_3_SQL)
    tier_3_count = cursor.rowcount
    results["steps"].append({
        "step": "exclude_tier_3", 
        "status": "success",
        "rows_updated": tier_3_count
    })
    
    # Add comment
    cursor.execute("""
        COMMENT ON COLUMN os_modules_v3_1.is_launch_v1 IS 
            'Launch v1 flag: TRUE for TIER 1 + TIER 2 products only. Tier 3 excluded.';
    """)
    results["steps"].append({"step": "add_comment", "status": "success"})
    
    return results


def validate_migration(cursor):
    """Run validation queries after migration."""
    results = {}
    
    for name, query in VALIDATION_QUERIES.items():
        cursor.execute(query)
        rows = cursor.fetchall()
        results[name] = [dict(row) for row in rows] if rows else []
    
    # Check constraints
    validation_passed = True
    
    # Tier 3 should NOT be in launch
    if results.get("tier_3_excluded") and results["tier_3_excluded"][0].get("tier_3_in_launch", 0) > 0:
        validation_passed = False
        results["error"] = "TIER 3 found in launch - constraint violated"
    
    results["validation_passed"] = validation_passed
    return results


if __name__ == "__main__":
    print(f"Migration: {MIGRATION_NAME}")
    print(f"Version: {MIGRATION_VERSION}")
    print("\n=== Full Migration SQL ===")
    print(FULL_MIGRATION_SQL)
    print("\n=== Rollback SQL ===")
    print(ROLLBACK_SQL)
