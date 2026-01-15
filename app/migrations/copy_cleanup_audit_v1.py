"""
Migration: Create copy_cleanup_audit_v1 table
Version: 3.25.0
Date: 2025-01-15

Creates audit table for tracking copy cleanup operations.
"""

MIGRATION_SQL = """
-- Copy Cleanup Audit Table
-- Tracks all field-level changes during placeholder removal

CREATE TABLE IF NOT EXISTS copy_cleanup_audit_v1 (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID NOT NULL,
    module_code TEXT NOT NULL,
    shopify_handle TEXT,
    field_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_copy_audit_batch_id 
    ON copy_cleanup_audit_v1(batch_id);
CREATE INDEX IF NOT EXISTS idx_copy_audit_module_code 
    ON copy_cleanup_audit_v1(module_code);
CREATE INDEX IF NOT EXISTS idx_copy_audit_created_at 
    ON copy_cleanup_audit_v1(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_copy_audit_field_name 
    ON copy_cleanup_audit_v1(field_name);

-- Comment on table
COMMENT ON TABLE copy_cleanup_audit_v1 IS 
    'Audit trail for copy cleanup operations - tracks placeholder removal from label/body fields';
"""

def run_migration(cursor):
    """Execute the migration."""
    cursor.execute(MIGRATION_SQL)
    return {"status": "success", "table": "copy_cleanup_audit_v1"}


if __name__ == "__main__":
    print("Migration: copy_cleanup_audit_v1")
    print(MIGRATION_SQL)
