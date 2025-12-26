"""
Migration: Suspend DIG-NATURA-M-087 and DIG-NATURA-F-087

Reason: NO_ACTIVE_SUPLIFUL_PRODUCT
- Historical Supliful product removed from catalog (404)
- No Supplement Facts or regulatory data exists

This script can be run standalone or via the /migrate-dig-natura-inactive endpoint.
"""

MODULES_TO_SUSPEND = [
    "DIG-NATURA-M-087",
    "DIG-NATURA-F-087"
]

REASON = "NO_ACTIVE_SUPLIFUL_PRODUCT: Historical Supliful product removed from catalog (404). No Supplement Facts or regulatory data exists. Decision final and locked."

SQL_UPDATE = """
UPDATE os_modules_v3_1
SET 
    supplier_status = 'INACTIVE',
    supplier_status_details = %s,
    supplier_last_checked_at = NOW()
WHERE module_code = ANY(%s)
RETURNING module_code, product_name, os_environment, supplier_status;
"""

SQL_AUDIT = """
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES ('os_module', NULL, 'supplier_status_update', %s, NOW());
"""


def run_migration(conn):
    """Execute the migration using an existing database connection."""
    import json
    
    cur = conn.cursor()
    
    # Execute the update
    cur.execute(SQL_UPDATE, (REASON, MODULES_TO_SUSPEND))
    updated = [dict(row) for row in cur.fetchall()]
    
    # Log to audit
    for module in updated:
        cur.execute(SQL_AUDIT, (json.dumps({
            "module_code": module["module_code"],
            "new_status": "INACTIVE",
            "reason": REASON,
            "migration": "suspend_dig_natura"
        }),))
    
    conn.commit()
    cur.close()
    
    return {
        "status": "success",
        "migration": "suspend_dig_natura",
        "updated_count": len(updated),
        "updated_modules": updated,
        "reason": REASON
    }


if __name__ == "__main__":
    import os
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable not set")
        exit(1)
    
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    result = run_migration(conn)
    conn.close()
    
    print(f"Migration complete: {result}")
