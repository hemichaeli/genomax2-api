"""
Admin endpoints for GenoMAX² API
Module status management and migrations
"""

import json
from api_server import app, get_db, now_iso


@app.get("/migrate-dig-natura-inactive")
def migrate_dig_natura_inactive():
    """
    One-time migration: Suspend DIG-NATURA-M-087 and DIG-NATURA-F-087.
    
    Reason: NO_ACTIVE_SUPLIFUL_PRODUCT
    - Historical Supliful product removed from catalog (404)
    - No Supplement Facts or regulatory data exists
    
    Decision is FINAL and LOCKED.
    """
    MODULES_TO_SUSPEND = [
        "DIG-NATURA-M-087",
        "DIG-NATURA-F-087"
    ]
    
    REASON = "NO_ACTIVE_SUPLIFUL_PRODUCT: Historical Supliful product removed from catalog (404). No Supplement Facts or regulatory data exists. Decision final and locked."
    
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    
    try:
        cur = conn.cursor()
        
        # Check if supplier_status column exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'os_modules_v3_1' AND column_name = 'supplier_status'
        """)
        if not cur.fetchone():
            cur.close()
            conn.close()
            return {"error": "supplier_status column not found. Run /migrate-supplier-status first."}
        
        # Execute the update
        cur.execute("""
            UPDATE os_modules_v3_1
            SET 
                supplier_status = 'INACTIVE',
                supplier_status_details = %s,
                supplier_last_checked_at = NOW()
            WHERE module_code = ANY(%s)
            RETURNING module_code, product_name, os_environment, supplier_status
        """, (REASON, MODULES_TO_SUSPEND))
        
        updated = [dict(row) for row in cur.fetchall()]
        
        # Log to audit
        for module in updated:
            cur.execute("""
                INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
                VALUES ('os_module', NULL, 'supplier_status_update', %s, NOW())
            """, (json.dumps({
                "module_code": module["module_code"],
                "new_status": "INACTIVE",
                "reason": REASON,
                "migration": "suspend_dig_natura"
            }),))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "suspend_dig_natura",
            "timestamp": now_iso(),
            "updated_count": len(updated),
            "updated_modules": updated,
            "reason": REASON,
            "effects": {
                "routing": "EXCLUDED - supplier_status IN ('ACTIVE','UNKNOWN') filter excludes INACTIVE",
                "frontend": "EXCLUDED - should filter by supplier_status != 'INACTIVE'",
                "shopify_sync": "EXCLUDED - no active product to sync",
                "label_generation": "EXCLUDED - no Supplement Facts data",
                "audit_reference": "PRESERVED - module remains in catalog for historical reference"
            }
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.post("/admin/module-status")
def update_module_status_generic(module_codes: list, status: str, reason: str):
    """
    Update supplier_status for one or more modules.
    
    Valid statuses: ACTIVE, UNKNOWN, DISCONTINUING_SOON, INACTIVE
    """
    from fastapi import HTTPException
    
    valid_statuses = ["ACTIVE", "UNKNOWN", "DISCONTINUING_SOON", "INACTIVE"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE os_modules_v3_1
            SET 
                supplier_status = %s,
                supplier_status_details = %s,
                supplier_last_checked_at = NOW()
            WHERE module_code = ANY(%s)
            RETURNING module_code, product_name, os_environment, supplier_status
        """, (status, reason, module_codes))
        
        updated = [dict(row) for row in cur.fetchall()]
        
        for module in updated:
            cur.execute("""
                INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
                VALUES ('os_module', NULL, 'supplier_status_update', %s, NOW())
            """, (json.dumps({
                "module_code": module["module_code"],
                "new_status": status,
                "reason": reason
            }),))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "updated_count": len(updated),
            "updated_modules": updated,
            "timestamp": now_iso()
        }
        
    except Exception as e:
        try: conn.close()
        except: pass
        raise HTTPException(status_code=500, detail=str(e))
