-- ============================================================
-- GenoMAX² Module Suspension Migration
-- DIG-NATURA-M-087 and DIG-NATURA-F-087
-- ============================================================
-- Reason: NO_ACTIVE_SUPLIFUL_PRODUCT
-- - Historical Supliful product removed from catalog (404)
-- - No Supplement Facts or regulatory data exists
-- Decision: FINAL and LOCKED
-- ============================================================

-- Step 1: Update modules to INACTIVE status
UPDATE os_modules_v3_1
SET 
    supplier_status = 'INACTIVE',
    supplier_status_details = 'NO_ACTIVE_SUPLIFUL_PRODUCT: Historical Supliful product removed from catalog (404). No Supplement Facts or regulatory data exists. Decision final and locked.',
    supplier_last_checked_at = NOW()
WHERE module_code IN ('DIG-NATURA-M-087', 'DIG-NATURA-F-087')
RETURNING module_code, product_name, os_environment, supplier_status;

-- Step 2: Log to audit_log
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
SELECT 
    'os_module',
    NULL,
    'supplier_status_update',
    jsonb_build_object(
        'module_code', module_code,
        'new_status', 'INACTIVE',
        'reason', 'NO_ACTIVE_SUPLIFUL_PRODUCT: Historical Supliful product removed from catalog (404). No Supplement Facts or regulatory data exists. Decision final and locked.',
        'migration', 'suspend_dig_natura',
        'executed_at', NOW()
    ),
    NOW()
FROM os_modules_v3_1
WHERE module_code IN ('DIG-NATURA-M-087', 'DIG-NATURA-F-087');

-- Step 3: Verify the update
SELECT 
    module_code,
    product_name,
    os_environment,
    supplier_status,
    supplier_status_details,
    supplier_last_checked_at
FROM os_modules_v3_1
WHERE module_code IN ('DIG-NATURA-M-087', 'DIG-NATURA-F-087');
