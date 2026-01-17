-- ============================================
-- Migration 010: Delete brain-focus duplicates
-- GenoMAX² Launch v1 Data Quality Fix
-- ============================================
--
-- Problem:
--   3 different supplements incorrectly share shopify_handle 'brain-focus-formula-maximo':
--   - GLU-BACOPA-M-007 (Bacopa) - CANONICAL, KEEP
--   - GLU-DMAE-M-108 (DMAE) - DUPLICATE, DELETE
--   - GLU-HUPERZ-M-109 (Huperzine A) - DUPLICATE, DELETE
--
-- Root cause:
--   Data entry error assigned same shopify_handle to different products.
--
-- Fix:
--   Hard delete the non-canonical rows (DMAE and Huperzine).
--   Keep only GLU-BACOPA-M-007 and GLU-BACOPA-F-007 as the brain-focus pair.
--
-- Safety:
--   Full row snapshot saved to delete_audit_v1 before deletion.
--
-- ============================================

BEGIN;

-- 1) Create audit table if not exists
CREATE TABLE IF NOT EXISTS delete_audit_v1 (
  id BIGSERIAL PRIMARY KEY,
  deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  migration_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  module_code TEXT,
  product_name TEXT,
  shopify_handle TEXT,
  base_handle TEXT,
  os_environment TEXT,
  tier TEXT,
  supplier_status TEXT,
  is_launch_v1 BOOLEAN,
  full_row JSONB
);

-- 2) Snapshot offenders into audit table BEFORE delete
INSERT INTO delete_audit_v1 (
  migration_id,
  reason, 
  module_code, 
  product_name,
  shopify_handle, 
  base_handle, 
  os_environment,
  tier,
  supplier_status,
  is_launch_v1,
  full_row
)
SELECT
  '010_delete_brain_focus_duplicates',
  'brain-focus duplicate removal: non-canonical row sharing shopify_handle with GLU-BACOPA-M-007',
  module_code, 
  product_name,
  shopify_handle, 
  base_handle, 
  os_environment,
  tier,
  supplier_status,
  is_launch_v1,
  to_jsonb(os_modules_v3_1.*)
FROM os_modules_v3_1
WHERE module_code IN ('GLU-DMAE-M-108', 'GLU-HUPERZ-M-109');

-- 3) Hard delete the incorrect rows (exact module_code list, no fuzzy matching)
DELETE FROM os_modules_v3_1
WHERE module_code IN ('GLU-DMAE-M-108', 'GLU-HUPERZ-M-109');

-- 4) Log migration to main audit_log
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES (
  'migration',
  NULL,
  'migration_010_delete_brain_focus_duplicates',
  jsonb_build_object(
    'version', '010',
    'action', 'HARD_DELETE',
    'deleted_module_codes', ARRAY['GLU-DMAE-M-108', 'GLU-HUPERZ-M-109'],
    'canonical_kept', ARRAY['GLU-BACOPA-M-007', 'GLU-BACOPA-F-007'],
    'reason', 'Non-canonical rows incorrectly sharing brain-focus-formula shopify_handle',
    'audit_table', 'delete_audit_v1'
  ),
  NOW()
);

COMMIT;
