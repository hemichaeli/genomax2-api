-- Migration 010: Hard delete brain-focus duplicates (with audit)
-- Version: 3.28.0
-- Date: 2026-01-17
--
-- Purpose:
-- Remove incorrect duplicate rows that share the same shopify_handle.
-- Keep only the canonical Bacopa pair (GLU-BACOPA-M-007, GLU-BACOPA-F-007).
--
-- Rows to DELETE (explicit list, no fuzzy matching):
-- - GLU-DMAE-M-108 (DMAE incorrectly using brain-focus-formula-maximo handle)
-- - GLU-HUPERZ-M-109 (Huperzine A incorrectly using brain-focus-formula-maximo handle)
--
-- Safety:
-- 1. Create audit table if not exists
-- 2. Snapshot full rows before delete
-- 3. Delete only exact module_codes listed
-- 4. Verify deletion count matches expected

BEGIN;

-- 1) Create audit table for tracking deletions
CREATE TABLE IF NOT EXISTS delete_audit_v1 (
  id BIGSERIAL PRIMARY KEY,
  deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reason TEXT NOT NULL,
  module_code TEXT NOT NULL,
  product_name TEXT,
  shopify_handle TEXT,
  base_handle TEXT,
  os_environment TEXT,
  tier TEXT,
  supplier_status TEXT,
  is_launch_v1 BOOLEAN,
  full_row_json JSONB
);

-- 2) Snapshot rows to be deleted (full data preservation)
INSERT INTO delete_audit_v1 (
  reason, 
  module_code, 
  product_name,
  shopify_handle, 
  base_handle, 
  os_environment,
  tier,
  supplier_status,
  is_launch_v1,
  full_row_json
)
SELECT
  'brain-focus duplicate removal - keep canonical Bacopa pair only (GLU-BACOPA-M/F-007)',
  module_code,
  product_name,
  shopify_handle,
  REGEXP_REPLACE(shopify_handle, '-maximo$|-maxima$', '', 'i'),
  os_environment,
  tier,
  supplier_status,
  is_launch_v1,
  to_jsonb(m.*)
FROM os_modules_v3_1 m
WHERE module_code IN ('GLU-DMAE-M-108', 'GLU-HUPERZ-M-109');

-- 3) Hard delete the incorrect rows (EXPLICIT module_code list only)
DELETE FROM os_modules_v3_1
WHERE module_code IN ('GLU-DMAE-M-108', 'GLU-HUPERZ-M-109');

-- 4) Log migration to main audit_log
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES (
  'migration',
  NULL,
  'migration_010_delete_brain_focus_duplicates',
  jsonb_build_object(
    'version', '3.28.0',
    'action', 'hard_delete',
    'reason', 'Duplicate shopify_handle brain-focus-formula-maximo shared by 3 modules',
    'canonical_kept', ARRAY['GLU-BACOPA-M-007', 'GLU-BACOPA-F-007'],
    'deleted_modules', ARRAY['GLU-DMAE-M-108', 'GLU-HUPERZ-M-109'],
    'deleted_count', 2,
    'audit_table', 'delete_audit_v1'
  ),
  NOW()
);

COMMIT;

-- Verification queries (run after migration):

-- 1) Confirm only 2 brain-focus modules remain (the Bacopa pair)
-- SELECT module_code, shopify_handle, os_environment 
-- FROM os_modules_v3_1 
-- WHERE shopify_handle ILIKE '%brain-focus%';
-- Expected: GLU-BACOPA-M-007 and GLU-BACOPA-F-007 only

-- 2) Confirm no duplicate handles exist for brain-focus
-- SELECT shopify_handle, os_environment, COUNT(*) 
-- FROM os_modules_v3_1 
-- WHERE shopify_handle ILIKE '%brain-focus%'
-- GROUP BY shopify_handle, os_environment
-- HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- 3) Confirm audit records exist
-- SELECT * FROM delete_audit_v1 WHERE reason ILIKE '%brain-focus%';
-- Expected: 2 rows (GLU-DMAE-M-108, GLU-HUPERZ-M-109)
