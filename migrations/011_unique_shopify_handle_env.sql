-- Migration 011: Add unique constraint to prevent future shopify_handle duplicates
-- Version: 3.28.0
-- Date: 2026-01-17
--
-- Purpose:
-- Enforce uniqueness of (shopify_handle, os_environment) to prevent
-- multiple modules from sharing the same Shopify handle in the same environment.
--
-- Prerequisite:
-- Migration 010 must be run first to delete existing duplicates.
-- This constraint will FAIL if duplicates still exist.
--
-- Safety:
-- 1. Check for existing violations before creating index
-- 2. Create unique index (acts as constraint)
-- 3. Log migration

BEGIN;

-- 1) Check for existing violations (will abort transaction if any found)
DO $$
DECLARE
  violation_count INTEGER;
  violation_details TEXT;
BEGIN
  SELECT COUNT(*), string_agg(shopify_handle || ' (' || os_environment || ')', ', ')
  INTO violation_count, violation_details
  FROM (
    SELECT shopify_handle, os_environment
    FROM os_modules_v3_1
    WHERE shopify_handle IS NOT NULL
    GROUP BY shopify_handle, os_environment
    HAVING COUNT(*) > 1
  ) violations;
  
  IF violation_count > 0 THEN
    RAISE EXCEPTION 'Cannot create unique constraint: % duplicate(s) found: %', 
      violation_count, violation_details;
  END IF;
END$$;

-- 2) Create unique index (enforces constraint)
CREATE UNIQUE INDEX IF NOT EXISTS ux_os_modules_shopify_handle_env
ON os_modules_v3_1(shopify_handle, os_environment)
WHERE shopify_handle IS NOT NULL;

-- 3) Log migration
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES (
  'migration',
  NULL,
  'migration_011_unique_shopify_handle_env',
  jsonb_build_object(
    'version', '3.28.0',
    'action', 'create_unique_index',
    'index_name', 'ux_os_modules_shopify_handle_env',
    'columns', ARRAY['shopify_handle', 'os_environment'],
    'purpose', 'Prevent duplicate shopify_handle within same environment'
  ),
  NOW()
);

COMMIT;

-- Verification queries:

-- 1) Confirm index exists
-- SELECT indexname, indexdef FROM pg_indexes 
-- WHERE tablename = 'os_modules_v3_1' AND indexname = 'ux_os_modules_shopify_handle_env';

-- 2) Test constraint (should fail)
-- INSERT INTO os_modules_v3_1 (module_code, shopify_handle, os_environment, ...) 
-- VALUES ('TEST-DUP-001', 'brain-focus-formula-maximo', 'MAXimo²', ...);
-- Expected: ERROR: duplicate key value violates unique constraint
