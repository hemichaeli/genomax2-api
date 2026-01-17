-- ============================================
-- Migration 011: Unique constraint for shopify_handle + environment
-- GenoMAX² Launch v1 Data Integrity
-- ============================================
--
-- Purpose:
--   Prevent future duplicate shopify_handle entries within the same environment.
--   This enforces that each Shopify handle can only appear once per environment.
--
-- Constraint:
--   UNIQUE(shopify_handle, os_environment)
--
-- Pre-requisites:
--   Migration 010 must run first to delete existing duplicates.
--   If any duplicates remain, this migration will fail (as intended).
--
-- ============================================

BEGIN;

-- 1) Verify no duplicates exist before adding constraint
-- This query should return 0 rows. If it returns rows, migration 010 didn't complete.
DO $$
DECLARE
  dup_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO dup_count
  FROM (
    SELECT shopify_handle, os_environment, COUNT(*) as cnt
    FROM os_modules_v3_1
    WHERE shopify_handle IS NOT NULL
    GROUP BY shopify_handle, os_environment
    HAVING COUNT(*) > 1
  ) dups;
  
  IF dup_count > 0 THEN
    RAISE EXCEPTION 'Cannot create unique constraint: % duplicate (shopify_handle, os_environment) combinations exist. Run migration 010 first.', dup_count;
  END IF;
END$$;

-- 2) Create unique index to enforce constraint
-- Using unique index instead of constraint for more flexibility
CREATE UNIQUE INDEX IF NOT EXISTS ux_os_modules_shopify_handle_env
ON os_modules_v3_1(shopify_handle, os_environment)
WHERE shopify_handle IS NOT NULL;

-- 3) Log migration to audit
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES (
  'migration',
  NULL,
  'migration_011_unique_shopify_handle_env',
  jsonb_build_object(
    'version', '011',
    'constraint_type', 'UNIQUE INDEX',
    'index_name', 'ux_os_modules_shopify_handle_env',
    'columns', ARRAY['shopify_handle', 'os_environment'],
    'purpose', 'Prevent duplicate shopify_handle per environment'
  ),
  NOW()
);

COMMIT;
