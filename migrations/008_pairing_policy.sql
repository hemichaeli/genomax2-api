-- Migration 008: Add pairing_policy column and base_handle
-- Version: 3.28.0
-- Date: 2026-01-17
--
-- Purpose:
-- - Add explicit pairing_policy to eliminate guessing in QA
-- - Add base_handle column for deterministic grouping
--
-- Policy types:
-- - REQUIRED_PAIR: Must have exactly 2 environments (MAXimo² + MAXima²)
-- - SINGLE_ENV_ALLOWED: Must have exactly 1 environment (gender-specific product)

BEGIN;

-- 1) Create enum type (safe create - check if exists first)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pairing_policy_enum') THEN
    CREATE TYPE pairing_policy_enum AS ENUM ('REQUIRED_PAIR', 'SINGLE_ENV_ALLOWED');
  END IF;
END$$;

-- 2) Add pairing_policy column to os_modules_v3_1
-- Default is REQUIRED_PAIR (most products require both environments)
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS pairing_policy VARCHAR(20) NOT NULL DEFAULT 'REQUIRED_PAIR';

-- 3) Add base_handle column if not already present
-- base_handle = shopify_handle without -maximo/-maxima suffix
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS base_handle TEXT;

-- 4) Backfill base_handle deterministically from shopify_handle
UPDATE os_modules_v3_1
SET base_handle = REGEXP_REPLACE(shopify_handle, '-maximo$|-maxima$', '', 'i')
WHERE base_handle IS NULL 
  AND shopify_handle IS NOT NULL;

-- 5) Index for pairing queries (grouping by base_handle)
CREATE INDEX IF NOT EXISTS idx_os_modules_v3_1_base_handle
ON os_modules_v3_1(base_handle);

-- 6) Index for pairing_policy filtering
CREATE INDEX IF NOT EXISTS idx_os_modules_v3_1_pairing_policy
ON os_modules_v3_1(pairing_policy);

-- 7) Log migration to audit
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES (
  'migration', 
  NULL, 
  'migration_008_pairing_policy',
  '{"version": "3.28.0", "columns_added": ["pairing_policy", "base_handle"], "indexes_created": ["idx_os_modules_v3_1_base_handle", "idx_os_modules_v3_1_pairing_policy"]}'::jsonb,
  NOW()
);

COMMIT;

-- Verification query (run after migration):
-- SELECT pairing_policy, COUNT(*) FROM os_modules_v3_1 GROUP BY pairing_policy;
-- SELECT base_handle, COUNT(*) FROM os_modules_v3_1 WHERE base_handle IS NOT NULL GROUP BY base_handle ORDER BY 2 DESC LIMIT 10;
