-- ============================================
-- Migration 008: Add pairing_policy column
-- GenoMAX² Launch v1 Pairing Policy
-- ============================================
-- 
-- Purpose:
--   Add explicit pairing policy to eliminate guessing in QA.
--   REQUIRED_PAIR = must have exactly 2 envs (MAXimo² + MAXima²)
--   SINGLE_ENV_ALLOWED = legitimate single-environment product
--
-- ============================================

BEGIN;

-- 1) Create enum type (safe create - skip if exists)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pairing_policy_enum') THEN
    CREATE TYPE pairing_policy_enum AS ENUM ('REQUIRED_PAIR', 'SINGLE_ENV_ALLOWED');
  END IF;
END$$;

-- 2) Add pairing_policy column to os_modules_v3_1
-- Default: REQUIRED_PAIR (most products must have both environments)
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS pairing_policy pairing_policy_enum NOT NULL DEFAULT 'REQUIRED_PAIR';

-- 3) Add base_handle column if not present
-- base_handle = shopify_handle without -maximo/-maxima suffix
ALTER TABLE os_modules_v3_1
ADD COLUMN IF NOT EXISTS base_handle TEXT;

-- 4) Backfill base_handle deterministically from shopify_handle
UPDATE os_modules_v3_1
SET base_handle = REGEXP_REPLACE(shopify_handle, '-maximo$|-maxima$', '', 'i')
WHERE base_handle IS NULL 
  AND shopify_handle IS NOT NULL;

-- 5) Create index for pairing queries
CREATE INDEX IF NOT EXISTS idx_os_modules_v3_1_base_handle
ON os_modules_v3_1(base_handle);

-- 6) Create index for pairing_policy queries
CREATE INDEX IF NOT EXISTS idx_os_modules_v3_1_pairing_policy
ON os_modules_v3_1(pairing_policy);

-- 7) Log migration to audit
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
VALUES (
  'migration', 
  NULL, 
  'migration_008_pairing_policy',
  '{"version": "008", "columns_added": ["pairing_policy", "base_handle"], "default_policy": "REQUIRED_PAIR"}',
  NOW()
);

COMMIT;
