-- Migration 009: Seed known legitimate single-environment products
-- Version: 3.28.0
-- Date: 2026-01-17
--
-- Purpose:
-- Mark products that are intentionally gender-specific (no male equivalent exists)
-- These should NOT be flagged as pairing errors by QA
--
-- Products marked SINGLE_ENV_ALLOWED:
-- - women-s-vitality-formula (IMM-MACA-F-060) - Female-only vitality product
-- - female-enhancement (GLU-MACA-F-073) - Female-only enhancement product

BEGIN;

-- 1) Update known single-environment products
UPDATE os_modules_v3_1
SET 
  pairing_policy = 'SINGLE_ENV_ALLOWED',
  updated_at = NOW()
WHERE base_handle IN ('women-s-vitality-formula', 'female-enhancement');

-- 2) Log migration to audit with affected module_codes
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
SELECT 
  'migration',
  NULL,
  'migration_009_pairing_policy_seed',
  jsonb_build_object(
    'version', '3.28.0',
    'action', 'set_single_env_allowed',
    'base_handles', ARRAY['women-s-vitality-formula', 'female-enhancement'],
    'affected_modules', array_agg(module_code),
    'affected_count', COUNT(*)
  ),
  NOW()
FROM os_modules_v3_1
WHERE base_handle IN ('women-s-vitality-formula', 'female-enhancement');

COMMIT;

-- Verification query (run after migration):
-- SELECT base_handle, module_code, os_environment, pairing_policy 
-- FROM os_modules_v3_1 
-- WHERE pairing_policy = 'SINGLE_ENV_ALLOWED';
