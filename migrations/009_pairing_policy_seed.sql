-- ============================================
-- Migration 009: Seed pairing_policy for known single-env products
-- GenoMAX² Launch v1 Pairing Policy
-- ============================================
--
-- Purpose:
--   Mark legitimate single-environment products as SINGLE_ENV_ALLOWED.
--   These are gender-specific products that intentionally exist only
--   in one environment (MAXima² for female-specific products).
--
-- Products marked SINGLE_ENV_ALLOWED:
--   - women-s-vitality-formula (IMM-MACA-F-060) - Female wellness
--   - female-enhancement (GLU-MACA-F-073) - Female-specific
--
-- ============================================

BEGIN;

-- 1) Update known legitimate single-environment products
UPDATE os_modules_v3_1
SET 
  pairing_policy = 'SINGLE_ENV_ALLOWED',
  updated_at = NOW()
WHERE base_handle IN ('women-s-vitality-formula', 'female-enhancement')
  AND pairing_policy = 'REQUIRED_PAIR';

-- 2) Log migration to audit with specific products affected
INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
SELECT 
  'migration',
  NULL,
  'migration_009_pairing_policy_seed',
  jsonb_build_object(
    'version', '009',
    'products_marked_single_env', ARRAY['women-s-vitality-formula', 'female-enhancement'],
    'affected_modules', (
      SELECT jsonb_agg(jsonb_build_object(
        'module_code', module_code,
        'base_handle', base_handle,
        'os_environment', os_environment
      ))
      FROM os_modules_v3_1
      WHERE base_handle IN ('women-s-vitality-formula', 'female-enhancement')
    )
  ),
  NOW();

COMMIT;
