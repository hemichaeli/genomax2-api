-- migration 003: disclaimer_symbol + disclaimer_applicability
-- Add columns to support:
-- 1) Consistent disclaimer formatting (symbol), default '*'
-- 2) TOPICAL products exempt from DSHEA disclaimer requirement

BEGIN;

-- 1) Add columns
ALTER TABLE public.os_modules_v3_1
  ADD COLUMN IF NOT EXISTS disclaimer_symbol VARCHAR(8);

ALTER TABLE public.os_modules_v3_1
  ADD COLUMN IF NOT EXISTS disclaimer_applicability VARCHAR(16);

-- 2) Defaults
ALTER TABLE public.os_modules_v3_1
  ALTER COLUMN disclaimer_symbol SET DEFAULT '*';

ALTER TABLE public.os_modules_v3_1
  ALTER COLUMN disclaimer_applicability SET DEFAULT 'SUPPLEMENT';

-- 3) Backfill existing rows
UPDATE public.os_modules_v3_1
SET disclaimer_symbol = '*'
WHERE disclaimer_symbol IS NULL OR BTRIM(disclaimer_symbol) = '';

UPDATE public.os_modules_v3_1
SET disclaimer_applicability = 'SUPPLEMENT'
WHERE disclaimer_applicability IS NULL OR BTRIM(disclaimer_applicability) = '';

-- 4) CHECK constraint
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_os_modules_v3_1_disclaimer_applicability'
  ) THEN
    ALTER TABLE public.os_modules_v3_1
      ADD CONSTRAINT chk_os_modules_v3_1_disclaimer_applicability
      CHECK (disclaimer_applicability IN ('SUPPLEMENT', 'TOPICAL'));
  END IF;
END $$;

-- 5) Index for filtering
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'public'
      AND tablename = 'os_modules_v3_1'
      AND indexname = 'idx_os_modules_v3_1_disclaimer_applicability'
  ) THEN
    CREATE INDEX idx_os_modules_v3_1_disclaimer_applicability
      ON public.os_modules_v3_1 (disclaimer_applicability);
  END IF;
END $$;

COMMIT;
