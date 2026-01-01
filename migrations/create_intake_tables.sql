-- ============================================
-- GenoMAX² Product Intake System
-- Migration: Create catalog_intakes and catalog_snapshots tables
-- Version: intake_system_v1
-- ============================================

-- Enable UUID generation if not already enabled
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- Table: catalog_intakes
-- Tracks product intake submissions through the draft/approve/reject workflow
-- ============================================
CREATE TABLE IF NOT EXISTS catalog_intakes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Supplier info
    supplier TEXT NOT NULL DEFAULT 'supliful',
    product_url TEXT NOT NULL,
    
    -- Raw and parsed data
    supplier_payload JSONB,           -- Raw supplier API response
    parsed_payload JSONB,             -- Normalized: ingredients, serving, category
    
    -- Generated drafts (NOT inserted to os_modules until approved)
    draft_modules JSONB,              -- Proposed module rows (M/F if BOTH)
    draft_copy JSONB,                 -- front/back/description copy
    
    -- Workflow state
    status TEXT NOT NULL DEFAULT 'draft' 
        CHECK (status IN ('draft', 'approved', 'rejected')),
    
    -- Validation
    validation_flags JSONB,           -- warnings, blockers, duplicate checks
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Approval tracking
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    rejection_reason TEXT,
    
    -- Constraints
    CONSTRAINT unique_product_url UNIQUE (product_url)
);

-- Index for status filtering (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_catalog_intakes_status 
    ON catalog_intakes (status);

-- Index for supplier filtering
CREATE INDEX IF NOT EXISTS idx_catalog_intakes_supplier 
    ON catalog_intakes (supplier);

-- Index for created_at (for sorting/pagination)
CREATE INDEX IF NOT EXISTS idx_catalog_intakes_created_at 
    ON catalog_intakes (created_at DESC);

-- ============================================
-- Table: catalog_snapshots
-- Version-controlled exports of approved modules
-- ============================================
CREATE TABLE IF NOT EXISTS catalog_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Version tag (e.g., v5_LOCK, v6_LOCK)
    version_tag TEXT NOT NULL UNIQUE,
    
    -- Export metadata
    generated_files JSONB,            -- paths + hashes of exported files
    module_count INTEGER NOT NULL DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by TEXT NOT NULL        -- admin who triggered the snapshot
);

-- Index for version lookup
CREATE INDEX IF NOT EXISTS idx_catalog_snapshots_version 
    ON catalog_snapshots (version_tag);

-- ============================================
-- Trigger: Auto-update updated_at on catalog_intakes
-- ============================================
CREATE OR REPLACE FUNCTION update_catalog_intakes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_catalog_intakes_updated_at ON catalog_intakes;
CREATE TRIGGER trigger_catalog_intakes_updated_at
    BEFORE UPDATE ON catalog_intakes
    FOR EACH ROW
    EXECUTE FUNCTION update_catalog_intakes_updated_at();

-- ============================================
-- Comments for documentation
-- ============================================
COMMENT ON TABLE catalog_intakes IS 'Product intake submissions for Bio-OS catalog. Append-only governance: products must be approved before insertion to os_modules.';
COMMENT ON TABLE catalog_snapshots IS 'Version-controlled snapshots of the catalog. Each approval generates a new snapshot.';

COMMENT ON COLUMN catalog_intakes.status IS 'Workflow state: draft (pending review), approved (inserted to os_modules), rejected (blocked)';
COMMENT ON COLUMN catalog_intakes.draft_modules IS 'Proposed module rows. For BOTH os_environment, contains TWO rows (M/F). NOT inserted until approved.';
COMMENT ON COLUMN catalog_intakes.validation_flags IS 'JSON containing warnings, blockers, duplicate detection results';
