-- Migration 008: Allowlist Mapping Tables
-- Purpose: Explicit, human-approved mapping for NO_MATCH items
-- Resolves 44 modules with handle naming mismatch between our base handles and Supliful snapshot
--
-- RULES:
-- - No fuzzy matching
-- - Only explicit allowlist mappings applied
-- - Full audit trail with batch_id

-- ============================================================
-- Table: catalog_handle_map_allowlist_v1
-- Purpose: Store human-approved handle mappings
-- ============================================================

CREATE TABLE IF NOT EXISTS catalog_handle_map_allowlist_v1 (
    allowlist_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Our handle (base, without -maximo/-maxima suffix)
    shopify_base_handle TEXT NOT NULL,
    
    -- Supliful catalog handle
    supliful_handle TEXT NOT NULL,
    
    -- Full Supliful URL
    supplier_url TEXT NOT NULL,
    
    -- Source of this mapping
    source TEXT NOT NULL DEFAULT 'MANUAL_ALLOWLIST',
    
    -- Optional notes
    notes TEXT NULL,
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT uq_allowlist_shopify_base_handle UNIQUE (shopify_base_handle),
    CONSTRAINT uq_allowlist_supliful_handle UNIQUE (supliful_handle),
    CONSTRAINT chk_supplier_url_format CHECK (supplier_url LIKE 'https://supliful.com/catalog/%')
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_allowlist_shopify_base_handle 
    ON catalog_handle_map_allowlist_v1 (shopify_base_handle);

CREATE INDEX IF NOT EXISTS idx_allowlist_supliful_handle 
    ON catalog_handle_map_allowlist_v1 (supliful_handle);

COMMENT ON TABLE catalog_handle_map_allowlist_v1 IS 
    'Human-approved handle mappings for NO_MATCH modules. One mapping per base handle.';

-- ============================================================
-- Table: catalog_handle_map_allowlist_audit_v1
-- Purpose: Audit trail for allowlist apply operations
-- ============================================================

CREATE TABLE IF NOT EXISTS catalog_handle_map_allowlist_audit_v1 (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Batch identifier (one per apply request)
    batch_id UUID NOT NULL,
    
    -- Module identification
    module_code TEXT NOT NULL,
    shopify_handle TEXT NOT NULL,
    os_environment TEXT NOT NULL,
    
    -- Old values (before update)
    old_supliful_handle TEXT NULL,
    old_supplier_page_url TEXT NULL,
    
    -- New values (after update)
    new_supliful_handle TEXT NULL,
    new_supplier_page_url TEXT NULL,
    
    -- Rule used (always MANUAL_ALLOWLIST for this table)
    rule_used TEXT NOT NULL DEFAULT 'MANUAL_ALLOWLIST',
    
    -- Timestamp
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for audit queries
CREATE INDEX IF NOT EXISTS idx_allowlist_audit_batch_id 
    ON catalog_handle_map_allowlist_audit_v1 (batch_id);

CREATE INDEX IF NOT EXISTS idx_allowlist_audit_module_code 
    ON catalog_handle_map_allowlist_audit_v1 (module_code);

CREATE INDEX IF NOT EXISTS idx_allowlist_audit_created_at 
    ON catalog_handle_map_allowlist_audit_v1 (created_at DESC);

COMMENT ON TABLE catalog_handle_map_allowlist_audit_v1 IS 
    'Audit trail for allowlist apply operations. One row per module updated.';

-- ============================================================
-- Verification queries
-- ============================================================

-- Verify tables created
SELECT 
    'catalog_handle_map_allowlist_v1' as table_name,
    COUNT(*) as row_count
FROM catalog_handle_map_allowlist_v1
UNION ALL
SELECT 
    'catalog_handle_map_allowlist_audit_v1' as table_name,
    COUNT(*) as row_count
FROM catalog_handle_map_allowlist_audit_v1;
