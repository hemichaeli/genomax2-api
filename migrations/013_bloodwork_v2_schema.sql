-- =====================================================
-- Migration 013: Bloodwork Engine v2.0 Schema
-- =====================================================
-- Creates tables for:
-- 1. routing_constraint_blocks: Maps safety gates to ingredient blocks
-- 2. bloodwork_uploads: Stores OCR/API bloodwork uploads
-- 3. bloodwork_results: Stores processed bloodwork results
-- 4. bloodwork_markers: Individual marker results linked to results
--
-- Safety gates are defined in reference_ranges_v2_0.json (31 gates)
-- This migration creates the database layer for routing constraints
-- =====================================================

-- =====================================================
-- 1. ROUTING CONSTRAINT BLOCKS
-- Maps safety gate routing constraints to ingredients
-- =====================================================
CREATE TABLE IF NOT EXISTS routing_constraint_blocks (
    id SERIAL PRIMARY KEY,
    
    -- Safety gate reference (from reference_ranges_v2_0.json)
    gate_id VARCHAR(50) NOT NULL,          -- e.g., 'GATE_001', 'GATE_021'
    routing_constraint VARCHAR(100) NOT NULL, -- e.g., 'BLOCK_IRON', 'BLOCK_FOLIC_ACID'
    gate_tier INTEGER NOT NULL DEFAULT 1,   -- 1=Safety, 2=Optimization, 3=Genetic/Hormonal
    gate_action VARCHAR(20) NOT NULL DEFAULT 'BLOCK', -- BLOCK, CAUTION, FLAG
    
    -- Ingredient reference (can be specific ID or pattern)
    ingredient_canonical_name VARCHAR(255) NOT NULL, -- e.g., 'iron', 'folic_acid', 'ashwagandha'
    ingredient_pattern VARCHAR(255),        -- Optional regex pattern for matching variants
    block_reason TEXT NOT NULL,             -- Clinical rationale
    
    -- Exceptions
    exception_condition TEXT,               -- e.g., 'hs_crp > 3.0'
    exception_note TEXT,                    -- Explanation of exception
    
    -- Audit
    effective_from TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    effective_until TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(100) DEFAULT 'system',
    
    CONSTRAINT uk_gate_ingredient UNIQUE (gate_id, ingredient_canonical_name)
);

-- Index for fast lookups during routing
CREATE INDEX idx_rcb_routing_constraint ON routing_constraint_blocks(routing_constraint);
CREATE INDEX idx_rcb_ingredient ON routing_constraint_blocks(ingredient_canonical_name);
CREATE INDEX idx_rcb_gate_tier ON routing_constraint_blocks(gate_tier);
CREATE INDEX idx_rcb_active ON routing_constraint_blocks(effective_from, effective_until);

COMMENT ON TABLE routing_constraint_blocks IS 'Maps bloodwork safety gates to ingredient blocks. Used by routing engine to filter products.';

-- =====================================================
-- 2. BLOODWORK UPLOADS
-- Stores raw bloodwork data from OCR or API
-- =====================================================
CREATE TABLE IF NOT EXISTS bloodwork_uploads (
    id SERIAL PRIMARY KEY,
    upload_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- User/session reference
    user_id VARCHAR(255),                   -- Optional user identifier
    session_id VARCHAR(255),                -- Session tracking
    
    -- Source information
    source_type VARCHAR(50) NOT NULL,       -- 'ocr_upload', 'lab_api', 'manual_entry'
    source_provider VARCHAR(100),           -- 'google_vision', 'vital', 'quest', etc.
    original_filename VARCHAR(255),
    file_mime_type VARCHAR(100),
    file_size_bytes INTEGER,
    
    -- Raw data
    raw_ocr_text TEXT,                      -- Raw OCR output (for debugging)
    raw_api_response JSONB,                 -- Raw API response (for debugging)
    parsed_markers JSONB NOT NULL,          -- Parsed {code, value, unit}[] array
    
    -- Metadata
    lab_name VARCHAR(255),                  -- e.g., 'Quest Diagnostics', 'LabCorp'
    lab_report_date DATE,                   -- Date on the lab report
    patient_name VARCHAR(255),              -- Optional, for verification
    patient_dob DATE,                       -- Optional, for age calculation
    
    -- Processing status
    status VARCHAR(50) DEFAULT 'pending',   -- pending, processing, processed, failed
    error_message TEXT,
    processing_started_at TIMESTAMP WITH TIME ZONE,
    processing_completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for uploads
CREATE INDEX idx_bu_upload_id ON bloodwork_uploads(upload_id);
CREATE INDEX idx_bu_user_id ON bloodwork_uploads(user_id);
CREATE INDEX idx_bu_status ON bloodwork_uploads(status);
CREATE INDEX idx_bu_created ON bloodwork_uploads(created_at);
CREATE INDEX idx_bu_source ON bloodwork_uploads(source_type, source_provider);

COMMENT ON TABLE bloodwork_uploads IS 'Stores raw bloodwork data from OCR uploads or lab API integrations.';

-- =====================================================
-- 3. BLOODWORK RESULTS
-- Stores processed bloodwork engine results
-- =====================================================
CREATE TABLE IF NOT EXISTS bloodwork_results (
    id SERIAL PRIMARY KEY,
    result_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Link to upload
    upload_id UUID REFERENCES bloodwork_uploads(upload_id),
    
    -- User context
    user_id VARCHAR(255),
    sex VARCHAR(10),                        -- 'male', 'female'
    age INTEGER,
    
    -- Engine configuration
    lab_profile VARCHAR(50) NOT NULL,       -- e.g., 'GLOBAL_CONSERVATIVE'
    engine_version VARCHAR(20) NOT NULL,    -- e.g., '2.0.0'
    ruleset_version VARCHAR(100) NOT NULL,  -- e.g., 'registry_v2.0+ranges_v2.0'
    
    -- Processing timestamps
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Summary counts
    total_markers INTEGER NOT NULL,
    valid_markers INTEGER NOT NULL,
    unknown_markers INTEGER NOT NULL,
    optimal_markers INTEGER NOT NULL,
    
    -- Safety gate summary
    total_gates_triggered INTEGER DEFAULT 0,
    tier1_blocks INTEGER DEFAULT 0,
    tier1_cautions INTEGER DEFAULT 0,
    tier1_flags INTEGER DEFAULT 0,
    tier2_blocks INTEGER DEFAULT 0,
    tier2_cautions INTEGER DEFAULT 0,
    tier2_flags INTEGER DEFAULT 0,
    tier3_blocks INTEGER DEFAULT 0,
    tier3_cautions INTEGER DEFAULT 0,
    tier3_flags INTEGER DEFAULT 0,
    
    -- Routing constraints (array of constraint codes)
    routing_constraints TEXT[] NOT NULL DEFAULT '{}',
    
    -- Computed markers
    computed_markers JSONB,                 -- {code, name, value, formula, interpretation}[]
    
    -- Full result payload (for audit)
    full_result JSONB NOT NULL,
    
    -- Integrity hashes
    input_hash VARCHAR(64) NOT NULL,
    output_hash VARCHAR(64) NOT NULL,
    
    -- Review flag
    require_review BOOLEAN DEFAULT false,
    reviewed_at TIMESTAMP WITH TIME ZONE,
    reviewed_by VARCHAR(100),
    review_notes TEXT,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for results
CREATE INDEX idx_br_result_id ON bloodwork_results(result_id);
CREATE INDEX idx_br_upload_id ON bloodwork_results(upload_id);
CREATE INDEX idx_br_user_id ON bloodwork_results(user_id);
CREATE INDEX idx_br_processed ON bloodwork_results(processed_at);
CREATE INDEX idx_br_require_review ON bloodwork_results(require_review) WHERE require_review = true;
CREATE INDEX idx_br_routing ON bloodwork_results USING GIN(routing_constraints);

COMMENT ON TABLE bloodwork_results IS 'Stores processed bloodwork engine results with safety gates and routing constraints.';

-- =====================================================
-- 4. BLOODWORK MARKERS
-- Individual marker results (normalized)
-- =====================================================
CREATE TABLE IF NOT EXISTS bloodwork_markers (
    id SERIAL PRIMARY KEY,
    
    -- Link to result
    result_id UUID REFERENCES bloodwork_results(result_id) ON DELETE CASCADE,
    
    -- Marker identification
    original_code VARCHAR(100) NOT NULL,
    canonical_code VARCHAR(100),            -- NULL if unknown
    
    -- Values
    original_value NUMERIC,
    canonical_value NUMERIC,
    original_unit VARCHAR(50) NOT NULL,
    canonical_unit VARCHAR(50),
    
    -- Status
    status VARCHAR(50) NOT NULL,            -- VALID, UNKNOWN, CONVERSION_FAILED
    range_status VARCHAR(50) NOT NULL,      -- OPTIMAL, NORMAL, LOW, HIGH, CRITICAL_LOW, CRITICAL_HIGH
    
    -- Reference ranges used
    lab_profile_used VARCHAR(50),
    fallback_used BOOLEAN DEFAULT false,
    reference_low NUMERIC,
    reference_high NUMERIC,
    genomax_optimal_low NUMERIC,
    genomax_optimal_high NUMERIC,
    
    -- Conversion info
    conversion_applied BOOLEAN DEFAULT false,
    conversion_multiplier NUMERIC,
    
    -- Flags and log
    flags TEXT[] DEFAULT '{}',
    log_entries TEXT[] DEFAULT '{}',
    
    -- Genetic marker specific
    is_genetic BOOLEAN DEFAULT false,
    genetic_value VARCHAR(50),              -- e.g., 'TT', 'CT', 'CC'
    genetic_interpretation TEXT
);

-- Indexes for markers
CREATE INDEX idx_bm_result_id ON bloodwork_markers(result_id);
CREATE INDEX idx_bm_canonical ON bloodwork_markers(canonical_code);
CREATE INDEX idx_bm_range_status ON bloodwork_markers(range_status);
CREATE INDEX idx_bm_flags ON bloodwork_markers USING GIN(flags);

COMMENT ON TABLE bloodwork_markers IS 'Individual marker results normalized and linked to bloodwork_results.';

-- =====================================================
-- 5. SEED ROUTING CONSTRAINT BLOCKS
-- Maps the 31 safety gates from v2.0 to ingredients
-- =====================================================

-- TIER 1 SAFETY GATES (14)
INSERT INTO routing_constraint_blocks (gate_id, routing_constraint, gate_tier, gate_action, ingredient_canonical_name, block_reason, exception_condition, exception_note)
VALUES
    -- GATE_001: iron_block
    ('GATE_001', 'BLOCK_IRON', 1, 'BLOCK', 'iron', 'Ferritin >300(M)/200(F) indicates iron overload risk', 'hs_crp > 3.0', 'Acute inflammation may artificially elevate ferritin'),
    ('GATE_001', 'BLOCK_IRON', 1, 'BLOCK', 'iron_bisglycinate', 'Ferritin >300(M)/200(F) indicates iron overload risk', 'hs_crp > 3.0', 'Acute inflammation may artificially elevate ferritin'),
    ('GATE_001', 'BLOCK_IRON', 1, 'BLOCK', 'ferrous_sulfate', 'Ferritin >300(M)/200(F) indicates iron overload risk', 'hs_crp > 3.0', 'Acute inflammation may artificially elevate ferritin'),
    
    -- GATE_002: vitamin_d_caution
    ('GATE_002', 'CAUTION_VITAMIN_D', 1, 'CAUTION', 'vitamin_d3', 'Calcium >10.5 mg/dL - vitamin D may increase calcium absorption', NULL, NULL),
    ('GATE_002', 'CAUTION_VITAMIN_D', 1, 'CAUTION', 'vitamin_d2', 'Calcium >10.5 mg/dL - vitamin D may increase calcium absorption', NULL, NULL),
    
    -- GATE_003: hepatic_caution
    ('GATE_003', 'CAUTION_HEPATOTOXIC', 1, 'CAUTION', 'kava', 'ALT/AST >50(M)/40(F) - hepatotoxic risk', NULL, NULL),
    ('GATE_003', 'BLOCK_ASHWAGANDHA', 1, 'BLOCK', 'ashwagandha', 'ALT/AST >50(M)/40(F) - documented hepatotoxicity risk', NULL, NULL),
    ('GATE_003', 'CAUTION_HEPATOTOXIC', 1, 'CAUTION', 'green_tea_extract', 'ALT/AST >50(M)/40(F) - high-dose EGCG hepatotoxicity', NULL, NULL),
    ('GATE_003', 'CAUTION_HEPATOTOXIC', 1, 'CAUTION', 'niacin', 'ALT/AST >50(M)/40(F) - high-dose niacin hepatotoxicity', NULL, NULL),
    
    -- GATE_004: renal_caution
    ('GATE_004', 'CAUTION_RENAL', 1, 'CAUTION', 'creatine', 'eGFR <60 or Creatinine elevated - renal-cleared supplement', NULL, NULL),
    ('GATE_004', 'CAUTION_RENAL', 1, 'CAUTION', 'magnesium', 'eGFR <60 or Creatinine elevated - renal excretion required', NULL, NULL),
    
    -- GATE_006: potassium_block
    ('GATE_006', 'BLOCK_POTASSIUM', 1, 'BLOCK', 'potassium', 'K+ >5.0 mEq/L - hyperkalemia risk', NULL, NULL),
    ('GATE_006', 'BLOCK_POTASSIUM', 1, 'BLOCK', 'potassium_citrate', 'K+ >5.0 mEq/L - hyperkalemia risk', NULL, NULL),
    
    -- GATE_008: thyroid_iodine_block
    ('GATE_008', 'BLOCK_IODINE', 1, 'BLOCK', 'iodine', 'TSH <0.4 - hyperthyroid state, iodine contraindicated', NULL, NULL),
    ('GATE_008', 'BLOCK_IODINE', 1, 'BLOCK', 'kelp', 'TSH <0.4 - hyperthyroid state, high-iodine source', NULL, NULL),
    
    -- GATE_014: coagulation_caution
    ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'fish_oil', 'Platelets <100K - may potentiate bleeding', NULL, NULL),
    ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'vitamin_e', 'Platelets <100K - may potentiate bleeding', NULL, NULL),
    ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'ginkgo', 'Platelets <100K - may potentiate bleeding', NULL, NULL),
    ('GATE_014', 'CAUTION_BLOOD_THINNING', 1, 'CAUTION', 'garlic_extract', 'Platelets <100K - may potentiate bleeding', NULL, NULL)

ON CONFLICT (gate_id, ingredient_canonical_name) DO UPDATE SET
    routing_constraint = EXCLUDED.routing_constraint,
    block_reason = EXCLUDED.block_reason,
    exception_condition = EXCLUDED.exception_condition,
    exception_note = EXCLUDED.exception_note;

-- TIER 2 OPTIMIZATION GATES (6) - FLAGS only, no blocks
INSERT INTO routing_constraint_blocks (gate_id, routing_constraint, gate_tier, gate_action, ingredient_canonical_name, block_reason)
VALUES
    -- GATE_017: zinc_copper_imbalance
    ('GATE_017', 'CAUTION_ZINC_EXCESS', 2, 'CAUTION', 'zinc', 'Zn:Cu ratio >1.5 - may worsen copper deficiency'),
    ('GATE_017', 'CAUTION_ZINC_EXCESS', 2, 'CAUTION', 'zinc_picolinate', 'Zn:Cu ratio >1.5 - may worsen copper deficiency'),
    
    -- GATE_020: triglyceride_caution
    ('GATE_020', 'CAUTION_FISH_OIL_DOSE', 2, 'CAUTION', 'fish_oil', 'Triglycerides >500 - requires medical-grade fish oil dosing')

ON CONFLICT (gate_id, ingredient_canonical_name) DO UPDATE SET
    routing_constraint = EXCLUDED.routing_constraint,
    block_reason = EXCLUDED.block_reason;

-- TIER 3 GENETIC/HORMONAL GATES (11)
INSERT INTO routing_constraint_blocks (gate_id, routing_constraint, gate_tier, gate_action, ingredient_canonical_name, block_reason)
VALUES
    -- GATE_021: mthfr_methylfolate_required
    ('GATE_021', 'BLOCK_FOLIC_ACID', 3, 'BLOCK', 'folic_acid', 'MTHFR TT or compound heterozygous - cannot metabolize folic acid'),
    ('GATE_021', 'FLAG_METHYLFOLATE_REQUIRED', 3, 'FLAG', 'methylfolate', 'MTHFR mutation - requires methylated folate form')

ON CONFLICT (gate_id, ingredient_canonical_name) DO UPDATE SET
    routing_constraint = EXCLUDED.routing_constraint,
    block_reason = EXCLUDED.block_reason;

-- =====================================================
-- 6. UPDATE TRIGGER FOR bloodwork_uploads
-- =====================================================
CREATE OR REPLACE FUNCTION update_bloodwork_uploads_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_bloodwork_uploads_updated ON bloodwork_uploads;
CREATE TRIGGER trg_bloodwork_uploads_updated
    BEFORE UPDATE ON bloodwork_uploads
    FOR EACH ROW
    EXECUTE FUNCTION update_bloodwork_uploads_timestamp();

-- =====================================================
-- VERIFICATION
-- =====================================================
DO $$
DECLARE
    rcb_count INTEGER;
    bu_exists BOOLEAN;
    br_exists BOOLEAN;
    bm_exists BOOLEAN;
BEGIN
    SELECT COUNT(*) INTO rcb_count FROM routing_constraint_blocks;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bloodwork_uploads') INTO bu_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bloodwork_results') INTO br_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bloodwork_markers') INTO bm_exists;
    
    RAISE NOTICE '=== Migration 013 Bloodwork v2.0 Schema ===';
    RAISE NOTICE 'routing_constraint_blocks: % rows seeded', rcb_count;
    RAISE NOTICE 'bloodwork_uploads table: %', CASE WHEN bu_exists THEN 'CREATED' ELSE 'FAILED' END;
    RAISE NOTICE 'bloodwork_results table: %', CASE WHEN br_exists THEN 'CREATED' ELSE 'FAILED' END;
    RAISE NOTICE 'bloodwork_markers table: %', CASE WHEN bm_exists THEN 'CREATED' ELSE 'FAILED' END;
END $$;
