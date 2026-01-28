-- ============================================================================
-- GenoMAX² Database Migration V3.33.0
-- Lab Integration Tables: lab_orders, bloodwork_submissions
-- ============================================================================
-- Execute: psql $DATABASE_URL -f V3.33.0__lab_integration_tables.sql
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. LAB_ORDERS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS lab_orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID,
    
    -- Provider Information
    provider VARCHAR(50) NOT NULL CHECK (provider IN ('junction', 'labtestingapi', 'manual')),
    provider_order_id VARCHAR(255),
    provider_user_id VARCHAR(255),
    
    -- Panel Configuration
    panel_type VARCHAR(50) NOT NULL CHECK (panel_type IN ('essential', 'complete', 'custom')),
    panel_config JSONB,
    
    -- Collection Method
    collection_method VARCHAR(50) CHECK (collection_method IN (
        'walk_in', 'at_home_kit', 'at_home_phlebotomy', 'on_site_collection'
    )),
    
    -- Status Tracking
    status VARCHAR(50) NOT NULL DEFAULT 'created' CHECK (status IN (
        'created', 'pending_payment', 'requisition_ready', 'kit_shipped',
        'kit_delivered', 'appointment_scheduled', 'sample_collected',
        'sample_received', 'processing', 'completed', 'cancelled', 'failed'
    )),
    status_history JSONB DEFAULT '[]'::jsonb,
    
    -- Patient Data (encrypted in production)
    patient_data JSONB,
    
    -- URLs
    requisition_url TEXT,
    tracking_url TEXT,
    appointment_url TEXT,
    
    -- Results
    results_received_at TIMESTAMPTZ,
    results_raw JSONB,
    
    -- Pricing
    price_cents INTEGER,
    currency VARCHAR(3) DEFAULT 'USD',
    
    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT lab_orders_provider_order_unique UNIQUE (provider, provider_order_id)
);

CREATE INDEX IF NOT EXISTS idx_lab_orders_user_id ON lab_orders(user_id);
CREATE INDEX IF NOT EXISTS idx_lab_orders_status ON lab_orders(status);
CREATE INDEX IF NOT EXISTS idx_lab_orders_provider ON lab_orders(provider);
CREATE INDEX IF NOT EXISTS idx_lab_orders_created_at ON lab_orders(created_at DESC);

-- ============================================================================
-- 2. BLOODWORK_SUBMISSIONS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS bloodwork_submissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID,
    
    -- Source Tracking
    lab_order_id UUID REFERENCES lab_orders(id) ON DELETE SET NULL,
    source VARCHAR(50) NOT NULL CHECK (source IN (
        'junction', 'labtestingapi', 'ocr_upload', 'manual_entry', 'hl7_import'
    )),
    source_order_id VARCHAR(255),
    
    -- Upload Information (OCR)
    filename VARCHAR(255),
    file_url TEXT,
    file_hash VARCHAR(64),
    
    -- Raw Data
    raw_text TEXT,
    raw_data JSONB,
    
    -- Normalized Markers (GenoMAX² format)
    normalized_markers JSONB NOT NULL DEFAULT '[]'::jsonb,
    
    -- Quality Metrics
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),
    markers_count INTEGER GENERATED ALWAYS AS (jsonb_array_length(normalized_markers)) STORED,
    priority_markers_found JSONB DEFAULT '[]'::jsonb,
    
    -- Review Workflow
    needs_review BOOLEAN DEFAULT FALSE,
    review_reasons JSONB DEFAULT '[]'::jsonb,
    reviewed_by UUID,
    reviewed_at TIMESTAMPTZ,
    review_notes TEXT,
    
    -- Processing Status
    status VARCHAR(50) NOT NULL DEFAULT 'pending' CHECK (status IN (
        'pending', 'processing', 'pending_review', 'ready', 'processed', 'rejected', 'expired'
    )),
    
    -- Brain Pipeline Link
    brain_run_id UUID,
    processed_at TIMESTAMPTZ,
    
    -- Lab Metadata
    lab_name VARCHAR(255),
    collection_date DATE,
    report_date DATE,
    ordering_physician VARCHAR(255),
    
    -- Audit
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_user_id ON bloodwork_submissions(user_id);
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_status ON bloodwork_submissions(status);
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_source ON bloodwork_submissions(source);
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_lab_order_id ON bloodwork_submissions(lab_order_id);
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_brain_run_id ON bloodwork_submissions(brain_run_id);
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_created_at ON bloodwork_submissions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_file_hash ON bloodwork_submissions(file_hash);
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_needs_review ON bloodwork_submissions(needs_review) WHERE needs_review = TRUE;
CREATE INDEX IF NOT EXISTS idx_bloodwork_submissions_markers_gin ON bloodwork_submissions USING GIN (normalized_markers);

-- ============================================================================
-- 3. BIOMARKER_REFERENCE_RANGES TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS biomarker_reference_ranges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(50) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    loinc_codes JSONB DEFAULT '[]'::jsonb,
    standard_unit VARCHAR(50) NOT NULL,
    alternate_units JSONB DEFAULT '[]'::jsonb,
    reference_ranges JSONB NOT NULL,
    safety_thresholds JSONB,
    category VARCHAR(50),
    priority_tier INTEGER DEFAULT 2,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- 4. INSERT 13 PRIORITY BIOMARKERS
-- ============================================================================

INSERT INTO biomarker_reference_ranges (code, display_name, loinc_codes, standard_unit, reference_ranges, safety_thresholds, category, priority_tier, description)
VALUES
    ('ferritin', 'Ferritin', '["2498-4"]', 'ng/mL',
     '{"default": {"low": 30, "high": 300}, "male": {"low": 30, "high": 400}, "female": {"low": 13, "high": 150}}',
     '{"critical_low": 10, "low": 30, "high": 300, "critical_high": 1000, "blocks_ingredients": ["iron", "iron_bisglycinate"]}',
     'iron', 1, 'Iron storage protein. High levels block iron supplementation.'),
     
    ('serum_iron', 'Serum Iron', '["2502-6"]', 'mcg/dL',
     '{"default": {"low": 60, "high": 170}, "male": {"low": 65, "high": 175}, "female": {"low": 50, "high": 170}}',
     '{"critical_low": 30, "low": 60, "high": 170, "critical_high": 300}',
     'iron', 1, 'Circulating iron levels.'),
     
    ('tibc', 'Total Iron Binding Capacity', '["2500-0"]', 'mcg/dL',
     '{"default": {"low": 250, "high": 400}}',
     '{"low": 250, "high": 450}',
     'iron', 1, 'Iron transport capacity.'),
     
    ('transferrin_sat', 'Transferrin Saturation', '["2502-3"]', '%',
     '{"default": {"low": 20, "high": 45}, "male": {"low": 20, "high": 50}, "female": {"low": 15, "high": 45}}',
     '{"critical_low": 10, "low": 20, "high": 45, "critical_high": 70, "blocks_ingredients": ["iron", "iron_bisglycinate"]}',
     'iron', 1, 'Percentage of transferrin bound to iron. >45% blocks iron.'),
     
    ('vitamin_d_25oh', 'Vitamin D, 25-Hydroxy', '["1989-3", "62292-8"]', 'ng/mL',
     '{"default": {"low": 30, "high": 100}}',
     '{"critical_low": 10, "low": 20, "insufficient": 30, "optimal": 50, "high": 100, "critical_high": 150}',
     'vitamin', 1, 'Primary vitamin D status marker.'),
     
    ('vitamin_b12', 'Vitamin B12', '["2132-9"]', 'pg/mL',
     '{"default": {"low": 200, "high": 900}}',
     '{"critical_low": 150, "low": 200, "suboptimal": 400, "optimal": 600}',
     'vitamin', 1, 'Cobalamin. <200 deficient.'),
     
    ('folate', 'Folate', '["2284-8"]', 'ng/mL',
     '{"default": {"low": 3, "high": 20}}',
     '{"critical_low": 2, "low": 3, "optimal": 10}',
     'vitamin', 1, 'Folic acid.'),
     
    ('homocysteine', 'Homocysteine', '["13965-9"]', 'umol/L',
     '{"default": {"low": 5, "high": 15}}',
     '{"optimal": 8, "high": 15, "critical_high": 50}',
     'cardiovascular', 1, 'Cardiovascular risk marker.'),
     
    ('hscrp', 'hs-CRP', '["30522-7"]', 'mg/L',
     '{"default": {"low": 0, "high": 3}}',
     '{"optimal": 1, "moderate_risk": 3, "high_risk": 10}',
     'inflammation', 1, 'Systemic inflammation marker.'),
     
    ('omega3_index', 'Omega-3 Index', '["82810-3"]', '%',
     '{"default": {"low": 4, "high": 12}}',
     '{"deficient": 4, "suboptimal": 8, "optimal": 8}',
     'fatty_acid', 1, 'EPA+DHA as % of RBC membranes.'),
     
    ('hba1c', 'Hemoglobin A1c', '["4548-4"]', '%',
     '{"default": {"low": 4, "high": 5.6}}',
     '{"normal": 5.6, "prediabetic": 6.4, "diabetic": 6.5}',
     'metabolic', 1, 'Glycated hemoglobin.'),
     
    ('magnesium_rbc', 'Magnesium, RBC', '["19123-9"]', 'mg/dL',
     '{"default": {"low": 4.2, "high": 6.8}}',
     '{"critical_low": 3.5, "low": 4.2, "optimal": 5.5}',
     'mineral', 1, 'Intracellular magnesium.'),
     
    ('zinc', 'Zinc', '["2601-3"]', 'mcg/dL',
     '{"default": {"low": 60, "high": 130}}',
     '{"critical_low": 50, "low": 60, "high": 130, "critical_high": 200}',
     'mineral', 1, 'Essential trace mineral.')

ON CONFLICT (code) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    loinc_codes = EXCLUDED.loinc_codes,
    reference_ranges = EXCLUDED.reference_ranges,
    safety_thresholds = EXCLUDED.safety_thresholds,
    updated_at = NOW();

-- ============================================================================
-- 5. UPDATE TRIGGERS
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_lab_orders_updated_at ON lab_orders;
CREATE TRIGGER update_lab_orders_updated_at
    BEFORE UPDATE ON lab_orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_bloodwork_submissions_updated_at ON bloodwork_submissions;
CREATE TRIGGER update_bloodwork_submissions_updated_at
    BEFORE UPDATE ON bloodwork_submissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- 6. VERIFICATION
-- ============================================================================

COMMIT;

SELECT 'lab_orders' as table_name, count(*) as rows FROM lab_orders
UNION ALL SELECT 'bloodwork_submissions', count(*) FROM bloodwork_submissions
UNION ALL SELECT 'biomarker_reference_ranges', count(*) FROM biomarker_reference_ranges;

-- Rollback: DROP TABLE bloodwork_submissions, lab_orders, biomarker_reference_ranges CASCADE;
