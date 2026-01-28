-- ============================================================================
-- GenoMAX² v3.34.0 Brain Pipeline Database Migration
-- Creates brain_runs table for storing pipeline execution results
-- ============================================================================

-- Brain Runs Table
-- Stores complete audit trail of Brain pipeline executions
CREATE TABLE IF NOT EXISTS brain_runs (
    -- Primary identification
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id UUID REFERENCES bloodwork_submissions(submission_id),
    user_id VARCHAR(255),
    
    -- Run status
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    -- Valid statuses: pending, processing, completed, failed
    
    -- Processing metrics
    markers_processed INTEGER DEFAULT 0,
    priority_markers_found INTEGER DEFAULT 0,
    modules_evaluated INTEGER DEFAULT 0,
    
    -- Results (JSONB for flexible structure)
    deficiencies_json JSONB DEFAULT '[]'::jsonb,
    recommended_modules_json JSONB DEFAULT '[]'::jsonb,
    blocked_modules_json JSONB DEFAULT '[]'::jsonb,
    caution_modules_json JSONB DEFAULT '[]'::jsonb,
    
    -- Safety data (arrays for efficient querying)
    blocked_ingredients TEXT[] DEFAULT ARRAY[]::TEXT[],
    caution_ingredients TEXT[] DEFAULT ARRAY[]::TEXT[],
    safety_gates_triggered TEXT[] DEFAULT ARRAY[]::TEXT[],
    
    -- User context
    gender VARCHAR(20),
    lifecycle_phase VARCHAR(50),
    goals TEXT[] DEFAULT ARRAY[]::TEXT[],
    user_exclusions TEXT[] DEFAULT ARRAY[]::TEXT[],
    
    -- Performance tracking
    processing_time_ms INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Error handling
    error_message TEXT,
    error_details JSONB
);

-- ============================================================================
-- Indexes for common query patterns
-- ============================================================================

-- Fast lookup by submission (bloodwork-to-brain linkage)
CREATE INDEX IF NOT EXISTS idx_brain_runs_submission_id 
    ON brain_runs(submission_id);

-- User history queries
CREATE INDEX IF NOT EXISTS idx_brain_runs_user_id 
    ON brain_runs(user_id);

-- Status monitoring
CREATE INDEX IF NOT EXISTS idx_brain_runs_status 
    ON brain_runs(status);

-- Temporal queries for analytics
CREATE INDEX IF NOT EXISTS idx_brain_runs_created_at 
    ON brain_runs(created_at DESC);

-- Gender-based analytics
CREATE INDEX IF NOT EXISTS idx_brain_runs_gender 
    ON brain_runs(gender);

-- Lifecycle phase analytics
CREATE INDEX IF NOT EXISTS idx_brain_runs_lifecycle 
    ON brain_runs(lifecycle_phase);

-- GIN indexes for array containment queries
CREATE INDEX IF NOT EXISTS idx_brain_runs_blocked_ingredients 
    ON brain_runs USING GIN(blocked_ingredients);

CREATE INDEX IF NOT EXISTS idx_brain_runs_safety_gates 
    ON brain_runs USING GIN(safety_gates_triggered);

CREATE INDEX IF NOT EXISTS idx_brain_runs_goals 
    ON brain_runs USING GIN(goals);

-- ============================================================================
-- Comments for documentation
-- ============================================================================

COMMENT ON TABLE brain_runs IS 'Audit trail for Brain pipeline executions - stores all recommendation runs for compliance and analytics';

COMMENT ON COLUMN brain_runs.run_id IS 'Unique identifier for each Brain pipeline execution';
COMMENT ON COLUMN brain_runs.submission_id IS 'Links to bloodwork submission that triggered this run';
COMMENT ON COLUMN brain_runs.status IS 'Run status: pending, processing, completed, failed';
COMMENT ON COLUMN brain_runs.deficiencies_json IS 'Array of detected biomarker deficiencies with severity';
COMMENT ON COLUMN brain_runs.recommended_modules_json IS 'Ranked array of recommended supplement modules';
COMMENT ON COLUMN brain_runs.blocked_modules_json IS 'Modules excluded due to safety constraints';
COMMENT ON COLUMN brain_runs.caution_modules_json IS 'Modules with caution flags but still eligible';
COMMENT ON COLUMN brain_runs.blocked_ingredients IS 'Ingredients blocked by safety gates or user exclusions';
COMMENT ON COLUMN brain_runs.safety_gates_triggered IS 'Which safety gates were activated';
COMMENT ON COLUMN brain_runs.lifecycle_phase IS 'User lifecycle: pregnant, breastfeeding, perimenopause, postmenopausal, athletic';

-- ============================================================================
-- Verify table creation
-- ============================================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'brain_runs') THEN
        RAISE NOTICE 'brain_runs table created successfully';
    ELSE
        RAISE EXCEPTION 'brain_runs table creation failed';
    END IF;
END $$;
