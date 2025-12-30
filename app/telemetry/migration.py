"""
GenoMAX² Telemetry Migration Endpoint
Creates telemetry tables in PostgreSQL.
"""

TELEMETRY_MIGRATION_SQL = """
-- Telemetry Tables (Issue #9)
-- Run this migration to enable observability

CREATE TABLE IF NOT EXISTS telemetry_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    api_version VARCHAR(20),
    bloodwork_version VARCHAR(20),
    catalog_version VARCHAR(50),
    routing_version VARCHAR(50),
    matching_version VARCHAR(50),
    explainability_version VARCHAR(50),
    
    sex VARCHAR(10),
    age_bucket VARCHAR(20),
    
    has_bloodwork BOOLEAN DEFAULT FALSE,
    intents_count INTEGER DEFAULT 0,
    matched_items_count INTEGER DEFAULT 0,
    unmatched_intents_count INTEGER DEFAULT 0,
    blocked_skus_count INTEGER DEFAULT 0,
    auto_blocked_skus_count INTEGER DEFAULT 0,
    caution_flags_count INTEGER DEFAULT 0,
    confidence_level VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_telemetry_runs_created_at ON telemetry_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_runs_confidence ON telemetry_runs(confidence_level);
CREATE INDEX IF NOT EXISTS idx_telemetry_runs_bloodwork ON telemetry_runs(has_bloodwork);
CREATE INDEX IF NOT EXISTS idx_telemetry_runs_run_id ON telemetry_runs(run_id);

CREATE TABLE IF NOT EXISTS telemetry_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    event_type VARCHAR(50) NOT NULL,
    code VARCHAR(255) NOT NULL,
    count INTEGER DEFAULT 1,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_telemetry_events_run_id ON telemetry_events(run_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_type ON telemetry_events(event_type);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_created_at ON telemetry_events(created_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_code ON telemetry_events(code);

CREATE TABLE IF NOT EXISTS telemetry_daily_rollups (
    day DATE PRIMARY KEY,
    total_runs INTEGER DEFAULT 0,
    pct_has_bloodwork FLOAT DEFAULT 0,
    pct_low_confidence FLOAT DEFAULT 0,
    avg_unmatched_intents FLOAT DEFAULT 0,
    avg_blocked_skus FLOAT DEFAULT 0,
    top_block_reasons JSONB,
    top_missing_fields JSONB,
    top_unknown_ingredients JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def get_migration_sql() -> str:
    """Return the SQL migration script."""
    return TELEMETRY_MIGRATION_SQL
