-- =====================================================
-- Migration 014: Users, Lab Orders, and Webhook Support
-- =====================================================
-- Creates tables for:
-- 1. genomax_users: User profiles and preferences
-- 2. lab_orders: Lab order tracking
-- 3. safety_gate_triggers: Audit log of triggered safety gates
-- 4. webhook_events: Webhook event log
-- =====================================================

-- =====================================================
-- 1. GENOMAX USERS
-- User profiles for bloodwork tracking
-- =====================================================
CREATE TABLE IF NOT EXISTS genomax_users (
    id SERIAL PRIMARY KEY,
    user_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- External IDs
    external_id VARCHAR(255) UNIQUE,        -- Customer-provided ID
    junction_user_id VARCHAR(255),          -- Junction/Vital user ID
    lab_testing_api_patient_id VARCHAR(255), -- Lab Testing API patient ID
    
    -- Profile
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    sex VARCHAR(10),                        -- 'male', 'female'
    phone VARCHAR(50),
    
    -- Address
    address_street VARCHAR(255),
    address_city VARCHAR(100),
    address_state VARCHAR(50),
    address_zip VARCHAR(20),
    address_country VARCHAR(50) DEFAULT 'US',
    
    -- Preferences
    lab_profile VARCHAR(50) DEFAULT 'GLOBAL_CONSERVATIVE',
    preferred_lab_provider VARCHAR(50),     -- 'junction', 'lab_testing_api'
    notification_email BOOLEAN DEFAULT true,
    notification_sms BOOLEAN DEFAULT false,
    
    -- Subscription/Tier
    subscription_tier VARCHAR(50) DEFAULT 'free', -- 'free', 'basic', 'pro', 'enterprise'
    subscription_started_at TIMESTAMP WITH TIME ZONE,
    subscription_expires_at TIMESTAMP WITH TIME ZONE,
    
    -- Genetic markers (if known)
    mthfr_status VARCHAR(20),               -- 'normal', 'heterozygous', 'homozygous'
    comt_status VARCHAR(20),
    vdr_status VARCHAR(20),
    
    -- Status
    status VARCHAR(20) DEFAULT 'active',    -- 'active', 'inactive', 'suspended'
    verified_email BOOLEAN DEFAULT false,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login_at TIMESTAMP WITH TIME ZONE,
    last_bloodwork_at TIMESTAMP WITH TIME ZONE
);

-- Indexes for users
CREATE INDEX idx_gu_user_id ON genomax_users(user_id);
CREATE INDEX idx_gu_external_id ON genomax_users(external_id);
CREATE INDEX idx_gu_email ON genomax_users(email);
CREATE INDEX idx_gu_junction_id ON genomax_users(junction_user_id);
CREATE INDEX idx_gu_status ON genomax_users(status);

COMMENT ON TABLE genomax_users IS 'GenoMAX user profiles with preferences and lab provider IDs.';

-- Update trigger for users
CREATE OR REPLACE FUNCTION update_genomax_users_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_genomax_users_updated ON genomax_users;
CREATE TRIGGER trg_genomax_users_updated
    BEFORE UPDATE ON genomax_users
    FOR EACH ROW
    EXECUTE FUNCTION update_genomax_users_timestamp();

-- =====================================================
-- 2. LAB ORDERS
-- Track lab test orders across providers
-- =====================================================
CREATE TABLE IF NOT EXISTS lab_orders (
    id SERIAL PRIMARY KEY,
    order_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- User reference
    user_id UUID REFERENCES genomax_users(user_id),
    
    -- Provider info
    provider VARCHAR(50) NOT NULL,          -- 'junction', 'lab_testing_api'
    provider_order_id VARCHAR(255),         -- Order ID from provider
    provider_user_id VARCHAR(255),          -- User ID in provider system
    
    -- Order details
    lab_name VARCHAR(100),                  -- 'Quest Diagnostics', 'LabCorp', etc.
    collection_method VARCHAR(50),          -- 'walk_in_test', 'at_home_phlebotomy', 'testkit'
    tests_ordered JSONB NOT NULL DEFAULT '[]', -- [{test_id, name, price}]
    total_price NUMERIC(10, 2),
    
    -- Location (for walk-in)
    lab_location_id VARCHAR(255),
    lab_location_name VARCHAR(255),
    lab_location_address TEXT,
    
    -- Status tracking
    status VARCHAR(50) DEFAULT 'pending',   -- pending, requisition_ready, collecting, processing, completed, cancelled, failed
    status_history JSONB DEFAULT '[]',      -- [{status, timestamp, note}]
    
    -- Important URLs
    requisition_url TEXT,                   -- PDF for walk-in testing
    results_url TEXT,                       -- PDF results URL
    
    -- Timestamps
    ordered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    requisition_created_at TIMESTAMP WITH TIME ZONE,
    sample_collected_at TIMESTAMP WITH TIME ZONE,
    results_received_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    
    -- Link to results
    bloodwork_upload_id UUID,               -- References bloodwork_uploads.upload_id
    bloodwork_result_id UUID,               -- References bloodwork_results.result_id
    
    -- Error handling
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for lab_orders
CREATE INDEX idx_lo_order_id ON lab_orders(order_id);
CREATE INDEX idx_lo_user_id ON lab_orders(user_id);
CREATE INDEX idx_lo_provider_order ON lab_orders(provider, provider_order_id);
CREATE INDEX idx_lo_status ON lab_orders(status);
CREATE INDEX idx_lo_ordered_at ON lab_orders(ordered_at);

COMMENT ON TABLE lab_orders IS 'Lab test orders across Junction and Lab Testing API providers.';

-- Update trigger for lab_orders
CREATE OR REPLACE FUNCTION update_lab_orders_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    -- Add to status history if status changed
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        NEW.status_history = COALESCE(OLD.status_history, '[]'::jsonb) || 
            jsonb_build_object('status', NEW.status, 'timestamp', NOW(), 'previous', OLD.status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_lab_orders_updated ON lab_orders;
CREATE TRIGGER trg_lab_orders_updated
    BEFORE UPDATE ON lab_orders
    FOR EACH ROW
    EXECUTE FUNCTION update_lab_orders_timestamp();

-- =====================================================
-- 3. SAFETY GATE TRIGGERS
-- Audit log of triggered safety gates
-- =====================================================
CREATE TABLE IF NOT EXISTS safety_gate_triggers (
    id SERIAL PRIMARY KEY,
    trigger_id UUID DEFAULT gen_random_uuid() NOT NULL,
    
    -- Context
    user_id UUID,                           -- References genomax_users.user_id (optional)
    bloodwork_result_id UUID,               -- References bloodwork_results.result_id
    
    -- Gate information
    gate_id VARCHAR(50) NOT NULL,           -- e.g., 'GATE_001'
    gate_name VARCHAR(255) NOT NULL,        -- e.g., 'Ferritin High - Iron Block'
    gate_tier INTEGER NOT NULL,             -- 1, 2, or 3
    gate_action VARCHAR(20) NOT NULL,       -- 'BLOCK', 'CAUTION', 'FLAG'
    routing_constraint VARCHAR(100),        -- e.g., 'BLOCK_IRON'
    
    -- Trigger details
    trigger_marker VARCHAR(100) NOT NULL,   -- e.g., 'ferritin'
    trigger_value NUMERIC,                  -- Actual value that triggered
    trigger_unit VARCHAR(50),
    threshold_value NUMERIC,                -- Threshold that was exceeded
    threshold_operator VARCHAR(10),         -- '>', '>=', '<', '<=', '='
    
    -- Impact
    blocked_ingredients TEXT[] DEFAULT '{}',
    caution_ingredients TEXT[] DEFAULT '{}',
    recommended_ingredients TEXT[] DEFAULT '{}',
    
    -- Exception handling
    exception_active BOOLEAN DEFAULT false,
    exception_reason TEXT,
    exception_approved_by VARCHAR(100),
    exception_approved_at TIMESTAMP WITH TIME ZONE,
    
    -- Resolution
    resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(100),
    resolution_note TEXT,
    
    -- Audit
    triggered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for safety_gate_triggers
CREATE INDEX idx_sgt_trigger_id ON safety_gate_triggers(trigger_id);
CREATE INDEX idx_sgt_user_id ON safety_gate_triggers(user_id);
CREATE INDEX idx_sgt_result_id ON safety_gate_triggers(bloodwork_result_id);
CREATE INDEX idx_sgt_gate_id ON safety_gate_triggers(gate_id);
CREATE INDEX idx_sgt_gate_tier ON safety_gate_triggers(gate_tier);
CREATE INDEX idx_sgt_triggered_at ON safety_gate_triggers(triggered_at);
CREATE INDEX idx_sgt_unresolved ON safety_gate_triggers(resolved) WHERE resolved = false;

COMMENT ON TABLE safety_gate_triggers IS 'Audit log of all safety gates triggered during bloodwork processing.';

-- =====================================================
-- 4. WEBHOOK EVENTS
-- Log all incoming webhook events
-- =====================================================
CREATE TABLE IF NOT EXISTS webhook_events (
    id SERIAL PRIMARY KEY,
    event_id UUID DEFAULT gen_random_uuid() UNIQUE NOT NULL,
    
    -- Source
    provider VARCHAR(50) NOT NULL,          -- 'junction', 'lab_testing_api'
    event_type VARCHAR(100) NOT NULL,       -- 'order.completed', 'results.ready', etc.
    
    -- Payload
    raw_payload JSONB NOT NULL,
    parsed_order_id VARCHAR(255),
    parsed_user_id VARCHAR(255),
    
    -- Security
    signature_header TEXT,
    signature_valid BOOLEAN,
    ip_address VARCHAR(50),
    
    -- Processing
    status VARCHAR(50) DEFAULT 'received',  -- received, processing, processed, failed, ignored
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Linked entities
    lab_order_id UUID,                      -- References lab_orders.order_id
    bloodwork_upload_id UUID,               -- References bloodwork_uploads.upload_id
    bloodwork_result_id UUID,               -- References bloodwork_results.result_id
    
    -- Audit
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for webhook_events
CREATE INDEX idx_we_event_id ON webhook_events(event_id);
CREATE INDEX idx_we_provider ON webhook_events(provider);
CREATE INDEX idx_we_event_type ON webhook_events(event_type);
CREATE INDEX idx_we_status ON webhook_events(status);
CREATE INDEX idx_we_received_at ON webhook_events(received_at);
CREATE INDEX idx_we_order_id ON webhook_events(parsed_order_id);

COMMENT ON TABLE webhook_events IS 'Log of all incoming webhook events from lab providers.';

-- =====================================================
-- VERIFICATION
-- =====================================================
DO $$
DECLARE
    gu_exists BOOLEAN;
    lo_exists BOOLEAN;
    sgt_exists BOOLEAN;
    we_exists BOOLEAN;
BEGIN
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'genomax_users') INTO gu_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'lab_orders') INTO lo_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'safety_gate_triggers') INTO sgt_exists;
    SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'webhook_events') INTO we_exists;
    
    RAISE NOTICE '=== Migration 014 Users/Orders/Webhooks Schema ===';
    RAISE NOTICE 'genomax_users table: %', CASE WHEN gu_exists THEN 'CREATED' ELSE 'FAILED' END;
    RAISE NOTICE 'lab_orders table: %', CASE WHEN lo_exists THEN 'CREATED' ELSE 'FAILED' END;
    RAISE NOTICE 'safety_gate_triggers table: %', CASE WHEN sgt_exists THEN 'CREATED' ELSE 'FAILED' END;
    RAISE NOTICE 'webhook_events table: %', CASE WHEN we_exists THEN 'CREATED' ELSE 'FAILED' END;
END $$;
