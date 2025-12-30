# GenoMAX² Observability & QA Loop (Issue #9)

## Overview

This module implements the Observability & QA Loop as specified in the v1.1 proposal.
It provides read-only telemetry for production insights without PII collection.

## Principles

- **Read-only telemetry**: No modification of runtime decisions
- **Deterministic aggregation**: Same inputs produce same outputs
- **No raw user payloads**: Only counts and categorical data stored
- **Admin-only access**: All endpoints require X-Admin-API-Key header
- **Zero effect on runtime**: Telemetry fails silently, never blocks requests

## Components

### 1. Data Models (`models.py`)

- `TelemetryRun`: Aggregate metrics for a single protocol run
- `TelemetryEvent`: Individual events (blocks, unmatched intents, etc.)
- `TelemetryDailyRollup`: Daily aggregations for dashboards
- `AgeBucket`: Age bucketing to avoid storing exact ages

### 2. Event Emitter (`emitter.py`)

Singleton emitter with methods for each event type:

```python
from app.telemetry.emitter import get_emitter

emitter = get_emitter()

# Start a run
emitter.start_run(run_id, sex="male", age=35, has_bloodwork=True)

# Emit events
emitter.emit_routing_block(run_id, "BLOCK_IRON")
emitter.emit_unmatched_intent(run_id, "omega3_brain_support")
emitter.emit_safety_gate_triggered(run_id, "iron_block", "ferritin")

# Complete the run
emitter.complete_run(
    run_id,
    intents_count=5,
    matched_items_count=3,
    unmatched_intents_count=2,
    blocked_skus_count=1
)
```

### 3. Admin Endpoints (`admin.py`)

All endpoints require `X-Admin-API-Key` header.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/telemetry/health` | GET | Check telemetry system health |
| `/api/v1/admin/telemetry/summary` | GET | Summary for date range |
| `/api/v1/admin/telemetry/top-issues` | GET | Top issues across all types |
| `/api/v1/admin/telemetry/run/{run_id}` | GET | Detail for specific run |
| `/api/v1/admin/telemetry/trends` | GET | Trend data for N days |
| `/api/v1/admin/telemetry/rollup/run` | POST | Trigger daily rollup |
| `/api/v1/admin/telemetry/setup` | POST | Create telemetry tables |

### 4. Migration (`migration.py`)

SQL script to create telemetry tables.

## Setup

### 1. Set Environment Variables

```bash
# Enable telemetry (default: true)
TELEMETRY_ENABLED=true

# Admin API key for accessing telemetry endpoints
ADMIN_API_KEY=your-secret-key-here
```

### 2. Run Migration

Either call the setup endpoint:

```bash
curl -X POST https://your-api.com/api/v1/admin/telemetry/setup \
  -H "X-Admin-API-Key: your-secret-key"
```

Or run the migration SQL directly from `migration.py`.

### 3. Update api_server.py

Add these imports after the explainability import:

```python
# Telemetry Layer imports (v3.16.0 - Issue #9)
from app.telemetry.admin import router as telemetry_router
from app.telemetry.emitter import get_emitter
```

Register the router after explainability:

```python
# Register Telemetry Layer router (v3.16.0 - Issue #9)
app.include_router(telemetry_router)
```

Update version to "3.16.0" and add "telemetry" to features list.

## Event Types

| Event Type | Source | Description |
|------------|--------|-------------|
| `CATALOG_AUTO_BLOCK` | Catalog Governance | SKU auto-blocked by governance rules |
| `ROUTING_BLOCK` | Routing Layer | Target blocked by routing constraint |
| `MATCHING_UNMATCHED_INTENT` | Matching Layer | Intent without matching SKU |
| `MATCHING_REQUIREMENT_UNFULFILLED` | Matching Layer | Requirement cannot be fulfilled |
| `UNKNOWN_INGREDIENT` | Catalog Governance | Unknown ingredient encountered |
| `LOW_CONFIDENCE` | Explainability | Low confidence score |
| `SAFETY_GATE_TRIGGERED` | Bloodwork Engine | Safety gate activated |
| `BLOODWORK_MARKER_MISSING` | Bloodwork Engine | Requested marker missing |
| `UNIT_CONVERSION_APPLIED` | Bloodwork Engine | Unit conversion performed |

## Dashboard Queries

### Quality Overview

```sql
SELECT 
    DATE(created_at) as day,
    COUNT(*) as total_runs,
    AVG(CASE WHEN has_bloodwork THEN 100 ELSE 0 END) as pct_bloodwork,
    AVG(CASE WHEN confidence_level = 'low' THEN 100 ELSE 0 END) as pct_low_confidence
FROM telemetry_runs
WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY DATE(created_at)
ORDER BY day DESC;
```

### Top Blockers

```sql
SELECT code, SUM(count) as total
FROM telemetry_events
WHERE event_type IN ('ROUTING_BLOCK', 'CATALOG_AUTO_BLOCK')
AND created_at > NOW() - INTERVAL '30 days'
GROUP BY code
ORDER BY total DESC
LIMIT 10;
```

### Matching Health

```sql
SELECT code as intent, SUM(count) as times_unmatched
FROM telemetry_events
WHERE event_type = 'MATCHING_UNMATCHED_INTENT'
AND created_at > NOW() - INTERVAL '30 days'
GROUP BY code
ORDER BY times_unmatched DESC
LIMIT 10;
```

## Acceptance Criteria (Part A)

- [x] Telemetry tables defined (telemetry_runs, telemetry_events, telemetry_daily_rollups)
- [x] Event types defined for all layers
- [x] Admin endpoints secured with X-Admin-API-Key
- [x] No PII stored (age bucketed, no names/emails)
- [x] Deterministic rollup calculations
- [x] Zero effect on runtime decisions (fails silently)

## Next Steps

Once telemetry is collecting data:

1. Monitor top blockers to identify catalog gaps
2. Track unmatched intents to prioritize ingredient mapping
3. Use confidence distribution to improve explainability
4. Review safety gate frequency for v1.1 biomarker decisions

This data will inform Bloodwork Engine v1.1 expansion decisions per the proposal.
