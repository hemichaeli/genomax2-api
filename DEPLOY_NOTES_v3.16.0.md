# Deploy Notes v3.16.0

**Date**: 2025-12-30
**Version**: 3.16.0
**Issue**: #9 - Telemetry Admin Router Integration

## Changes
- Registered Telemetry Admin router in api_server.py
- Added import: `from app.telemetry.admin import router as telemetry_router`
- Added: `app.include_router(telemetry_router)`
- Updated version strings to 3.16.0
- Added "telemetry" to features list

## New Endpoints
All require `X-Admin-API-Key` header authentication:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/admin/telemetry/health` | Check telemetry system health |
| GET | `/api/v1/admin/telemetry/summary` | Get telemetry summary for date range |
| GET | `/api/v1/admin/telemetry/top-issues` | Get top issues across all event types |
| GET | `/api/v1/admin/telemetry/run/{run_id}` | Get telemetry for a specific run |
| GET | `/api/v1/admin/telemetry/trends` | Get trend data for last N days |
| POST | `/api/v1/admin/telemetry/setup` | Create telemetry tables if needed |
| POST | `/api/v1/admin/telemetry/rollup/run` | Trigger daily rollup calculation |

## Required Environment Variables
- `ADMIN_API_KEY` - Required for admin endpoint authentication

## Post-Deploy Steps
1. Set `ADMIN_API_KEY` env var in Railway
2. Call `POST /api/v1/admin/telemetry/setup` to create tables
3. Verify with `GET /api/v1/admin/telemetry/health`

## Testing
Use header: `X-Admin-API-Key: <your-admin-key>`
