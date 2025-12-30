# Telemetry Integration Instructions for api_server.py v3.16.0

## Required Changes to api_server.py

### 1. Update the docstring at the top
Change version from 3.15.1 to 3.16.0 and add:

```python
"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
Version 3.16.0 - Observability & Telemetry Layer (Issue #9)

v3.16.0:
- Integrate Telemetry Layer for observability (Issue #9)
- Admin endpoints at /api/v1/admin/telemetry/*
- Event emitter for tracking blocks, unmatched intents, safety gates
- No PII stored - age bucketed, counts only
- Principle: Data decides, not opinions. Observability BEFORE expansion.

v3.15.1:
...
"""
```

### 2. Add imports after explainability router import
```python
# Telemetry Layer imports (v3.16.0 - Issue #9)
from app.telemetry.admin import router as telemetry_router
from app.telemetry.emitter import get_emitter
```

### 3. Register telemetry router after explainability
```python
# Register Telemetry Layer router (v3.16.0 - Issue #9)
app.include_router(telemetry_router)
```

### 4. Update version in FastAPI app
```python
app = FastAPI(title="GenoMAX² API", description="Gender-Optimized Biological Operating System", version="3.16.0")
```

### 5. Update root and health endpoints
```python
@app.get("/")
def root():
    return {"service": "GenoMAX² API", "version": "3.16.0", "status": "operational"}


@app.get("/health")
def health():
    return {"status": "healthy", "version": "3.16.0"}
```

### 6. Update /version endpoint
```python
@app.get("/version")
def version():
    return {
        "api_version": "3.16.0",
        "brain_version": "1.5.0",
        "resolver_version": "1.0.0",
        "catalog_version": "catalog_governance_v1",
        "routing_version": "routing_layer_v1",
        "matching_version": "matching_layer_v1",
        "explainability_version": "explainability_v1",
        "telemetry_version": "telemetry_v1",  # ADD THIS
        "contract_version": CONTRACT_VERSION,
        "features": ["orchestrate", "orchestrate_v2", "compose", "route", "resolve", "supplier-gating", "catalog-governance", "routing-layer", "matching-layer", "explainability", "painpoints", "lifestyle-schema", "telemetry"]  # ADD "telemetry"
    }
```

## Environment Variables Required

Set these in Railway:
```
TELEMETRY_ENABLED=true
ADMIN_API_KEY=<your-secret-key>
```

## Post-Deployment Steps

1. Deploy the changes to Railway
2. Call setup endpoint to create tables:
   ```bash
   curl -X POST https://web-production-97b74.up.railway.app/api/v1/admin/telemetry/setup \
     -H "X-Admin-API-Key: YOUR_ADMIN_KEY"
   ```
3. Verify with health check:
   ```bash
   curl https://web-production-97b74.up.railway.app/api/v1/admin/telemetry/health \
     -H "X-Admin-API-Key: YOUR_ADMIN_KEY"
   ```

## Files Created in This Issue

- app/telemetry/__init__.py - Package exports
- app/telemetry/models.py - Pydantic data models
- app/telemetry/emitter.py - Singleton event emitter
- app/telemetry/admin.py - Admin-only endpoints
- app/telemetry/migration.py - SQL migration script
- app/telemetry/README.md - Documentation
