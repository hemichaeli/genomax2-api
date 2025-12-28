"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
Version 3.12.0 - Catalog Governance Integration

INSTRUCTIONS: Rename this file to api_server.py to activate

v3.12.0:
- Integrate Catalog Governance admin endpoints (Issue #5)
- /api/v1/admin/catalog/coverage - Coverage report
- /api/v1/admin/catalog/missing-metadata - Missing metadata report
- /api/v1/admin/catalog/unknown-ingredients - Unknown ingredients report
- /api/v1/admin/catalog/validate - Full validation results
- /api/v1/admin/catalog/health - Module health check
- Admin endpoints require X-Admin-API-Key header

v3.10.2:
- Fix railway.json to use main.py entry point
- Add /api/v1/brain/painpoints and /api/v1/brain/lifestyle-schema endpoints
"""

import os
import json
import hashlib
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any
from enum import Enum
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid

# Brain Resolver imports
from app.brain.contracts import (
    CONTRACT_VERSION,
    AssessmentContext as ResolverAssessmentContext,
    RoutingConstraints as ResolverRoutingConstraints,
    ProtocolIntents as ResolverProtocolIntents,
    ProtocolIntentItem,
    ResolverInput,
    ResolverOutput,
    empty_routing_constraints,
    empty_protocol_intents,
)
from app.brain.resolver import resolve_all, compute_hash as resolver_compute_hash
from app.brain.mocks import bloodwork_mock, lifestyle_mock, goals_mock

# Catalog Governance imports (NEW in v3.12.0)
from app.catalog.admin import router as catalog_router

app = FastAPI(title="GenoMAX² API", description="Gender-Optimized Biological Operating System", version="3.12.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://genomax2-frontend.vercel.app", "https://genomax2-frontend-git-main-hemis-projects-6782105b.vercel.app", "http://localhost:3000", "http://127.0.0.1:3000", "*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Register Catalog Governance admin router (NEW in v3.12.0)
app.include_router(catalog_router)

DATABASE_URL = os.getenv("DATABASE_URL")

# ... rest of the file is identical to api_server.py ...
# See api_server.py for full implementation
# This file contains just the key changes for v3.12.0
