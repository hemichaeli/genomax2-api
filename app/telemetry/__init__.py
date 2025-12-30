"""
GenoMAX² Telemetry & Observability Module (Issue #9)
Read-only telemetry for production insights without PII collection.

Principles:
- Read-only telemetry
- Deterministic aggregation  
- No raw user payloads
- Admin-only access
- Zero effect on runtime decisions
"""

from .models import (
    TelemetryRun,
    TelemetryEvent,
    TelemetryEventType,
    AgeBucket,
    ConfidenceLevel,
)
from .emitter import TelemetryEmitter, get_emitter
from .derive import (
    derive_run_summary,
    derive_events,
    derive_error_event,
    RunSummary,
    EventRecord,
)
from .admin import router as telemetry_router

__all__ = [
    "TelemetryRun",
    "TelemetryEvent", 
    "TelemetryEventType",
    "AgeBucket",
    "ConfidenceLevel",
    "TelemetryEmitter",
    "get_emitter",
    "derive_run_summary",
    "derive_events",
    "derive_error_event",
    "RunSummary",
    "EventRecord",
    "telemetry_router",
]
