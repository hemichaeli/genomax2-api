"""
GenoMAX² Bloodwork Engine v2.0
==============================
Safety and routing layer for biomarker normalization with complete
support for 40 biomarkers, 31 safety gates, genetic markers (MTHFR),
computed markers (HOMA-IR), and hormonal routing.

Usage:
    from bloodwork_engine import get_engine, get_loader
    from bloodwork_engine.api import register_bloodwork_endpoints
    from bloodwork_engine.safety_router import SafetyRouter, create_static_router

For v1 compatibility:
    from bloodwork_engine.engine import BloodworkEngine as BloodworkEngineV1
"""

# V2 Engine (default) - 40 markers, 31 gates, genetic/hormonal support
from bloodwork_engine.engine_v2 import (
    BloodworkEngineV2,
    BloodworkDataLoader,
    BloodworkResult,
    ProcessedMarker,
    ComputedMarker,
    MarkerStatus,
    RangeStatus,
    GateTier,
    GateAction,
    get_engine,
    get_loader
)

# Safety Router - connects engine output to ingredient filtering
from bloodwork_engine.safety_router import (
    SafetyRouter,
    RoutingConstraints,
    FilteredProduct,
    FlagType,
    create_static_router,
    get_static_ingredient_flags
)

# API endpoints
from bloodwork_engine.api import register_bloodwork_endpoints

# Aliases for backward compatibility
BloodworkEngine = BloodworkEngineV2

__version__ = "2.0.0"
__all__ = [
    # V2 Engine (default)
    "BloodworkEngine",
    "BloodworkEngineV2",
    "BloodworkDataLoader",
    "BloodworkResult",
    "ProcessedMarker",
    "ComputedMarker",
    "MarkerStatus",
    "RangeStatus",
    "GateTier",
    "GateAction",
    "get_engine",
    "get_loader",
    # Safety Router
    "SafetyRouter",
    "RoutingConstraints",
    "FilteredProduct",
    "FlagType",
    "create_static_router",
    "get_static_ingredient_flags",
    # API
    "register_bloodwork_endpoints"
]
