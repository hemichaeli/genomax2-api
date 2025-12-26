"""
GenoMAX² Bloodwork Engine v1.0
==============================
Safety and routing layer for biomarker normalization.

Usage:
    from bloodwork_engine import get_engine, get_loader
    from bloodwork_engine.api import register_bloodwork_endpoints
"""

from bloodwork_engine.engine import (
    BloodworkEngine,
    BloodworkDataLoader,
    BloodworkResult,
    ProcessedMarker,
    MarkerStatus,
    RangeStatus,
    get_engine,
    get_loader
)

from bloodwork_engine.api import register_bloodwork_endpoints

__version__ = "1.0.0"
__all__ = [
    "BloodworkEngine",
    "BloodworkDataLoader", 
    "BloodworkResult",
    "ProcessedMarker",
    "MarkerStatus",
    "RangeStatus",
    "get_engine",
    "get_loader",
    "register_bloodwork_endpoints"
]
