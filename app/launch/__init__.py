"""
Launch Gate Module for GenoMAX²
===============================
Manages Launch v1 scope and tier enforcement.

v3.27.0:
- Added enforcement router for Launch v1 pipeline enforcement
- Pairing QA validation
- Design export with LAUNCH_V1_SUMMARY
"""

from app.launch.router import router
from app.launch.enforcement import router as enforcement_router

__all__ = ["router", "enforcement_router"]
