"""
GenoMAX² Product Intake System

Bio-OS governed catalog intake with:
- Append-only governance
- Human-gated approval
- Deterministic module_code generation
- Full audit trail

Version: intake_system_v1
"""

from .admin import router
from .models import (
    IntakeStatus,
    IntakeCreateRequest,
    IntakeApproveRequest,
    IntakeRejectRequest,
    IntakeCreateResponse,
    IntakeApproveResponse,
    IntakeRejectResponse,
    IntakeListResponse,
    IntakeDetailResponse,
    DraftModule,
    DraftCopy,
    ValidationFlags,
)

__all__ = [
    "router",
    "IntakeStatus",
    "IntakeCreateRequest",
    "IntakeApproveRequest",
    "IntakeRejectRequest",
    "IntakeCreateResponse",
    "IntakeApproveResponse",
    "IntakeRejectResponse",
    "IntakeListResponse",
    "IntakeDetailResponse",
    "DraftModule",
    "DraftCopy",
    "ValidationFlags",
]
