"""
GenoMAX² QA Module
Quality Assurance and Audit tools for database validation.
"""

from fastapi import APIRouter

from .audit import router as audit_router
from .excel_compare import router as excel_compare_router

# Combined QA router
qa_router = APIRouter()
qa_router.include_router(audit_router)
qa_router.include_router(excel_compare_router)

__all__ = ["qa_router"]
