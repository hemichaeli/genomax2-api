"""
Brain Endpoints - Painpoints and Lifestyle Schema
These endpoints provide the painpoints dictionary and lifestyle schema for the assessment wizard.
"""
from fastapi import APIRouter
from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA

router = APIRouter(prefix="/api/v1/brain", tags=["brain"])


@router.get("/painpoints")
def brain_painpoints():
    """Return available painpoints and their mapped intents for the assessment wizard."""
    return {
        "status": "success",
        "painpoints": PAINPOINTS_DICTIONARY,
        "count": len(PAINPOINTS_DICTIONARY)
    }


@router.get("/lifestyle-schema")
def brain_lifestyle_schema():
    """Return the lifestyle assessment schema for dynamic form generation."""
    return {
        "status": "success",
        "schema": LIFESTYLE_SCHEMA,
        "question_count": len(LIFESTYLE_SCHEMA.get("questions", []))
    }
