"""
GenoMAX² Bloodwork Engine v2.0 - API Endpoints
===============================================
FastAPI endpoints for bloodwork processing and data access.

v2.0 Features:
- 40 biomarkers (13 original + 27 new)
- 31 safety gates across 3 tiers
- Genetic marker support (MTHFR)
- Computed markers (HOMA-IR, ratios)
- Hormonal routing
- OCR parsing for lab reports
- Lab API integration (Junction/Vital)
- Webhook endpoints for lab result notifications
"""

import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from dataclasses import asdict
from fastapi import UploadFile, File, Body, Query
from starlette.requests import Request

# ============================================================
# REQUEST/RESPONSE MODELS
# ============================================================

class MarkerInput(BaseModel):
    """Single marker input for processing."""
    code: str = Field(..., description="Marker code or alias (e.g., 'ferritin', 'Vitamin D')")
    value: Any = Field(..., description="Numeric value or categorical (for genetic markers)")
    unit: str = Field(..., description="Unit of measurement (e.g., 'ng/mL', 'nmol/L', 'categorical')")


class ProcessMarkersRequest(BaseModel):
    """Request to process multiple markers."""
    markers: List[MarkerInput] = Field(..., description="List of markers to process")
    lab_profile: str = Field(default="GLOBAL_CONSERVATIVE", description="Lab profile for reference ranges")
    sex: Optional[str] = Field(default=None, description="Biological sex ('male' or 'female')")
    age: Optional[int] = Field(default=None, description="Age in years")


class ProcessedMarkerResponse(BaseModel):
    """Response for a single processed marker."""
    original_code: str
    canonical_code: Optional[str]
    original_value: Any
    canonical_value: Any
    original_unit: str
    canonical_unit: Optional[str]
    status: str
    range_status: str
    reference_range: Optional[Dict[str, Optional[float]]] = None
    genomax_optimal: Optional[Dict[str, Optional[float]]] = None
    decision_limits: Optional[Dict[str, Optional[float]]] = None
    lab_profile_used: Optional[str] = None
    fallback_used: bool = False
    conversion_applied: bool = False
    conversion_multiplier: Optional[float] = None
    flags: List[str] = []
    log_entries: List[str] = []
    is_genetic: bool = False
    genetic_interpretation: Optional[str] = None


class ComputedMarkerResponse(BaseModel):
    """Response for a computed/derived marker."""
    code: str
    name: str
    value: float
    formula: str
    unit: str
    source_markers: List[str]


class SafetyGateResponse(BaseModel):
    """Response for a triggered safety gate."""
    gate_id: str
    name: str
    tier: str
    description: str
    trigger_marker: str
    trigger_value: Any
    threshold: Any
    action: str
    routing_constraint: str
    blocked_ingredients: List[str] = []
    caution_ingredients: List[str] = []
    recommended_ingredients: List[str] = []
    exception_active: bool = False
    exception_reason: Optional[str] = None


class ProcessMarkersResponse(BaseModel):
    """Response from processing markers."""
    processed_at: str
    engine_version: str
    lab_profile: str
    markers: List[ProcessedMarkerResponse]
    computed_markers: List[ComputedMarkerResponse] = []
    routing_constraints: List[str]
    safety_gates: List[SafetyGateResponse]
    require_review: bool
    summary: Dict[str, int]
    gate_summary: Dict[str, int]
    ruleset_version: str
    input_hash: str
    output_hash: str


# Lab API Models
class CreateVitalUserRequest(BaseModel):
    """Request to create a Junction/Vital user."""
    external_id: str = Field(..., description="GenoMAX user ID")
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None


class CreateLabOrderRequest(BaseModel):
    """Request to create a lab order."""
    user_id: str = Field(..., description="Junction user ID")
    lab_test_id: str = Field(..., description="Lab test ID to order")
    collection_method: str = Field(default="walk_in_test", description="walk_in_test, at_home_phlebotomy, or testkit")
    patient_details: Optional[Dict[str, Any]] = Field(default=None, description="Patient info: first_name, last_name, dob, gender, email, phone_number")
    patient_address: Optional[Dict[str, Any]] = Field(default=None, description="Address: street, city, state, zip_code, country")


# ============================================================
# ENDPOINT REGISTRATION
# ============================================================

def register_bloodwork_endpoints(app):
    """
    Register all bloodwork engine endpoints on a FastAPI app.
    
    Usage:
        from bloodwork_engine.api import register_bloodwork_endpoints
        register_bloodwork_endpoints(app)
    """
    from bloodwork_engine.engine_v2 import get_engine, get_loader, BloodworkDataLoader, GateTier, GateAction
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/lab-profiles
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/lab-profiles", tags=["Bloodwork Engine"])
    def list_lab_profiles():
        """List available lab profiles for reference range lookup."""
        loader = get_loader()
        return {
            "lab_profiles": loader.lab_profiles,
            "default": "GLOBAL_CONSERVATIVE",
            "lookup_order": ["<requested_profile>", "GLOBAL_CONSERVATIVE"]
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/markers
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/markers", tags=["Bloodwork Engine"])
    def list_markers(tier: Optional[str] = None):
        """List all allowed biomarkers in the registry."""
        loader = get_loader()
        registry = loader.marker_registry
        
        markers = registry.get("markers", [])
        
        if tier:
            tier_value = tier.lower().replace("tier ", "")
            markers = [m for m in markers if str(m.get("tier", "original")).lower() == tier_value]
        
        return {
            "version": registry.get("version"),
            "scope": registry.get("scope"),
            "allowed_marker_codes": loader.allowed_marker_codes,
            "marker_count": len(loader.allowed_marker_codes),
            "tier_counts": {
                "original": len([m for m in registry.get("markers", []) if m.get("tier", "original") == "original"]),
                "tier_1": len([m for m in registry.get("markers", []) if m.get("tier") == "1"]),
                "tier_2": len([m for m in registry.get("markers", []) if m.get("tier") == "2"]),
                "tier_3": len([m for m in registry.get("markers", []) if m.get("tier") == "3"])
            },
            "markers": [
                {
                    "code": m["code"],
                    "aliases": m.get("aliases", []),
                    "canonical_unit": m["canonical_unit"],
                    "allowed_units": m.get("allowed_units", []),
                    "sex_relevance": m.get("sex_relevance", "both"),
                    "has_conversions": len(m.get("conversions", [])) > 0,
                    "tier": m.get("tier", "original"),
                    "is_genetic": m.get("canonical_unit") == "genotype",
                    "notes": m.get("notes")
                }
                for m in markers
            ]
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/markers/{code}
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/markers/{code}", tags=["Bloodwork Engine"])
    def get_marker_details(code: str):
        """Get detailed information for a specific marker."""
        loader = get_loader()
        marker_def = loader.get_marker_definition(code)
        
        if marker_def is None:
            return {
                "error": "UNKNOWN_MARKER",
                "message": f"Marker '{code}' not found in registry",
                "allowed_markers": loader.allowed_marker_codes
            }
        
        return {
            "code": marker_def["code"],
            "aliases": marker_def.get("aliases", []),
            "canonical_unit": marker_def["canonical_unit"],
            "allowed_units": marker_def.get("allowed_units", []),
            "conversions": marker_def.get("conversions", []),
            "sex_relevance": marker_def.get("sex_relevance", "both"),
            "tier": marker_def.get("tier", "original"),
            "added_in": marker_def.get("added_in"),
            "notes": marker_def.get("notes"),
            "is_genetic": marker_def.get("canonical_unit") == "genotype",
            "valid_values": marker_def.get("valid_values")
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/reference-ranges
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/reference-ranges", tags=["Bloodwork Engine"])
    def list_reference_ranges(
        lab_profile: Optional[str] = None,
        marker_code: Optional[str] = None
    ):
        """List reference ranges."""
        loader = get_loader()
        ranges_data = loader.reference_ranges
        
        ranges = ranges_data.get("ranges", [])
        
        if lab_profile:
            ranges = [r for r in ranges if r.get("lab_profile") == lab_profile]
        if marker_code:
            canonical = loader.resolve_marker_code(marker_code)
            if canonical:
                ranges = [r for r in ranges if r.get("marker_code") == canonical]
        
        return {
            "version": ranges_data.get("version"),
            "scope": ranges_data.get("scope"),
            "philosophy": ranges_data.get("philosophy"),
            "policy": ranges_data.get("policy"),
            "lab_profiles": ranges_data.get("lab_profiles", []),
            "range_count": len(ranges),
            "ranges": ranges
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/safety-gates
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/safety-gates", tags=["Bloodwork Engine"])
    def list_safety_gates(tier