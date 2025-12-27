"""
GenoMAX² Bloodwork Engine v1.0 - API Endpoints
===============================================
FastAPI endpoints for bloodwork processing and data access.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from dataclasses import asdict

# These will be registered on the main FastAPI app
# Import: from bloodwork_engine.api import register_bloodwork_endpoints

# ============================================================
# REQUEST/RESPONSE MODELS
# ============================================================

class MarkerInput(BaseModel):
    """Single marker input for processing."""
    code: str = Field(..., description="Marker code or alias (e.g., 'ferritin', 'Vitamin D')")
    value: float = Field(..., description="Numeric value")
    unit: str = Field(..., description="Unit of measurement (e.g., 'ng/mL', 'nmol/L')")


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
    original_value: float
    canonical_value: float
    original_unit: str
    canonical_unit: Optional[str]
    status: str
    range_status: str
    reference_range: Optional[Dict[str, Optional[float]]]
    decision_limits: Optional[Dict[str, Optional[float]]]
    lab_profile_used: Optional[str]
    fallback_used: bool
    conversion_applied: bool
    conversion_multiplier: Optional[float]
    flags: List[str]
    log_entries: List[str]


class SafetyGateResponse(BaseModel):
    """Response for a triggered safety gate."""
    gate_id: str
    description: str
    trigger_marker: str
    trigger_value: float
    threshold: float
    routing_constraint: str
    exception_active: bool
    exception_reason: Optional[str]


class ProcessMarkersResponse(BaseModel):
    """Response from processing markers."""
    processed_at: str
    lab_profile: str
    markers: List[ProcessedMarkerResponse]
    routing_constraints: List[str]
    safety_gates: List[SafetyGateResponse]
    require_review: bool
    summary: Dict[str, int]
    ruleset_version: str
    input_hash: str
    output_hash: str


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
    from bloodwork_engine.engine import get_engine, get_loader
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/lab-profiles
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/lab-profiles", tags=["Bloodwork Engine"])
    def list_lab_profiles():
        """
        List available lab profiles for reference range lookup.
        
        Lab profiles determine which reference ranges are used for biomarker evaluation.
        """
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
    def list_markers():
        """
        List all allowed biomarkers in the registry.
        
        Only markers in this registry can be processed.
        Any other marker code will be flagged as UNKNOWN.
        """
        loader = get_loader()
        registry = loader.marker_registry
        
        return {
            "version": registry.get("version"),
            "scope": registry.get("scope"),
            "allowed_marker_codes": loader.allowed_marker_codes,
            "marker_count": len(loader.allowed_marker_codes),
            "markers": [
                {
                    "code": m["code"],
                    "aliases": m.get("aliases", []),
                    "canonical_unit": m["canonical_unit"],
                    "allowed_units": m.get("allowed_units", []),
                    "sex_relevance": m.get("sex_relevance", "both"),
                    "has_conversions": len(m.get("conversions", [])) > 0
                }
                for m in registry.get("markers", [])
            ]
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/markers/{code}
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/markers/{code}", tags=["Bloodwork Engine"])
    def get_marker_details(code: str):
        """
        Get detailed information for a specific marker.
        
        Accepts canonical code or any alias.
        """
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
            "sex_relevance": marker_def.get("sex_relevance", "both")
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/reference-ranges
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/reference-ranges", tags=["Bloodwork Engine"])
    def list_reference_ranges(
        lab_profile: Optional[str] = None,
        marker_code: Optional[str] = None
    ):
        """
        List reference ranges.
        
        v1.0 includes ranges for all 13 core biomarkers:
        - ferritin, vitamin_d_25oh, calcium_serum, hs_crp
        - fasting_glucose, hba1c, creatinine, egfr
        - alt, ast, magnesium_serum, hemoglobin, total_testosterone
        
        Optional filters:
        - lab_profile: Filter by specific lab profile
        - marker_code: Filter by specific marker code
        """
        loader = get_loader()
        ranges_data = loader.reference_ranges
        
        ranges = ranges_data.get("ranges", [])
        
        # Apply filters
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
            "ranges": ranges,
            "safety_gates": ranges_data.get("safety_gates", {})
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/safety-gates
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/safety-gates", tags=["Bloodwork Engine"])
    def list_safety_gates():
        """
        List all defined safety gates.
        
        Safety gates are cross-marker constraints that block or caution
        certain supplement categories based on biomarker values.
        
        Gates in v1.0:
        - iron_block: Block iron when ferritin high (unless CRP acute)
        - vitamin_d_caution: Caution vitamin D when calcium high
        - hepatic_caution: Caution hepatotoxic when ALT/AST high
        - renal_caution: Caution renal-cleared when eGFR low / creatinine high
        - acute_inflammation: Flag when hs-CRP indicates acute inflammation
        """
        loader = get_loader()
        return {
            "version": loader.reference_ranges.get("version"),
            "safety_gates": loader.get_safety_gates()
        }
    
    # ---------------------------------------------------------
    # POST /api/v1/bloodwork/process
    # ---------------------------------------------------------
    @app.post("/api/v1/bloodwork/process", tags=["Bloodwork Engine"], response_model=ProcessMarkersResponse)
    def process_markers(request: ProcessMarkersRequest):
        """
        Process biomarkers through the Bloodwork Engine.
        
        This endpoint:
        1. Validates markers against the registry (unknown -> UNKNOWN status)
        2. Converts units to canonical units (with logging)
        3. Looks up reference ranges (lab_profile -> GLOBAL_CONSERVATIVE fallback)
        4. Evaluates decision limits and flags breaches
        5. Triggers safety gates based on cross-marker analysis
        6. Returns processed markers with flags and routing constraints
        
        Safety gates evaluated:
        - BLOCK_IRON: Ferritin above iron_block_threshold
        - CAUTION_VITAMIN_D: Calcium above vitamin_d_caution_threshold
        - CAUTION_HEPATOTOXIC: ALT or AST above hepatic_caution_threshold
        - CAUTION_RENAL: eGFR below or creatinine above renal_caution_threshold
        - FLAG_ACUTE_INFLAMMATION: hs-CRP above acute_inflammation_threshold
        """
        engine = get_engine(lab_profile=request.lab_profile)
        
        # Convert Pydantic models to dicts
        markers_input = [
            {"code": m.code, "value": m.value, "unit": m.unit}
            for m in request.markers
        ]
        
        result = engine.process_markers(
            markers=markers_input,
            sex=request.sex,
            age=request.age
        )
        
        # Convert dataclass result to response
        return ProcessMarkersResponse(
            processed_at=result.processed_at,
            lab_profile=result.lab_profile,
            markers=[
                ProcessedMarkerResponse(
                    original_code=m.original_code,
                    canonical_code=m.canonical_code,
                    original_value=m.original_value,
                    canonical_value=m.canonical_value,
                    original_unit=m.original_unit,
                    canonical_unit=m.canonical_unit,
                    status=m.status.value if hasattr(m.status, 'value') else m.status,
                    range_status=m.range_status.value if hasattr(m.range_status, 'value') else m.range_status,
                    reference_range=m.reference_range,
                    decision_limits=m.decision_limits,
                    lab_profile_used=m.lab_profile_used,
                    fallback_used=m.fallback_used,
                    conversion_applied=m.conversion_applied,
                    conversion_multiplier=m.conversion_multiplier,
                    flags=m.flags,
                    log_entries=m.log_entries
                )
                for m in result.markers
            ],
            routing_constraints=result.routing_constraints,
            safety_gates=[
                SafetyGateResponse(
                    gate_id=g.gate_id,
                    description=g.description,
                    trigger_marker=g.trigger_marker,
                    trigger_value=g.trigger_value,
                    threshold=g.threshold,
                    routing_constraint=g.routing_constraint,
                    exception_active=g.exception_active,
                    exception_reason=g.exception_reason
                )
                for g in result.safety_gates
            ],
            require_review=result.require_review,
            summary=result.summary,
            ruleset_version=result.ruleset_version,
            input_hash=result.input_hash,
            output_hash=result.output_hash
        )
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/status
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/status", tags=["Bloodwork Engine"])
    def bloodwork_engine_status():
        """
        Get Bloodwork Engine status and configuration.
        """
        loader = get_loader()
        registry = loader.marker_registry
        ranges = loader.reference_ranges
        
        return {
            "engine_version": "1.0",
            "status": "operational",
            "marker_registry": {
                "version": registry.get("version"),
                "marker_count": len(loader.allowed_marker_codes),
                "loaded": True
            },
            "reference_ranges": {
                "version": ranges.get("version"),
                "range_count": loader.range_count,
                "loaded": True,
                "ranges_active": loader.range_count > 0
            },
            "safety_gates": {
                "defined": len(loader.get_safety_gates()),
                "gates": list(loader.get_safety_gates().keys())
            },
            "lab_profiles": loader.lab_profiles,
            "policy": ranges.get("policy", {}),
            "ruleset_version": loader.ruleset_version
        }
    
    return app
