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
"""

import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from dataclasses import asdict

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
    def list_safety_gates(tier: Optional[str] = None):
        """List all defined safety gates."""
        loader = get_loader()
        all_gates = loader.get_safety_gates()
        
        if tier:
            tier_num = tier.replace("tier", "").replace(" ", "").strip()
            all_gates = {k: v for k, v in all_gates.items() if str(v.get("tier", "1")) == tier_num}
        
        tier1_gates = {k: v for k, v in all_gates.items() if v.get("tier") == 1}
        tier2_gates = {k: v for k, v in all_gates.items() if v.get("tier") == 2}
        tier3_gates = {k: v for k, v in all_gates.items() if v.get("tier") == 3}
        
        return {
            "version": loader.reference_ranges.get("version"),
            "total_gates": len(all_gates),
            "tier_summary": {
                "tier1_safety": len(tier1_gates),
                "tier2_optimization": len(tier2_gates),
                "tier3_genetic_hormonal": len(tier3_gates)
            },
            "gates_by_tier": {
                "tier1_safety": tier1_gates,
                "tier2_optimization": tier2_gates,
                "tier3_genetic_hormonal": tier3_gates
            },
            "all_gates": all_gates if not tier else None
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/computed-markers
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/computed-markers", tags=["Bloodwork Engine"])
    def list_computed_markers():
        """List available computed/derived markers."""
        return {
            "computed_markers": [
                {
                    "code": "homa_ir",
                    "name": "HOMA-IR (Insulin Resistance)",
                    "formula": "(fasting_glucose * fasting_insulin) / 405",
                    "required_markers": ["fasting_glucose", "fasting_insulin"],
                    "interpretation": {"optimal": "<1.0", "normal": "<2.5", "elevated": "2.5-3.0", "high": ">3.0"}
                },
                {
                    "code": "zinc_copper_ratio",
                    "name": "Zinc:Copper Ratio",
                    "formula": "zinc_serum / copper_serum",
                    "required_markers": ["zinc_serum", "copper_serum"],
                    "interpretation": {"optimal": "0.7-1.0", "elevated_zinc": ">1.5 (caution)"}
                },
                {
                    "code": "rt3_ft3_ratio",
                    "name": "Reverse T3 to Free T3 Ratio",
                    "formula": "(reverse_t3 / free_t3) * 100",
                    "required_markers": ["reverse_t3", "free_t3"],
                    "interpretation": {"optimal": "<10", "elevated": ">10 (poor T4-T3 conversion)"}
                },
                {
                    "code": "estradiol_progesterone_ratio",
                    "name": "Estradiol:Progesterone Ratio",
                    "formula": "estradiol / progesterone",
                    "required_markers": ["estradiol", "progesterone"],
                    "sex_specific": "female",
                    "interpretation": {"optimal_luteal": "10-20", "estrogen_dominance": ">20"}
                }
            ]
        }
    
    # ---------------------------------------------------------
    # POST /api/v1/bloodwork/process
    # ---------------------------------------------------------
    @app.post("/api/v1/bloodwork/process", tags=["Bloodwork Engine"])
    def process_markers(request: ProcessMarkersRequest):
        """Process biomarkers through the Bloodwork Engine v2.0."""
        engine = get_engine(lab_profile=request.lab_profile)
        
        markers_input = [
            {"code": m.code, "value": m.value, "unit": m.unit}
            for m in request.markers
        ]
        
        result = engine.process_markers(
            markers=markers_input,
            sex=request.sex,
            age=request.age
        )
        
        # Convert computed markers
        computed_markers_response = []
        if hasattr(result, 'computed_markers') and result.computed_markers:
            for cm in result.computed_markers:
                computed_markers_response.append({
                    "code": cm.code,
                    "name": cm.name,
                    "value": cm.value,
                    "formula": cm.formula,
                    "unit": cm.unit,
                    "source_markers": cm.source_markers
                })
        
        # Convert safety gates
        safety_gates_response = []
        for g in result.safety_gates:
            safety_gates_response.append({
                "gate_id": g.gate_id,
                "name": g.name,
                "tier": g.tier.value if hasattr(g.tier, 'value') else str(g.tier),
                "description": g.description,
                "trigger_marker": g.trigger_marker,
                "trigger_value": g.trigger_value,
                "threshold": g.threshold,
                "action": g.action.value if hasattr(g.action, 'value') else str(g.action),
                "routing_constraint": g.routing_constraint,
                "blocked_ingredients": getattr(g, 'blocked_ingredients', []),
                "caution_ingredients": getattr(g, 'caution_ingredients', []),
                "recommended_ingredients": getattr(g, 'recommended_ingredients', []),
                "exception_active": g.exception_active,
                "exception_reason": g.exception_reason
            })
        
        # Convert markers
        markers_response = []
        for m in result.markers:
            markers_response.append({
                "original_code": m.original_code,
                "canonical_code": m.canonical_code,
                "original_value": m.original_value,
                "canonical_value": m.canonical_value,
                "original_unit": m.original_unit,
                "canonical_unit": m.canonical_unit,
                "status": m.status.value if hasattr(m.status, 'value') else m.status,
                "range_status": m.range_status.value if hasattr(m.range_status, 'value') else m.range_status,
                "reference_range": m.reference_range,
                "genomax_optimal": getattr(m, 'genomax_optimal', None),
                "decision_limits": getattr(m, 'decision_limits', None),
                "lab_profile_used": m.lab_profile_used,
                "fallback_used": m.fallback_used,
                "conversion_applied": m.conversion_applied,
                "conversion_multiplier": m.conversion_multiplier,
                "flags": m.flags,
                "log_entries": m.log_entries,
                "is_genetic": getattr(m, 'is_genetic', False),
                "genetic_interpretation": getattr(m, 'genetic_interpretation', None)
            })
        
        return {
            "processed_at": result.processed_at,
            "engine_version": result.engine_version,
            "lab_profile": result.lab_profile,
            "markers": markers_response,
            "computed_markers": computed_markers_response,
            "routing_constraints": result.routing_constraints,
            "safety_gates": safety_gates_response,
            "require_review": result.require_review,
            "summary": result.summary,
            "gate_summary": result.gate_summary,
            "ruleset_version": result.ruleset_version,
            "input_hash": result.input_hash,
            "output_hash": result.output_hash
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/status
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/status", tags=["Bloodwork Engine"])
    def bloodwork_engine_status():
        """Get Bloodwork Engine status and configuration."""
        loader = get_loader()
        registry = loader.marker_registry
        ranges = loader.reference_ranges
        
        engine_file = Path(__file__).resolve()
        data_dir = engine_file.parent / "data"
        registry_path = data_dir / "marker_registry_v2_0.json"
        ranges_path = data_dir / "reference_ranges_v2_0.json"
        
        all_gates = loader.get_safety_gates()
        tier1_count = len([g for g in all_gates.values() if g.get("tier") == 1])
        tier2_count = len([g for g in all_gates.values() if g.get("tier") == 2])
        tier3_count = len([g for g in all_gates.values() if g.get("tier") == 3])
        
        range_count = len(ranges.get("ranges", []))
        
        return {
            "engine_version": "2.0.0",
            "status": "operational",
            "marker_registry": {
                "version": registry.get("version"),
                "marker_count": len(loader.allowed_marker_codes),
                "loaded": True,
                "tier_breakdown": {
                    "original": len([m for m in registry.get("markers", []) if m.get("tier", "original") == "original"]),
                    "tier_1": len([m for m in registry.get("markers", []) if m.get("tier") == "1"]),
                    "tier_2": len([m for m in registry.get("markers", []) if m.get("tier") == "2"]),
                    "tier_3": len([m for m in registry.get("markers", []) if m.get("tier") == "3"])
                }
            },
            "reference_ranges": {
                "version": ranges.get("version"),
                "range_count": range_count,
                "loaded": True,
                "ranges_active": range_count > 0
            },
            "safety_gates": {
                "total_defined": len(all_gates),
                "tier1_safety": tier1_count,
                "tier2_optimization": tier2_count,
                "tier3_genetic_hormonal": tier3_count,
                "gates": list(all_gates.keys())
            },
            "computed_markers": {
                "available": ["homa_ir", "zinc_copper_ratio", "rt3_ft3_ratio", "estradiol_progesterone_ratio"]
            },
            "lab_profiles": loader.lab_profiles,
            "policy": ranges.get("policy", {}),
            "ruleset_version": loader.ruleset_version,
            "diagnostics": {
                "engine_file": str(engine_file),
                "data_dir": str(data_dir),
                "data_dir_exists": data_dir.exists(),
                "registry_file": str(registry_path),
                "registry_exists": registry_path.exists(),
                "registry_size_bytes": registry_path.stat().st_size if registry_path.exists() else 0,
                "ranges_file": str(ranges_path),
                "ranges_exists": ranges_path.exists(),
                "ranges_size_bytes": ranges_path.stat().st_size if ranges_path.exists() else 0,
                "cwd": os.getcwd(),
                "singleton_loaded": BloodworkDataLoader._loaded
            }
        }
    
    # ---------------------------------------------------------
    # POST /api/v1/bloodwork/reload (ADMIN)
    # ---------------------------------------------------------
    @app.post("/api/v1/bloodwork/reload", tags=["Bloodwork Engine"])
    def reload_bloodwork_data():
        """Force reload of bloodwork data files."""
        BloodworkDataLoader.reset()
        loader = get_loader()
        all_gates = loader.get_safety_gates()
        range_count = len(loader.reference_ranges.get("ranges", []))
        
        return {
            "status": "reloaded",
            "engine_version": "2.0.0",
            "marker_count": len(loader.allowed_marker_codes),
            "range_count": range_count,
            "safety_gate_count": len(all_gates),
            "ruleset_version": loader.ruleset_version
        }
    
    return app
