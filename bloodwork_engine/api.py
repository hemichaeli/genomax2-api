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
"""

import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from dataclasses import asdict
from fastapi import UploadFile, File, Body, Query

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
    
    # =========================================================
    # OCR ENDPOINTS
    # =========================================================
    
    # ---------------------------------------------------------
    # GET /api/v1/bloodwork/ocr/status
    # ---------------------------------------------------------
    @app.get("/api/v1/bloodwork/ocr/status", tags=["Bloodwork OCR"])
    def ocr_status():
        """Get OCR configuration status."""
        try:
            from bloodwork_engine.ocr_parser import OCRParser, get_ocr_status
            status = get_ocr_status()
            return {
                "ocr_available": status["configured"],
                "configuration": status,
                "supported_formats": ["image/png", "image/jpeg", "image/gif", "image/webp", "application/pdf"],
                "max_file_size_mb": 20,
                "provider": "Google Cloud Vision" if status["configured"] else None
            }
        except ImportError:
            return {
                "ocr_available": False,
                "error": "google-cloud-vision package not installed",
                "configuration": {"configured": False}
            }
        except Exception as e:
            return {
                "ocr_available": False,
                "error": str(e),
                "configuration": {"configured": False}
            }
    
    # ---------------------------------------------------------
    # POST /api/v1/bloodwork/ocr/parse
    # ---------------------------------------------------------
    @app.post("/api/v1/bloodwork/ocr/parse", tags=["Bloodwork OCR"])
    async def parse_bloodwork_upload(
        file: UploadFile = File(...),
        lab_profile: str = "GLOBAL_CONSERVATIVE",
        sex: Optional[str] = None,
        age: Optional[int] = None
    ):
        """
        Parse a bloodwork report image/PDF using OCR and process markers.
        
        Accepts: PNG, JPEG, GIF, WebP, PDF
        Max size: 20MB
        """
        from bloodwork_engine.ocr_parser import OCRParser, get_ocr_status
        
        # Check configuration
        status = get_ocr_status()
        if not status["configured"]:
            return {
                "error": "OCR_NOT_CONFIGURED",
                "message": "Google Cloud Vision credentials not configured",
                "setup_instructions": {
                    "option_1": "Set GOOGLE_APPLICATION_CREDENTIALS environment variable",
                    "option_2": "Set GOOGLE_CREDENTIALS_BASE64 with base64-encoded service account JSON"
                }
            }
        
        # Validate file type
        content_type = file.content_type or ""
        valid_types = ["image/png", "image/jpeg", "image/gif", "image/webp", "application/pdf"]
        if content_type not in valid_types:
            return {
                "error": "INVALID_FILE_TYPE",
                "message": f"Unsupported file type: {content_type}",
                "supported_types": valid_types
            }
        
        # Read file
        content = await file.read()
        
        # Check size (20MB max)
        max_size = 20 * 1024 * 1024
        if len(content) > max_size:
            return {
                "error": "FILE_TOO_LARGE",
                "message": f"File size {len(content)} exceeds maximum {max_size} bytes",
                "max_size_mb": 20
            }
        
        try:
            # Parse with OCR
            parser = OCRParser()
            parse_result = parser.parse_image(content, content_type)
            
            # If markers found, process through engine
            engine_result = None
            if parse_result.markers:
                engine = get_engine(lab_profile=lab_profile)
                markers_input = parse_result.to_engine_input()
                engine_result = engine.process_markers(
                    markers=markers_input,
                    sex=sex,
                    age=age
                )
            
            return {
                "status": "success",
                "ocr_result": {
                    "markers_found": len(parse_result.markers),
                    "lab_detected": parse_result.lab_name,
                    "report_date": parse_result.report_date,
                    "parse_stats": parse_result.parse_stats,
                    "raw_markers": [
                        {
                            "code": m.code,
                            "value": m.value,
                            "unit": m.unit,
                            "confidence": m.confidence,
                            "raw_text": m.raw_text
                        }
                        for m in parse_result.markers
                    ]
                },
                "engine_result": {
                    "processed_at": engine_result.processed_at,
                    "markers": len(engine_result.markers),
                    "routing_constraints": engine_result.routing_constraints,
                    "safety_gates": len(engine_result.safety_gates),
                    "require_review": engine_result.require_review,
                    "summary": engine_result.summary
                } if engine_result else None
            }
            
        except Exception as e:
            return {
                "error": "OCR_PROCESSING_ERROR",
                "message": str(e)
            }
    
    # ---------------------------------------------------------
    # POST /api/v1/bloodwork/ocr/parse-text
    # ---------------------------------------------------------
    @app.post("/api/v1/bloodwork/ocr/parse-text", tags=["Bloodwork OCR"])
    def parse_text_input(
        text: str = Body(..., embed=True),
        lab_profile: str = "GLOBAL_CONSERVATIVE",
        sex: Optional[str] = None,
        age: Optional[int] = None
    ):
        """
        Parse raw text (copy-pasted from lab report) without OCR.
        
        Useful for:
        - Pre-extracted text
        - Manual entry
        - Testing
        """
        from bloodwork_engine.ocr_parser import parse_text_fallback
        
        try:
            # Parse text
            parse_result = parse_text_fallback(text)
            
            # Process through engine if markers found
            engine_result = None
            if parse_result.markers:
                engine = get_engine(lab_profile=lab_profile)
                markers_input = parse_result.to_engine_input()
                engine_result = engine.process_markers(
                    markers=markers_input,
                    sex=sex,
                    age=age
                )
            
            return {
                "status": "success",
                "parse_result": {
                    "markers_found": len(parse_result.markers),
                    "lab_detected": parse_result.lab_name,
                    "report_date": parse_result.report_date,
                    "parse_stats": parse_result.parse_stats,
                    "raw_markers": [
                        {
                            "code": m.code,
                            "value": m.value,
                            "unit": m.unit,
                            "confidence": m.confidence
                        }
                        for m in parse_result.markers
                    ]
                },
                "engine_result": {
                    "processed_at": engine_result.processed_at,
                    "markers": len(engine_result.markers),
                    "routing_constraints": engine_result.routing_constraints,
                    "safety_gates": len(engine_result.safety_gates),
                    "require_review": engine_result.require_review,
                    "summary": engine_result.summary
                } if engine_result else None
            }
            
        except Exception as e:
            return {
                "error": "PARSE_ERROR",
                "message": str(e)
            }
    
    # =========================================================
    # LAB API ENDPOINTS (Junction/Vital Integration)
    # =========================================================
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/status
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/status", tags=["Lab API"])
    def labs_status():
        """Get status of all configured lab providers."""
        try:
            from bloodwork_engine.lab_adapters import list_providers, get_vital_status
            
            providers = list_providers()
            vital_status = get_vital_status()
            
            return {
                "status": "operational",
                "providers": providers,
                "primary_provider": "vital",
                "vital_configured": vital_status.get("valid", False),
                "documentation": "https://docs.junction.com/"
            }
        except ImportError as e:
            return {
                "status": "error",
                "error": f"Lab adapters not available: {e}",
                "providers": []
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "providers": []
            }
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/vital/status
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/vital/status", tags=["Lab API"])
    def vital_status():
        """Check Junction (formerly Vital) API configuration and connectivity."""
        api_key = os.environ.get("VITAL_API_KEY")
        environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
        
        if not api_key:
            return {
                "configured": False,
                "error": "VITAL_API_KEY environment variable not set",
                "environment": environment,
                "setup_instructions": {
                    "step_1": "Sign up at https://tryvital.io (now Junction)",
                    "step_2": "Get API key from dashboard",
                    "step_3": "Set VITAL_API_KEY environment variable",
                    "step_4": "Optionally set VITAL_ENVIRONMENT to 'production' (default: sandbox)",
                    "step_5": "Contact Junction to enable Lab Testing API on your account"
                }
            }
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            validation = adapter.validate_credentials()
            
            # Check if lab testing is enabled by trying to list labs
            lab_testing_enabled = False
            lab_testing_error = None
            available_labs = []
            try:
                labs = adapter.list_labs()
                lab_testing_enabled = True
                available_labs = labs
            except Exception as e:
                lab_testing_error = str(e)
                if "not authorized" in str(e).lower():
                    lab_testing_error = "Lab Testing API not enabled. Contact Junction to enable lab testing on your account."
            
            return {
                "configured": True,
                "api_key_valid": validation.get("valid", False),
                "lab_testing_enabled": lab_testing_enabled,
                "lab_testing_error": lab_testing_error,
                "environment": environment,
                "api_key_preview": f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***",
                "base_url": adapter.base_url,
                "api_version": "v3",
                "validation_result": validation,
                "available_labs": available_labs if lab_testing_enabled else [],
                "features": {
                    "walk_in_tests": lab_testing_enabled,
                    "at_home_phlebotomy": lab_testing_enabled,
                    "test_kits": lab_testing_enabled,
                    "quest_locations": lab_testing_enabled,
                    "labcorp_locations": lab_testing_enabled,
                    "restricted_states": ["NY", "NJ", "RI"] if lab_testing_enabled else []
                },
                "next_steps": {
                    "action_required": "Contact Junction to enable Lab Testing API",
                    "contact": "support@junction.com",
                    "book_intro_call": "https://tryvital.io/labs",
                    "documentation": "https://docs.junction.com/lab/overview/introduction"
                } if not lab_testing_enabled else {
                    "ready": True,
                    "documentation": "https://docs.junction.com/lab/overview/introduction"
                }
            }
        except Exception as e:
            return {
                "configured": True,
                "api_key_valid": False,
                "error": str(e),
                "environment": environment
            }
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/vital/tests
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/vital/tests", tags=["Lab API"])
    def list_vital_tests():
        """List available lab tests from Junction."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            tests = adapter.list_lab_tests()
            
            return {
                "test_count": len(tests),
                "tests": [
                    {
                        "test_id": t.test_id,
                        "name": t.name,
                        "description": t.description,
                        "markers": t.markers,
                        "sample_type": t.sample_type,
                        "fasting_required": t.fasting_required,
                        "turnaround_days": t.turnaround_days,
                        "lab_provider": t.lab_provider
                    }
                    for t in tests
                ]
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/vital/markers
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/vital/markers", tags=["Lab API"])
    def list_vital_markers():
        """List available biomarkers from Junction."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            markers = adapter.list_markers()
            
            return {
                "marker_count": len(markers),
                "markers": markers,
                "genomax_panel": adapter.GENOMAX_PANEL_MARKERS
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/vital/locations
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/vital/locations", tags=["Lab API"])
    def find_vital_locations(
        zip_code: str = Query(..., description="US ZIP code"),
        radius_miles: int = Query(default=25, description="Search radius in miles"),
        lab: Optional[str] = Query(default=None, description="Filter by lab (quest, labcorp)")
    ):
        """Find nearby lab locations for walk-in tests."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            locations = adapter.find_lab_locations(
                zip_code=zip_code,
                radius_miles=radius_miles,
                lab_id=lab
            )
            
            return {
                "zip_code": zip_code,
                "radius_miles": radius_miles,
                "location_count": len(locations),
                "locations": locations
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # POST /api/v1/labs/vital/users
    # ---------------------------------------------------------
    @app.post("/api/v1/labs/vital/users", tags=["Lab API"])
    def create_vital_user(request: CreateVitalUserRequest):
        """Create a user in Junction for lab ordering."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter, LabPatient
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            
            patient = LabPatient(
                external_id=request.external_id,
                first_name=request.first_name,
                last_name=request.last_name,
                email=request.email
            )
            
            result = adapter.create_user(patient)
            
            return {
                "success": True,
                "user_id": result.get("user_id"),
                "client_user_id": result.get("client_user_id"),
                "created_at": result.get("created_at")
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # POST /api/v1/labs/vital/orders
    # ---------------------------------------------------------
    @app.post("/api/v1/labs/vital/orders", tags=["Lab API"])
    def create_vital_order(request: CreateLabOrderRequest):
        """Create a lab test order."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            
            order = adapter.create_order(
                user_id=request.user_id,
                lab_test_id=request.lab_test_id,
                collection_method=request.collection_method,
                patient_details=request.patient_details,
                patient_address=request.patient_address
            )
            
            return {
                "success": True,
                "order_id": order.order_id,
                "status": order.status,
                "lab_provider": order.lab_provider,
                "collection_method": order.collection_method,
                "requisition_url": order.requisition_url,
                "ordered_at": order.ordered_at.isoformat() if order.ordered_at else None
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/vital/orders/{order_id}
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/vital/orders/{order_id}", tags=["Lab API"])
    def get_vital_order_status(order_id: str):
        """Get the status of a lab order."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            
            order = adapter.get_order_status(order_id)
            
            return {
                "order_id": order.order_id,
                "status": order.status,
                "lab_provider": order.lab_provider,
                "collection_method": order.collection_method,
                "requisition_url": order.requisition_url,
                "ordered_at": order.ordered_at.isoformat() if order.ordered_at else None,
                "completed_at": order.completed_at.isoformat() if order.completed_at else None
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # POST /api/v1/labs/vital/orders/{order_id}/cancel
    # ---------------------------------------------------------
    @app.post("/api/v1/labs/vital/orders/{order_id}/cancel", tags=["Lab API"])
    def cancel_vital_order(order_id: str):
        """Cancel a pending lab order."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            
            result = adapter.cancel_order(order_id)
            
            return {
                "order_id": order_id,
                "cancelled": True,
                "result": result
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/vital/results
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/vital/results", tags=["Lab API"])
    def get_vital_results(
        user_id: Optional[str] = Query(default=None, description="Junction user ID"),
        order_id: Optional[str] = Query(default=None, description="Specific order ID"),
        process_through_engine: bool = Query(default=True, description="Process results through Bloodwork Engine"),
        lab_profile: str = Query(default="GLOBAL_CONSERVATIVE", description="Lab profile for engine processing"),
        sex: Optional[str] = Query(default=None, description="Biological sex for engine processing"),
        age: Optional[int] = Query(default=None, description="Age for engine processing")
    ):
        """
        Fetch lab results from Junction.
        
        Optionally processes results through the Bloodwork Engine to get
        safety gates, routing constraints, and optimization recommendations.
        """
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        if not user_id and not order_id:
            return {"error": "Either user_id or order_id is required"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            
            results = adapter.fetch_results(user_id=user_id, order_id=order_id)
            
            if not results:
                return {
                    "results_found": 0,
                    "message": "No results found",
                    "user_id": user_id,
                    "order_id": order_id
                }
            
            # Format results
            formatted_results = []
            for r in results:
                result_data = {
                    "result_id": r.result_id,
                    "order_id": r.order_id,
                    "lab_provider": r.lab_provider,
                    "report_date": r.report_date.isoformat() if r.report_date else None,
                    "status": r.status,
                    "pdf_url": r.pdf_url,
                    "markers": [
                        {
                            "code": m.marker_code,
                            "name": m.marker_name,
                            "value": m.value,
                            "unit": m.unit,
                            "reference_low": m.reference_low,
                            "reference_high": m.reference_high,
                            "flag": m.flag,
                            "loinc_code": m.loinc_code
                        }
                        for m in r.markers
                    ]
                }
                
                # Process through engine if requested
                if process_through_engine and r.markers:
                    engine = get_engine(lab_profile=lab_profile)
                    markers_input = adapter.to_engine_input(r)
                    
                    if markers_input:
                        engine_result = engine.process_markers(
                            markers=markers_input,
                            sex=sex,
                            age=age
                        )
                        
                        result_data["engine_result"] = {
                            "processed_at": engine_result.processed_at,
                            "markers_processed": len(engine_result.markers),
                            "routing_constraints": engine_result.routing_constraints,
                            "safety_gates_triggered": len(engine_result.safety_gates),
                            "require_review": engine_result.require_review,
                            "summary": engine_result.summary,
                            "gate_summary": engine_result.gate_summary
                        }
                
                formatted_results.append(result_data)
            
            return {
                "results_found": len(formatted_results),
                "user_id": user_id,
                "order_id": order_id,
                "results": formatted_results
            }
        except Exception as e:
            return {"error": str(e)}
    
    # ---------------------------------------------------------
    # GET /api/v1/labs/vital/results/{order_id}/pdf
    # ---------------------------------------------------------
    @app.get("/api/v1/labs/vital/results/{order_id}/pdf", tags=["Lab API"])
    def get_vital_result_pdf(order_id: str):
        """Get PDF URL for lab results."""
        api_key = os.environ.get("VITAL_API_KEY")
        if not api_key:
            return {"error": "VITAL_API_KEY not configured"}
        
        try:
            from bloodwork_engine.lab_adapters import VitalAdapter
            
            environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
            adapter = VitalAdapter(api_key=api_key, environment=environment)
            
            pdf_url = adapter.get_result_pdf(order_id)
            
            return {
                "order_id": order_id,
                "pdf_url": pdf_url
            }
        except Exception as e:
            return {"error": str(e)}
    
    return app
