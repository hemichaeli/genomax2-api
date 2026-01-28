"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
Version 3.34.0 - Brain Pipeline Integration (#16)

v3.34.0:
- NEW: Brain Orchestrator module (bloodwork_engine/brain_orchestrator.py)
- NEW: Brain Routes (/api/v1/brain/*) for full pipeline execution
- POST /api/v1/brain/run - Execute complete Brain pipeline
- GET /api/v1/brain/run/{run_id} - Get run status/results
- POST /api/v1/brain/evaluate - Quick deficiency evaluation
- POST /api/v1/brain/canonical-handoff - Complete bloodwork-to-brain integration
- GET /api/v1/brain/health - Brain health check
- GET /api/v1/brain/config - Configuration info
- GET /api/v1/brain/deficiency-thresholds - Priority biomarker thresholds
- GET /api/v1/brain/lifecycle-recommendations - Lifecycle phase recommendations
- 13 priority biomarkers with gender-specific thresholds
- Module scoring: evidence + biomarker match + goal alignment + lifecycle
- Safety enforcement: "Blood does not negotiate" principle
- Gender optimization: MAXimo²/MAXima² filtering
- Lifecycle awareness: pregnancy, breastfeeding, perimenopause, athletic
- Full audit trail in brain_runs table

v3.32.0:
- NEW: Constraint Translator admin endpoints for testing and inspection
- GET  /api/v1/constraints/health - Module health check
- GET  /api/v1/constraints/mappings - List all constraint mappings
- GET  /api/v1/constraints/mappings/{code} - Get specific mapping
- POST /api/v1/constraints/translate - Translate constraint codes
- POST /api/v1/constraints/test - Test with sample bloodwork
- GET  /api/v1/constraints/test-scenario/{scenario} - Test predefined QA scenarios
- GET  /api/v1/constraints/qa-matrix - Run full QA validation matrix

v3.29.5:
- BUGFIX: ANONYMOUS_USER_UUID now stored as string for psycopg2 compatibility
- psycopg2 cannot adapt uuid.UUID type directly, requires string conversion
- No functional change from v3.29.4, just type compatibility fix

v3.29.4:
- BUGFIX: compose endpoint now uses ANONYMOUS_USER_UUID (00000000-...) for protocol_runs
- protocol_runs.user_id has NOT NULL constraint - cannot pass None
- brain_runs.user_id is nullable, but protocol_runs requires valid UUID
- Stable anonymous UUID enables tracking while satisfying constraint

v3.29.3:
- BUGFIX: orchestrate/v2 brain_runs INSERT now uses None (NULL) for missing user_id
- brain_runs.user_id is UUID type - cannot accept "anonymous" string
- This was causing silent INSERT failure, breaking orchestrate→compose flow
- decision_outputs INSERT was also skipped (same try block)

v3.29.1:
- BUGFIX: orchestrate/v2 bloodwork_input handler now correctly calls
  orchestrate_with_bloodwork_input() (removed erroneous await)
- BUGFIX: Correct parameters passed to orchestrate_with_bloodwork_input()
- BUGFIX: Correct result extraction from BloodworkIntegrationResult
- BUGFIX: Correct exception attribute (error_code not error_type)
- BUGFIX: Correct error enum (BLOODWORK_INVALID_HANDOFF not INVALID_HANDOFF)

v3.29.0:
- orchestrate/v2 now supports bloodwork_input mode (raw markers)
- Dual mode: bloodwork_input (recommended) or bloodwork_signal (legacy)
- bloodwork_input sends markers to Bloodwork Engine for processing
- STRICT MODE: Bloodwork Engine failures are hard aborts (503/502)
- New models: MarkerInputModel, BloodworkInputModel

v3.28.0:
- Bloodwork Engine upgraded to v2.0 (40 markers, 31 safety gates)
- Auto-migration runner on startup
- OCR parser service for blood test uploads
- Lab adapter interface for API integrations
- Safety routing service for ingredient filtering

v3.27.0:
- Launch v1 enforcement with HARD GUARDRAILS
- GET /api/v1/qa/launch-v1/pairing - Environment pairing validation
- GET /api/v1/launch-v1/export/design - Excel export with LAUNCH_V1_SUMMARY
- GET /api/v1/launch-v1/products - List Launch v1 products with base_handle
- Shopify endpoints now enforce is_launch_v1 = TRUE filter
- All external pipelines use LAUNCH_V1_SCOPE_FILTER
"""

import os
import json
import hashlib
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any
from enum import Enum
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid

# Brain Resolver imports
from app.brain.contracts import (
    CONTRACT_VERSION,
    AssessmentContext as ResolverAssessmentContext,
    RoutingConstraints as ResolverRoutingConstraints,
    ProtocolIntents as ResolverProtocolIntents,
    ProtocolIntentItem,
    ResolverInput,
    ResolverOutput,
    empty_routing_constraints,
    empty_protocol_intents,
)
from app.brain.resolver import resolve_all, compute_hash as resolver_compute_hash
from app.brain.mocks import bloodwork_mock, lifestyle_mock, goals_mock

# Painpoints and Lifestyle Schema imports (v3.15.1 - Issue #2)
from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA

# Catalog Governance imports (v3.12.0)
from app.catalog.admin import router as catalog_router

# Routing Layer imports (v3.13.0)
from app.routing.admin import router as routing_router

# Matching Layer imports (v3.14.0)
from app.matching.admin import router as matching_router

# Explainability Layer imports (v3.15.0)
from app.explainability.admin import router as explainability_router

# Telemetry Admin imports (v3.16.0 - Issue #9)
from app.telemetry.admin import router as telemetry_router

# Product Intake System imports (v3.18.0)
from app.intake.admin import router as intake_router

# Safety Gate imports (v3.19.0)
from app.brain.safety_admin import router as safety_router

# QA Audit imports (v3.20.0)
from app.qa import qa_router

# Excel Override imports (v3.21.0)
from app.catalog.override import router as override_router

# Constraint Translator Admin imports (v3.32.0 - Issue #16)
from app.brain.constraint_admin import router as constraint_router

# Telemetry Emitter imports (v3.17.0 - Issue #9 Stage 2)
from app.telemetry import get_emitter, derive_run_summary, derive_events

# Bloodwork Handoff imports (v3.29.0 - orchestrate/v2 bloodwork_input support)
from app.brain.orchestrate_v2_bloodwork import (
    BloodworkInputV2,
    MarkerInput,
    orchestrate_with_bloodwork_input,
    build_bloodwork_error_response
)
from app.brain.bloodwork_handoff import (
    BloodworkHandoffException,
    BloodworkHandoffError
)

API_VERSION = "3.34.0"

# Stable UUID for anonymous users - used when user_id is not provided
# This allows protocol_runs to comply with NOT NULL constraint while tracking anonymous sessions
# Stored as string for psycopg2 compatibility
ANONYMOUS_USER_UUID = "00000000-0000-0000-0000-000000000000"

app = FastAPI(title="GenoMAX² API", description="Gender-Optimized Biological Operating System", version=API_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://genomax2-frontend.vercel.app", "https://genomax2-frontend-git-main-hemis-projects-6782105b.vercel.app", "http://localhost:3000", "http://127.0.0.1:3000", "*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Register Catalog Governance admin router (v3.12.0)
app.include_router(catalog_router)

# Register Routing Layer router (v3.13.0)
app.include_router(routing_router)

# Register Matching Layer router (v3.14.0)
app.include_router(matching_router)

# Register Explainability Layer router (v3.15.0)
app.include_router(explainability_router)

# Register Telemetry Admin router (v3.16.0 - Issue #9)
app.include_router(telemetry_router)

# Register Product Intake System router (v3.18.0)
app.include_router(intake_router)

# Register Safety Gate router (v3.19.0)
app.include_router(safety_router)

# Register QA Audit router (v3.20.0)
app.include_router(qa_router)

# Register Excel Override router (v3.21.0)
app.include_router(override_router)

# Register Constraint Translator Admin router (v3.32.0 - Issue #16)
app.include_router(constraint_router)
print("Constraint Translator endpoints registered successfully (Issue #16)")

DATABASE_URL = os.getenv("DATABASE_URL")

# Initialize telemetry emitter (v3.17.0)
_telemetry = get_emitter()

SUPPLIER_STATUS_ACTIVE = "ACTIVE"
SUPPLIER_STATUS_UNKNOWN = "UNKNOWN"
SUPPLIER_STATUS_DISCONTINUING = "DISCONTINUING_SOON"
SUPPLIER_STATUS_INACTIVE = "INACTIVE"
SUPPLIER_STATUS_STRICT = [SUPPLIER_STATUS_ACTIVE, SUPPLIER_STATUS_UNKNOWN]
SUPPLIER_STATUS_FALLBACK = [SUPPLIER_STATUS_ACTIVE, SUPPLIER_STATUS_UNKNOWN, SUPPLIER_STATUS_DISCONTINUING]


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def parse_jsonb(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict) or isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    try:
        return json.loads(str(value))
    except:
        return value


class GateStatus(str, Enum):
    ALLOWED = "allowed"
    BLOCKED = "blocked"
    CAUTION = "caution"


class GenoMAXStatus(str, Enum):
    OPTIMAL = "optimal"
    SUBOPTIMAL_LOW = "suboptimal_low"
    SUBOPTIMAL_HIGH = "suboptimal_high"
    OUT_OF_RANGE_LOW = "out_of_range_low"
    OUT_OF_RANGE_HIGH = "out_of_range_high"
    UNKNOWN = "unknown"


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ProcessedBiomarker(BaseModel):
    canonical_name: str
    original_name: str
    value: float
    original_value: float
    unit: str
    original_unit: str
    status: GenoMAXStatus
    severity: int = Field(ge=0, le=100)
    optimal_range: tuple
    confidence: Confidence
    flags: List[str] = Field(default_factory=list)
    context_applied: Dict[str, Any] = Field(default_factory=dict)


class TargetScore(BaseModel):
    target_id: str
    score: float = Field(ge=0, le=100)
    contributing_biomarkers: List[str]
    primary_biomarker: Optional[str] = None
    flags: List[str] = Field(default_factory=list)


class SupplementGate(BaseModel):
    target_id: str
    gate_status: GateStatus
    reason: str
    blocking_biomarkers: List[str] = Field(default_factory=list)
    caution_biomarkers: List[str] = Field(default_factory=list)


class GlobalFlag(BaseModel):
    flag_id: str
    severity: str
    message: str
    triggered_by: List[str] = Field(default_factory=list)


class SignalAudit(BaseModel):
    engine_version: str
    generated_at: str
    input_hash: str
    output_hash: str
    processing_time_ms: int = 0


class BloodworkSignalV1(BaseModel):
    schema_version: str = "1.1"
    signal_id: str
    user_id: str
    context: Dict[str, Any]
    biomarkers: Dict[str, ProcessedBiomarker]
    target_scores: Dict[str, TargetScore]
    supplement_gates: Dict[str, SupplementGate]
    global_flags: List[GlobalFlag] = Field(default_factory=list)
    audit: SignalAudit

    @field_validator('schema_version')
    @classmethod
    def validate_version(cls, v: str) -> str:
        if v != "1.1":
            raise ValueError(f"Invalid schema version: {v}. Expected 1.1")
        return v


class MarkerInputModel(BaseModel):
    """Single biomarker input for orchestrate/v2."""
    code: str = Field(..., description="Marker code (e.g., 'ferritin', 'vitamin_d')")
    value: float = Field(..., description="Numeric value")
    unit: str = Field(..., description="Unit of measurement (e.g., 'ng/mL')")


class BloodworkInputModel(BaseModel):
    """Raw bloodwork input - sends markers to Bloodwork Engine."""
    markers: List[MarkerInputModel] = Field(..., min_length=1, description="Array of biomarker readings")
    lab_profile: str = Field(default="GLOBAL_CONSERVATIVE", description="Lab profile for reference ranges")
    sex: Optional[str] = Field(default=None, description="Biological sex (male/female)")
    age: Optional[int] = Field(default=None, ge=0, le=150, description="Age in years")


class OrchestrateInputV2(BaseModel):
    """
    Orchestrate V2 request supporting two modes:
    1. bloodwork_input (RECOMMENDED): Raw markers sent to Bloodwork Engine
    2. bloodwork_signal (LEGACY): Pre-computed bloodwork signal
    
    If bloodwork_input is provided, it takes precedence.
    """
    bloodwork_signal: Optional[BloodworkSignalV1] = Field(default=None, description="Pre-computed bloodwork signal (legacy mode)")
    bloodwork_input: Optional[BloodworkInputModel] = Field(default=None, description="Raw markers to send to Bloodwork Engine (recommended)")
    verify_hash: bool = True
    selected_goals: List[str] = Field(default_factory=list)
    assessment_context: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('assessment_context')
    @classmethod
    def reject_raw_markers(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        forbidden_keys = {'markers', 'biomarkers', 'labs', 'lab_results', 'blood_values'}
        found = forbidden_keys.intersection(set(v.keys()))
        if found:
            raise ValueError(f"Raw marker data detected: {found}. Use bloodwork_signal or bloodwork_input.")
        return v


class OrchestrateOutputV2(BaseModel):
    run_id: str
    signal_id: str
    signal_hash: str
    hash_verified: bool
    routing_constraints: Dict[str, Any]
    blocked_targets: List[str]
    caution_targets: List[str]
    assessment_context: Dict[str, Any]
    selected_goals: List[str]
    chain_of_custody: List[Dict[str, Any]]
    next_phase: str = "compose"


class IntakeCreate(BaseModel):
    gender_os: str
    age: int
    health_goals: List[str]
    current_supplements: Optional[List[str]] = []
    medications: Optional[List[str]] = []
    allergies: Optional[List[str]] = []


class BloodworkInput(BaseModel):
    user_id: str
    markers: Dict[str, float]
    lab_source: Optional[str] = None
    test_date: Optional[str] = None


class OrchestrateRequest(BaseModel):
    user_id: str
    signal_data: Dict[str, Any]
    signal_hash: Optional[str] = None


class ComposeRequest(BaseModel):
    run_id: str
    selected_goals: List[str]


class RouteRequest(BaseModel):
    protocol_id: str
    protocol_intents: Dict[str, Any]
    routing_constraints: Dict[str, Any]
    allow_discontinuing_fallback: bool = True


class SKUItem(BaseModel):
    sku: str
    intent_id: str
    target_id: str
    shopify_store: str
    shopify_handle: str
    reason_codes: List[str] = Field(default_factory=list)


class SKUPlan(BaseModel):
    items: List[SKUItem] = Field(default_factory=list)


class SkippedIntent(BaseModel):
    intent_id: str
    reason: str
    reason_codes: List[str] = Field(default_factory=list)
    details: Optional[Dict[str, Any]] = None


class ResolveRequest(BaseModel):
    protocol_id: Optional[str] = Field(None)
    run_id: Optional[str] = Field(None)
    assessment_context: Optional[Dict[str, Any]] = Field(None)
    bloodwork_constraints: Optional[Dict[str, Any]] = Field(None)
    lifestyle_constraints: Optional[Dict[str, Any]] = Field(None)
    raw_goals: List[str] = Field(default_factory=list)
    raw_painpoints: List[str] = Field(default_factory=list)
    goals_intents: Optional[Dict[str, Any]] = Field(None)
    painpoint_intents: Optional[Dict[str, Any]] = Field(None)
    use_mocks: bool = Field(default=True)


def compute_hash(data: Any) -> str:
    json_str = json.dumps(data, sort_keys=True, default=str)
    return f"sha256:{hashlib.sha256(json_str.encode()).hexdigest()}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def verify_signal_hash(signal: BloodworkSignalV1) -> bool:
    signal_dict = signal.model_dump()
    signal_dict["audit"]["output_hash"] = ""
    canonical = json.dumps(signal_dict, sort_keys=True, default=str)
    expected = f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"
    return signal.audit.output_hash == expected


def build_routing_constraints_from_gates(signal: BloodworkSignalV1) -> Dict[str, Any]:
    constraints = {"blocked_targets": [], "caution_targets": [], "allowed_targets": [], "target_details": {}}
    for target_id, gate in signal.supplement_gates.items():
        detail = {"gate_status": gate.gate_status.value, "reason": gate.reason, "blocking_biomarkers": gate.blocking_biomarkers, "caution_biomarkers": gate.caution_biomarkers}
        constraints["target_details"][target_id] = detail
        if gate.gate_status == GateStatus.BLOCKED:
            constraints["blocked_targets"].append(target_id)
        elif gate.gate_status == GateStatus.CAUTION:
            constraints["caution_targets"].append(target_id)
        else:
            constraints["allowed_targets"].append(target_id)
    constraints["global_flags"] = [{"flag_id": f.flag_id, "severity": f.severity, "message": f.message} for f in signal.global_flags]
    critical_flags = [f for f in signal.global_flags if f.severity == "critical"]
    constraints["has_critical_flags"] = len(critical_flags) > 0
    if critical_flags:
        constraints["critical_flag_ids"] = [f.flag_id for f in critical_flags]
    return constraints


GOAL_INTENT_MAP = {
    "sleep": {"lifestyle": [{"intent_id": "improve_sleep_quality", "base_priority": 0.85, "depends_on": []}, {"intent_id": "regulate_circadian_rhythm", "base_priority": 0.70, "depends_on": []}], "nutrition": [{"intent_id": "evening_carb_timing", "base_priority": 0.60, "depends_on": []}], "supplements": [{"intent_id": "magnesium_for_sleep", "base_priority": 0.80, "depends_on": ["magnesium"]}, {"intent_id": "glycine_for_sleep", "base_priority": 0.65, "depends_on": ["glycine"]}]},
    "energy": {"lifestyle": [{"intent_id": "optimize_energy_levels", "base_priority": 0.85, "depends_on": []}, {"intent_id": "morning_light_exposure", "base_priority": 0.70, "depends_on": []}], "nutrition": [{"intent_id": "blood_sugar_stability", "base_priority": 0.75, "depends_on": []}], "supplements": [{"intent_id": "b12_energy_support", "base_priority": 0.80, "depends_on": ["vitamin_b12"]}, {"intent_id": "coq10_cellular_energy", "base_priority": 0.70, "depends_on": ["coq10"]}, {"intent_id": "iron_energy_support", "base_priority": 0.75, "depends_on": ["iron"]}]},
    "stress": {"lifestyle": [{"intent_id": "reduce_stress_response", "base_priority": 0.85, "depends_on": []}, {"intent_id": "breathwork_practice", "base_priority": 0.70, "depends_on": []}], "nutrition": [{"intent_id": "anti_stress_nutrition", "base_priority": 0.65, "depends_on": []}], "supplements": [{"intent_id": "magnesium_stress_support", "base_priority": 0.80, "depends_on": ["magnesium"]}, {"intent_id": "adaptogen_support", "base_priority": 0.70, "depends_on": ["adaptogen"]}]},
    "focus": {"lifestyle": [{"intent_id": "enhance_cognitive_function", "base_priority": 0.85, "depends_on": []}, {"intent_id": "attention_training", "base_priority": 0.65, "depends_on": []}], "nutrition": [{"intent_id": "brain_fuel_optimization", "base_priority": 0.70, "depends_on": []}], "supplements": [{"intent_id": "omega3_brain_support", "base_priority": 0.80, "depends_on": ["omega3"]}, {"intent_id": "lions_mane_cognition", "base_priority": 0.65, "depends_on": ["lions_mane"]}]},
    "immunity": {"lifestyle": [{"intent_id": "immune_resilience", "base_priority": 0.80, "depends_on": []}], "nutrition": [{"intent_id": "immune_nutrition", "base_priority": 0.70, "depends_on": []}], "supplements": [{"intent_id": "vitamin_d_immune", "base_priority": 0.85, "depends_on": ["vitamin_d"]}, {"intent_id": "zinc_immune_support", "base_priority": 0.75, "depends_on": ["zinc"]}, {"intent_id": "vitamin_c_immune", "base_priority": 0.70, "depends_on": ["vitamin_c"]}]},
    "heart": {"lifestyle": [{"intent_id": "cardiovascular_health", "base_priority": 0.85, "depends_on": []}], "nutrition": [{"intent_id": "heart_healthy_diet", "base_priority": 0.80, "depends_on": []}], "supplements": [{"intent_id": "omega3_cardiovascular", "base_priority": 0.85, "depends_on": ["omega3"]}, {"intent_id": "coq10_heart_support", "base_priority": 0.75, "depends_on": ["coq10"]}, {"intent_id": "magnesium_heart", "base_priority": 0.70, "depends_on": ["magnesium"]}]},
    "gut": {"lifestyle": [{"intent_id": "gut_health_optimization", "base_priority": 0.80, "depends_on": []}], "nutrition": [{"intent_id": "fiber_diversity", "base_priority": 0.75, "depends_on": []}, {"intent_id": "fermented_foods", "base_priority": 0.70, "depends_on": []}], "supplements": [{"intent_id": "probiotic_support", "base_priority": 0.80, "depends_on": ["probiotic"]}, {"intent_id": "digestive_enzyme_support", "base_priority": 0.65, "depends_on": ["digestive_enzyme"]}]},
    "inflammation": {"lifestyle": [{"intent_id": "reduce_systemic_inflammation", "base_priority": 0.85, "depends_on": []}], "nutrition": [{"intent_id": "anti_inflammatory_diet", "base_priority": 0.80, "depends_on": []}], "supplements": [{"intent_id": "omega3_antiinflammatory", "base_priority": 0.85, "depends_on": ["omega3"]}, {"intent_id": "curcumin_inflammation", "base_priority": 0.75, "depends_on": ["curcumin"]}]},
    "liver": {"lifestyle": [{"intent_id": "liver_health_support", "base_priority": 0.85, "depends_on": []}], "nutrition": [{"intent_id": "liver_supportive_diet", "base_priority": 0.80, "depends_on": []}], "supplements": [{"intent_id": "milk_thistle_liver", "base_priority": 0.75, "depends_on": ["milk_thistle", "hepatotoxic"]}, {"intent_id": "nac_liver_support", "base_priority": 0.70, "depends_on": ["nac"]}]},
    "cognitive": {"lifestyle": [{"intent_id": "cognitive_optimization", "base_priority": 0.85, "depends_on": []}], "nutrition": [{"intent_id": "brain_nutrition", "base_priority": 0.75, "depends_on": []}], "supplements": [{"intent_id": "omega3_brain_support", "base_priority": 0.85, "depends_on": ["omega3"]}, {"intent_id": "lions_mane_cognition", "base_priority": 0.70, "depends_on": ["lions_mane"]}]}
}

ROUTING_RULES = {
    "ferritin": {"high_threshold": 300, "constraint": {"ingredient_class": "iron", "constraint_type": "blocked", "reason": "Ferritin elevated ({value} ng/mL). Iron contraindicated.", "severity": "hard"}},
    "alt": {"high_threshold": 50, "constraints": [{"ingredient_class": "hepatotoxic", "constraint_type": "blocked", "reason": "ALT elevated ({value} U/L). Hepatotoxic supplements blocked.", "severity": "hard"}, {"ingredient_class": "kava", "constraint_type": "blocked", "reason": "ALT elevated ({value} U/L). Kava contraindicated.", "severity": "hard"}]},
    "potassium": {"high_threshold": 5.0, "constraint": {"ingredient_class": "potassium", "constraint_type": "blocked", "reason": "Potassium elevated ({value} mEq/L). Supplementation contraindicated.", "severity": "hard"}},
    "vitamin_d": {"low_threshold": 30, "constraint": {"ingredient_class": "vitamin_d", "constraint_type": "required", "reason": "Vitamin D deficient ({value} ng/mL). Supplementation recommended.", "severity": "soft"}},
    "b12": {"low_threshold": 400, "constraint": {"ingredient_class": "vitamin_b12", "constraint_type": "required", "reason": "B12 suboptimal ({value} pg/mL). Supplementation recommended.", "severity": "soft"}}
}

INTENT_CATALOG = {
    "magnesium_for_sleep": {"must_have_tags": ["magnesium"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "glycine_for_sleep": {"must_have_tags": ["gaba-oral", "l-theanine"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "omega3_brain_support": {"must_have_tags": ["omega-3-epa"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "omega3_cardiovascular": {"must_have_tags": ["omega-3-epa"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "omega3_antiinflammatory": {"must_have_tags": ["omega-3-epa"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "iron_energy_support": {"must_have_tags": ["iron-when-deficient"], "blocked_by_targets": ["iron_boost", "ferritin_elevated"], "caution_by_targets": [], "max_modules": 1},
    "b12_energy_support": {"must_have_tags": ["niacin-vitamin-b3"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "coq10_cellular_energy": {"must_have_tags": ["coq10-ubiquinone", "ubiquinol"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "coq10_heart_support": {"must_have_tags": ["coq10-ubiquinone", "ubiquinol"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "magnesium_stress_support": {"must_have_tags": ["magnesium"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "magnesium_heart": {"must_have_tags": ["magnesium"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "adaptogen_support": {"must_have_tags": ["holy-basil-tulsi", "panax-ginseng"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "lions_mane_cognition": {"must_have_tags": ["lion-s-mane-hericium-erinaceus"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "vitamin_d_immune": {"must_have_tags": ["vitamin-d3"], "blocked_by_targets": ["hypercalcemia"], "caution_by_targets": ["high_dose_vitamin_d"], "max_modules": 1},
    "zinc_immune_support": {"must_have_tags": ["zinc"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "vitamin_c_immune": {"must_have_tags": ["vitamin-c"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "probiotic_support": {"must_have_tags": ["probiotics-multi-strain"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "digestive_enzyme_support": {"must_have_tags": ["digestive-enzymes"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "curcumin_inflammation": {"must_have_tags": ["curcumin"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "milk_thistle_liver": {"must_have_tags": ["milk-thistle-silymarin"], "blocked_by_targets": ["hepatotoxic"], "caution_by_targets": [], "max_modules": 1},
    "nac_liver_support": {"must_have_tags": ["nac-n-acetyl-cysteine"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "creatine_performance": {"must_have_tags": ["creatine-monohydrate"], "blocked_by_targets": ["kidney_impairment"], "caution_by_targets": [], "max_modules": 1},
    "protein_muscle_support": {"must_have_tags": ["whey-protein"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "collagen_joint_support": {"must_have_tags": ["collagen-peptides"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "berberine_glucose": {"must_have_tags": ["berberine"], "blocked_by_targets": [], "caution_by_targets": ["metformin_interaction"], "max_modules": 1},
    "fiber_gut_health": {"must_have_tags": ["psyllium-husk"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1},
    "melatonin_sleep": {"must_have_tags": ["melatonin"], "blocked_by_targets": [], "caution_by_targets": [], "max_modules": 1}
}


def derive_os_environment(assessment_context: Dict[str, Any]) -> Optional[str]:
    gender = assessment_context.get("gender", "").lower()
    if gender == "male":
        return "MAXimo²"
    elif gender == "female":
        return "MAXima²"
    return None


def is_blocked_by_target(intent_spec: Dict, blocked_targets: List[str]) -> bool:
    return any(t in blocked_targets for t in intent_spec.get("blocked_by_targets", []))


def has_caution_target(intent_spec: Dict, caution_targets: List[str]) -> bool:
    return any(t in caution_targets for t in intent_spec.get("caution_by_targets", []))


def build_module_query(os_env: str, must_have_tags: List[str], blocked_ingredients: List[str], max_modules: int = 1) -> tuple:
    query_parts = ["SELECT module_code, product_name, os_environment, os_layer, biological_domain, ingredient_tags, shopify_store, shopify_handle FROM os_modules WHERE os_environment = %s"]
    params = [os_env]
    if must_have_tags:
        tag_conditions = []
        for tag in must_have_tags:
            tag_conditions.append("ingredient_tags ILIKE %s")
            params.append(f"%'{tag}'%")
        query_parts.append(f"AND ({' OR '.join(tag_conditions)})")
    for blocked in blocked_ingredients:
        query_parts.append("AND ingredient_tags NOT ILIKE %s")
        params.append(f"%'{blocked}'%")
    query_parts.append("ORDER BY CASE os_layer WHEN 'Core' THEN 1 WHEN 'Adaptive' THEN 2 ELSE 3 END, LENGTH(ingredient_tags) - LENGTH(REPLACE(ingredient_tags, ',', '')), module_code LIMIT %s")
    params.append(max_modules)
    return " ".join(query_parts), tuple(params)


def derive_routing_constraints(markers: Dict[str, float]) -> List[Dict[str, Any]]:
    constraints = []
    for marker, value in markers.items():
        marker_lower = marker.lower()
        if marker_lower not in ROUTING_RULES:
            continue
        rule = ROUTING_RULES[marker_lower]
        if "high_threshold" in rule and value > rule["high_threshold"]:
            if "constraint" in rule:
                constraint = rule["constraint"].copy()
                constraint["reason"] = constraint["reason"].format(value=value)
                constraint["source_marker"] = marker_lower
                constraint["source_value"] = value
                constraints.append(constraint)
            if "constraints" in rule:
                for c in rule["constraints"]:
                    constraint = c.copy()
                    constraint["reason"] = constraint["reason"].format(value=value)
                    constraint["source_marker"] = marker_lower
                    constraint["source_value"] = value
                    constraints.append(constraint)
        if "low_threshold" in rule and value < rule["low_threshold"]:
            if "constraint" in rule:
                constraint = rule["constraint"].copy()
                constraint["reason"] = constraint["reason"].format(value=value)
                constraint["source_marker"] = marker_lower
                constraint["source_value"] = value
                constraints.append(constraint)
    return constraints


def build_assessment_context(user_id: str, signal_data: Dict[str, Any]) -> Dict[str, Any]:
    markers = signal_data.get("markers", {})
    gender = signal_data.get("gender", "unknown")
    deficient, suboptimal, optimal, elevated = [], [], [], []
    thresholds = {"ferritin": {"low": 30, "optimal_low": 50, "optimal_high": 200, "high": 300, "unit": "ng/mL"}, "vitamin_d": {"low": 20, "optimal_low": 40, "optimal_high": 60, "high": 100, "unit": "ng/mL"}, "b12": {"low": 200, "optimal_low": 500, "optimal_high": 900, "high": 1500, "unit": "pg/mL"}, "alt": {"low": 0, "optimal_low": 7, "optimal_high": 40, "high": 50, "unit": "U/L"}, "potassium": {"low": 3.5, "optimal_low": 3.8, "optimal_high": 4.8, "high": 5.0, "unit": "mEq/L"}}
    for marker, value in markers.items():
        marker_lower = marker.lower()
        thresh = thresholds.get(marker_lower, {"low": 0, "optimal_low": 0, "optimal_high": 999, "high": 999, "unit": ""})
        marker_entry = {"marker": marker_lower, "value": value, "unit": thresh["unit"], "status": "optimal"}
        if value < thresh["low"]:
            marker_entry["status"] = "deficient"
            deficient.append(marker_entry)
        elif value < thresh["optimal_low"]:
            marker_entry["status"] = "suboptimal"
            suboptimal.append(marker_entry)
        elif value > thresh["high"]:
            marker_entry["status"] = "elevated"
            elevated.append(marker_entry)
        elif value > thresh["optimal_high"]:
            marker_entry["status"] = "elevated"
            elevated.append(marker_entry)
        else:
            optimal.append(marker_entry)
    return {"user_id": user_id, "gender": gender, "test_date": signal_data.get("test_date"), "lab_source": signal_data.get("lab_source"), "markers_analyzed": len(markers), "summary": {"deficient_count": len(deficient), "suboptimal_count": len(suboptimal), "optimal_count": len(optimal), "elevated_count": len(elevated)}, "deficient": deficient, "suboptimal": suboptimal, "optimal": optimal, "elevated": elevated}


def get_blocked_ingredient_classes(routing_constraints: Any) -> set:
    blocked = set()
    if isinstance(routing_constraints, dict):
        for target_id, detail in routing_constraints.get("target_details", {}).items():
            if detail.get("gate_status") == "blocked":
                blocked.add(target_id)
        return blocked
    if isinstance(routing_constraints, list):
        for constraint in routing_constraints:
            if isinstance(constraint, dict) and constraint.get("constraint_type") == "blocked":
                blocked.add(constraint.get("ingredient_class"))
    return blocked


def is_intent_blocked(intent: Dict, blocked_classes: set) -> bool:
    for dep in intent.get("depends_on", []):
        if dep in blocked_classes:
            return True
    return False


def calculate_priority(base_priority: float, assessment_context: Dict, intent_id: str) -> float:
    priority = base_priority
    deficient_markers = [m["marker"] for m in assessment_context.get("deficient", [])]
    suboptimal_markers = [m["marker"] for m in assessment_context.get("suboptimal", [])]
    if "b12" in intent_id and ("b12" in deficient_markers or "b12" in suboptimal_markers):
        priority = min(1.0, priority + 0.15)
    if "vitamin_d" in intent_id and ("vitamin_d" in deficient_markers or "vitamin_d" in suboptimal_markers):
        priority = min(1.0, priority + 0.15)
    if "iron" in intent_id and ("ferritin" in deficient_markers or "ferritin" in suboptimal_markers):
        priority = min(1.0, priority + 0.15)
    return round(priority, 2)


def compose_intents(selected_goals: List[str], routing_constraints: Any, assessment_context: Dict) -> Dict[str, List[Dict]]:
    blocked_classes = get_blocked_ingredient_classes(routing_constraints)
    protocol_intents = {"lifestyle": [], "nutrition": [], "supplements": []}
    seen_intents = set()
    for goal in selected_goals:
        goal_lower = goal.lower()
        if goal_lower not in GOAL_INTENT_MAP:
            continue
        goal_intents = GOAL_INTENT_MAP[goal_lower]
        for category in ["lifestyle", "nutrition", "supplements"]:
            for intent in goal_intents.get(category, []):
                intent_id = intent["intent_id"]
                if intent_id in seen_intents or is_intent_blocked(intent, blocked_classes):
                    continue
                priority = calculate_priority(intent["base_priority"], assessment_context, intent_id)
                protocol_intents[category].append({"intent_id": intent_id, "source_goal": goal_lower, "priority": priority, "blocked": False})
                seen_intents.add(intent_id)
    for category in protocol_intents:
        protocol_intents[category].sort(key=lambda x: x["priority"], reverse=True)
    return protocol_intents


# ===== TELEMETRY HELPERS (v3.17.0) =====

def _emit_telemetry_for_phase(
    run_id: str,
    phase: str,
    request_dict: Dict[str, Any],
    response_dict: Dict[str, Any],
    has_bloodwork: bool = False,
):
    """
    Emit telemetry for a Brain phase.
    Called at end of successful request handlers.
    Telemetry errors never break requests (fail-safe).
    """
    try:
        # Derive summary from request/response
        summary = derive_run_summary(request_dict, response_dict, phase)
        
        # Extract sex and age from assessment context
        ctx = response_dict.get("assessment_context") or request_dict.get("assessment_context") or {}
        if hasattr(ctx, "model_dump"):
            ctx = ctx.model_dump()
        sex = ctx.get("sex") or ctx.get("gender")
        age = ctx.get("age")
        
        # Start telemetry run
        _telemetry.start_run(
            run_id=run_id,
            sex=sex,
            age=age,
            has_bloodwork=has_bloodwork or summary.has_bloodwork,
            api_version=API_VERSION,
        )
        
        # Complete run with aggregates
        _telemetry.complete_run(
            run_id=run_id,
            intents_count=summary.intents_count,
            matched_items_count=summary.matched_items_count,
            unmatched_intents_count=summary.unmatched_intents_count,
            blocked_skus_count=summary.blocked_skus_count,
            auto_blocked_skus_count=summary.auto_blocked_skus_count,
            caution_flags_count=summary.caution_flags_count,
            confidence_level=summary.confidence_level,
        )
        
        # Derive and emit events
        events = derive_events(response_dict, phase)
        for event in events:
            if event.event_type == "ROUTING_BLOCK":
                _telemetry.emit_routing_block(run_id, event.code, event.count)
            elif event.event_type == "MATCHING_UNMATCHED_INTENT":
                _telemetry.emit_unmatched_intent(run_id, event.code, event.count)
            elif event.event_type == "LOW_CONFIDENCE":
                _telemetry.emit_low_confidence(run_id, event.code, event.count)
    except Exception as e:
        # Telemetry must never break the request
        print(f"[Telemetry] Emission error in {phase}: {e}")


# ===== ENDPOINTS =====

@app.get("/")
def root():
    return {"service": "GenoMAX² API", "version": API_VERSION, "status": "operational"}


@app.get("/health")
def health():
    return {"status": "healthy", "version": API_VERSION}


@app.get("/version")
def version():
    return {
        "api_version": API_VERSION,
        "brain_version": "1.5.0",
        "brain_orchestrator_version": "1.0.0",
        "resolver_version": "1.0.0",
        "bloodwork_engine_version": "2.0.0",
        "constraint_translator_version": "1.0.0",
        "catalog_version": "catalog_governance_v1",
        "routing_version": "routing_layer_v1",
        "matching_version": "matching_layer_v1",
        "explainability_version": "explainability_v1",
        "telemetry_version": "telemetry_instrumented_v1",
        "intake_version": "intake_system_v1",
        "safety_gate_version": "safety_gate_v1",
        "qa_audit_version": "qa_audit_v1",
        "override_version": "excel_override_v1",
        "launch_v1_version": "launch_enforcement_v1",
        "contract_version": CONTRACT_VERSION,
        "features": ["orchestrate", "orchestrate_v2", "orchestrate_v2_bloodwork_input", "compose", "route", "resolve", "supplier-gating", "catalog-governance", "routing-layer", "matching-layer", "explainability", "painpoints", "lifestyle-schema", "telemetry", "telemetry-instrumented", "intake-system", "safety-gate", "qa-audit", "excel-override", "launch-v1-enforcement", "bloodwork-engine-v2", "constraint-translator", "brain-pipeline"]
    }


@app.get("/api/v1/brain/health")
def brain_health():
    return {"status": "healthy", "service": "brain", "version": "1.5.0", "resolver_version": "1.0.0", "contract_version": CONTRACT_VERSION}


# ===== PAINPOINTS AND LIFESTYLE SCHEMA ENDPOINTS (v3.15.1 - Issue #2) =====

@app.get("/api/v1/brain/painpoints")
def get_painpoints():
    return {
        "status": "success",
        "version": "1.0.0",
        "painpoints": PAINPOINTS_DICTIONARY,
        "count": len(PAINPOINTS_DICTIONARY)
    }


@app.get("/api/v1/brain/lifestyle-schema")
def get_lifestyle_schema():
    return {
        "status": "success",
        "version": "1.0.0",
        "schema": LIFESTYLE_SCHEMA,
        "question_count": len(LIFESTYLE_SCHEMA.get("questions", []))
    }


# ===== MIGRATION AND DEBUG ENDPOINTS =====

@app.get("/migrate-supplier-status")
def migrate_supplier_status():
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("ALTER TABLE public.os_modules_v3_1 ADD COLUMN IF NOT EXISTS supplier_status TEXT NOT NULL DEFAULT 'UNKNOWN', ADD COLUMN IF NOT EXISTS supplier_status_details TEXT, ADD COLUMN IF NOT EXISTS supplier_last_checked_at TIMESTAMPTZ, ADD COLUMN IF NOT EXISTS supplier_http_status INTEGER, ADD COLUMN IF NOT EXISTS supplier_page_url TEXT; CREATE INDEX IF NOT EXISTS idx_os_modules_v3_1_supplier_status ON public.os_modules_v3_1 (supplier_status);")
        conn.commit()
        cur.execute("CREATE OR REPLACE VIEW public.os_modules AS SELECT * FROM public.os_modules_v3_1;")
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "supplier_status columns added"}
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.get("/migrate-brain-full")
def migrate_brain_full():
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS brain_runs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), user_id UUID, status VARCHAR(20) DEFAULT 'running', input_hash VARCHAR(128), output_hash VARCHAR(128), created_at TIMESTAMPTZ DEFAULT NOW(), completed_at TIMESTAMPTZ); CREATE TABLE IF NOT EXISTS signal_registry (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), user_id UUID NOT NULL, signal_type VARCHAR(50) NOT NULL, signal_hash VARCHAR(128) NOT NULL, signal_json JSONB NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(user_id, signal_type, signal_hash)); CREATE TABLE IF NOT EXISTS decision_outputs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), run_id UUID, phase VARCHAR(30) NOT NULL, output_json JSONB NOT NULL, output_hash VARCHAR(128), created_at TIMESTAMPTZ DEFAULT NOW()); CREATE TABLE IF NOT EXISTS protocol_runs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), user_id UUID NOT NULL, run_id UUID, phase VARCHAR(30) NOT NULL, request_json JSONB, output_json JSONB, output_hash VARCHAR(128), status VARCHAR(20) DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT NOW(), completed_at TIMESTAMPTZ); CREATE TABLE IF NOT EXISTS audit_log (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), entity_type VARCHAR(50) NOT NULL, entity_id UUID, action VARCHAR(30) NOT NULL, actor_id UUID, before_hash VARCHAR(128), after_hash VARCHAR(128), metadata JSONB, created_at TIMESTAMPTZ DEFAULT NOW());")
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "tables": ["brain_runs", "signal_registry", "decision_outputs", "protocol_runs", "audit_log"]}
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.get("/debug/supplier-status")
def debug_supplier_status():
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("SELECT supplier_status, COUNT(*) as count FROM os_modules_v3_1 GROUP BY supplier_status ORDER BY count DESC")
        distribution = {row["supplier_status"]: row["count"] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return {"status_distribution": distribution}
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.post("/api/v1/brain/resolve")
def brain_resolve(request: ResolveRequest):
    created_at = now_iso()
    protocol_id = request.protocol_id or str(uuid.uuid4())
    run_id = request.run_id
    db_context = None
    db_constraints = None
    conn = get_db()
    if conn and (request.protocol_id or request.run_id):
        try:
            cur = conn.cursor()
            if request.protocol_id and not run_id:
                cur.execute("SELECT run_id FROM protocol_runs WHERE id = %s LIMIT 1", (request.protocol_id,))
                row = cur.fetchone()
                if row:
                    run_id = str(row["run_id"])
            if run_id:
                cur.execute("SELECT output_json FROM decision_outputs WHERE run_id = %s AND phase IN ('orchestrate_v2', 'orchestrate') ORDER BY created_at DESC LIMIT 1", (run_id,))
                row = cur.fetchone()
                if row:
                    orchestrate_output = parse_jsonb(row["output_json"])
                    db_context = orchestrate_output.get("assessment_context", {})
                    db_constraints = orchestrate_output.get("routing_constraints", {})
            cur.close()
        except Exception as e:
            print(f"DB load error: {e}")
        finally:
            try: conn.close()
            except: pass
    raw_context = request.assessment_context or db_context or {}
    if not raw_context.get("protocol_id"):
        raw_context["protocol_id"] = protocol_id
    if not raw_context.get("run_id"):
        raw_context["run_id"] = run_id or str(uuid.uuid4())
    if not raw_context.get("gender"):
        raise HTTPException(status_code=422, detail={"error": "MISSING_GENDER", "message": "assessment_context.gender is required"})
    try:
        assessment_context = ResolverAssessmentContext(protocol_id=raw_context.get("protocol_id"), run_id=raw_context.get("run_id"), gender=raw_context.get("gender"), age=raw_context.get("age"), height_cm=raw_context.get("height_cm"), weight_kg=raw_context.get("weight_kg"), meds=raw_context.get("meds", raw_context.get("medications", [])), conditions=raw_context.get("conditions", []), allergies=raw_context.get("allergies", []), flags=raw_context.get("flags", {}))
    except Exception as e:
        raise HTTPException(status_code=422, detail={"error": "INVALID_ASSESSMENT_CONTEXT", "message": str(e)})
    
    has_bloodwork = bool(request.bloodwork_constraints)
    
    if request.use_mocks:
        bloodwork_constraints = bloodwork_mock(assessment_context)
        lifestyle_constraints = lifestyle_mock(assessment_context)
        has_bloodwork = True  # Mocks simulate bloodwork
    else:
        raw_bw = request.bloodwork_constraints or db_constraints or {}
        raw_ls = request.lifestyle_constraints or {}
        bloodwork_constraints = ResolverRoutingConstraints(blocked_targets=raw_bw.get("blocked_targets", []), caution_targets=raw_bw.get("caution_targets", []), allowed_targets=raw_bw.get("allowed_targets", []), blocked_ingredients=raw_bw.get("blocked_ingredients", []), has_critical_flags=raw_bw.get("has_critical_flags", False), global_flags=raw_bw.get("global_flags", []))
        lifestyle_constraints = ResolverRoutingConstraints(blocked_targets=raw_ls.get("blocked_targets", []), caution_targets=raw_ls.get("caution_targets", []), allowed_targets=raw_ls.get("allowed_targets", []), blocked_ingredients=raw_ls.get("blocked_ingredients", []), has_critical_flags=raw_ls.get("has_critical_flags", False), global_flags=raw_ls.get("global_flags", []))
    if request.use_mocks and (request.raw_goals or request.raw_painpoints):
        goals_intents = goals_mock(request.raw_goals, request.raw_painpoints)
        painpoint_intents = empty_protocol_intents()
    elif request.goals_intents or request.painpoint_intents:
        goals_intents = empty_protocol_intents()
        painpoint_intents = empty_protocol_intents()
        if request.goals_intents:
            goals_intents = ResolverProtocolIntents(lifestyle=request.goals_intents.get("lifestyle", []), nutrition=request.goals_intents.get("nutrition", []), supplements=[ProtocolIntentItem(**s) if isinstance(s, dict) else s for s in request.goals_intents.get("supplements", [])])
        if request.painpoint_intents:
            painpoint_intents = ResolverProtocolIntents(lifestyle=request.painpoint_intents.get("lifestyle", []), nutrition=request.painpoint_intents.get("nutrition", []), supplements=[ProtocolIntentItem(**s) if isinstance(s, dict) else s for s in request.painpoint_intents.get("supplements", [])])
    else:
        goals_intents = empty_protocol_intents()
        painpoint_intents = empty_protocol_intents()
    resolver_input = ResolverInput(assessment_context=assessment_context, bloodwork_constraints=bloodwork_constraints, lifestyle_constraints=lifestyle_constraints, raw_goals=request.raw_goals, raw_painpoints=request.raw_painpoints, goals_intents=goals_intents, painpoint_intents=painpoint_intents)
    try:
        output = resolve_all(resolver_input)
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "RESOLVER_ERROR", "message": str(e)})
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO decision_outputs (run_id, phase, output_json, output_hash, created_at) VALUES (%s, 'resolve', %s, %s, NOW())", (output.run_id, json.dumps(output.model_dump(), default=str), output.audit.output_hash))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"DB store error: {e}")
            try: conn.close()
            except: pass
    
    # Build response
    response_dict = {
        "status": "success",
        "phase": "resolve",
        "contract_version": output.contract_version,
        "protocol_id": output.protocol_id,
        "run_id": output.run_id,
        "resolved_constraints": output.resolved_constraints.model_dump(),
        "resolved_intents": output.resolved_intents.model_dump(),
        "assessment_context": output.assessment_context.model_dump(),
        "audit": output.audit.model_dump(),
        "next_phase": "route",
        "created_at": created_at
    }
    
    # Emit telemetry (v3.17.0)
    _emit_telemetry_for_phase(
        run_id=output.run_id,
        phase="resolve",
        request_dict=request.model_dump(),
        response_dict=response_dict,
        has_bloodwork=has_bloodwork,
    )
    
    return response_dict


@app.post("/api/v1/brain/orchestrate/v2", response_model=OrchestrateOutputV2)
def brain_orchestrate_v2(request: OrchestrateInputV2) -> OrchestrateOutputV2:
    """
    Orchestrate V2 endpoint with dual mode support:
    1. bloodwork_input (RECOMMENDED): Raw markers sent to Bloodwork Engine for processing
    2. bloodwork_signal (LEGACY): Pre-computed BloodworkSignalV1
    
    If bloodwork_input is provided, it takes precedence.
    """
    created_at = now_iso()
    
    # Validate at least one input mode is provided
    if not request.bloodwork_input and not request.bloodwork_signal:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "NO_BLOODWORK_DATA", "message": "Either bloodwork_input or bloodwork_signal is required"}
        )
    
    # MODE 1: bloodwork_input - send raw markers to Bloodwork Engine
    if request.bloodwork_input:
        # Generate run_id upfront for consistency
        run_id = str(uuid.uuid4())
        db_conn = None
        
        try:
            # Convert API models to internal BloodworkInputV2
            markers_internal = [
                MarkerInput(code=m.code, value=m.value, unit=m.unit)
                for m in request.bloodwork_input.markers
            ]
            bloodwork_input_v2 = BloodworkInputV2(
                markers=markers_internal,
                lab_profile=request.bloodwork_input.lab_profile,
                sex=request.bloodwork_input.sex or request.assessment_context.get("sex") or request.assessment_context.get("gender"),
                age=request.bloodwork_input.age or request.assessment_context.get("age")
            )
            
            # Get DB connection for persistence (passed to orchestrate function)
            db_conn = get_db()
            
            # Call Bloodwork Engine integration (SYNCHRONOUS - no await!)
            # Correct function signature: bloodwork_input, brain_constraints, run_id, db_conn
            result = orchestrate_with_bloodwork_input(
                bloodwork_input=bloodwork_input_v2,
                brain_constraints=None,  # No existing brain constraints
                run_id=run_id,
                db_conn=db_conn
            )
            
            # Check for success (BloodworkIntegrationResult has success attribute)
            if not result.success:
                error_msg = result.error_message or "Unknown bloodwork integration error"
                error_code = result.error_code or "BLOODWORK_ERROR"
                raise HTTPException(
                    status_code=result.http_code or 500,
                    detail={"error": error_code, "message": error_msg}
                )
            
            # Extract data from BloodworkIntegrationResult
            # result.merged_constraints is Dict[str, List[str]] with keys:
            # blocked_ingredients, blocked_categories, caution_flags, requirements, reason_codes
            merged = result.merged_constraints
            
            # Convert merged_constraints to routing_constraints format expected by response
            routing_constraints = {
                "blocked_targets": merged.get("blocked_ingredients", []) + merged.get("blocked_categories", []),
                "caution_targets": merged.get("caution_flags", []),
                "allowed_targets": [],
                "blocked_ingredients": merged.get("blocked_ingredients", []),
                "requirements": merged.get("requirements", []),
                "reason_codes": merged.get("reason_codes", []),
                "target_details": {},
                "global_flags": [],
                "has_critical_flags": False
            }
            
            # Extract handoff data for signal_id and hashes
            handoff_data = result.handoff.to_dict() if result.handoff else {}
            signal_id = f"bloodwork_input_{run_id}"
            signal_hash = handoff_data.get("audit", {}).get("output_hash", "") or compute_hash({"bloodwork_input": bloodwork_input_v2.model_dump()})
            output_hash = result.persistence_data.get("output_hash", "") if result.persistence_data else compute_hash(routing_constraints)
            hash_verified = True  # Hash verification N/A for bloodwork_input mode
            
            chain_entry = {
                "stage": "brain_orchestrate_v2_bloodwork_input",
                "engine": "brain_1.5.0",
                "bloodwork_engine": "2.0.0",
                "timestamp": created_at,
                "input_mode": "bloodwork_input",
                "output_hash": output_hash
            }
            
            # Persist to brain_runs and decision_outputs
            # Note: orchestrate_with_bloodwork_input already persists to decision_outputs
            # We just need to add brain_runs entry
            if db_conn:
                try:
                    cur = db_conn.cursor()
                    output_data = {
                        "run_id": run_id,
                        "signal_id": signal_id,
                        "input_mode": "bloodwork_input",
                        "routing_constraints": routing_constraints,
                        "selected_goals": request.selected_goals,
                        "assessment_context": request.assessment_context,
                        "handoff": handoff_data
                    }
                    # FIX v3.29.3: Use None for missing user_id instead of "anonymous" string
                    # brain_runs.user_id is UUID type - cannot accept string values
                    user_id_raw = request.assessment_context.get("user_id")
                    user_id_for_db = user_id_raw if user_id_raw else None
                    cur.execute(
                        "INSERT INTO brain_runs (id, user_id, status, input_hash, output_hash, created_at) VALUES (%s, %s, %s, %s, %s, NOW())",
                        (run_id, user_id_for_db, "completed", signal_hash, output_hash)
                    )
                    cur.execute(
                        "INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, %s, %s, %s)",
                        (run_id, "orchestrate_v2", json.dumps(output_data, default=str), output_hash)
                    )
                    db_conn.commit()
                    cur.close()
                except Exception as e:
                    print(f"[orchestrate_v2] DB persist error: {e}")
                finally:
                    try: db_conn.close()
                    except: pass
                    db_conn = None
            
            response = OrchestrateOutputV2(
                run_id=run_id,
                signal_id=signal_id,
                signal_hash=signal_hash,
                hash_verified=hash_verified,
                routing_constraints=routing_constraints,
                blocked_targets=routing_constraints.get("blocked_targets", []),
                caution_targets=routing_constraints.get("caution_targets", []),
                assessment_context=request.assessment_context,
                selected_goals=request.selected_goals,
                chain_of_custody=[chain_entry],
                next_phase="compose"
            )
            
            # Emit telemetry
            _emit_telemetry_for_phase(
                run_id=run_id,
                phase="orchestrate_v2",
                request_dict={"bloodwork_input": True, "markers_count": len(request.bloodwork_input.markers), "selected_goals": request.selected_goals},
                response_dict=response.model_dump(),
                has_bloodwork=True,
            )
            
            return response
            
        except BloodworkHandoffException as e:
            # STRICT MODE: Bloodwork Engine failures are hard aborts
            # Close DB connection if open
            if db_conn:
                try: db_conn.close()
                except: pass
            
            error_response = build_bloodwork_error_response(e)
            # FIX: Use e.error_code (not e.error_type)
            if e.error_code == BloodworkHandoffError.BLOODWORK_UNAVAILABLE:
                raise HTTPException(status_code=503, detail=error_response)
            # FIX: Use BLOODWORK_INVALID_HANDOFF (not INVALID_HANDOFF)
            elif e.error_code == BloodworkHandoffError.BLOODWORK_INVALID_HANDOFF:
                raise HTTPException(status_code=502, detail=error_response)
            else:
                raise HTTPException(status_code=500, detail=error_response)
        except HTTPException:
            # Re-raise HTTP exceptions
            if db_conn:
                try: db_conn.close()
                except: pass
            raise
        except Exception as e:
            # Catch-all for unexpected errors
            if db_conn:
                try: db_conn.close()
                except: pass
            raise HTTPException(
                status_code=500,
                detail={"error": "BLOODWORK_INPUT_ERROR", "message": str(e)}
            )
    
    # MODE 2: bloodwork_signal (legacy) - use pre-computed signal
    signal = request.bloodwork_signal
    hash_verified = True if not request.verify_hash else verify_signal_hash(signal)
    if request.verify_hash and not hash_verified:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "SIGNAL_HASH_MISMATCH"})
    if signal.schema_version != "1.1":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "INVALID_SCHEMA_VERSION"})
    routing_constraints = build_routing_constraints_from_gates(signal)
    run_id = str(uuid.uuid4())
    output_data = {"run_id": run_id, "signal_id": signal.signal_id, "input_mode": "bloodwork_signal", "routing_constraints": routing_constraints, "selected_goals": request.selected_goals, "assessment_context": request.assessment_context}
    output_hash = compute_hash(output_data)
    chain_entry = {"stage": "brain_orchestrate_v2", "engine": "brain_1.5.0", "timestamp": created_at, "input_mode": "bloodwork_signal", "input_hashes": [signal.audit.output_hash], "output_hash": output_hash}
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO brain_runs (id, user_id, status, input_hash, output_hash, created_at) VALUES (%s, %s, %s, %s, %s, NOW())", (run_id, signal.user_id, "completed", signal.audit.output_hash, output_hash))
            cur.execute("INSERT INTO signal_registry (user_id, signal_type, signal_hash, signal_json) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (signal.user_id, "bloodwork_v1.1", signal.audit.output_hash, json.dumps(signal.model_dump(), default=str)))
            cur.execute("INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, %s, %s, %s)", (run_id, "orchestrate_v2", json.dumps(output_data, default=str), output_hash))
            conn.commit()
            cur.close()
            conn.close()
        except:
            try: conn.close()
            except: pass
    
    response = OrchestrateOutputV2(
        run_id=run_id,
        signal_id=signal.signal_id,
        signal_hash=signal.audit.output_hash,
        hash_verified=hash_verified,
        routing_constraints=routing_constraints,
        blocked_targets=routing_constraints["blocked_targets"],
        caution_targets=routing_constraints["caution_targets"],
        assessment_context=request.assessment_context,
        selected_goals=request.selected_goals,
        chain_of_custody=[chain_entry],
        next_phase="compose"
    )
    
    # Emit telemetry (v3.17.0)
    _emit_telemetry_for_phase(
        run_id=run_id,
        phase="orchestrate_v2",
        request_dict={"bloodwork_signal": True, "selected_goals": request.selected_goals, "assessment_context": request.assessment_context},
        response_dict=response.model_dump(),
        has_bloodwork=True,
    )
    
    return response


@app.post("/api/v1/brain/orchestrate")
def brain_orchestrate_legacy(request: OrchestrateRequest):
    run_id = str(uuid.uuid4())
    created_at = now_iso()
    signal_hash = request.signal_hash or compute_hash(request.signal_data)
    markers = request.signal_data.get("markers", {})
    if not markers:
        raise HTTPException(status_code=400, detail="No markers provided")
    routing_constraints = derive_routing_constraints(markers)
    has_hard_blocks = any(c.get("severity") == "hard" for c in routing_constraints)
    assessment_context = build_assessment_context(request.user_id, request.signal_data)
    output = {"run_id": run_id, "routing_constraints": routing_constraints, "override_allowed": not has_hard_blocks, "assessment_context": assessment_context}
    output_hash = compute_hash(output)
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO brain_runs (id, user_id, status, input_hash, output_hash, created_at) VALUES (%s, %s, %s, %s, %s, NOW())", (run_id, request.user_id, "completed", signal_hash, output_hash))
            cur.execute("INSERT INTO signal_registry (user_id, signal_type, signal_hash, signal_json) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (request.user_id, "bloodwork", signal_hash, json.dumps(request.signal_data)))
            cur.execute("INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, %s, %s, %s)", (run_id, "orchestrate", json.dumps(output), output_hash))
            conn.commit()
            cur.close()
            conn.close()
        except:
            try: conn.close()
            except: pass
    
    response_dict = {
        "run_id": run_id,
        "status": "success",
        "phase": "orchestrate",
        "signal_hash": signal_hash,
        "routing_constraints": routing_constraints,
        "override_allowed": not has_hard_blocks,
        "assessment_context": assessment_context,
        "next_phase": "compose",
        "audit": {"created_at": created_at, "output_hash": output_hash}
    }
    
    # Emit telemetry (v3.17.0)
    _emit_telemetry_for_phase(
        run_id=run_id,
        phase="orchestrate",
        request_dict={"user_id": request.user_id, "signal_data": {"markers_count": len(markers)}},
        response_dict=response_dict,
        has_bloodwork=bool(markers),
    )
    
    return response_dict


@app.post("/api/v1/brain/compose")
def brain_compose(request: ComposeRequest):
    protocol_id = str(uuid.uuid4())
    created_at = now_iso()
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cur = conn.cursor()
        cur.execute("SELECT output_json, output_hash FROM decision_outputs WHERE run_id = %s AND phase IN ('orchestrate', 'orchestrate_v2') ORDER BY created_at DESC LIMIT 1", (request.run_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"No orchestrate output for run_id: {request.run_id}")
        orchestrate_output = parse_jsonb(row["output_json"])
        orchestrate_hash = row["output_hash"]
        cur.execute("SELECT user_id FROM brain_runs WHERE id = %s", (request.run_id,))
        run_row = cur.fetchone()
        if not run_row:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"No brain run for run_id: {request.run_id}")
        # Handle NULL user_id gracefully (v3.29.3 stores NULL for anonymous users)
        user_id = str(run_row["user_id"]) if run_row["user_id"] else "anonymous"
        routing_constraints = orchestrate_output.get("routing_constraints", {})
        assessment_context = orchestrate_output.get("assessment_context", {})
        protocol_intents = compose_intents(request.selected_goals, routing_constraints, assessment_context)
        compose_output = {"protocol_id": protocol_id, "run_id": request.run_id, "selected_goals": request.selected_goals, "protocol_intents": protocol_intents, "routing_constraints": routing_constraints, "assessment_context": assessment_context, "constraints_applied": 0, "intents_generated": {k: len(v) for k, v in protocol_intents.items()}}
        output_hash = compute_hash(compose_output)
        cur.execute("INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, %s, %s, %s)", (request.run_id, "compose", json.dumps(compose_output), output_hash))
        # FIX v3.29.4: Use ANONYMOUS_USER_UUID for protocol_runs when user_id is NULL
        # protocol_runs.user_id has NOT NULL constraint, cannot use None
        user_id_for_db = run_row["user_id"] if run_row["user_id"] else ANONYMOUS_USER_UUID
        cur.execute("INSERT INTO protocol_runs (id, user_id, run_id, phase, request_json, output_json, output_hash, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (protocol_id, user_id_for_db, request.run_id, "compose", json.dumps({"run_id": request.run_id, "selected_goals": request.selected_goals}), json.dumps(compose_output), output_hash, "completed"))
        conn.commit()
        cur.close()
        conn.close()
    except HTTPException:
        raise
    except Exception as e:
        try: conn.close()
        except: pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    response_dict = {
        "protocol_id": protocol_id,
        "status": "success",
        "phase": "compose",
        "run_id": request.run_id,
        "selected_goals": request.selected_goals,
        "protocol_intents": protocol_intents,
        "summary": {"constraints_applied": compose_output["constraints_applied"], "intents_generated": compose_output["intents_generated"]},
        "next_phase": "route",
        "audit": {"created_at": created_at, "input_hashes": [orchestrate_hash], "output_hash": output_hash}
    }
    
    # Emit telemetry (v3.17.0)
    _emit_telemetry_for_phase(
        run_id=request.run_id,
        phase="compose",
        request_dict={"run_id": request.run_id, "selected_goals": request.selected_goals},
        response_dict=response_dict,
    )
    
    return response_dict


@app.post("/api/v1/brain/route")
def brain_route(request: RouteRequest):
    created_at = now_iso()
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cur = conn.cursor()
        cur.execute("SELECT pr.run_id FROM protocol_runs pr WHERE pr.id = %s LIMIT 1", (request.protocol_id,))
        run_row = cur.fetchone()
        if not run_row or not run_row.get("run_id"):
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"No run_id found for protocol_id: {request.protocol_id}")
        run_id = run_row["run_id"]
        cur.execute("SELECT COALESCE(d.output_json #>> '{assessment_context,gender}', d.output_json #>> '{assessment_context,sex}') AS gender FROM decision_outputs d WHERE d.run_id = %s AND d.phase IN ('orchestrate_v2', 'orchestrate') ORDER BY d.created_at DESC LIMIT 1", (run_id,))
        gender_row = cur.fetchone()
        gender = gender_row.get("gender") if gender_row else None
        if not gender:
            cur.close()
            conn.close()
            raise HTTPException(status_code=422, detail={"error": "MISSING_OS_ENVIRONMENT"})
        gender_lower = gender.lower()
        os_env = "MAXimo²" if gender_lower == "male" else "MAXima²" if gender_lower == "female" else None
        if not os_env:
            cur.close()
            conn.close()
            raise HTTPException(status_code=422, detail={"error": "INVALID_GENDER"})
        blocked_targets = request.routing_constraints.get("blocked_targets", [])
        caution_targets = request.routing_constraints.get("caution_targets", [])
        blocked_ingredients = request.routing_constraints.get("blocked_ingredients", [])
        supplement_intents = request.protocol_intents.get("supplements", [])
        sku_items = []
        skipped_intents = []
        used_modules = set()
        for intent in supplement_intents:
            intent_id = intent.get("intent_id")
            target_id = intent.get("target_id", intent_id)
            intent_spec = INTENT_CATALOG.get(intent_id)
            if not intent_spec:
                skipped_intents.append({"intent_id": intent_id, "reason": "INTENT_NOT_IN_CATALOG"})
                continue
            if is_blocked_by_target(intent_spec, blocked_targets):
                skipped_intents.append({"intent_id": intent_id, "reason": "BLOCKED_BY_TARGET"})
                continue
            must_have_tags = intent_spec.get("must_have_tags", [])
            must_patterns = [f"%{tag}%" for tag in must_have_tags] if must_have_tags else ["%__match_all__%"]
            blocked_patterns = [f"%{ing}%" for ing in blocked_ingredients] if blocked_ingredients else ["%__never_match__%"]
            cur.execute("SELECT module_code, product_name, os_layer, biological_domain, shopify_store, shopify_handle FROM os_modules WHERE os_environment = %s AND ingredient_tags ILIKE ANY(%s) AND NOT (ingredient_tags ILIKE ANY(%s)) ORDER BY CASE os_layer WHEN 'Core' THEN 1 WHEN 'Adaptive' THEN 2 ELSE 3 END, module_code LIMIT 1", (os_env, must_patterns, blocked_patterns))
            row = cur.fetchone()
            if not row:
                skipped_intents.append({"intent_id": intent_id, "reason": "NO_MATCHING_MODULE"})
                continue
            module_code = row["module_code"]
            if module_code in used_modules:
                continue
            used_modules.add(module_code)
            reason_codes = []
            if has_caution_target(intent_spec, caution_targets):
                reason_codes.append("CAUTION_TARGET")
            sku_items.append({"sku": module_code, "intent_id": intent_id, "target_id": target_id, "shopify_store": row["shopify_store"] or "", "shopify_handle": row["shopify_handle"] or "", "reason_codes": reason_codes})
        output_data = {"protocol_id": request.protocol_id, "sku_plan": {"items": sku_items}, "skipped_intents": skipped_intents}
        output_hash = compute_hash(output_data)
        cur.execute("INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, 'route', %s, %s)", (run_id, json.dumps(output_data, default=str), output_hash))
        conn.commit()
        cur.close()
        conn.close()
        
        response_dict = {
            "protocol_id": request.protocol_id,
            "sku_plan": {"items": sku_items},
            "skipped_intents": skipped_intents,
            "audit": {"status": "SUCCESS", "run_id": str(run_id), "os_environment": os_env, "created_at": created_at, "output_hash": output_hash}
        }
        
        # Emit telemetry (v3.17.0)
        _emit_telemetry_for_phase(
            run_id=str(run_id),
            phase="route",
            request_dict={"protocol_id": request.protocol_id, "intents_count": len(supplement_intents)},
            response_dict=response_dict,
        )
        
        return response_dict
    except HTTPException:
        raise
    except Exception as e:
        try: conn.close()
        except: pass
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/intake")
def create_intake(intake: IntakeCreate):
    return {"status": "received", "gender_os": intake.gender_os, "next_step": "bloodwork_upload"}


@app.get("/api/v1/intake/{user_id}")
def get_intake(user_id: str):
    return {"user_id": user_id, "status": "pending"}


@app.post("/api/v1/bloodwork/analyze")
def analyze_bloodwork(bloodwork: BloodworkInput):
    markers = bloodwork.markers
    routing_constraints = derive_routing_constraints(markers)
    assessment = build_assessment_context(bloodwork.user_id, {"markers": markers})
    return {"user_id": bloodwork.user_id, "analysis_id": str(uuid.uuid4()), "markers_analyzed": len(markers), "routing_constraints": routing_constraints, "assessment": assessment}


@app.get("/api/v1/protocol/{user_id}")
def get_protocol(user_id: str):
    return {"user_id": user_id, "protocol_status": "awaiting_bloodwork"}


@app.get("/debug/catalog-check")
def check_catalog():
    conn = get_db()
    if not conn:
        return {"error": "DB connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as total FROM fulfillment_catalog")
        total = cur.fetchone()["total"]
        cur.close()
        conn.close()
        return {"total_products": total}
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}


@app.get("/debug/catalog-gaps")
def catalog_gaps():
    conn = get_db()
    if not conn:
        return {"error": "DB connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM fulfillment_catalog WHERE ingredient_id IS NULL")
        unlinked = cur.fetchone()["count"]
        cur.close()
        conn.close()
        return {"unlinked_products": unlinked}
    except Exception as e:
        try: conn.close()
        except: pass
        return {"error": str(e)}
