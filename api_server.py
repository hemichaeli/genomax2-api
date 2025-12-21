"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
Version 3.7.0 - Orchestrate Fix (BloodworkSignalV1 Only)

CRITICAL FIX v3.7.0:
- Orchestrate now ONLY accepts BloodworkSignalV1 (immutable, hash-verified)
- Raw markers are REJECTED
- Added /api/v1/brain/orchestrate/v2 (new fixed endpoint)
- Deprecated /api/v1/brain/orchestrate (legacy, will be removed)
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

# ============================================
# App Configuration
# ============================================
app = FastAPI(
    title="GenoMAX² API",
    description="Gender-Optimized Biological Operating System",
    version="3.7.0"
)

# ============================================
# CORS Configuration
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://genomax2-frontend.vercel.app",
        "https://genomax2-frontend-git-main-hemis-projects-6782105b.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ============================================
# Database Connection
# ============================================
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


# ============================================
# NEW: BloodworkSignalV1 Schema (v3.7.0 Fix)
# ============================================

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
    """Single biomarker after processing through Bloodwork Engine."""
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
    """Aggregated score for a biological target."""
    target_id: str
    score: float = Field(ge=0, le=100)
    contributing_biomarkers: List[str]
    primary_biomarker: Optional[str] = None
    flags: List[str] = Field(default_factory=list)


class SupplementGate(BaseModel):
    """Gate status for a supplement category/target."""
    target_id: str
    gate_status: GateStatus
    reason: str
    blocking_biomarkers: List[str] = Field(default_factory=list)
    caution_biomarkers: List[str] = Field(default_factory=list)


class GlobalFlag(BaseModel):
    """System-wide flag requiring attention."""
    flag_id: str
    severity: str
    message: str
    triggered_by: List[str] = Field(default_factory=list)


class SignalAudit(BaseModel):
    """Audit trail for signal generation."""
    engine_version: str
    generated_at: str
    input_hash: str
    output_hash: str
    processing_time_ms: int = 0


class BloodworkSignalV1(BaseModel):
    """
    IMMUTABLE signal output from Bloodwork Engine v1.1
    This is the ONLY valid input to Brain Orchestrate v2.
    """
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


class OrchestrateInputV2(BaseModel):
    """
    STRICT input schema for Brain Orchestrate v2 endpoint.
    ONLY accepts BloodworkSignalV1.
    """
    bloodwork_signal: BloodworkSignalV1
    verify_hash: bool = True
    selected_goals: List[str] = Field(default_factory=list)
    assessment_context: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('assessment_context')
    @classmethod
    def reject_raw_markers(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """DEFENSE-IN-DEPTH: Reject any attempt to sneak raw markers."""
        forbidden_keys = {'markers', 'biomarkers', 'labs', 'lab_results', 'blood_values'}
        found = forbidden_keys.intersection(set(v.keys()))
        if found:
            raise ValueError(
                f"Raw marker data detected in assessment_context: {found}. "
                "Use bloodwork_signal field with processed BloodworkSignalV1 only."
            )
        return v


class OrchestrateOutputV2(BaseModel):
    """Output from Brain Orchestrate v2 phase."""
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


# ============================================
# Legacy Pydantic Models (for backward compatibility)
# ============================================
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
    """DEPRECATED: Use OrchestrateInputV2 with /api/v1/brain/orchestrate/v2"""
    user_id: str
    signal_data: Dict[str, Any]
    signal_hash: Optional[str] = None


class ComposeRequest(BaseModel):
    run_id: str
    selected_goals: List[str]


# ============================================
# Hashing Utilities
# ============================================
def compute_hash(data: Any) -> str:
    json_str = json.dumps(data, sort_keys=True, default=str)
    return f"sha256:{hashlib.sha256(json_str.encode()).hexdigest()}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def verify_signal_hash(signal: BloodworkSignalV1) -> bool:
    """Verify that signal hash matches content."""
    signal_dict = signal.model_dump()
    signal_dict["audit"]["output_hash"] = ""
    canonical = json.dumps(signal_dict, sort_keys=True, default=str)
    expected = f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"
    return signal.audit.output_hash == expected


# ============================================
# NEW: Build routing constraints from gates (v3.7.0)
# ============================================
def build_routing_constraints_from_gates(signal: BloodworkSignalV1) -> Dict[str, Any]:
    """
    Build routing constraints from signal gates.
    These constraints are IMMUTABLE - downstream phases cannot override.
    """
    constraints = {
        "blocked_targets": [],
        "caution_targets": [],
        "allowed_targets": [],
        "target_details": {},
    }

    for target_id, gate in signal.supplement_gates.items():
        detail = {
            "gate_status": gate.gate_status.value,
            "reason": gate.reason,
            "blocking_biomarkers": gate.blocking_biomarkers,
            "caution_biomarkers": gate.caution_biomarkers,
        }
        constraints["target_details"][target_id] = detail

        if gate.gate_status == GateStatus.BLOCKED:
            constraints["blocked_targets"].append(target_id)
        elif gate.gate_status == GateStatus.CAUTION:
            constraints["caution_targets"].append(target_id)
        else:
            constraints["allowed_targets"].append(target_id)

    # Add global flags
    constraints["global_flags"] = [
        {"flag_id": f.flag_id, "severity": f.severity, "message": f.message}
        for f in signal.global_flags
    ]

    critical_flags = [f for f in signal.global_flags if f.severity == "critical"]
    constraints["has_critical_flags"] = len(critical_flags) > 0
    if critical_flags:
        constraints["critical_flag_ids"] = [f.flag_id for f in critical_flags]

    return constraints


# ============================================
# Goal -> Intent Mapping (unchanged)
# ============================================
GOAL_INTENT_MAP = {
    "sleep": {
        "lifestyle": [
            {"intent_id": "improve_sleep_quality", "base_priority": 0.85, "depends_on": []},
            {"intent_id": "regulate_circadian_rhythm", "base_priority": 0.70, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "evening_carb_timing", "base_priority": 0.60, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "magnesium_for_sleep", "base_priority": 0.80, "depends_on": ["magnesium"]},
            {"intent_id": "glycine_for_sleep", "base_priority": 0.65, "depends_on": ["glycine"]}
        ]
    },
    "energy": {
        "lifestyle": [
            {"intent_id": "optimize_energy_levels", "base_priority": 0.85, "depends_on": []},
            {"intent_id": "morning_light_exposure", "base_priority": 0.70, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "blood_sugar_stability", "base_priority": 0.75, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "b12_energy_support", "base_priority": 0.80, "depends_on": ["vitamin_b12"]},
            {"intent_id": "coq10_cellular_energy", "base_priority": 0.70, "depends_on": ["coq10"]},
            {"intent_id": "iron_energy_support", "base_priority": 0.75, "depends_on": ["iron"]}
        ]
    },
    "stress": {
        "lifestyle": [
            {"intent_id": "reduce_stress_response", "base_priority": 0.85, "depends_on": []},
            {"intent_id": "breathwork_practice", "base_priority": 0.70, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "anti_stress_nutrition", "base_priority": 0.65, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "magnesium_stress_support", "base_priority": 0.80, "depends_on": ["magnesium"]},
            {"intent_id": "adaptogen_support", "base_priority": 0.70, "depends_on": ["adaptogen"]}
        ]
    },
    "focus": {
        "lifestyle": [
            {"intent_id": "enhance_cognitive_function", "base_priority": 0.85, "depends_on": []},
            {"intent_id": "attention_training", "base_priority": 0.65, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "brain_fuel_optimization", "base_priority": 0.70, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "omega3_brain_support", "base_priority": 0.80, "depends_on": ["omega3"]},
            {"intent_id": "lions_mane_cognition", "base_priority": 0.65, "depends_on": ["lions_mane"]}
        ]
    },
    "immunity": {
        "lifestyle": [
            {"intent_id": "immune_resilience", "base_priority": 0.80, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "immune_nutrition", "base_priority": 0.70, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "vitamin_d_immune", "base_priority": 0.85, "depends_on": ["vitamin_d"]},
            {"intent_id": "zinc_immune_support", "base_priority": 0.75, "depends_on": ["zinc"]},
            {"intent_id": "vitamin_c_immune", "base_priority": 0.70, "depends_on": ["vitamin_c"]}
        ]
    },
    "heart": {
        "lifestyle": [
            {"intent_id": "cardiovascular_health", "base_priority": 0.85, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "heart_healthy_diet", "base_priority": 0.80, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "omega3_cardiovascular", "base_priority": 0.85, "depends_on": ["omega3"]},
            {"intent_id": "coq10_heart_support", "base_priority": 0.75, "depends_on": ["coq10"]},
            {"intent_id": "magnesium_heart", "base_priority": 0.70, "depends_on": ["magnesium"]}
        ]
    },
    "gut": {
        "lifestyle": [
            {"intent_id": "gut_health_optimization", "base_priority": 0.80, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "fiber_diversity", "base_priority": 0.75, "depends_on": []},
            {"intent_id": "fermented_foods", "base_priority": 0.70, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "probiotic_support", "base_priority": 0.80, "depends_on": ["probiotic"]},
            {"intent_id": "digestive_enzyme_support", "base_priority": 0.65, "depends_on": ["digestive_enzyme"]}
        ]
    },
    "inflammation": {
        "lifestyle": [
            {"intent_id": "reduce_systemic_inflammation", "base_priority": 0.85, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "anti_inflammatory_diet", "base_priority": 0.80, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "omega3_antiinflammatory", "base_priority": 0.85, "depends_on": ["omega3"]},
            {"intent_id": "curcumin_inflammation", "base_priority": 0.75, "depends_on": ["curcumin"]}
        ]
    },
    "liver": {
        "lifestyle": [
            {"intent_id": "liver_health_support", "base_priority": 0.85, "depends_on": []}
        ],
        "nutrition": [
            {"intent_id": "liver_supportive_diet", "base_priority": 0.80, "depends_on": []}
        ],
        "supplements": [
            {"intent_id": "milk_thistle_liver", "base_priority": 0.75, "depends_on": ["milk_thistle", "hepatotoxic"]},
            {"intent_id": "nac_liver_support", "base_priority": 0.70, "depends_on": ["nac"]}
        ]
    }
}

# Ingredient class -> blocked intents mapping
INGREDIENT_BLOCKS = {
    "iron": ["iron_energy_support"],
    "potassium": ["potassium_support"],
    "vitamin_d": ["vitamin_d_immune"],
    "hepatotoxic": ["milk_thistle_liver", "adaptogen_support"],
    "kava": ["adaptogen_support"],
    "high_dose_niacin": ["niacin_support"],
    "green_tea_extract_high": ["green_tea_support"]
}

# ============================================
# Legacy Routing Constraint Rules (for backward compatibility)
# ============================================
ROUTING_RULES = {
    "ferritin": {
        "high_threshold": 300,
        "constraint": {
            "ingredient_class": "iron",
            "constraint_type": "blocked",
            "reason": "Ferritin elevated ({value} ng/mL). Iron supplementation contraindicated.",
            "severity": "hard"
        }
    },
    "alt": {
        "high_threshold": 50,
        "constraints": [
            {"ingredient_class": "hepatotoxic", "constraint_type": "blocked", "reason": "ALT significantly elevated ({value} U/L). Hepatotoxic supplements blocked.", "severity": "hard"},
            {"ingredient_class": "kava", "constraint_type": "blocked", "reason": "ALT elevated ({value} U/L). kava contraindicated.", "severity": "hard"},
            {"ingredient_class": "high_dose_niacin", "constraint_type": "blocked", "reason": "ALT elevated ({value} U/L). high_dose_niacin contraindicated.", "severity": "hard"},
            {"ingredient_class": "green_tea_extract_high", "constraint_type": "blocked", "reason": "ALT elevated ({value} U/L). green_tea_extract_high contraindicated.", "severity": "hard"}
        ]
    },
    "potassium": {
        "high_threshold": 5.0,
        "constraint": {
            "ingredient_class": "potassium",
            "constraint_type": "blocked",
            "reason": "Potassium elevated ({value} mEq/L). Supplementation contraindicated.",
            "severity": "hard"
        }
    },
    "vitamin_d": {
        "low_threshold": 30,
        "constraint": {
            "ingredient_class": "vitamin_d",
            "constraint_type": "required",
            "reason": "Vitamin D deficient ({value} ng/mL). Supplementation recommended.",
            "severity": "soft"
        }
    },
    "b12": {
        "low_threshold": 400,
        "constraint": {
            "ingredient_class": "vitamin_b12",
            "constraint_type": "required",
            "reason": "B12 suboptimal ({value} pg/mL). Supplementation recommended.",
            "severity": "soft"
        }
    }
}


def derive_routing_constraints(markers: Dict[str, float]) -> List[Dict[str, Any]]:
    """DEPRECATED: Use build_routing_constraints_from_gates instead."""
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
    """DEPRECATED: Use BloodworkSignalV1.context instead."""
    markers = signal_data.get("markers", {})
    gender = signal_data.get("gender", "unknown")
    deficient, suboptimal, optimal, elevated = [], [], [], []
    thresholds = {
        "ferritin": {"low": 30, "optimal_low": 50, "optimal_high": 200, "high": 300, "unit": "ng/mL"},
        "vitamin_d": {"low": 20, "optimal_low": 40, "optimal_high": 60, "high": 100, "unit": "ng/mL"},
        "b12": {"low": 200, "optimal_low": 500, "optimal_high": 900, "high": 1500, "unit": "pg/mL"},
        "alt": {"low": 0, "optimal_low": 7, "optimal_high": 40, "high": 50, "unit": "U/L"},
        "potassium": {"low": 3.5, "optimal_low": 3.8, "optimal_high": 4.8, "high": 5.0, "unit": "mEq/L"},
    }
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
    return {
        "user_id": user_id, "gender": gender, "test_date": signal_data.get("test_date"),
        "lab_source": signal_data.get("lab_source"), "markers_analyzed": len(markers),
        "summary": {"deficient_count": len(deficient), "suboptimal_count": len(suboptimal), "optimal_count": len(optimal), "elevated_count": len(elevated)},
        "deficient": deficient, "suboptimal": suboptimal, "optimal": optimal, "elevated": elevated
    }


# ============================================
# Compose Logic (unchanged)
# ============================================
def get_blocked_ingredient_classes(routing_constraints: List[Dict]) -> set:
    blocked = set()
    for constraint in routing_constraints:
        if constraint.get("constraint_type") == "blocked":
            blocked.add(constraint.get("ingredient_class"))
    return blocked


def get_blocked_targets_from_v2(routing_constraints: Dict[str, Any]) -> set:
    """Extract blocked targets from v2 routing constraints."""
    return set(routing_constraints.get("blocked_targets", []))


def is_intent_blocked(intent: Dict, blocked_classes: set) -> bool:
    depends_on = intent.get("depends_on", [])
    for dep in depends_on:
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


def compose_intents(selected_goals: List[str], routing_constraints: List[Dict], assessment_context: Dict) -> Dict[str, List[Dict]]:
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
                if intent_id in seen_intents:
                    continue
                if is_intent_blocked(intent, blocked_classes):
                    continue

                priority = calculate_priority(intent["base_priority"], assessment_context, intent_id)
                protocol_intents[category].append({
                    "intent_id": intent_id,
                    "source_goal": goal_lower,
                    "priority": priority,
                    "blocked": False
                })
                seen_intents.add(intent_id)

    for category in protocol_intents:
        protocol_intents[category].sort(key=lambda x: x["priority"], reverse=True)

    return protocol_intents


# ============================================
# Migration Endpoints (unchanged)
# ============================================
@app.get("/migrate-brain")
def migrate_brain():
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brain_runs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID,
                status VARCHAR(20) DEFAULT 'running',
                input_hash VARCHAR(128),
                output_hash VARCHAR(128),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS signal_registry (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL,
                signal_type VARCHAR(50) NOT NULL,
                signal_hash VARCHAR(128) NOT NULL,
                signal_json JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, signal_type, signal_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_signal_user ON signal_registry(user_id);
            CREATE INDEX IF NOT EXISTS idx_brain_runs_user ON brain_runs(user_id);
        """)
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Brain tables created"}
    except Exception as e:
        conn.close()
        return {"error": str(e)}


@app.get("/migrate-brain-full")
def migrate_brain_full():
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brain_runs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID,
                status VARCHAR(20) DEFAULT 'running',
                input_hash VARCHAR(128),
                output_hash VARCHAR(128),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS signal_registry (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL,
                signal_type VARCHAR(50) NOT NULL,
                signal_hash VARCHAR(128) NOT NULL,
                signal_json JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, signal_type, signal_hash)
            );
            CREATE TABLE IF NOT EXISTS decision_outputs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                run_id UUID,
                phase VARCHAR(30) NOT NULL,
                output_json JSONB NOT NULL,
                output_hash VARCHAR(128),
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS protocol_runs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL,
                run_id UUID,
                phase VARCHAR(30) NOT NULL,
                request_json JSONB,
                output_json JSONB,
                output_hash VARCHAR(128),
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                entity_type VARCHAR(50) NOT NULL,
                entity_id UUID,
                action VARCHAR(30) NOT NULL,
                actor_id UUID,
                before_hash VARCHAR(128),
                after_hash VARCHAR(128),
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_signal_user ON signal_registry(user_id);
            CREATE INDEX IF NOT EXISTS idx_brain_runs_user ON brain_runs(user_id);
            CREATE INDEX IF NOT EXISTS idx_decision_run ON decision_outputs(run_id);
            CREATE INDEX IF NOT EXISTS idx_protocol_user ON protocol_runs(user_id);
            CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity_type, entity_id);
        """)
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "tables": ["brain_runs", "signal_registry", "decision_outputs", "protocol_runs", "audit_log"]}
    except Exception as e:
        conn.close()
        return {"error": str(e)}


# ============================================
# Core Endpoints
# ============================================
@app.get("/")
def root():
    return {
        "service": "GenoMAX² API",
        "version": "3.7.0",
        "status": "operational",
        "brain_version": "1.3.0",
        "fix": "Orchestrate v2 now ONLY accepts BloodworkSignalV1"
    }


@app.get("/health")
def health():
    return {"status": "healthy", "version": "3.7.0"}


@app.get("/version")
def version():
    return {
        "api_version": "3.7.0",
        "brain_version": "1.3.0",
        "features": ["orchestrate", "orchestrate_v2", "compose", "migrate-brain-full"],
        "breaking_changes": ["orchestrate_v2 requires BloodworkSignalV1"]
    }


# ============================================
# Brain API v1 Endpoints
# ============================================
@app.get("/api/v1/brain/health")
def brain_health():
    return {
        "status": "healthy",
        "service": "brain",
        "version": "1.3.0",
        "endpoints": {
            "orchestrate_legacy": "/api/v1/brain/orchestrate (DEPRECATED)",
            "orchestrate_v2": "/api/v1/brain/orchestrate/v2 (RECOMMENDED)",
        }
    }


@app.get("/api/v1/brain/info")
def brain_info():
    return {
        "service": "GenoMAX² Brain",
        "version": "1.3.0",
        "phases": ["orchestrate", "compose", "route"],
        "status": "operational",
        "critical_update": "Use /api/v1/brain/orchestrate/v2 with BloodworkSignalV1"
    }


# ============================================
# NEW: Orchestrate v2 - FIXED (BloodworkSignalV1 only)
# ============================================
@app.post(
    "/api/v1/brain/orchestrate/v2",
    response_model=OrchestrateOutputV2,
    summary="Orchestrate Brain pipeline from BloodworkSignalV1",
    description="""
    **FIXED ENDPOINT (v3.7.0)**

    Initiates the Brain pipeline by consuming an IMMUTABLE BloodworkSignalV1.

    **CRITICAL**: This endpoint ONLY accepts processed signals from the
    Bloodwork Engine. Raw markers are REJECTED.

    **Gate Semantics**:
    - `allowed`: Blood does not block this target (NOT a recommendation)
    - `blocked`: Hard constraint - downstream MUST NOT route to this target
    - `caution`: Proceed with reduced confidence and monitoring
    """
)
async def brain_orchestrate_v2(request: OrchestrateInputV2) -> OrchestrateOutputV2:
    """Process BloodworkSignalV1 and create routing constraints."""
    signal = request.bloodwork_signal
    created_at = now_iso()

    # STEP 1: Verify signal hash
    hash_verified = False
    if request.verify_hash:
        hash_verified = verify_signal_hash(signal)
        if not hash_verified:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "SIGNAL_HASH_MISMATCH",
                    "message": "Signal hash verification failed. Signal may have been tampered.",
                    "expected_hash": signal.audit.output_hash,
                    "signal_id": signal.signal_id,
                }
            )
    else:
        hash_verified = True

    # STEP 2: Validate schema version
    if signal.schema_version != "1.1":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "INVALID_SCHEMA_VERSION",
                "message": f"Expected schema version 1.1, got {signal.schema_version}",
                "signal_id": signal.signal_id,
            }
        )

    # STEP 3: Build routing constraints from gates
    routing_constraints = build_routing_constraints_from_gates(signal)

    # STEP 4: Generate run ID
    run_id = str(uuid.uuid4())

    # Compute output hash
    output_data = {
        "run_id": run_id,
        "signal_id": signal.signal_id,
        "routing_constraints": routing_constraints,
        "selected_goals": request.selected_goals,
    }
    output_hash = compute_hash(output_data)

    # Create chain entry
    chain_entry = {
        "stage": "brain_orchestrate_v2",
        "engine": "brain_1.3.0",
        "timestamp": created_at,
        "input_hashes": [signal.audit.output_hash],
        "output_hash": output_hash,
        "metadata": {
            "signal_schema_version": signal.schema_version,
            "blocked_count": len(routing_constraints["blocked_targets"]),
            "caution_count": len(routing_constraints["caution_targets"]),
            "allowed_count": len(routing_constraints["allowed_targets"]),
        }
    }

    # STEP 5: Save to database
    db_status = "success"
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO brain_runs (id, user_id, status, input_hash, output_hash, created_at) VALUES (%s, %s, %s, %s, %s, NOW())",
                (run_id, signal.user_id, "completed", signal.audit.output_hash, output_hash)
            )
            cur.execute(
                "INSERT INTO signal_registry (user_id, signal_type, signal_hash, signal_json) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, signal_type, signal_hash) DO NOTHING",
                (signal.user_id, "bloodwork_v1.1", signal.audit.output_hash, json.dumps(signal.model_dump(), default=str))
            )
            cur.execute(
                "INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, %s, %s, %s)",
                (run_id, "orchestrate_v2", json.dumps(output_data, default=str), output_hash)
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            db_status = f"db_error: {str(e)}"
            try:
                conn.close()
            except:
                pass
    else:
        db_status = "db_unavailable"

    return OrchestrateOutputV2(
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
        next_phase="compose",
    )


# ============================================
# RAW MARKERS REJECTION ENDPOINT
# ============================================
@app.post(
    "/api/v1/brain/orchestrate/raw",
    summary="[ALWAYS REJECTS] Raw markers endpoint",
    description="This endpoint exists to catch and reject raw marker submissions."
)
async def orchestrate_raw_rejected(data: Dict[str, Any]):
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={
            "error": "RAW_MARKERS_REJECTED",
            "message": (
                "Raw markers are not accepted. Blood data must be processed "
                "through the Bloodwork Engine v1.1 first. Submit a BloodworkSignalV1 "
                "to /api/v1/brain/orchestrate/v2 instead."
            ),
            "received_keys": list(data.keys()) if isinstance(data, dict) else "non-dict",
            "correct_endpoint": "/api/v1/brain/orchestrate/v2",
        }
    )


# ============================================
# LEGACY: Orchestrate (DEPRECATED - will be removed)
# ============================================
@app.post("/api/v1/brain/orchestrate")
def brain_orchestrate_legacy(request: OrchestrateRequest):
    """
    DEPRECATED: Use /api/v1/brain/orchestrate/v2 with BloodworkSignalV1.

    This endpoint accepts raw markers for backward compatibility only.
    It will be removed in a future version.
    """
    run_id = str(uuid.uuid4())
    created_at = now_iso()
    signal_hash = request.signal_hash or compute_hash(request.signal_data)
    markers = request.signal_data.get("markers", {})

    if not markers:
        raise HTTPException(status_code=400, detail="No markers provided in signal_data")

    routing_constraints = derive_routing_constraints(markers)
    has_hard_blocks = any(c.get("severity") == "hard" for c in routing_constraints)
    override_allowed = not has_hard_blocks
    assessment_context = build_assessment_context(request.user_id, request.signal_data)

    output = {
        "run_id": run_id,
        "routing_constraints": routing_constraints,
        "override_allowed": override_allowed,
        "assessment_context": assessment_context
    }
    output_hash = compute_hash(output)

    db_status = "success"
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO brain_runs (id, user_id, status, input_hash, output_hash, created_at) VALUES (%s, %s, %s, %s, %s, NOW())",
                (run_id, request.user_id, "completed", signal_hash, output_hash)
            )
            cur.execute(
                "INSERT INTO signal_registry (user_id, signal_type, signal_hash, signal_json) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, signal_type, signal_hash) DO NOTHING",
                (request.user_id, "bloodwork", signal_hash, json.dumps(request.signal_data))
            )
            cur.execute(
                "INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, %s, %s, %s)",
                (run_id, "orchestrate", json.dumps(output), output_hash)
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            db_status = f"db_error: {str(e)}"
            try:
                conn.close()
            except:
                pass
    else:
        db_status = "db_error"

    return {
        "run_id": run_id,
        "status": db_status,
        "phase": "orchestrate",
        "signal_hash": signal_hash,
        "routing_constraints": routing_constraints,
        "override_allowed": override_allowed,
        "assessment_context": assessment_context,
        "next_phase": "compose",
        "audit": {"created_at": created_at, "output_hash": output_hash},
        "deprecation_warning": "This endpoint is DEPRECATED. Use /api/v1/brain/orchestrate/v2 with BloodworkSignalV1."
    }


# ============================================
# Compose (unchanged)
# ============================================
@app.post("/api/v1/brain/compose")
def brain_compose(request: ComposeRequest):
    protocol_id = str(uuid.uuid4())
    created_at = now_iso()

    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT output_json, output_hash FROM decision_outputs
            WHERE run_id = %s AND phase IN ('orchestrate', 'orchestrate_v2')
            ORDER BY created_at DESC LIMIT 1
        """, (request.run_id,))

        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"No orchestrate output found for run_id: {request.run_id}")

        orchestrate_output = row["output_json"]
        orchestrate_hash = row["output_hash"]

        cur.execute("SELECT user_id FROM brain_runs WHERE id = %s", (request.run_id,))
        run_row = cur.fetchone()
        if not run_row:
            cur.close()
            conn.close()
            raise HTTPException(status_code=404, detail=f"No brain run found for run_id: {request.run_id}")

        user_id = str(run_row["user_id"])

        routing_constraints = orchestrate_output.get("routing_constraints", [])
        assessment_context = orchestrate_output.get("assessment_context", {})

        protocol_intents = compose_intents(request.selected_goals, routing_constraints, assessment_context)

        compose_output = {
            "protocol_id": protocol_id,
            "run_id": request.run_id,
            "selected_goals": request.selected_goals,
            "protocol_intents": protocol_intents,
            "constraints_applied": len([c for c in routing_constraints if isinstance(c, dict) and c.get("constraint_type") == "blocked"]),
            "intents_generated": {
                "lifestyle": len(protocol_intents["lifestyle"]),
                "nutrition": len(protocol_intents["nutrition"]),
                "supplements": len(protocol_intents["supplements"])
            }
        }
        output_hash = compute_hash(compose_output)

        cur.execute("""
            INSERT INTO decision_outputs (run_id, phase, output_json, output_hash)
            VALUES (%s, %s, %s, %s)
        """, (request.run_id, "compose", json.dumps(compose_output), output_hash))

        cur.execute("""
            INSERT INTO protocol_runs (id, user_id, run_id, phase, request_json, output_json, output_hash, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            protocol_id,
            user_id,
            request.run_id,
            "compose",
            json.dumps({"run_id": request.run_id, "selected_goals": request.selected_goals}),
            json.dumps(compose_output),
            output_hash,
            "completed"
        ))

        conn.commit()
        cur.close()
        conn.close()
        db_status = "success"

    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    return {
        "protocol_id": protocol_id,
        "status": db_status,
        "phase": "compose",
        "run_id": request.run_id,
        "selected_goals": request.selected_goals,
        "protocol_intents": protocol_intents,
        "summary": {
            "constraints_applied": compose_output["constraints_applied"],
            "intents_generated": compose_output["intents_generated"]
        },
        "next_phase": "route",
        "audit": {
            "created_at": created_at,
            "input_hashes": [orchestrate_hash],
            "output_hash": output_hash
        }
    }


# ============================================
# Legacy Endpoints (unchanged)
# ============================================
@app.post("/api/v1/intake")
def create_intake(intake: IntakeCreate):
    return {"status": "received", "gender_os": intake.gender_os, "next_step": "bloodwork_upload"}


@app.get("/api/v1/intake/{user_id}")
def get_intake(user_id: str):
    return {"user_id": user_id, "status": "pending", "message": "Intake data not found"}


@app.post("/api/v1/bloodwork/analyze")
def analyze_bloodwork(bloodwork: BloodworkInput):
    markers = bloodwork.markers
    routing_constraints = derive_routing_constraints(markers)
    assessment = build_assessment_context(bloodwork.user_id, {"markers": markers})
    return {
        "user_id": bloodwork.user_id,
        "analysis_id": str(uuid.uuid4()),
        "markers_analyzed": len(markers),
        "routing_constraints": routing_constraints,
        "assessment": assessment,
        "recommendations_ready": True,
        "note": "Use /api/v1/brain/orchestrate/v2 for the new flow with BloodworkSignalV1"
    }


@app.get("/api/v1/protocol/{user_id}")
def get_protocol(user_id: str):
    return {"user_id": user_id, "protocol_status": "awaiting_bloodwork", "message": "Complete bloodwork analysis to generate protocol"}
