"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
"""

import os
import json
import hashlib
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid

# ============================================
# App Configuration
# ============================================
app = FastAPI(
    title="GenoMAX² API",
    description="Gender-Optimized Biological Operating System",
    version="3.5.0"
)

# ============================================
# CORS Configuration - CRITICAL FOR VERCEL
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://genomax2-frontend.vercel.app",
        "https://genomax2-frontend-git-main-hemis-projects-6782105b.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
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
# Pydantic Models
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
    user_id: str
    signal_data: Dict[str, Any]
    signal_hash: Optional[str] = None

# ============================================
# Hashing Utilities
# ============================================
def compute_hash(data: Any) -> str:
    json_str = json.dumps(data, sort_keys=True, default=str)
    return f"sha256:{hashlib.sha256(json_str.encode()).hexdigest()}"

# ============================================
# Routing Constraint Rules
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
# Migration Endpoints
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

@app.get("/fix-hash-columns")
def fix_hash_columns():
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("""
            ALTER TABLE brain_runs ALTER COLUMN input_hash TYPE VARCHAR(128);
            ALTER TABLE brain_runs ALTER COLUMN output_hash TYPE VARCHAR(128);
            ALTER TABLE signal_registry ALTER COLUMN signal_hash TYPE VARCHAR(128);
            ALTER TABLE decision_outputs ALTER COLUMN output_hash TYPE VARCHAR(128);
            ALTER TABLE protocol_runs ALTER COLUMN output_hash TYPE VARCHAR(128);
            ALTER TABLE audit_log ALTER COLUMN before_hash TYPE VARCHAR(128);
            ALTER TABLE audit_log ALTER COLUMN after_hash TYPE VARCHAR(128);
        """)
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Hash columns extended to 128 chars"}
    except Exception as e:
        conn.close()
        return {"error": str(e)}

# ============================================
# Core Endpoints
# ============================================
@app.get("/")
def root():
    return {"service": "GenoMAX² API", "version": "3.5.0", "status": "operational", "brain_version": "1.1.0"}

@app.get("/health")
def health():
    return {"status": "healthy", "version": "3.5.0"}

@app.get("/version")
def version():
    return {"api_version": "3.5.0", "brain_version": "1.1.0", "features": ["orchestrate", "migrate-brain-full", "fix-hash-columns"]}

# ============================================
# Brain API v1 Endpoints
# ============================================
@app.get("/api/v1/brain/health")
def brain_health():
    return {"status": "healthy", "service": "brain", "version": "1.1.0"}

@app.get("/api/v1/brain/info")
def brain_info():
    return {"service": "GenoMAX² Brain", "version": "1.1.0", "phases": ["orchestrate", "compose", "route"], "status": "operational"}

@app.post("/api/v1/brain/orchestrate")
def brain_orchestrate(request: OrchestrateRequest):
    run_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"
    signal_hash = request.signal_hash or compute_hash(request.signal_data)
    markers = request.signal_data.get("markers", {})
    if not markers:
        raise HTTPException(status_code=400, detail="No markers provided in signal_data")
    routing_constraints = derive_routing_constraints(markers)
    has_hard_blocks = any(c.get("severity") == "hard" for c in routing_constraints)
    override_allowed = not has_hard_blocks
    assessment_context = build_assessment_context(request.user_id, request.signal_data)
    output = {"run_id": run_id, "routing_constraints": routing_constraints, "override_allowed": override_allowed, "assessment_context": assessment_context}
    output_hash = compute_hash(output)
    db_status = "success"
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO brain_runs (id, user_id, status, input_hash, output_hash, created_at) VALUES (%s, %s, %s, %s, %s, NOW())",
                (run_id, request.user_id, "completed", signal_hash, output_hash))
            cur.execute("INSERT INTO signal_registry (user_id, signal_type, signal_hash, signal_json) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, signal_type, signal_hash) DO NOTHING",
                (request.user_id, "bloodwork", signal_hash, json.dumps(request.signal_data)))
            cur.execute("INSERT INTO decision_outputs (run_id, phase, output_json, output_hash) VALUES (%s, %s, %s, %s)",
                (run_id, "orchestrate", json.dumps(output), output_hash))
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
        "run_id": run_id, "status": db_status, "phase": "orchestrate", "signal_hash": signal_hash,
        "routing_constraints": routing_constraints, "override_allowed": override_allowed,
        "assessment_context": assessment_context, "next_phase": "compose",
        "audit": {"created_at": created_at, "output_hash": output_hash}
    }

# ============================================
# Legacy Endpoints
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
    return {"user_id": bloodwork.user_id, "analysis_id": str(uuid.uuid4()), "markers_analyzed": len(markers), "routing_constraints": routing_constraints, "assessment": assessment, "recommendations_ready": True}

@app.get("/api/v1/protocol/{user_id}")
def get_protocol(user_id: str):
    return {"user_id": user_id, "protocol_status": "awaiting_bloodwork", "message": "Complete bloodwork analysis to generate protocol"}