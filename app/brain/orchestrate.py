"""
GenoMAX2 Brain Orchestrate Phase
Phase 1 of the Brain Pipeline.

Responsibilities:
1. Validate incoming bloodwork signal
2. Verify signal immutability (hash check)
3. Store signal in registry
4. Build assessment context
5. Generate routing constraints (blocked/caution/required)
6. Persist run to database
7. Return constraints for compose phase

NOT responsible for:
- SKUs
- Dosages
- Recommendations
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

from app.shared.hashing import canonicalize_and_hash, verify_hash


class OrchestrateStatus(str, Enum):
    SUCCESS = "success"
    IMMUTABILITY_VIOLATION = "immutability_violation"
    VALIDATION_ERROR = "validation_error"
    DB_ERROR = "db_error"


@dataclass
class RoutingConstraint:
    """A single routing constraint derived from bloodwork"""
    ingredient_class: str
    constraint_type: str  # "blocked", "caution", "required"
    reason: str
    source_marker: str
    source_value: float
    severity: str  # "hard" (cannot override) or "soft" (can override with warning)


@dataclass
class OrchestrateResult:
    """Result of orchestrate phase"""
    run_id: str
    status: OrchestrateStatus
    signal_hash: str
    routing_constraints: List[Dict]
    override_allowed: bool
    assessment_context: Dict
    audit: Dict
    error: Optional[str] = None


# ============================================
# VALIDATION
# ============================================

REQUIRED_SIGNAL_FIELDS = ["user_id", "markers"]
SUPPORTED_MARKERS = [
    "vitamin_d", "b12", "ferritin", "iron", "magnesium", "zinc",
    "omega3_index", "homocysteine", "hba1c", "fasting_glucose",
    "crp", "alt", "ast", "creatinine", "egfr", "potassium",
    "sodium", "calcium", "tsh", "free_t4", "free_t3",
    "testosterone", "estradiol", "cortisol", "dhea_s"
]


def validate_signal(signal_data: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate bloodwork signal structure.
    Returns: (is_valid, error_message)
    """
    for field in REQUIRED_SIGNAL_FIELDS:
        if field not in signal_data:
            return False, f"Missing required field: {field}"
    
    markers = signal_data.get("markers", {})
    if not isinstance(markers, dict):
        return False, "markers must be a dictionary"
    
    if len(markers) == 0:
        return False, "markers cannot be empty"
    
    # Validate marker values are numeric
    for marker, value in markers.items():
        if not isinstance(value, (int, float)):
            return False, f"Marker {marker} must be numeric, got {type(value).__name__}"
    
    return True, None


# ============================================
# ROUTING CONSTRAINTS LOGIC
# ============================================

def derive_routing_constraints(markers: Dict[str, float], gender: str = "unknown") -> List[RoutingConstraint]:
    """
    Derive routing constraints from bloodwork markers.
    
    This determines what the compose phase CAN and CANNOT do.
    No recommendations here - just constraints.
    """
    constraints = []
    
    # -----------------------------------------
    # IRON / FERRITIN CONSTRAINTS
    # -----------------------------------------
    if "ferritin" in markers:
        val = markers["ferritin"]
        if val > 300:
            # High ferritin - block iron supplementation
            constraints.append(RoutingConstraint(
                ingredient_class="iron",
                constraint_type="blocked",
                reason=f"Ferritin elevated ({val} ng/mL). Iron supplementation contraindicated.",
                source_marker="ferritin",
                source_value=val,
                severity="hard"
            ))
        elif val < 20:
            # Very low ferritin - iron required
            constraints.append(RoutingConstraint(
                ingredient_class="iron",
                constraint_type="required",
                reason=f"Ferritin critically low ({val} ng/mL). Iron supplementation indicated.",
                source_marker="ferritin",
                source_value=val,
                severity="soft"
            ))
    
    # -----------------------------------------
    # KIDNEY FUNCTION CONSTRAINTS
    # -----------------------------------------
    if "creatinine" in markers:
        val = markers["creatinine"]
        if val > 1.5:
            # Elevated creatinine - caution with certain supplements
            constraints.append(RoutingConstraint(
                ingredient_class="potassium",
                constraint_type="blocked",
                reason=f"Creatinine elevated ({val} mg/dL). Potassium supplementation risky.",
                source_marker="creatinine",
                source_value=val,
                severity="hard"
            ))
            constraints.append(RoutingConstraint(
                ingredient_class="magnesium",
                constraint_type="caution",
                reason=f"Creatinine elevated ({val} mg/dL). Magnesium clearance may be impaired.",
                source_marker="creatinine",
                source_value=val,
                severity="soft"
            ))
    
    if "egfr" in markers:
        val = markers["egfr"]
        if val < 60:
            # Reduced kidney function
            constraints.append(RoutingConstraint(
                ingredient_class="protein_supplements",
                constraint_type="caution",
                reason=f"eGFR reduced ({val} mL/min). High protein may stress kidneys.",
                source_marker="egfr",
                source_value=val,
                severity="soft"
            ))
    
    # -----------------------------------------
    # LIVER FUNCTION CONSTRAINTS
    # -----------------------------------------
    if "alt" in markers:
        val = markers["alt"]
        if val > 100:
            # Significantly elevated ALT
            constraints.append(RoutingConstraint(
                ingredient_class="hepatotoxic",
                constraint_type="blocked",
                reason=f"ALT significantly elevated ({val} U/L). Hepatotoxic supplements blocked.",
                source_marker="alt",
                source_value=val,
                severity="hard"
            ))
            # Block specific ingredients known for liver concerns
            for ingredient in ["kava", "high_dose_niacin", "green_tea_extract_high"]:
                constraints.append(RoutingConstraint(
                    ingredient_class=ingredient,
                    constraint_type="blocked",
                    reason=f"ALT elevated ({val} U/L). {ingredient} contraindicated.",
                    source_marker="alt",
                    source_value=val,
                    severity="hard"
                ))
    
    # -----------------------------------------
    # POTASSIUM CONSTRAINTS
    # -----------------------------------------
    if "potassium" in markers:
        val = markers["potassium"]
        if val > 5.0:
            constraints.append(RoutingConstraint(
                ingredient_class="potassium",
                constraint_type="blocked",
                reason=f"Potassium elevated ({val} mEq/L). Supplementation contraindicated.",
                source_marker="potassium",
                source_value=val,
                severity="hard"
            ))
        elif val < 3.5:
            constraints.append(RoutingConstraint(
                ingredient_class="potassium",
                constraint_type="required",
                reason=f"Potassium low ({val} mEq/L). Supplementation may be indicated.",
                source_marker="potassium",
                source_value=val,
                severity="soft"
            ))
    
    # -----------------------------------------
    # CALCIUM CONSTRAINTS
    # -----------------------------------------
    if "calcium" in markers:
        val = markers["calcium"]
        if val > 10.5:
            constraints.append(RoutingConstraint(
                ingredient_class="calcium",
                constraint_type="blocked",
                reason=f"Calcium elevated ({val} mg/dL). Supplementation contraindicated.",
                source_marker="calcium",
                source_value=val,
                severity="hard"
            ))
            constraints.append(RoutingConstraint(
                ingredient_class="vitamin_d_high",
                constraint_type="caution",
                reason=f"Calcium elevated ({val} mg/dL). High-dose Vitamin D may worsen.",
                source_marker="calcium",
                source_value=val,
                severity="soft"
            ))
    
    # -----------------------------------------
    # THYROID CONSTRAINTS
    # -----------------------------------------
    if "tsh" in markers:
        val = markers["tsh"]
        if val < 0.4:
            # Hyperthyroid - avoid iodine
            constraints.append(RoutingConstraint(
                ingredient_class="iodine",
                constraint_type="blocked",
                reason=f"TSH suppressed ({val} mIU/L). Iodine supplementation contraindicated.",
                source_marker="tsh",
                source_value=val,
                severity="hard"
            ))
        elif val > 4.5:
            # Hypothyroid - iodine may help but caution
            constraints.append(RoutingConstraint(
                ingredient_class="iodine",
                constraint_type="caution",
                reason=f"TSH elevated ({val} mIU/L). Iodine needs medical supervision.",
                source_marker="tsh",
                source_value=val,
                severity="soft"
            ))
    
    # -----------------------------------------
    # BLOOD SUGAR CONSTRAINTS
    # -----------------------------------------
    if "hba1c" in markers:
        val = markers["hba1c"]
        if val >= 6.5:
            # Diabetic range
            constraints.append(RoutingConstraint(
                ingredient_class="high_glycemic",
                constraint_type="blocked",
                reason=f"HbA1c in diabetic range ({val}%). High-glycemic supplements blocked.",
                source_marker="hba1c",
                source_value=val,
                severity="soft"
            ))
            constraints.append(RoutingConstraint(
                ingredient_class="blood_sugar_support",
                constraint_type="required",
                reason=f"HbA1c elevated ({val}%). Blood sugar support indicated.",
                source_marker="hba1c",
                source_value=val,
                severity="soft"
            ))
    
    # -----------------------------------------
    # INFLAMMATION CONSTRAINTS
    # -----------------------------------------
    if "crp" in markers:
        val = markers["crp"]
        if val > 10:
            # Acute inflammation - defer supplementation
            constraints.append(RoutingConstraint(
                ingredient_class="immune_stimulants",
                constraint_type="caution",
                reason=f"CRP highly elevated ({val} mg/L). Acute inflammation - medical review needed.",
                source_marker="crp",
                source_value=val,
                severity="soft"
            ))
    
    return constraints


# ============================================
# ASSESSMENT CONTEXT BUILDER
# ============================================

def build_assessment_context(signal_data: Dict) -> Dict:
    """
    Build assessment context from bloodwork signal.
    This context is passed to compose phase.
    """
    markers = signal_data.get("markers", {})
    
    # Categorize markers by status
    deficient = []
    suboptimal = []
    optimal = []
    elevated = []
    
    # Vitamin D
    if "vitamin_d" in markers:
        val = markers["vitamin_d"]
        if val < 20:
            deficient.append({"marker": "vitamin_d", "value": val, "unit": "ng/mL", "status": "deficient"})
        elif val < 30:
            suboptimal.append({"marker": "vitamin_d", "value": val, "unit": "ng/mL", "status": "suboptimal"})
        elif val <= 80:
            optimal.append({"marker": "vitamin_d", "value": val, "unit": "ng/mL", "status": "optimal"})
        else:
            elevated.append({"marker": "vitamin_d", "value": val, "unit": "ng/mL", "status": "elevated"})
    
    # B12
    if "b12" in markers:
        val = markers["b12"]
        if val < 200:
            deficient.append({"marker": "b12", "value": val, "unit": "pg/mL", "status": "deficient"})
        elif val < 400:
            suboptimal.append({"marker": "b12", "value": val, "unit": "pg/mL", "status": "suboptimal"})
        elif val <= 900:
            optimal.append({"marker": "b12", "value": val, "unit": "pg/mL", "status": "optimal"})
        else:
            elevated.append({"marker": "b12", "value": val, "unit": "pg/mL", "status": "elevated"})
    
    # Ferritin
    if "ferritin" in markers:
        val = markers["ferritin"]
        gender = signal_data.get("gender", "unknown").lower()
        low_threshold = 30 if gender == "female" else 50
        if val < low_threshold / 2:
            deficient.append({"marker": "ferritin", "value": val, "unit": "ng/mL", "status": "deficient"})
        elif val < low_threshold:
            suboptimal.append({"marker": "ferritin", "value": val, "unit": "ng/mL", "status": "suboptimal"})
        elif val <= 200:
            optimal.append({"marker": "ferritin", "value": val, "unit": "ng/mL", "status": "optimal"})
        else:
            elevated.append({"marker": "ferritin", "value": val, "unit": "ng/mL", "status": "elevated"})
    
    # Magnesium
    if "magnesium" in markers:
        val = markers["magnesium"]
        if val < 1.8:
            deficient.append({"marker": "magnesium", "value": val, "unit": "mg/dL", "status": "deficient"})
        elif val < 2.0:
            suboptimal.append({"marker": "magnesium", "value": val, "unit": "mg/dL", "status": "suboptimal"})
        elif val <= 2.5:
            optimal.append({"marker": "magnesium", "value": val, "unit": "mg/mL", "status": "optimal"})
        else:
            elevated.append({"marker": "magnesium", "value": val, "unit": "mg/dL", "status": "elevated"})
    
    # Omega-3 Index
    if "omega3_index" in markers:
        val = markers["omega3_index"]
        if val < 4:
            deficient.append({"marker": "omega3_index", "value": val, "unit": "%", "status": "deficient"})
        elif val < 8:
            suboptimal.append({"marker": "omega3_index", "value": val, "unit": "%", "status": "suboptimal"})
        else:
            optimal.append({"marker": "omega3_index", "value": val, "unit": "%", "status": "optimal"})
    
    # Homocysteine
    if "homocysteine" in markers:
        val = markers["homocysteine"]
        if val > 15:
            elevated.append({"marker": "homocysteine", "value": val, "unit": "umol/L", "status": "elevated"})
        elif val > 10:
            suboptimal.append({"marker": "homocysteine", "value": val, "unit": "umol/L", "status": "suboptimal"})
        else:
            optimal.append({"marker": "homocysteine", "value": val, "unit": "umol/L", "status": "optimal"})
    
    return {
        "user_id": signal_data.get("user_id"),
        "gender": signal_data.get("gender", "unknown"),
        "test_date": signal_data.get("test_date"),
        "lab_source": signal_data.get("lab_source"),
        "markers_analyzed": len(markers),
        "summary": {
            "deficient_count": len(deficient),
            "suboptimal_count": len(suboptimal),
            "optimal_count": len(optimal),
            "elevated_count": len(elevated)
        },
        "deficient": deficient,
        "suboptimal": suboptimal,
        "optimal": optimal,
        "elevated": elevated
    }


# ============================================
# MAIN ORCHESTRATE FUNCTION
# ============================================

def run_orchestrate(
    signal_data: Dict[str, Any],
    provided_hash: Optional[str] = None,
    db_conn = None
) -> OrchestrateResult:
    """
    Execute orchestrate phase.
    
    Args:
        signal_data: Bloodwork signal with markers
        provided_hash: Optional hash to verify immutability
        db_conn: Database connection (optional, for persistence)
    
    Returns:
        OrchestrateResult with routing constraints
    """
    run_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"
    
    # -----------------------------------------
    # Step 1: Validate Signal
    # -----------------------------------------
    is_valid, error_msg = validate_signal(signal_data)
    if not is_valid:
        return OrchestrateResult(
            run_id=run_id,
            status=OrchestrateStatus.VALIDATION_ERROR,
            signal_hash="",
            routing_constraints=[],
            override_allowed=False,
            assessment_context={},
            audit={"created_at": now},
            error=error_msg
        )
    
    # -----------------------------------------
    # Step 2: Compute Signal Hash
    # -----------------------------------------
    # Hash only the markers for immutability check
    markers_to_hash = {
        "user_id": signal_data.get("user_id"),
        "markers": signal_data.get("markers")
    }
    computed_hash = canonicalize_and_hash(markers_to_hash)
    
    # -----------------------------------------
    # Step 3: Verify Immutability (if hash provided)
    # -----------------------------------------
    if provided_hash and provided_hash != computed_hash:
        return OrchestrateResult(
            run_id=run_id,
            status=OrchestrateStatus.IMMUTABILITY_VIOLATION,
            signal_hash=computed_hash,
            routing_constraints=[],
            override_allowed=False,
            assessment_context={},
            audit={
                "created_at": now,
                "expected_hash": provided_hash,
                "computed_hash": computed_hash
            },
            error="Signal hash mismatch. Data may have been modified."
        )
    
    # -----------------------------------------
    # Step 4: Build Assessment Context
    # -----------------------------------------
    assessment_context = build_assessment_context(signal_data)
    
    # -----------------------------------------
    # Step 5: Derive Routing Constraints
    # -----------------------------------------
    markers = signal_data.get("markers", {})
    gender = signal_data.get("gender", "unknown")
    constraints = derive_routing_constraints(markers, gender)
    
    # Convert to dict for serialization
    constraints_list = [asdict(c) for c in constraints]
    
    # Determine if override is allowed (only if no hard blocks)
    hard_blocks = [c for c in constraints if c.severity == "hard"]
    override_allowed = len(hard_blocks) == 0
    
    # -----------------------------------------
    # Step 6: Compute Output Hash
    # -----------------------------------------
    output_data = {
        "run_id": run_id,
        "signal_hash": computed_hash,
        "routing_constraints": constraints_list,
        "assessment_context": assessment_context
    }
    output_hash = canonicalize_and_hash(output_data)
    
    # -----------------------------------------
    # Step 7: Persist to Database (if connection provided)
    # -----------------------------------------
    if db_conn:
        try:
            _persist_orchestrate_run(
                db_conn,
                run_id=run_id,
                user_id=signal_data.get("user_id"),
                signal_data=signal_data,
                signal_hash=computed_hash,
                output_data=output_data,
                output_hash=output_hash,
                constraints_list=constraints_list
            )
        except Exception as e:
            return OrchestrateResult(
                run_id=run_id,
                status=OrchestrateStatus.DB_ERROR,
                signal_hash=computed_hash,
                routing_constraints=constraints_list,
                override_allowed=override_allowed,
                assessment_context=assessment_context,
                audit={"created_at": now, "output_hash": output_hash},
                error=f"Database error: {str(e)}"
            )
    
    # -----------------------------------------
    # Step 8: Return Result
    # -----------------------------------------
    return OrchestrateResult(
        run_id=run_id,
        status=OrchestrateStatus.SUCCESS,
        signal_hash=computed_hash,
        routing_constraints=constraints_list,
        override_allowed=override_allowed,
        assessment_context=assessment_context,
        audit={
            "created_at": now,
            "output_hash": output_hash,
            "persisted": db_conn is not None
        }
    )


def _persist_orchestrate_run(
    conn,
    run_id: str,
    user_id: str,
    signal_data: Dict,
    signal_hash: str,
    output_data: Dict,
    output_hash: str,
    constraints_list: List[Dict]
):
    """Persist orchestrate run to database."""
    import json
    
    cur = conn.cursor()
    
    # Insert brain_run
    cur.execute("""
        INSERT INTO brain_runs (id, user_id, status, input_hash, output_hash, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (run_id, user_id, "completed", signal_hash, output_hash))
    
    # Insert signal to registry (upsert)
    cur.execute("""
        INSERT INTO signal_registry (user_id, signal_type, signal_hash, signal_json, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (user_id, signal_type, signal_hash) DO NOTHING
    """, (user_id, "bloodwork", signal_hash.replace("sha256:", ""), json.dumps(signal_data)))
    
    # Insert decision output
    cur.execute("""
        INSERT INTO decision_outputs (run_id, phase, output_json, output_hash, created_at)
        VALUES (%s, %s, %s, %s, NOW())
    """, (run_id, "orchestrate", json.dumps(output_data), output_hash.replace("sha256:", "")))
    
    # Insert audit log
    cur.execute("""
        INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
        VALUES (%s, %s, %s, %s, NOW())
    """, ("brain_run", run_id, "orchestrate_completed", json.dumps({
        "signal_hash": signal_hash,
        "output_hash": output_hash,
        "constraints_count": len(constraints_list)
    })))
    
    conn.commit()
    cur.close()
