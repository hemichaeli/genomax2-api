"""
GenoMAX² Brain - Orchestrate V2 Bloodwork Integration
=====================================================
Integration layer connecting Brain Orchestrate V2 endpoint
with the Bloodwork Engine via the handoff protocol.

STRICT MODE: Blood does not negotiate.
- Bloodwork unavailable = 503 BLOODWORK_UNAVAILABLE (hard abort)
- Invalid schema = 502 BLOODWORK_INVALID_HANDOFF (hard abort)
- No graceful degradation permitted

Usage:
    from app.brain.orchestrate_v2_bloodwork import (
        BloodworkInputV2,
        orchestrate_with_bloodwork_input
    )
"""

import uuid
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from pydantic import BaseModel, Field, validator

from app.brain.bloodwork_handoff import (
    BloodworkHandoffV1,
    BloodworkHandoffException,
    BloodworkHandoffError,
    fetch_bloodwork_handoff,
    merge_routing_constraints,
    handoff_to_decision_output
)
from app.shared.hashing import canonicalize_and_hash


# ============================================
# PYDANTIC MODELS FOR API
# ============================================

class MarkerInput(BaseModel):
    """Single biomarker input."""
    code: str = Field(..., description="Marker code (e.g., 'ferritin', 'vitamin_d')")
    value: float = Field(..., description="Numeric value")
    unit: str = Field(..., description="Unit of measurement (e.g., 'ng/mL')")


class BloodworkInputV2(BaseModel):
    """
    Bloodwork input for Brain Orchestrate V2.
    
    When provided, orchestrator will call Bloodwork Engine
    and integrate the handoff into routing constraints.
    """
    markers: List[MarkerInput] = Field(
        ..., 
        min_items=1,
        description="Array of biomarker readings"
    )
    lab_profile: str = Field(
        default="GLOBAL_CONSERVATIVE",
        description="Lab profile for reference ranges"
    )
    sex: Optional[str] = Field(
        default=None,
        description="Biological sex (male/female) for sex-specific ranges"
    )
    age: Optional[int] = Field(
        default=None,
        ge=0,
        le=150,
        description="Age in years"
    )
    
    @validator('sex')
    def validate_sex(cls, v):
        if v is not None and v not in ['male', 'female']:
            raise ValueError('sex must be "male" or "female"')
        return v
    
    def to_markers_list(self) -> List[Dict[str, Any]]:
        """Convert to format expected by Bloodwork Engine."""
        return [
            {"code": m.code, "value": m.value, "unit": m.unit}
            for m in self.markers
        ]


# ============================================
# RESULT DATACLASS
# ============================================

@dataclass
class BloodworkIntegrationResult:
    """Result of bloodwork integration into orchestration."""
    success: bool
    handoff: Optional[BloodworkHandoffV1]
    merged_constraints: Dict[str, List[str]]
    persistence_data: Optional[Dict[str, Any]]
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    http_code: int = 200


# ============================================
# MAIN INTEGRATION FUNCTION
# ============================================

def orchestrate_with_bloodwork_input(
    bloodwork_input: BloodworkInputV2,
    brain_constraints: Optional[Dict[str, List[str]]] = None,
    run_id: Optional[str] = None,
    db_conn = None
) -> BloodworkIntegrationResult:
    """
    Orchestrate with bloodwork input integration.
    
    STRICT MODE:
    - Raises BloodworkHandoffException on failure (caller should handle)
    - No fallback behavior
    - Blood constraints always take precedence
    
    Args:
        bloodwork_input: Validated BloodworkInputV2 model
        brain_constraints: Optional existing constraints from brain logic
        run_id: Optional run ID (generated if not provided)
        db_conn: Optional database connection for persistence
    
    Returns:
        BloodworkIntegrationResult with handoff and merged constraints
    
    Raises:
        BloodworkHandoffException: On bloodwork unavailability or invalid response
    """
    if run_id is None:
        run_id = str(uuid.uuid4())
    
    if brain_constraints is None:
        brain_constraints = {
            "blocked_ingredients": [],
            "blocked_categories": [],
            "caution_flags": [],
            "requirements": [],
            "reason_codes": []
        }
    
    # -----------------------------------------
    # Step 1: Call Bloodwork Engine
    # This will raise BloodworkHandoffException on failure
    # -----------------------------------------
    handoff = fetch_bloodwork_handoff(
        markers=bloodwork_input.to_markers_list(),
        lab_profile=bloodwork_input.lab_profile,
        sex=bloodwork_input.sex,
        age=bloodwork_input.age
    )
    
    # -----------------------------------------
    # Step 2: Extract blood constraints
    # -----------------------------------------
    blood_constraints = handoff.output.get("routing_constraints", {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "caution_flags": [],
        "requirements": [],
        "reason_codes": []
    })
    
    # -----------------------------------------
    # Step 3: Merge constraints (blood takes precedence)
    # Union, dedupe, alphabetical sort for determinism
    # -----------------------------------------
    merged_constraints = merge_routing_constraints(
        blood_constraints,
        brain_constraints
    )
    
    # -----------------------------------------
    # Step 4: Prepare persistence data
    # -----------------------------------------
    persistence_data = handoff_to_decision_output(handoff, run_id)
    
    # -----------------------------------------
    # Step 5: Persist to database if connection provided
    # -----------------------------------------
    if db_conn is not None:
        _persist_handoff(db_conn, persistence_data)
    
    return BloodworkIntegrationResult(
        success=True,
        handoff=handoff,
        merged_constraints=merged_constraints,
        persistence_data=persistence_data,
        http_code=200
    )


def _persist_handoff(conn, persistence_data: Dict[str, Any]) -> None:
    """
    Persist handoff to decision_outputs table.
    
    Args:
        conn: Database connection
        persistence_data: Output from handoff_to_decision_output()
    """
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO decision_outputs (run_id, phase, output_json, output_hash, created_at)
        VALUES (%s, %s, %s, %s, NOW())
    """, (
        persistence_data["run_id"],
        persistence_data["phase"],
        json.dumps(persistence_data["output_json"]),
        persistence_data["output_hash"].replace("sha256:", "")
    ))
    
    conn.commit()
    cur.close()


# ============================================
# HELPER: ERROR RESPONSE BUILDER
# ============================================

def build_bloodwork_error_response(
    exception: BloodworkHandoffException
) -> Dict[str, Any]:
    """
    Build API error response from BloodworkHandoffException.
    
    For use in FastAPI exception handlers.
    """
    return {
        "success": False,
        "error": {
            "code": exception.error_code.value,
            "message": exception.message,
            "type": "BLOODWORK_HANDOFF_ERROR"
        },
        "http_status": exception.http_code
    }


# ============================================
# EXPORTS
# ============================================

__all__ = [
    "BloodworkInputV2",
    "MarkerInput",
    "BloodworkIntegrationResult",
    "orchestrate_with_bloodwork_input",
    "build_bloodwork_error_response"
]
