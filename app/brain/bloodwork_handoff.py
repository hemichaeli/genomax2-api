"""
GenoMAX² Brain - Bloodwork Handoff Integration
===============================================
Handles communication between Brain Orchestrator and Bloodwork Engine.

STRICT MODE: Blood does not negotiate.
- Bloodwork unavailable = 503 BLOODWORK_UNAVAILABLE (hard abort)
- Invalid schema = 502 BLOODWORK_INVALID_HANDOFF (hard abort)
- Incomplete panel = proceed with BLOODWORK_INCOMPLETE_PANEL flag

v1.1.0 - Hash Format Fix:
- Added _is_valid_sha256_hash() helper function
- _build_handoff_from_response() now validates and regenerates hashes
- Ensures audit.input_hash and audit.output_hash match schema pattern
"""

import re
import json
import httpx
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Conditional import for jsonschema
try:
    import jsonschema
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False

from app.shared.hashing import canonicalize_and_hash


# ============================================
# CONFIGURATION
# ============================================

# FIXED: Was incorrectly pointing to PostgreSQL URL (web-production-97b74)
# Correct API URL is web-production-7110
BLOODWORK_BASE_URL = "https://web-production-7110.up.railway.app"
BLOODWORK_ENDPOINT = "/api/v1/bloodwork/process"
BLOODWORK_TIMEOUT_SECONDS = 30.0
ENGINE_VERSION = "1.0.0"

# Minimum markers to NOT flag as incomplete
MINIMUM_PANEL_SIZE = 3

# Hash format validation pattern (matches schema requirement)
SHA256_HASH_PATTERN = re.compile(r'^sha256:[a-f0-9]{64}$')


# ============================================
# ERROR CODES
# ============================================

class BloodworkHandoffError(Enum):
    BLOODWORK_UNAVAILABLE = "BLOODWORK_UNAVAILABLE"
    BLOODWORK_INVALID_HANDOFF = "BLOODWORK_INVALID_HANDOFF"
    BLOODWORK_TIMEOUT = "BLOODWORK_TIMEOUT"
    BLOODWORK_API_ERROR = "BLOODWORK_API_ERROR"


class BloodworkHandoffException(Exception):
    """Exception for bloodwork handoff failures."""
    
    def __init__(self, error_code: BloodworkHandoffError, message: str, http_code: int = 503):
        self.error_code = error_code
        self.message = message
        self.http_code = http_code
        super().__init__(f"{error_code.value}: {message}")


# ============================================
# HANDOFF DATA STRUCTURES
# ============================================

@dataclass
class RoutingConstraints:
    """Routing constraints derived from bloodwork."""
    blocked_ingredients: List[str] = field(default_factory=list)
    blocked_categories: List[str] = field(default_factory=list)
    caution_flags: List[str] = field(default_factory=list)
    requirements: List[str] = field(default_factory=list)
    reason_codes: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, List[str]]:
        return {
            "blocked_ingredients": sorted(set(self.blocked_ingredients)),
            "blocked_categories": sorted(set(self.blocked_categories)),
            "caution_flags": sorted(set(self.caution_flags)),
            "requirements": sorted(set(self.requirements)),
            "reason_codes": sorted(set(self.reason_codes))
        }


@dataclass
class BloodworkHandoffV1:
    """Canonical handoff object from Bloodwork Engine."""
    handoff_version: str
    source: Dict[str, str]
    input: Dict[str, Any]
    output: Dict[str, Any]
    audit: Dict[str, str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "handoff_version": self.handoff_version,
            "source": self.source,
            "input": self.input,
            "output": self.output,
            "audit": self.audit
        }


# ============================================
# HASH VALIDATION HELPER
# ============================================

def _is_valid_sha256_hash(hash_value: Any) -> bool:
    """
    Check if hash matches the schema-required format: sha256:<64-hex-chars>
    
    Returns:
        True if hash matches pattern ^sha256:[a-f0-9]{64}$
        False otherwise
    """
    if not isinstance(hash_value, str):
        return False
    return bool(SHA256_HASH_PATTERN.match(hash_value))


# ============================================
# SCHEMA LOADING AND VALIDATION
# ============================================

_schema_cache: Optional[Dict] = None


def load_handoff_schema() -> Dict:
    """Load the BloodworkHandoffV1 JSON Schema."""
    global _schema_cache
    
    if _schema_cache is not None:
        return _schema_cache
    
    schema_path = Path(__file__).parent / "schemas" / "bloodwork_handoff.schema.v1.json"
    
    if not schema_path.exists():
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_INVALID_HANDOFF,
            f"Schema file not found: {schema_path}",
            http_code=500
        )
    
    with open(schema_path, 'r') as f:
        _schema_cache = json.load(f)
    
    return _schema_cache


def validate_handoff_schema(handoff: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate handoff object against JSON Schema.
    
    Returns: (is_valid, error_message)
    """
    if not JSONSCHEMA_AVAILABLE:
        # Fallback: basic structural validation
        return _basic_validate(handoff)
    
    try:
        schema = load_handoff_schema()
        jsonschema.validate(instance=handoff, schema=schema)
        return True, None
    except jsonschema.ValidationError as e:
        return False, f"Schema validation failed: {e.message} at path {list(e.path)}"
    except Exception as e:
        return False, f"Schema validation error: {str(e)}"


def _basic_validate(handoff: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Fallback validation when jsonschema not available."""
    required_keys = ["handoff_version", "source", "input", "output", "audit"]
    
    for key in required_keys:
        if key not in handoff:
            return False, f"Missing required field: {key}"
    
    if handoff.get("handoff_version") != "bloodwork_handoff.v1":
        return False, f"Invalid handoff_version: {handoff.get('handoff_version')}"
    
    output = handoff.get("output", {})
    if "routing_constraints" not in output:
        return False, "Missing output.routing_constraints"
    
    constraints = output["routing_constraints"]
    required_constraint_fields = [
        "blocked_ingredients", "blocked_categories", 
        "caution_flags", "requirements", "reason_codes"
    ]
    
    for fld in required_constraint_fields:
        if fld not in constraints:
            return False, f"Missing routing_constraints.{fld}"
        if not isinstance(constraints[fld], list):
            return False, f"routing_constraints.{fld} must be an array"
    
    audit = handoff.get("audit", {})
    required_audit_fields = ["input_hash", "output_hash", "ruleset_version", "processed_at"]
    
    for fld in required_audit_fields:
        if fld not in audit:
            return False, f"Missing audit.{fld}"
    
    return True, None


# ============================================
# BLOODWORK ENGINE CLIENT
# ============================================

async def fetch_bloodwork_handoff_async(
    markers: List[Dict[str, Any]],
    lab_profile: str = "GLOBAL_CONSERVATIVE",
    sex: Optional[str] = None,
    age: Optional[int] = None
) -> BloodworkHandoffV1:
    """
    Fetch bloodwork processing result from Bloodwork Engine (async).
    
    STRICT MODE: Raises BloodworkHandoffException on failure.
    """
    request_payload = {
        "markers": markers,
        "lab_profile": lab_profile,
        "sex": sex,
        "age": age
    }
    
    try:
        async with httpx.AsyncClient(timeout=BLOODWORK_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{BLOODWORK_BASE_URL}{BLOODWORK_ENDPOINT}",
                json=request_payload
            )
            
            if response.status_code != 200:
                raise BloodworkHandoffException(
                    BloodworkHandoffError.BLOODWORK_API_ERROR,
                    f"Bloodwork Engine returned HTTP {response.status_code}: {response.text}",
                    http_code=502
                )
            
            return _build_handoff_from_response(response.json(), request_payload)
            
    except httpx.TimeoutException:
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_TIMEOUT,
            f"Bloodwork Engine request timed out after {BLOODWORK_TIMEOUT_SECONDS}s",
            http_code=503
        )
    except httpx.ConnectError as e:
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_UNAVAILABLE,
            f"Cannot connect to Bloodwork Engine: {str(e)}",
            http_code=503
        )
    except BloodworkHandoffException:
        raise
    except Exception as e:
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_UNAVAILABLE,
            f"Unexpected error calling Bloodwork Engine: {str(e)}",
            http_code=503
        )


def fetch_bloodwork_handoff(
    markers: List[Dict[str, Any]],
    lab_profile: str = "GLOBAL_CONSERVATIVE",
    sex: Optional[str] = None,
    age: Optional[int] = None
) -> BloodworkHandoffV1:
    """
    Fetch bloodwork processing result from Bloodwork Engine (sync).
    
    STRICT MODE: Raises BloodworkHandoffException on failure.
    """
    request_payload = {
        "markers": markers,
        "lab_profile": lab_profile,
        "sex": sex,
        "age": age
    }
    
    try:
        with httpx.Client(timeout=BLOODWORK_TIMEOUT_SECONDS) as client:
            response = client.post(
                f"{BLOODWORK_BASE_URL}{BLOODWORK_ENDPOINT}",
                json=request_payload
            )
            
            if response.status_code != 200:
                raise BloodworkHandoffException(
                    BloodworkHandoffError.BLOODWORK_API_ERROR,
                    f"Bloodwork Engine returned HTTP {response.status_code}: {response.text}",
                    http_code=502
                )
            
            return _build_handoff_from_response(response.json(), request_payload)
            
    except httpx.TimeoutException:
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_TIMEOUT,
            f"Bloodwork Engine request timed out after {BLOODWORK_TIMEOUT_SECONDS}s",
            http_code=503
        )
    except httpx.ConnectError as e:
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_UNAVAILABLE,
            f"Cannot connect to Bloodwork Engine: {str(e)}",
            http_code=503
        )
    except BloodworkHandoffException:
        raise
    except Exception as e:
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_UNAVAILABLE,
            f"Unexpected error calling Bloodwork Engine: {str(e)}",
            http_code=503
        )


def _build_handoff_from_response(
    api_response: Dict[str, Any],
    request_payload: Dict[str, Any]
) -> BloodworkHandoffV1:
    """
    Build canonical BloodworkHandoffV1 object from API response.
    
    Maps Bloodwork Engine response format to handoff schema.
    
    v1.1.0: Now validates hash format and regenerates if invalid.
    """
    # Extract routing constraints from API response
    routing_constraints_raw = api_response.get("routing_constraints", [])
    safety_gates = api_response.get("safety_gates", [])
    markers_response = api_response.get("markers", [])
    
    # Parse routing constraints
    blocked_ingredients = []
    blocked_categories = []
    caution_flags = []
    requirements = []
    reason_codes = []
    
    for constraint in routing_constraints_raw:
        if isinstance(constraint, str):
            # String format: "BLOCK_IRON", "CAUTION_VITAMIN_D", etc.
            if constraint.startswith("BLOCK_"):
                # Extract ingredient/category from constraint code
                ingredient = constraint.replace("BLOCK_", "").lower()
                blocked_ingredients.append(ingredient)
                reason_codes.append(constraint)
            elif constraint.startswith("CAUTION_"):
                item = constraint.replace("CAUTION_", "").lower()
                caution_flags.append(item)
                reason_codes.append(constraint)
            elif constraint.startswith("REQUIRE_"):
                item = constraint.replace("REQUIRE_", "").lower()
                requirements.append(item)
                reason_codes.append(constraint)
            else:
                reason_codes.append(constraint)
    
    # Also extract from safety gates for more detailed info
    for gate in safety_gates:
        gate_constraint = gate.get("routing_constraint", "")
        if gate_constraint.startswith("BLOCK_"):
            ingredient = gate_constraint.replace("BLOCK_", "").lower()
            if ingredient not in blocked_ingredients:
                blocked_ingredients.append(ingredient)
            if gate_constraint not in reason_codes:
                reason_codes.append(gate_constraint)
        elif gate_constraint.startswith("CAUTION_"):
            item = gate_constraint.replace("CAUTION_", "").lower()
            if item not in caution_flags:
                caution_flags.append(item)
            if gate_constraint not in reason_codes:
                reason_codes.append(gate_constraint)
    
    # Build signal flags
    signal_flags = []
    unknown_biomarkers = []
    
    valid_markers = 0
    for marker in markers_response:
        status = marker.get("status", "")
        if status == "UNKNOWN":
            unknown_biomarkers.append(marker.get("original_code", ""))
        elif status in ["RESOLVED", "RESOLVED_CONVERTED", "VALID"]:
            valid_markers += 1
    
    # Check for incomplete panel
    if valid_markers < MINIMUM_PANEL_SIZE:
        signal_flags.append("BLOODWORK_INCOMPLETE_PANEL")
    
    # Build processed markers summary
    processed_markers = []
    for marker in markers_response:
        processed_markers.append({
            "original_code": marker.get("original_code"),
            "canonical_code": marker.get("canonical_code"),
            "original_value": marker.get("original_value"),
            "canonical_value": marker.get("canonical_value"),
            "original_unit": marker.get("original_unit"),
            "canonical_unit": marker.get("canonical_unit"),
            "status": marker.get("status"),
            "range_status": marker.get("range_status"),
            "flags": marker.get("flags", [])
        })
    
    # Build summary
    summary = api_response.get("summary", {})
    
    # Build input object for hashing
    input_obj = {
        "lab_profile": request_payload.get("lab_profile", "GLOBAL_CONSERVATIVE"),
        "sex": request_payload.get("sex"),
        "age": request_payload.get("age"),
        "markers": request_payload.get("markers", [])
    }
    
    # Build output routing constraints object for hashing
    output_constraints = {
        "blocked_ingredients": sorted(set(blocked_ingredients)),
        "blocked_categories": sorted(set(blocked_categories)),
        "caution_flags": sorted(set(caution_flags)),
        "requirements": sorted(set(requirements)),
        "reason_codes": sorted(set(reason_codes))
    }
    
    # ============================================
    # HASH VALIDATION AND REGENERATION (v1.1.0)
    # ============================================
    # Validate API-returned hashes; if invalid, regenerate proper hashes
    api_input_hash = api_response.get("input_hash", "")
    api_output_hash = api_response.get("output_hash", "")
    
    # Use API hash if valid, otherwise regenerate
    if _is_valid_sha256_hash(api_input_hash):
        input_hash = api_input_hash
    else:
        input_hash = canonicalize_and_hash(input_obj)
    
    if _is_valid_sha256_hash(api_output_hash):
        output_hash = api_output_hash
    else:
        # Hash the full output object (constraints + other output fields)
        output_for_hash = {
            "routing_constraints": output_constraints,
            "signal_flags": signal_flags,
            "unknown_biomarkers": unknown_biomarkers,
            "processed_markers": processed_markers,
            "require_review": api_response.get("require_review", False)
        }
        output_hash = canonicalize_and_hash(output_for_hash)
    
    # Build the canonical handoff
    handoff_dict = {
        "handoff_version": "bloodwork_handoff.v1",
        "source": {
            "service": "bloodwork_engine",
            "base_url": BLOODWORK_BASE_URL,
            "endpoint": BLOODWORK_ENDPOINT,
            "engine_version": ENGINE_VERSION
        },
        "input": input_obj,
        "output": {
            "routing_constraints": output_constraints,
            "signal_flags": signal_flags,
            "unknown_biomarkers": unknown_biomarkers,
            "processed_markers": processed_markers,
            "safety_gates": [
                {
                    "gate_id": g.get("gate_id"),
                    "description": g.get("description"),
                    "trigger_marker": g.get("trigger_marker"),
                    "trigger_value": g.get("trigger_value"),
                    "threshold": g.get("threshold"),
                    "routing_constraint": g.get("routing_constraint"),
                    "exception_active": g.get("exception_active", False),
                    "exception_reason": g.get("exception_reason")
                }
                for g in safety_gates
            ],
            "summary": summary,
            "require_review": api_response.get("require_review", False)
        },
        "audit": {
            "input_hash": input_hash,
            "output_hash": output_hash,
            "ruleset_version": api_response.get("ruleset_version", "unknown"),
            "marker_registry_version": "registry_v1.0",
            "reference_ranges_version": "ranges_v1.0",
            "processed_at": api_response.get("processed_at", datetime.utcnow().isoformat() + "Z")
        }
    }
    
    # Validate against schema
    is_valid, error_msg = validate_handoff_schema(handoff_dict)
    if not is_valid:
        raise BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_INVALID_HANDOFF,
            f"Handoff schema validation failed: {error_msg}",
            http_code=502
        )
    
    return BloodworkHandoffV1(
        handoff_version=handoff_dict["handoff_version"],
        source=handoff_dict["source"],
        input=handoff_dict["input"],
        output=handoff_dict["output"],
        audit=handoff_dict["audit"]
    )


# ============================================
# CONSTRAINT MERGING
# ============================================

def merge_routing_constraints(
    blood_constraints: Dict[str, List[str]],
    brain_constraints: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """
    Merge bloodwork and brain-derived routing constraints.
    
    Rules:
    1. Union all sources
    2. Deduplicate
    3. Alphabetical sort (determinism for hash stability)
    
    PRECEDENCE: Blood constraints cannot be removed.
    Brain can add constraints but never remove blood-derived ones.
    """
    merged = {}
    
    for fld in ["blocked_ingredients", "blocked_categories", "caution_flags", "requirements", "reason_codes"]:
        blood_items = set(blood_constraints.get(fld, []))
        brain_items = set(brain_constraints.get(fld, []))
        
        # Union, dedupe, sort
        merged[fld] = sorted(blood_items | brain_items)
    
    return merged


def blood_blocks_ingredient(
    handoff: BloodworkHandoffV1,
    ingredient: str
) -> bool:
    """
    Check if bloodwork blocks a specific ingredient.
    
    Used by downstream layers to enforce blood precedence.
    """
    blocked = handoff.output.get("routing_constraints", {}).get("blocked_ingredients", [])
    return ingredient.lower() in [b.lower() for b in blocked]


def get_blood_cautions(handoff: BloodworkHandoffV1) -> List[str]:
    """Get list of ingredients requiring caution from bloodwork."""
    return handoff.output.get("routing_constraints", {}).get("caution_flags", [])


def has_incomplete_panel(handoff: BloodworkHandoffV1) -> bool:
    """Check if bloodwork panel was flagged as incomplete."""
    flags = handoff.output.get("signal_flags", [])
    return "BLOODWORK_INCOMPLETE_PANEL" in flags


# ============================================
# PERSISTENCE HELPERS
# ============================================

def handoff_to_decision_output(handoff: BloodworkHandoffV1, run_id: str) -> Dict[str, Any]:
    """
    Prepare handoff for persistence to decision_outputs table.
    
    Returns dict with:
    - run_id
    - phase: "bloodwork_handoff"
    - output_json: canonical handoff
    - output_hash: hash of handoff
    """
    handoff_dict = handoff.to_dict()
    output_hash = canonicalize_and_hash(handoff_dict)
    
    return {
        "run_id": run_id,
        "phase": "bloodwork_handoff",
        "output_json": handoff_dict,
        "output_hash": output_hash
    }
