"""
GenoMAX² Bloodwork Engine v2.0
==============================
Safety and routing layer for biomarker normalization.

This module:
- Normalizes biomarkers to canonical units
- Flags out-of-range or missing data
- Evaluates safety gates based on decision limits (31 gates across 3 tiers)
- Emits RoutingConstraints

This module MUST NOT:
- Diagnose conditions
- Generate recommendations
- Infer missing values
- Expand biomarker scope beyond allowed markers

CHANGELOG:
- v2.0: Dynamic safety gate loading from reference_ranges_v2_0.json (31 gates)
- v1.0: Initial release with 5 hardcoded safety gates
"""

import json
import logging
import hashlib
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime

# Configure logging
logger = logging.getLogger("bloodwork_engine")
logger.setLevel(logging.INFO)

# ============================================================
# ENUMS AND DATA CLASSES
# ============================================================

class MarkerStatus(str, Enum):
    """Status of a processed marker."""
    VALID = "VALID"
    UNKNOWN = "UNKNOWN"  # Marker not in registry
    MISSING_RANGE = "MISSING_RANGE"  # No reference range available
    INVALID_UNIT = "INVALID_UNIT"  # Unit not allowed for marker
    CONVERSION_APPLIED = "CONVERSION_APPLIED"  # Unit was converted


class RangeStatus(str, Enum):
    """Status of range evaluation."""
    IN_RANGE = "IN_RANGE"
    OPTIMAL = "OPTIMAL"
    LOW = "LOW"
    HIGH = "HIGH"
    CRITICAL_LOW = "CRITICAL_LOW"
    CRITICAL_HIGH = "CRITICAL_HIGH"
    MISSING_RANGE = "MISSING_RANGE"
    REQUIRE_REVIEW = "REQUIRE_REVIEW"


@dataclass
class ProcessedMarker:
    """Result of processing a single biomarker."""
    original_code: str
    canonical_code: Optional[str]
    original_value: float
    canonical_value: float
    original_unit: str
    canonical_unit: Optional[str]
    status: MarkerStatus
    range_status: RangeStatus
    reference_range: Optional[Dict[str, float]] = None
    genomax_optimal: Optional[Dict[str, float]] = None
    decision_limits: Optional[Dict[str, float]] = None
    lab_profile_used: Optional[str] = None
    fallback_used: bool = False
    conversion_applied: bool = False
    conversion_multiplier: Optional[float] = None
    flags: List[str] = field(default_factory=list)
    log_entries: List[str] = field(default_factory=list)
    is_genetic_marker: bool = False
    is_computed: bool = False


@dataclass
class SafetyGate:
    """A triggered safety gate."""
    gate_id: str
    name: str
    tier: str  # tier1_safety, tier2_optimization, tier3_genetic_hormonal
    description: str
    trigger_marker: Union[str, List[str]]
    trigger_value: float
    threshold: float
    action: str
    routing_constraint: str
    blocked_ingredients: List[str] = field(default_factory=list)
    caution_ingredients: List[str] = field(default_factory=list)
    recommended_ingredients: List[str] = field(default_factory=list)
    exception_active: bool = False
    exception_reason: Optional[str] = None
    sex_specific: Optional[str] = None


@dataclass
class BloodworkResult:
    """Complete result of bloodwork processing."""
    processed_at: str
    lab_profile: str
    markers: List[ProcessedMarker]
    routing_constraints: List[str]
    safety_gates: List[SafetyGate]
    require_review: bool
    summary: Dict[str, int]
    ruleset_version: str
    input_hash: str
    output_hash: str


# ============================================================
# DATA LOADER (SINGLETON CACHE)
# ============================================================

class BloodworkDataLoader:
    """
    Singleton loader for marker registry and reference ranges.
    Loads data once at startup and caches in memory.
    """
    _instance = None
    _loaded = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not BloodworkDataLoader._loaded:
            self._marker_registry = None
            self._reference_ranges = None
            self._marker_lookup = {}  # alias -> canonical code
            self._conversion_lookup = {}  # (code, from_unit, to_unit) -> multiplier
            self._safety_gates_config = {}  # Loaded gate definitions
            self._load_data()
            BloodworkDataLoader._loaded = True
    
    @classmethod
    def reset(cls):
        """Reset the singleton for testing purposes."""
        cls._instance = None
        cls._loaded = False
    
    def _load_data(self):
        """Load JSON data files from disk."""
        data_dir = Path(__file__).parent / "data"
        
        # Load marker registry
        registry_path = data_dir / "marker_registry_v1_0.json"
        if registry_path.exists():
            with open(registry_path, "r", encoding="utf-8") as f:
                self._marker_registry = json.load(f)
            logger.info(f"Loaded marker registry v{self._marker_registry.get('version', '?')}")
        else:
            logger.error(f"Marker registry not found: {registry_path}")
            self._marker_registry = {"markers": [], "allowed_marker_codes": [], "lab_profiles": []}
        
        # Load reference ranges - v2.0 with comprehensive safety gates
        ranges_path = data_dir / "reference_ranges_v2_0.json"
        if not ranges_path.exists():
            # Fallback to v1.0 if v2.0 doesn't exist
            ranges_path = data_dir / "reference_ranges_v1_0.json"
            logger.warning(f"v2.0 ranges not found, falling back to v1.0")
        
        if ranges_path.exists():
            with open(ranges_path, "r", encoding="utf-8") as f:
                self._reference_ranges = json.load(f)
            range_count = len(self._reference_ranges.get("ranges", []))
            gate_count = self._count_safety_gates()
            logger.info(f"Loaded reference ranges v{self._reference_ranges.get('version', '?')} ({range_count} ranges, {gate_count} safety gates)")
        else:
            logger.warning(f"Reference ranges not found")
            self._reference_ranges = {"ranges": [], "lab_profiles": [], "policy": {}, "safety_gates": {}}
        
        # Load safety gates configuration
        self._safety_gates_config = self._reference_ranges.get("safety_gates", {})
        
        # Build lookup indexes
        self._build_indexes()
        
        # Validate ranges against registry
        self._validate_ranges()
    
    def _count_safety_gates(self) -> int:
        """Count total safety gates across all tiers."""
        gates = self._reference_ranges.get("safety_gates", {})
        count = 0
        for tier_name, tier_data in gates.items():
            if isinstance(tier_data, dict) and "gates" in tier_data:
                count += len(tier_data["gates"])
        return count
    
    def _build_indexes(self):
        """Build fast lookup indexes from registry data."""
        # Alias -> canonical code mapping
        for marker in self._marker_registry.get("markers", []):
            code = marker["code"]
            self._marker_lookup[code.lower()] = code
            for alias in marker.get("aliases", []):
                self._marker_lookup[alias.lower()] = code
        
        # Conversion lookup: (code, from_unit, to_unit) -> multiplier
        for marker in self._marker_registry.get("markers", []):
            code = marker["code"]
            for conv in marker.get("conversions", []):
                key = (code, conv["from"].lower(), conv["to"].lower())
                self._conversion_lookup[key] = conv["multiplier"]
        
        logger.info(f"Built indexes: {len(self._marker_lookup)} aliases, {len(self._conversion_lookup)} conversions")
    
    def _validate_ranges(self):
        """Validate that all ranges reference valid markers and units."""
        allowed_codes = set(self._marker_registry.get("allowed_marker_codes", []))
        marker_units = {m["code"]: m["canonical_unit"] for m in self._marker_registry.get("markers", [])}
        
        for r in self._reference_ranges.get("ranges", []):
            marker_code = r.get("marker_code")
            
            # Check marker exists
            if marker_code not in allowed_codes:
                logger.error(f"VALIDATION_ERROR: Range references unknown marker '{marker_code}'")
            
            # Check unit matches
            expected_unit = marker_units.get(marker_code)
            range_unit = r.get("canonical_unit")
            if expected_unit and range_unit and expected_unit != range_unit:
                logger.error(f"VALIDATION_ERROR: Unit mismatch for '{marker_code}': expected '{expected_unit}', got '{range_unit}'")
    
    @property
    def allowed_marker_codes(self) -> List[str]:
        """Get list of allowed marker codes."""
        return self._marker_registry.get("allowed_marker_codes", [])
    
    @property
    def lab_profiles(self) -> List[str]:
        """Get list of supported lab profiles."""
        return self._marker_registry.get("lab_profiles", [])
    
    @property
    def marker_registry(self) -> Dict:
        """Get full marker registry."""
        return self._marker_registry
    
    @property
    def reference_ranges(self) -> Dict:
        """Get full reference ranges."""
        return self._reference_ranges
    
    @property
    def range_count(self) -> int:
        """Get count of defined ranges."""
        return len(self._reference_ranges.get("ranges", []))
    
    @property
    def ruleset_version(self) -> str:
        """Get combined ruleset version string."""
        registry_v = self._marker_registry.get("version", "?")
        ranges_v = self._reference_ranges.get("version", "?")
        return f"registry_v{registry_v}+ranges_v{ranges_v}"
    
    def get_safety_gates(self) -> Dict:
        """Get safety gate definitions."""
        return self._safety_gates_config
    
    def get_safety_gate_summary(self) -> Dict:
        """Get summary of safety gates by tier."""
        summary = {
            "total_gates": 0,
            "tier1_safety": 0,
            "tier2_optimization": 0,
            "tier3_genetic_hormonal": 0,
            "gates_by_tier": {}
        }
        
        for tier_name, tier_data in self._safety_gates_config.items():
            if isinstance(tier_data, dict) and "gates" in tier_data:
                gates = tier_data["gates"]
                gate_count = len(gates)
                summary["total_gates"] += gate_count
                summary[tier_name] = gate_count
                summary["gates_by_tier"][tier_name] = {
                    g["gate_id"]: {
                        "name": g.get("name"),
                        "trigger_marker": g.get("trigger_marker"),
                        "action": g.get("action")
                    }
                    for g in gates
                }
        
        return summary
    
    def get_marker_definition(self, code_or_alias: str) -> Optional[Dict]:
        """Get marker definition by code or alias."""
        canonical = self._marker_lookup.get(code_or_alias.lower())
        if not canonical:
            return None
        for marker in self._marker_registry.get("markers", []):
            if marker["code"] == canonical:
                return marker
        return None
    
    def resolve_marker_code(self, code_or_alias: str) -> Optional[str]:
        """Resolve alias to canonical marker code."""
        return self._marker_lookup.get(code_or_alias.lower())
    
    def get_conversion_multiplier(self, code: str, from_unit: str, to_unit: str) -> Optional[float]:
        """Get conversion multiplier for unit conversion."""
        key = (code, from_unit.lower(), to_unit.lower())
        return self._conversion_lookup.get(key)
    
    def get_reference_range(
        self, 
        marker_code: str, 
        lab_profile: str, 
        sex: Optional[str] = None,
        age: Optional[int] = None
    ) -> Tuple[Optional[Dict], str, bool]:
        """
        Get reference range for a marker.
        
        Returns:
            (range_dict, profile_used, fallback_used)
            If no range found, returns (None, None, False)
        """
        ranges = self._reference_ranges.get("ranges", [])
        
        # First try: exact lab profile match with exact sex match
        for r in ranges:
            if r.get("marker_code") == marker_code and r.get("lab_profile") == lab_profile:
                if self._matches_demographics(r, sex, age):
                    return (r, lab_profile, False)
        
        # Second try: GLOBAL_CONSERVATIVE fallback with exact sex match
        if lab_profile != "GLOBAL_CONSERVATIVE":
            for r in ranges:
                if r.get("marker_code") == marker_code and r.get("lab_profile") == "GLOBAL_CONSERVATIVE":
                    if self._matches_demographics(r, sex, age):
                        return (r, "GLOBAL_CONSERVATIVE", True)
        
        # Third try: "both" sex as fallback if sex-specific not found
        if sex:
            for r in ranges:
                if r.get("marker_code") == marker_code and r.get("lab_profile") == "GLOBAL_CONSERVATIVE":
                    if r.get("sex") == "both" and self._matches_age(r, age):
                        return (r, "GLOBAL_CONSERVATIVE", True)
        
        # No range found
        return (None, None, False)
    
    def _matches_demographics(self, range_def: Dict, sex: Optional[str], age: Optional[int]) -> bool:
        """Check if range definition matches demographics."""
        range_sex = range_def.get("sex")
        
        # Sex filter - "both" matches any sex, specific sex must match
        if range_sex and range_sex != "both":
            if sex and range_sex != sex:
                return False
        
        return self._matches_age(range_def, age)
    
    def _matches_age(self, range_def: Dict, age: Optional[int]) -> bool:
        """Check if range definition matches age."""
        age_min = range_def.get("age_min")
        age_max = range_def.get("age_max")
        if age is not None:
            if age_min is not None and age < age_min:
                return False
            if age_max is not None and age > age_max:
                return False
        return True


# ============================================================
# BLOODWORK ENGINE
# ============================================================

class BloodworkEngine:
    """
    Main engine for processing bloodwork data.
    
    Responsibilities:
    - Normalize markers to canonical codes and units
    - Apply unit conversions
    - Lookup reference ranges
    - Evaluate safety gates based on decision limits (31 gates)
    - Emit routing constraints and flags
    """
    
    def __init__(self, lab_profile: str = "GLOBAL_CONSERVATIVE"):
        self.loader = BloodworkDataLoader()
        self.lab_profile = lab_profile
        
        if lab_profile not in self.loader.lab_profiles:
            logger.warning(f"Unknown lab profile '{lab_profile}', using GLOBAL_CONSERVATIVE")
            self.lab_profile = "GLOBAL_CONSERVATIVE"
    
    def process_markers(
        self,
        markers: List[Dict[str, Any]],
        sex: Optional[str] = None,
        age: Optional[int] = None
    ) -> BloodworkResult:
        """
        Process a list of biomarkers.
        
        Args:
            markers: List of dicts with keys: code, value, unit
            sex: Optional sex for sex-specific ranges ("male" or "female")
            age: Optional age for age-specific ranges
        
        Returns:
            BloodworkResult with processed markers, constraints, and safety gates
        """
        # Compute input hash for determinism verification
        input_hash = self._compute_hash({"markers": markers, "sex": sex, "age": age, "lab_profile": self.lab_profile})
        
        processed = []
        routing_constraints = []
        require_review = False
        
        # Process each marker
        for marker_input in markers:
            result = self._process_single_marker(
                code=marker_input.get("code", ""),
                value=marker_input.get("value"),
                unit=marker_input.get("unit", ""),
                sex=sex,
                age=age
            )
            processed.append(result)
            
            # Collect basic routing constraints
            if result.range_status == RangeStatus.REQUIRE_REVIEW:
                require_review = True
                routing_constraints.append(f"REQUIRE_REVIEW:{result.canonical_code or result.original_code}")
            
            if result.status == MarkerStatus.UNKNOWN:
                routing_constraints.append(f"UNKNOWN_MARKER:{result.original_code}")
            
            if result.range_status in [RangeStatus.CRITICAL_LOW, RangeStatus.CRITICAL_HIGH]:
                routing_constraints.append(f"CRITICAL:{result.canonical_code}:{result.range_status.value}")
        
        # Evaluate cross-marker safety gates (all 31 gates from v2.0)
        safety_gates = self._evaluate_safety_gates(processed, sex, age)
        
        # Add safety gate routing constraints
        for gate in safety_gates:
            if not gate.exception_active:
                routing_constraints.append(gate.routing_constraint)
        
        # Build summary
        summary = {
            "total": len(processed),
            "valid": sum(1 for p in processed if p.status in [MarkerStatus.VALID, MarkerStatus.CONVERSION_APPLIED]),
            "unknown": sum(1 for p in processed if p.status == MarkerStatus.UNKNOWN),
            "missing_range": sum(1 for p in processed if p.range_status == RangeStatus.MISSING_RANGE),
            "require_review": sum(1 for p in processed if p.range_status == RangeStatus.REQUIRE_REVIEW),
            "conversions_applied": sum(1 for p in processed if p.conversion_applied),
            "in_range": sum(1 for p in processed if p.range_status == RangeStatus.IN_RANGE),
            "optimal": sum(1 for p in processed if p.range_status == RangeStatus.OPTIMAL),
            "out_of_range": sum(1 for p in processed if p.range_status in [RangeStatus.LOW, RangeStatus.HIGH]),
            "critical": sum(1 for p in processed if p.range_status in [RangeStatus.CRITICAL_LOW, RangeStatus.CRITICAL_HIGH]),
            "genetic_markers": sum(1 for p in processed if p.is_genetic_marker),
            "computed_markers": sum(1 for p in processed if p.is_computed)
        }
        
        # Create result (without output_hash initially)
        result = BloodworkResult(
            processed_at=datetime.utcnow().isoformat() + "Z",
            lab_profile=self.lab_profile,
            markers=processed,
            routing_constraints=list(set(routing_constraints)),  # Deduplicate
            safety_gates=safety_gates,
            require_review=require_review,
            summary=summary,
            ruleset_version=self.loader.ruleset_version,
            input_hash=input_hash,
            output_hash=""  # Will be set below
        )
        
        # Compute output hash for determinism verification
        result.output_hash = self._compute_result_hash(result)
        
        return result
    
    def _process_single_marker(
        self,
        code: str,
        value: Any,
        unit: str,
        sex: Optional[str],
        age: Optional[int]
    ) -> ProcessedMarker:
        """Process a single biomarker."""
        log_entries = []
        flags = []
        
        # Step 1: Resolve marker code
        canonical_code = self.loader.resolve_marker_code(code)
        
        if canonical_code is None:
            # UNKNOWN marker - not in registry
            logger.warning(f"UNKNOWN marker: '{code}' - not in allowed registry")
            log_entries.append(f"UNKNOWN: '{code}' not found in marker registry")
            
            return ProcessedMarker(
                original_code=code,
                canonical_code=None,
                original_value=float(value) if value is not None else 0.0,
                canonical_value=float(value) if value is not None else 0.0,
                original_unit=unit,
                canonical_unit=None,
                status=MarkerStatus.UNKNOWN,
                range_status=RangeStatus.REQUIRE_REVIEW,
                flags=["UNKNOWN_MARKER"],
                log_entries=log_entries
            )
        
        # Step 2: Get marker definition
        marker_def = self.loader.get_marker_definition(canonical_code)
        canonical_unit = marker_def["canonical_unit"]
        allowed_units = [u.lower() for u in marker_def.get("allowed_units", [])]
        is_genetic = marker_def.get("is_genetic", False)
        is_computed = marker_def.get("is_computed", False)
        
        # Step 3: Validate and convert unit
        original_value = float(value) if value is not None else 0.0
        canonical_value = original_value
        conversion_applied = False
        conversion_multiplier = None
        
        if unit.lower() not in allowed_units:
            # Invalid unit
            logger.warning(f"Invalid unit '{unit}' for marker '{canonical_code}'")
            log_entries.append(f"INVALID_UNIT: '{unit}' not in allowed units {allowed_units}")
            flags.append("INVALID_UNIT")
            
            return ProcessedMarker(
                original_code=code,
                canonical_code=canonical_code,
                original_value=original_value,
                canonical_value=original_value,
                original_unit=unit,
                canonical_unit=canonical_unit,
                status=MarkerStatus.INVALID_UNIT,
                range_status=RangeStatus.REQUIRE_REVIEW,
                flags=flags,
                log_entries=log_entries
            )
        
        # Step 4: Apply unit conversion if needed
        if unit.lower() != canonical_unit.lower():
            multiplier = self.loader.get_conversion_multiplier(canonical_code, unit, canonical_unit)
            if multiplier is not None:
                canonical_value = original_value * multiplier
                conversion_applied = True
                conversion_multiplier = multiplier
                logger.info(f"CONVERSION: {canonical_code} {original_value} {unit} -> {canonical_value} {canonical_unit} (x{multiplier})")
                log_entries.append(f"CONVERSION: {original_value} {unit} -> {canonical_value} {canonical_unit} (multiplier: {multiplier})")
            else:
                logger.warning(f"No conversion found: {canonical_code} from {unit} to {canonical_unit}")
                log_entries.append(f"CONVERSION_MISSING: no multiplier for {unit} -> {canonical_unit}")
                flags.append("CONVERSION_MISSING")
        
        # Step 5: Lookup reference range
        range_def, profile_used, fallback_used = self.loader.get_reference_range(
            marker_code=canonical_code,
            lab_profile=self.lab_profile,
            sex=sex,
            age=age
        )
        
        if fallback_used:
            logger.info(f"FALLBACK: {canonical_code} using {profile_used} instead of {self.lab_profile}")
            log_entries.append(f"FALLBACK: used {profile_used} (requested: {self.lab_profile})")
        
        # Step 6: Evaluate range status
        if range_def is None:
            # MISSING_RANGE - no reference range available
            logger.warning(f"MISSING_RANGE: {canonical_code} in profile {self.lab_profile}")
            log_entries.append(f"MISSING_RANGE: no reference range for {canonical_code}")
            flags.append("MISSING_RANGE")
            flags.append("REQUIRE_REVIEW")
            
            return ProcessedMarker(
                original_code=code,
                canonical_code=canonical_code,
                original_value=original_value,
                canonical_value=canonical_value,
                original_unit=unit,
                canonical_unit=canonical_unit,
                status=MarkerStatus.VALID if not conversion_applied else MarkerStatus.CONVERSION_APPLIED,
                range_status=RangeStatus.MISSING_RANGE,
                reference_range=None,
                genomax_optimal=None,
                decision_limits=None,
                lab_profile_used=profile_used,
                fallback_used=fallback_used,
                conversion_applied=conversion_applied,
                conversion_multiplier=conversion_multiplier,
                flags=flags,
                log_entries=log_entries,
                is_genetic_marker=is_genetic,
                is_computed=is_computed
            )
        
        # Step 7: Evaluate value against range (v2.0 format with genomax_optimal)
        range_status = self._evaluate_range_v2(canonical_value, range_def)
        
        # Step 8: Extract decision limits for safety gate evaluation
        decision_limits = range_def.get("decision_limits")
        genomax_optimal = range_def.get("genomax_optimal")
        lab_reference = range_def.get("lab_reference")
        critical = range_def.get("critical")
        
        # Build reference range dict for compatibility
        reference_range = {}
        if lab_reference:
            reference_range["low"] = lab_reference.get("low")
            reference_range["high"] = lab_reference.get("high")
        if critical:
            reference_range["critical_low"] = critical.get("low")
            reference_range["critical_high"] = critical.get("high")
        
        # Log decision limit breaches
        if decision_limits:
            for limit_name, limit_value in decision_limits.items():
                if limit_value is not None:
                    if canonical_value > limit_value:
                        log_entries.append(f"DECISION_LIMIT: {limit_name} breached ({canonical_value} > {limit_value})")
                        flags.append(f"LIMIT_BREACH:{limit_name}")
        
        return ProcessedMarker(
            original_code=code,
            canonical_code=canonical_code,
            original_value=original_value,
            canonical_value=canonical_value,
            original_unit=unit,
            canonical_unit=canonical_unit,
            status=MarkerStatus.VALID if not conversion_applied else MarkerStatus.CONVERSION_APPLIED,
            range_status=range_status,
            reference_range=reference_range if reference_range else None,
            genomax_optimal=genomax_optimal,
            decision_limits=decision_limits,
            lab_profile_used=profile_used,
            fallback_used=fallback_used,
            conversion_applied=conversion_applied,
            conversion_multiplier=conversion_multiplier,
            flags=flags,
            log_entries=log_entries,
            is_genetic_marker=is_genetic,
            is_computed=is_computed
        )
    
    def _evaluate_range_v2(self, value: float, range_def: Dict) -> RangeStatus:
        """Evaluate a value against v2.0 reference range format."""
        # Get critical limits
        critical = range_def.get("critical", {})
        critical_low = critical.get("low") if isinstance(critical, dict) else None
        critical_high = critical.get("high") if isinstance(critical, dict) else None
        
        # Get lab reference (normal range)
        lab_ref = range_def.get("lab_reference", {})
        low = lab_ref.get("low") if isinstance(lab_ref, dict) else None
        high = lab_ref.get("high") if isinstance(lab_ref, dict) else None
        
        # Get genomax optimal
        optimal = range_def.get("genomax_optimal", {})
        optimal_low = optimal.get("low") if isinstance(optimal, dict) else None
        optimal_high = optimal.get("high") if isinstance(optimal, dict) else None
        
        # Check critical first
        if critical_low is not None and value < critical_low:
            return RangeStatus.CRITICAL_LOW
        if critical_high is not None and value > critical_high:
            return RangeStatus.CRITICAL_HIGH
        
        # Check lab reference (normal range)
        if low is not None and value < low:
            return RangeStatus.LOW
        if high is not None and value > high:
            return RangeStatus.HIGH
        
        # Check optimal range
        if optimal_low is not None and optimal_high is not None:
            if optimal_low <= value <= optimal_high:
                return RangeStatus.OPTIMAL
        
        return RangeStatus.IN_RANGE
    
    def _evaluate_safety_gates(
        self, 
        processed_markers: List[ProcessedMarker], 
        sex: Optional[str],
        age: Optional[int] = None
    ) -> List[SafetyGate]:
        """
        Evaluate all safety gates dynamically from v2.0 configuration.
        
        Loads 31 gates across 3 tiers:
        - tier1_safety: Hard safety gates (14 gates)
        - tier2_optimization: Optimization flags (6 gates)
        - tier3_genetic_hormonal: Advanced routing (11 gates)
        """
        safety_gates = []
        
        # Build lookup for quick marker access
        marker_values = {}
        marker_decision_limits = {}
        for m in processed_markers:
            if m.canonical_code:
                marker_values[m.canonical_code] = m.canonical_value
                if m.decision_limits:
                    marker_decision_limits[m.canonical_code] = m.decision_limits
        
        # Get CRP value for exception checking
        crp_value = marker_values.get("hs_crp")
        crp_acute = crp_value is not None and crp_value > 3.0
        
        # Get safety gates configuration
        gates_config = self.loader.get_safety_gates()
        
        # Process each tier
        for tier_name, tier_data in gates_config.items():
            if not isinstance(tier_data, dict) or "gates" not in tier_data:
                continue
            
            for gate_def in tier_data["gates"]:
                triggered_gate = self._evaluate_single_gate(
                    gate_def=gate_def,
                    tier_name=tier_name,
                    marker_values=marker_values,
                    sex=sex,
                    age=age,
                    crp_acute=crp_acute
                )
                
                if triggered_gate:
                    safety_gates.append(triggered_gate)
        
        return safety_gates
    
    def _evaluate_single_gate(
        self,
        gate_def: Dict,
        tier_name: str,
        marker_values: Dict[str, float],
        sex: Optional[str],
        age: Optional[int],
        crp_acute: bool
    ) -> Optional[SafetyGate]:
        """Evaluate a single safety gate definition."""
        gate_id = gate_def.get("gate_id", "")
        gate_name = gate_def.get("name", "")
        trigger_marker = gate_def.get("trigger_marker")
        condition = gate_def.get("condition", "")
        action = gate_def.get("action", "")
        
        # Check sex-specific gates
        gate_sex = gate_def.get("sex")
        if gate_sex and sex and gate_sex != sex:
            return None  # Skip sex-specific gate for wrong sex
        
        # Handle single marker triggers
        if isinstance(trigger_marker, str):
            value = marker_values.get(trigger_marker)
            if value is None:
                return None  # Marker not present
            
            # Get threshold (sex-specific or general)
            threshold = None
            if sex == "male" and "threshold_male" in gate_def:
                threshold = gate_def["threshold_male"]
            elif sex == "female" and "threshold_female" in gate_def:
                threshold = gate_def["threshold_female"]
            elif "threshold" in gate_def:
                threshold = gate_def["threshold"]
            
            if threshold is None:
                return None
            
            # Evaluate condition
            triggered = False
            if ">" in condition:
                triggered = value > threshold
            elif "<" in condition:
                triggered = value < threshold
            
            if not triggered:
                return None
            
            # Check for exceptions
            exception_active = False
            exception_reason = None
            
            # Iron block exception for acute inflammation
            if gate_name == "iron_block" and crp_acute:
                exception_active = True
                exception_reason = "Acute inflammation detected (hs-CRP > 3.0) - defer ferritin interpretation"
            
            # Build routing constraint
            routing_constraint = action
            
            return SafetyGate(
                gate_id=gate_id,
                name=gate_name,
                tier=tier_name,
                description=gate_def.get("rationale", ""),
                trigger_marker=trigger_marker,
                trigger_value=value,
                threshold=threshold,
                action=action,
                routing_constraint=routing_constraint,
                blocked_ingredients=gate_def.get("blocked_ingredients", []),
                caution_ingredients=gate_def.get("caution_ingredients", []),
                recommended_ingredients=gate_def.get("recommended_ingredients", []),
                exception_active=exception_active,
                exception_reason=exception_reason,
                sex_specific=gate_sex
            )
        
        # Handle multi-marker triggers (e.g., ["alt", "ast"])
        elif isinstance(trigger_marker, list):
            # For OR conditions (any marker triggers)
            if " OR " in condition:
                for marker in trigger_marker:
                    value = marker_values.get(marker)
                    if value is None:
                        continue
                    
                    # Get threshold
                    threshold = None
                    threshold_key = f"threshold_{sex}" if sex else "threshold"
                    if threshold_key in gate_def:
                        threshold = gate_def[threshold_key]
                    elif "threshold" in gate_def:
                        threshold = gate_def["threshold"]
                    
                    # Check marker-specific thresholds
                    if f"{marker}_threshold" in gate_def:
                        threshold = gate_def[f"{marker}_threshold"]
                    
                    if threshold is None:
                        continue
                    
                    triggered = False
                    if ">" in condition:
                        triggered = value > threshold
                    elif "<" in condition:
                        triggered = value < threshold
                    
                    if triggered:
                        return SafetyGate(
                            gate_id=gate_id,
                            name=gate_name,
                            tier=tier_name,
                            description=gate_def.get("rationale", ""),
                            trigger_marker=marker,
                            trigger_value=value,
                            threshold=threshold,
                            action=action,
                            routing_constraint=action,
                            blocked_ingredients=gate_def.get("blocked_ingredients", []),
                            caution_ingredients=gate_def.get("caution_ingredients", []),
                            recommended_ingredients=gate_def.get("recommended_ingredients", []),
                            sex_specific=gate_def.get("sex")
                        )
            
            # For AND conditions (all markers must trigger)
            elif " AND " in condition:
                all_values = {m: marker_values.get(m) for m in trigger_marker}
                if None in all_values.values():
                    return None  # Not all markers present
                
                # Complex AND logic - simplified for now
                # Would need more sophisticated parsing for real conditions
        
        return None
    
    def _compute_hash(self, data: Any) -> str:
        """Compute deterministic hash of input data."""
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]
    
    def _compute_result_hash(self, result: BloodworkResult) -> str:
        """Compute hash of result for determinism verification."""
        # Create a hashable representation excluding the output_hash itself
        hash_data = {
            "lab_profile": result.lab_profile,
            "markers": [
                {
                    "code": m.canonical_code,
                    "value": m.canonical_value,
                    "status": m.status.value if isinstance(m.status, Enum) else m.status,
                    "range_status": m.range_status.value if isinstance(m.range_status, Enum) else m.range_status
                }
                for m in result.markers
            ],
            "routing_constraints": sorted(result.routing_constraints),
            "safety_gates": [g.gate_id for g in result.safety_gates],
            "ruleset_version": result.ruleset_version
        }
        return self._compute_hash(hash_data)


# ============================================================
# MODULE-LEVEL ACCESSOR
# ============================================================

def get_engine(lab_profile: str = "GLOBAL_CONSERVATIVE") -> BloodworkEngine:
    """Get a BloodworkEngine instance."""
    return BloodworkEngine(lab_profile=lab_profile)


def get_loader() -> BloodworkDataLoader:
    """Get the singleton data loader."""
    return BloodworkDataLoader()
