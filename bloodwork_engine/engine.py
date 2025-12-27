"""
GenoMAX² Bloodwork Engine v1.0
==============================
Safety and routing layer for biomarker normalization.

This module:
- Normalizes biomarkers to canonical units
- Flags out-of-range or missing data
- Evaluates safety gates based on decision limits
- Emits RoutingConstraints

This module MUST NOT:
- Diagnose conditions
- Generate recommendations
- Infer missing values
- Expand biomarker scope beyond allowed markers
"""

import json
import logging
import hashlib
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
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
    decision_limits: Optional[Dict[str, float]] = None
    lab_profile_used: Optional[str] = None
    fallback_used: bool = False
    conversion_applied: bool = False
    conversion_multiplier: Optional[float] = None
    flags: List[str] = field(default_factory=list)
    log_entries: List[str] = field(default_factory=list)


@dataclass
class SafetyGate:
    """A triggered safety gate."""
    gate_id: str
    description: str
    trigger_marker: str
    trigger_value: float
    threshold: float
    routing_constraint: str
    exception_active: bool = False
    exception_reason: Optional[str] = None


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
        
        # Load reference ranges
        ranges_path = data_dir / "reference_ranges_v1_0.json"
        if ranges_path.exists():
            with open(ranges_path, "r", encoding="utf-8") as f:
                self._reference_ranges = json.load(f)
            range_count = len(self._reference_ranges.get("ranges", []))
            logger.info(f"Loaded reference ranges v{self._reference_ranges.get('version', '?')} ({range_count} ranges)")
        else:
            logger.warning(f"Reference ranges not found: {ranges_path}")
            self._reference_ranges = {"ranges": [], "lab_profiles": [], "policy": {}}
        
        # Build lookup indexes
        self._build_indexes()
        
        # Validate ranges against registry
        self._validate_ranges()
    
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
        return self._reference_ranges.get("safety_gates", {})
    
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
    - Evaluate safety gates based on decision limits
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
        
        # Evaluate cross-marker safety gates
        safety_gates = self._evaluate_safety_gates(processed, sex)
        
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
            "out_of_range": sum(1 for p in processed if p.range_status in [RangeStatus.LOW, RangeStatus.HIGH]),
            "critical": sum(1 for p in processed if p.range_status in [RangeStatus.CRITICAL_LOW, RangeStatus.CRITICAL_HIGH]),
            "safety_gates_triggered": len([g for g in safety_gates if not g.exception_active])
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
                decision_limits=None,
                lab_profile_used=profile_used,
                fallback_used=fallback_used,
                conversion_applied=conversion_applied,
                conversion_multiplier=conversion_multiplier,
                flags=flags,
                log_entries=log_entries
            )
        
        # Step 7: Evaluate value against range
        range_status = self._evaluate_range(canonical_value, range_def)
        
        # Step 8: Extract decision limits for safety gate evaluation
        decision_limits = range_def.get("decision_limits")
        
        # Log decision limit breaches
        if decision_limits:
            for limit_name, limit_value in decision_limits.items():
                if limit_value is not None:
                    if "threshold" in limit_name.lower() or "flag" in limit_name.lower():
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
            reference_range={
                "low": range_def.get("low"),
                "high": range_def.get("high"),
                "critical_low": range_def.get("critical_low"),
                "critical_high": range_def.get("critical_high")
            },
            decision_limits=decision_limits,
            lab_profile_used=profile_used,
            fallback_used=fallback_used,
            conversion_applied=conversion_applied,
            conversion_multiplier=conversion_multiplier,
            flags=flags,
            log_entries=log_entries
        )
    
    def _evaluate_range(self, value: float, range_def: Dict) -> RangeStatus:
        """Evaluate a value against a reference range."""
        critical_low = range_def.get("critical_low")
        critical_high = range_def.get("critical_high")
        low = range_def.get("low")
        high = range_def.get("high")
        
        if critical_low is not None and value < critical_low:
            return RangeStatus.CRITICAL_LOW
        if critical_high is not None and value > critical_high:
            return RangeStatus.CRITICAL_HIGH
        if low is not None and value < low:
            return RangeStatus.LOW
        if high is not None and value > high:
            return RangeStatus.HIGH
        
        return RangeStatus.IN_RANGE
    
    def _evaluate_safety_gates(self, processed_markers: List[ProcessedMarker], sex: Optional[str]) -> List[SafetyGate]:
        """
        Evaluate cross-marker safety gates.
        
        Safety gates are triggered based on decision_limits and may have exceptions
        based on other markers (e.g., iron block exception when CRP indicates acute inflammation).
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
        crp_limits = marker_decision_limits.get("hs_crp", {})
        acute_inflammation_threshold = crp_limits.get("acute_inflammation_threshold", 3.0)
        crp_acute = crp_value is not None and crp_value > acute_inflammation_threshold
        
        # 1. IRON BLOCK GATE
        ferritin_value = marker_values.get("ferritin")
        ferritin_limits = marker_decision_limits.get("ferritin", {})
        iron_block_threshold = ferritin_limits.get("iron_block_threshold")
        
        if ferritin_value is not None and iron_block_threshold is not None:
            if ferritin_value > iron_block_threshold:
                safety_gates.append(SafetyGate(
                    gate_id="iron_block",
                    description="Block iron supplementation when ferritin is elevated",
                    trigger_marker="ferritin",
                    trigger_value=ferritin_value,
                    threshold=iron_block_threshold,
                    routing_constraint="BLOCK_IRON",
                    exception_active=crp_acute,
                    exception_reason="Acute inflammation detected (hs-CRP > threshold) - defer ferritin interpretation" if crp_acute else None
                ))
        
        # 2. VITAMIN D CAUTION GATE
        calcium_value = marker_values.get("calcium_serum")
        calcium_limits = marker_decision_limits.get("calcium_serum", {})
        vitamin_d_caution_threshold = calcium_limits.get("vitamin_d_caution_threshold")
        
        if calcium_value is not None and vitamin_d_caution_threshold is not None:
            if calcium_value > vitamin_d_caution_threshold:
                safety_gates.append(SafetyGate(
                    gate_id="vitamin_d_caution",
                    description="Caution on vitamin D when calcium is elevated",
                    trigger_marker="calcium_serum",
                    trigger_value=calcium_value,
                    threshold=vitamin_d_caution_threshold,
                    routing_constraint="CAUTION_VITAMIN_D"
                ))
        
        # 3. HEPATIC CAUTION GATE (ALT or AST)
        alt_value = marker_values.get("alt")
        ast_value = marker_values.get("ast")
        alt_limits = marker_decision_limits.get("alt", {})
        ast_limits = marker_decision_limits.get("ast", {})
        
        alt_threshold = alt_limits.get("hepatic_caution_threshold")
        ast_threshold = ast_limits.get("hepatic_caution_threshold")
        
        hepatic_triggered = False
        hepatic_trigger_marker = None
        hepatic_trigger_value = None
        hepatic_trigger_threshold = None
        
        if alt_value is not None and alt_threshold is not None and alt_value > alt_threshold:
            hepatic_triggered = True
            hepatic_trigger_marker = "alt"
            hepatic_trigger_value = alt_value
            hepatic_trigger_threshold = alt_threshold
        
        if ast_value is not None and ast_threshold is not None and ast_value > ast_threshold:
            if not hepatic_triggered or (ast_value > alt_value if alt_value else True):
                hepatic_triggered = True
                hepatic_trigger_marker = "ast"
                hepatic_trigger_value = ast_value
                hepatic_trigger_threshold = ast_threshold
        
        if hepatic_triggered:
            safety_gates.append(SafetyGate(
                gate_id="hepatic_caution",
                description="Caution on hepatotoxic supplements when liver enzymes elevated",
                trigger_marker=hepatic_trigger_marker,
                trigger_value=hepatic_trigger_value,
                threshold=hepatic_trigger_threshold,
                routing_constraint="CAUTION_HEPATOTOXIC"
            ))
        
        # 4. RENAL CAUTION GATE (eGFR or Creatinine)
        egfr_value = marker_values.get("egfr")
        creatinine_value = marker_values.get("creatinine")
        egfr_limits = marker_decision_limits.get("egfr", {})
        creatinine_limits = marker_decision_limits.get("creatinine", {})
        
        egfr_threshold = egfr_limits.get("renal_caution_threshold")
        creatinine_threshold = creatinine_limits.get("renal_caution_threshold")
        
        renal_triggered = False
        renal_trigger_marker = None
        renal_trigger_value = None
        renal_trigger_threshold = None
        
        # eGFR low (below threshold)
        if egfr_value is not None and egfr_threshold is not None and egfr_value < egfr_threshold:
            renal_triggered = True
            renal_trigger_marker = "egfr"
            renal_trigger_value = egfr_value
            renal_trigger_threshold = egfr_threshold
        
        # Creatinine high (above threshold)
        if creatinine_value is not None and creatinine_threshold is not None and creatinine_value > creatinine_threshold:
            renal_triggered = True
            renal_trigger_marker = "creatinine"
            renal_trigger_value = creatinine_value
            renal_trigger_threshold = creatinine_threshold
        
        if renal_triggered:
            safety_gates.append(SafetyGate(
                gate_id="renal_caution",
                description="Caution on renally-cleared supplements when kidney function impaired",
                trigger_marker=renal_trigger_marker,
                trigger_value=renal_trigger_value,
                threshold=renal_trigger_threshold,
                routing_constraint="CAUTION_RENAL"
            ))
        
        # 5. ACUTE INFLAMMATION FLAG
        if crp_acute:
            safety_gates.append(SafetyGate(
                gate_id="acute_inflammation",
                description="Flag acute inflammation - defer iron interpretation",
                trigger_marker="hs_crp",
                trigger_value=crp_value,
                threshold=acute_inflammation_threshold,
                routing_constraint="FLAG_ACUTE_INFLAMMATION"
            ))
        
        return safety_gates
    
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
