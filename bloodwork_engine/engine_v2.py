"""
GenoMAX² Bloodwork Engine v2.0
==============================
Safety and routing layer for biomarker normalization with expanded gates.

V2.0 Changes:
- 40 biomarkers (13 core + 27 expanded)
- 31 safety gates across 3 tiers
- Genetic marker support (MTHFR)
- Computed markers (HOMA-IR, ratios)
- Hormonal routing

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
from typing import Optional, Dict, List, Any, Tuple, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime

# Configure logging
logger = logging.getLogger("bloodwork_engine_v2")
logger.setLevel(logging.INFO)

# ============================================================
# VERSION CONSTANTS
# ============================================================

ENGINE_VERSION = "2.0.0"
REGISTRY_FILE = "marker_registry_v2_0.json"
RANGES_FILE = "reference_ranges_v2_0.json"

# ============================================================
# ENUMS AND DATA CLASSES
# ============================================================

class MarkerStatus(str, Enum):
    """Status of a processed marker."""
    VALID = "VALID"
    UNKNOWN = "UNKNOWN"
    MISSING_RANGE = "MISSING_RANGE"
    INVALID_UNIT = "INVALID_UNIT"
    CONVERSION_APPLIED = "CONVERSION_APPLIED"
    GENETIC = "GENETIC"  # New in v2.0


class RangeStatus(str, Enum):
    """Status of range evaluation."""
    IN_RANGE = "IN_RANGE"
    LOW = "LOW"
    HIGH = "HIGH"
    CRITICAL_LOW = "CRITICAL_LOW"
    CRITICAL_HIGH = "CRITICAL_HIGH"
    MISSING_RANGE = "MISSING_RANGE"
    REQUIRE_REVIEW = "REQUIRE_REVIEW"
    GENETIC_VARIANT = "GENETIC_VARIANT"  # New in v2.0
    OPTIMAL = "OPTIMAL"  # New in v2.0


class GateTier(str, Enum):
    """Safety gate tier classification."""
    TIER1_SAFETY = "tier1_safety"
    TIER2_OPTIMIZATION = "tier2_optimization"
    TIER3_GENETIC_HORMONAL = "tier3_genetic_hormonal"


class GateAction(str, Enum):
    """Safety gate action types."""
    BLOCK = "BLOCK"
    CAUTION = "CAUTION"
    FLAG = "FLAG"


@dataclass
class ProcessedMarker:
    """Result of processing a single biomarker."""
    original_code: str
    canonical_code: Optional[str]
    original_value: Union[float, str]
    canonical_value: Union[float, str]
    original_unit: str
    canonical_unit: Optional[str]
    status: MarkerStatus
    range_status: RangeStatus
    reference_range: Optional[Dict[str, Any]] = None
    genomax_optimal: Optional[Dict[str, float]] = None
    decision_limits: Optional[Dict[str, float]] = None
    lab_profile_used: Optional[str] = None
    fallback_used: bool = False
    conversion_applied: bool = False
    conversion_multiplier: Optional[float] = None
    is_genetic: bool = False
    genetic_interpretation: Optional[str] = None
    flags: List[str] = field(default_factory=list)
    log_entries: List[str] = field(default_factory=list)


@dataclass
class SafetyGate:
    """A triggered safety gate."""
    gate_id: str
    name: str
    tier: GateTier
    description: str
    trigger_marker: str
    trigger_value: Union[float, str]
    threshold: Union[float, str, None]
    action: GateAction
    routing_constraint: str
    blocked_ingredients: List[str] = field(default_factory=list)
    caution_ingredients: List[str] = field(default_factory=list)
    recommended_ingredients: List[str] = field(default_factory=list)
    exception_active: bool = False
    exception_reason: Optional[str] = None


@dataclass
class ComputedMarker:
    """A computed marker (e.g., HOMA-IR, ratios)."""
    code: str
    name: str
    value: float
    unit: str
    formula: str
    source_markers: List[str]


@dataclass
class BloodworkResult:
    """Complete result of bloodwork processing."""
    processed_at: str
    engine_version: str
    lab_profile: str
    markers: List[ProcessedMarker]
    computed_markers: List[ComputedMarker]
    routing_constraints: List[str]
    safety_gates: List[SafetyGate]
    require_review: bool
    summary: Dict[str, int]
    gate_summary: Dict[str, int]
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
            self._marker_lookup = {}
            self._conversion_lookup = {}
            self._safety_gates_lookup = {}
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
        registry_path = data_dir / REGISTRY_FILE
        if registry_path.exists():
            with open(registry_path, "r", encoding="utf-8") as f:
                self._marker_registry = json.load(f)
            logger.info(f"Loaded marker registry v{self._marker_registry.get('version', '?')}")
        else:
            logger.error(f"Marker registry not found: {registry_path}")
            self._marker_registry = {"markers": [], "allowed_marker_codes": [], "lab_profiles": []}
        
        # Load reference ranges
        ranges_path = data_dir / RANGES_FILE
        if ranges_path.exists():
            with open(ranges_path, "r", encoding="utf-8") as f:
                self._reference_ranges = json.load(f)
            range_count = len(self._reference_ranges.get("ranges", []))
            logger.info(f"Loaded reference ranges v{self._reference_ranges.get('version', '?')} ({range_count} ranges)")
        else:
            logger.warning(f"Reference ranges not found: {ranges_path}")
            self._reference_ranges = {"ranges": [], "lab_profiles": [], "policy": {}, "safety_gates": {}}
        
        self._build_indexes()
        self._build_safety_gates_index()
        self._validate_ranges()
    
    def _build_indexes(self):
        """Build fast lookup indexes from registry data."""
        for marker in self._marker_registry.get("markers", []):
            code = marker["code"]
            self._marker_lookup[code.lower()] = code
            for alias in marker.get("aliases", []):
                self._marker_lookup[alias.lower()] = code
        
        for marker in self._marker_registry.get("markers", []):
            code = marker["code"]
            for conv in marker.get("conversions", []):
                key = (code, conv["from"].lower(), conv["to"].lower())
                self._conversion_lookup[key] = conv.get("multiplier", 1.0)
        
        logger.info(f"Built indexes: {len(self._marker_lookup)} aliases, {len(self._conversion_lookup)} conversions")
    
    def _build_safety_gates_index(self):
        """Build safety gates lookup index."""
        safety_gates = self._reference_ranges.get("safety_gates", {})
        
        for tier_key, tier_data in safety_gates.items():
            if isinstance(tier_data, dict) and "gates" in tier_data:
                for gate in tier_data["gates"]:
                    gate_id = gate.get("gate_id")
                    if gate_id:
                        self._safety_gates_lookup[gate_id] = {
                            "tier": tier_key,
                            "definition": gate
                        }
        
        logger.info(f"Indexed {len(self._safety_gates_lookup)} safety gates")
    
    def _validate_ranges(self):
        """Validate that all ranges reference valid markers and units."""
        allowed_codes = set(self._marker_registry.get("allowed_marker_codes", []))
        marker_units = {m["code"]: m["canonical_unit"] for m in self._marker_registry.get("markers", [])}
        
        for r in self._reference_ranges.get("ranges", []):
            marker_code = r.get("marker_code")
            if marker_code not in allowed_codes:
                logger.error(f"VALIDATION_ERROR: Range references unknown marker '{marker_code}'")
    
    @property
    def allowed_marker_codes(self) -> List[str]:
        return self._marker_registry.get("allowed_marker_codes", [])
    
    @property
    def lab_profiles(self) -> List[str]:
        return self._marker_registry.get("lab_profiles", [])
    
    @property
    def marker_registry(self) -> Dict:
        return self._marker_registry
    
    @property
    def reference_ranges(self) -> Dict:
        return self._reference_ranges
    
    @property
    def ruleset_version(self) -> str:
        registry_v = self._marker_registry.get("version", "?")
        ranges_v = self._reference_ranges.get("version", "?")
        return f"registry_v{registry_v}+ranges_v{ranges_v}"
    
    def get_safety_gates(self) -> Dict:
        return self._reference_ranges.get("safety_gates", {})
    
    def get_safety_gate_summary(self) -> Dict:
        """
        Get a summary of safety gates with correct tier counts.
        
        Returns dict with:
            - total: total number of gates
            - tier1_safety: count of tier 1 gates
            - tier2_optimization: count of tier 2 gates
            - tier3_genetic_hormonal: count of tier 3 gates
        """
        safety_gates = self.get_safety_gates()
        
        tier1_count = len(safety_gates.get("tier1_safety", {}).get("gates", []))
        tier2_count = len(safety_gates.get("tier2_optimization", {}).get("gates", []))
        tier3_count = len(safety_gates.get("tier3_genetic_hormonal", {}).get("gates", []))
        
        return {
            "total": tier1_count + tier2_count + tier3_count,
            "tier1_safety": tier1_count,
            "tier2_optimization": tier2_count,
            "tier3_genetic_hormonal": tier3_count
        }
    
    def get_all_gates_flat(self) -> Dict[str, Dict]:
        """
        Get all safety gates as a flat dictionary keyed by gate_id.
        
        Returns dict where keys are gate_ids and values are gate definitions
        with tier information added.
        """
        all_gates = {}
        safety_gates = self.get_safety_gates()
        
        for tier_key, tier_data in safety_gates.items():
            if isinstance(tier_data, dict) and "gates" in tier_data:
                for gate in tier_data["gates"]:
                    gate_id = gate.get("gate_id")
                    if gate_id:
                        gate_with_tier = gate.copy()
                        gate_with_tier["tier_key"] = tier_key
                        all_gates[gate_id] = gate_with_tier
        
        return all_gates
    
    def get_gates_by_tier(self) -> Dict[str, Dict]:
        """
        Get safety gates organized by tier with gate definitions.
        
        Returns dict with tier keys mapping to dicts of gate_id -> gate definition.
        """
        safety_gates = self.get_safety_gates()
        result = {
            "tier1_safety": {},
            "tier2_optimization": {},
            "tier3_genetic_hormonal": {}
        }
        
        for tier_key in result.keys():
            tier_data = safety_gates.get(tier_key, {})
            if isinstance(tier_data, dict) and "gates" in tier_data:
                for gate in tier_data["gates"]:
                    gate_id = gate.get("gate_id")
                    if gate_id:
                        result[tier_key][gate_id] = gate
        
        return result
    
    def get_gate_definition(self, gate_id: str) -> Optional[Dict]:
        """Get a specific safety gate definition."""
        gate_data = self._safety_gates_lookup.get(gate_id)
        if gate_data:
            return gate_data["definition"]
        return None
    
    def get_gate_tier(self, gate_id: str) -> Optional[str]:
        """Get the tier for a safety gate."""
        gate_data = self._safety_gates_lookup.get(gate_id)
        if gate_data:
            return gate_data["tier"]
        return None
    
    def get_marker_definition(self, code_or_alias: str) -> Optional[Dict]:
        canonical = self._marker_lookup.get(code_or_alias.lower())
        if not canonical:
            return None
        for marker in self._marker_registry.get("markers", []):
            if marker["code"] == canonical:
                return marker
        return None
    
    def resolve_marker_code(self, code_or_alias: str) -> Optional[str]:
        return self._marker_lookup.get(code_or_alias.lower())
    
    def get_conversion_multiplier(self, code: str, from_unit: str, to_unit: str) -> Optional[float]:
        key = (code, from_unit.lower(), to_unit.lower())
        return self._conversion_lookup.get(key)
    
    def is_genetic_marker(self, code: str) -> bool:
        """Check if marker is a genetic marker."""
        marker_def = self.get_marker_definition(code)
        if marker_def:
            return marker_def.get("canonical_unit") == "genotype"
        return False
    
    def get_reference_range(
        self, 
        marker_code: str, 
        lab_profile: str, 
        sex: Optional[str] = None,
        age: Optional[int] = None
    ) -> Tuple[Optional[Dict], str, bool]:
        """Get reference range for a marker."""
        ranges = self._reference_ranges.get("ranges", [])
        
        for r in ranges:
            if r.get("marker_code") == marker_code and r.get("lab_profile") == lab_profile:
                if self._matches_demographics(r, sex, age):
                    return (r, lab_profile, False)
        
        if lab_profile != "GLOBAL_CONSERVATIVE":
            for r in ranges:
                if r.get("marker_code") == marker_code and r.get("lab_profile") == "GLOBAL_CONSERVATIVE":
                    if self._matches_demographics(r, sex, age):
                        return (r, "GLOBAL_CONSERVATIVE", True)
        
        if sex:
            for r in ranges:
                if r.get("marker_code") == marker_code and r.get("lab_profile") == "GLOBAL_CONSERVATIVE":
                    if r.get("sex") == "both" and self._matches_age(r, age):
                        return (r, "GLOBAL_CONSERVATIVE", True)
        
        return (None, None, False)
    
    def _matches_demographics(self, range_def: Dict, sex: Optional[str], age: Optional[int]) -> bool:
        range_sex = range_def.get("sex")
        if range_sex and range_sex != "both":
            if sex and range_sex != sex:
                return False
        return self._matches_age(range_def, age)
    
    def _matches_age(self, range_def: Dict, age: Optional[int]) -> bool:
        age_min = range_def.get("age_min")
        age_max = range_def.get("age_max")
        if age is not None:
            if age_min is not None and age < age_min:
                return False
            if age_max is not None and age > age_max:
                return False
        return True


# ============================================================
# BLOODWORK ENGINE V2
# ============================================================

class BloodworkEngineV2:
    """
    Main engine for processing bloodwork data - Version 2.0.
    
    Expanded features:
    - 31 safety gates across 3 tiers
    - Genetic marker support
    - Computed markers (HOMA-IR, ratios)
    - Hormonal routing
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
        input_hash = self._compute_hash({
            "markers": markers, 
            "sex": sex, 
            "age": age, 
            "lab_profile": self.lab_profile,
            "engine_version": ENGINE_VERSION
        })
        
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
            
            if result.range_status == RangeStatus.REQUIRE_REVIEW:
                require_review = True
                routing_constraints.append(f"REQUIRE_REVIEW:{result.canonical_code or result.original_code}")
            
            if result.status == MarkerStatus.UNKNOWN:
                routing_constraints.append(f"UNKNOWN_MARKER:{result.original_code}")
            
            if result.range_status in [RangeStatus.CRITICAL_LOW, RangeStatus.CRITICAL_HIGH]:
                routing_constraints.append(f"CRITICAL:{result.canonical_code}:{result.range_status.value}")
        
        # Compute derived markers
        computed_markers = self._compute_derived_markers(processed, sex)
        
        # Evaluate all safety gates
        safety_gates = self._evaluate_all_safety_gates(processed, computed_markers, sex, age)
        
        # Add safety gate routing constraints
        for gate in safety_gates:
            if not gate.exception_active:
                routing_constraints.append(gate.routing_constraint)
        
        # Build summaries
        summary = {
            "total": len(processed),
            "valid": sum(1 for p in processed if p.status in [MarkerStatus.VALID, MarkerStatus.CONVERSION_APPLIED, MarkerStatus.GENETIC]),
            "unknown": sum(1 for p in processed if p.status == MarkerStatus.UNKNOWN),
            "missing_range": sum(1 for p in processed if p.range_status == RangeStatus.MISSING_RANGE),
            "require_review": sum(1 for p in processed if p.range_status == RangeStatus.REQUIRE_REVIEW),
            "conversions_applied": sum(1 for p in processed if p.conversion_applied),
            "in_range": sum(1 for p in processed if p.range_status == RangeStatus.IN_RANGE),
            "optimal": sum(1 for p in processed if p.range_status == RangeStatus.OPTIMAL),
            "out_of_range": sum(1 for p in processed if p.range_status in [RangeStatus.LOW, RangeStatus.HIGH]),
            "critical": sum(1 for p in processed if p.range_status in [RangeStatus.CRITICAL_LOW, RangeStatus.CRITICAL_HIGH]),
            "genetic_markers": sum(1 for p in processed if p.is_genetic),
            "computed_markers": len(computed_markers)
        }
        
        gate_summary = {
            "total_triggered": len(safety_gates),
            "active": len([g for g in safety_gates if not g.exception_active]),
            "excepted": len([g for g in safety_gates if g.exception_active]),
            "blocks": len([g for g in safety_gates if g.action == GateAction.BLOCK and not g.exception_active]),
            "cautions": len([g for g in safety_gates if g.action == GateAction.CAUTION and not g.exception_active]),
            "flags": len([g for g in safety_gates if g.action == GateAction.FLAG and not g.exception_active]),
            "tier1": len([g for g in safety_gates if g.tier == GateTier.TIER1_SAFETY]),
            "tier2": len([g for g in safety_gates if g.tier == GateTier.TIER2_OPTIMIZATION]),
            "tier3": len([g for g in safety_gates if g.tier == GateTier.TIER3_GENETIC_HORMONAL])
        }
        
        result = BloodworkResult(
            processed_at=datetime.utcnow().isoformat() + "Z",
            engine_version=ENGINE_VERSION,
            lab_profile=self.lab_profile,
            markers=processed,
            computed_markers=computed_markers,
            routing_constraints=list(set(routing_constraints)),
            safety_gates=safety_gates,
            require_review=require_review,
            summary=summary,
            gate_summary=gate_summary,
            ruleset_version=self.loader.ruleset_version,
            input_hash=input_hash,
            output_hash=""
        )
        
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
        
        canonical_code = self.loader.resolve_marker_code(code)
        
        if canonical_code is None:
            logger.warning(f"UNKNOWN marker: '{code}'")
            log_entries.append(f"UNKNOWN: '{code}' not found")
            
            return ProcessedMarker(
                original_code=code,
                canonical_code=None,
                original_value=value,
                canonical_value=value,
                original_unit=unit,
                canonical_unit=None,
                status=MarkerStatus.UNKNOWN,
                range_status=RangeStatus.REQUIRE_REVIEW,
                flags=["UNKNOWN_MARKER"],
                log_entries=log_entries
            )
        
        marker_def = self.loader.get_marker_definition(canonical_code)
        canonical_unit = marker_def["canonical_unit"]
        
        # Handle genetic markers
        if canonical_unit == "genotype":
            return self._process_genetic_marker(code, canonical_code, value, marker_def, log_entries)
        
        # Numeric marker processing
        allowed_units = [u.lower() for u in marker_def.get("allowed_units", [])]
        original_value = float(value) if value is not None else 0.0
        canonical_value = original_value
        conversion_applied = False
        conversion_multiplier = None
        
        if unit.lower() not in allowed_units:
            logger.warning(f"Invalid unit '{unit}' for marker '{canonical_code}'")
            log_entries.append(f"INVALID_UNIT: '{unit}' not in {allowed_units}")
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
        
        if unit.lower() != canonical_unit.lower():
            multiplier = self.loader.get_conversion_multiplier(canonical_code, unit, canonical_unit)
            if multiplier is not None:
                canonical_value = original_value * multiplier
                conversion_applied = True
                conversion_multiplier = multiplier
                log_entries.append(f"CONVERSION: {original_value} {unit} -> {canonical_value} {canonical_unit}")
        
        range_def, profile_used, fallback_used = self.loader.get_reference_range(
            marker_code=canonical_code,
            lab_profile=self.lab_profile,
            sex=sex,
            age=age
        )
        
        if fallback_used:
            log_entries.append(f"FALLBACK: used {profile_used}")
        
        if range_def is None:
            logger.warning(f"MISSING_RANGE: {canonical_code}")
            log_entries.append(f"MISSING_RANGE: no reference range")
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
                lab_profile_used=profile_used,
                fallback_used=fallback_used,
                conversion_applied=conversion_applied,
                conversion_multiplier=conversion_multiplier,
                flags=flags,
                log_entries=log_entries
            )
        
        range_status = self._evaluate_range(canonical_value, range_def)
        genomax_optimal = range_def.get("genomax_optimal")
        decision_limits = range_def.get("decision_limits")
        
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
                "lab_reference": range_def.get("lab_reference"),
                "critical": range_def.get("critical")
            },
            genomax_optimal=genomax_optimal,
            decision_limits=decision_limits,
            lab_profile_used=profile_used,
            fallback_used=fallback_used,
            conversion_applied=conversion_applied,
            conversion_multiplier=conversion_multiplier,
            flags=flags,
            log_entries=log_entries
        )
    
    def _process_genetic_marker(
        self,
        original_code: str,
        canonical_code: str,
        value: Any,
        marker_def: Dict,
        log_entries: List[str]
    ) -> ProcessedMarker:
        """Process a genetic marker (e.g., MTHFR)."""
        valid_values = marker_def.get("valid_values", [])
        str_value = str(value).upper() if value else ""
        
        if str_value not in [v.upper() for v in valid_values]:
            log_entries.append(f"INVALID_GENETIC_VALUE: '{value}' not in {valid_values}")
            return ProcessedMarker(
                original_code=original_code,
                canonical_code=canonical_code,
                original_value=value,
                canonical_value=str_value,
                original_unit="genotype",
                canonical_unit="genotype",
                status=MarkerStatus.GENETIC,
                range_status=RangeStatus.REQUIRE_REVIEW,
                is_genetic=True,
                genetic_interpretation="Invalid genotype value",
                flags=["INVALID_GENETIC_VALUE"],
                log_entries=log_entries
            )
        
        # Interpret genetic value
        interpretation = self._interpret_genetic_value(canonical_code, str_value)
        range_status = RangeStatus.GENETIC_VARIANT if interpretation.get("variant") else RangeStatus.IN_RANGE
        
        log_entries.append(f"GENETIC: {canonical_code}={str_value} -> {interpretation.get('description', 'Normal')}")
        
        return ProcessedMarker(
            original_code=original_code,
            canonical_code=canonical_code,
            original_value=value,
            canonical_value=str_value,
            original_unit="genotype",
            canonical_unit="genotype",
            status=MarkerStatus.GENETIC,
            range_status=range_status,
            is_genetic=True,
            genetic_interpretation=interpretation.get("description"),
            flags=interpretation.get("flags", []),
            log_entries=log_entries
        )
    
    def _interpret_genetic_value(self, code: str, value: str) -> Dict:
        """Interpret a genetic marker value."""
        interpretations = {
            "mthfr_c677t": {
                "CC": {"variant": False, "description": "Normal/wild-type", "flags": []},
                "CT": {"variant": True, "description": "Heterozygous - 40-60% reduced enzyme activity", "flags": ["MTHFR_HETEROZYGOUS"]},
                "TT": {"variant": True, "description": "Homozygous - 70-80% reduced enzyme activity", "flags": ["MTHFR_HOMOZYGOUS", "METHYLFOLATE_REQUIRED"]}
            },
            "mthfr_a1298c": {
                "AA": {"variant": False, "description": "Normal/wild-type", "flags": []},
                "AC": {"variant": True, "description": "Heterozygous - mild reduction", "flags": ["MTHFR_A1298C_HETEROZYGOUS"]},
                "CC": {"variant": True, "description": "Homozygous - moderate reduction", "flags": ["MTHFR_A1298C_HOMOZYGOUS"]}
            }
        }
        
        code_interp = interpretations.get(code, {})
        return code_interp.get(value.upper(), {"variant": False, "description": "Unknown", "flags": []})
    
    def _evaluate_range(self, value: float, range_def: Dict) -> RangeStatus:
        """Evaluate a value against reference ranges."""
        critical = range_def.get("critical", {})
        lab_ref = range_def.get("lab_reference", {})
        optimal = range_def.get("genomax_optimal", {})
        
        critical_low = critical.get("low")
        critical_high = critical.get("high")
        lab_low = lab_ref.get("low")
        lab_high = lab_ref.get("high")
        optimal_low = optimal.get("low") if optimal else None
        optimal_high = optimal.get("high") if optimal else None
        
        if critical_low is not None and value < critical_low:
            return RangeStatus.CRITICAL_LOW
        if critical_high is not None and value > critical_high:
            return RangeStatus.CRITICAL_HIGH
        
        # Check optimal range first
        if optimal_low is not None and optimal_high is not None:
            if optimal_low <= value <= optimal_high:
                return RangeStatus.OPTIMAL
        
        if lab_low is not None and value < lab_low:
            return RangeStatus.LOW
        if lab_high is not None and value > lab_high:
            return RangeStatus.HIGH
        
        return RangeStatus.IN_RANGE
    
    def _compute_derived_markers(
        self,
        processed: List[ProcessedMarker],
        sex: Optional[str]
    ) -> List[ComputedMarker]:
        """Compute derived markers like HOMA-IR and ratios."""
        computed = []
        
        # Build marker value lookup
        marker_values = {}
        for m in processed:
            if m.canonical_code and isinstance(m.canonical_value, (int, float)):
                marker_values[m.canonical_code] = m.canonical_value
        
        # HOMA-IR calculation
        glucose = marker_values.get("fasting_glucose")
        insulin = marker_values.get("fasting_insulin")
        if glucose is not None and insulin is not None:
            homa_ir = (glucose * insulin) / 405
            computed.append(ComputedMarker(
                code="homa_ir",
                name="HOMA-IR (Insulin Resistance Index)",
                value=round(homa_ir, 2),
                unit="ratio",
                formula="(fasting_glucose * fasting_insulin) / 405",
                source_markers=["fasting_glucose", "fasting_insulin"]
            ))
        
        # Zinc:Copper ratio
        zinc = marker_values.get("zinc_serum")
        copper = marker_values.get("copper_serum")
        if zinc is not None and copper is not None and copper > 0:
            zn_cu_ratio = zinc / copper
            computed.append(ComputedMarker(
                code="zinc_copper_ratio",
                name="Zinc:Copper Ratio",
                value=round(zn_cu_ratio, 2),
                unit="ratio",
                formula="zinc_serum / copper_serum",
                source_markers=["zinc_serum", "copper_serum"]
            ))
        
        # rT3:fT3 ratio (thyroid conversion)
        rt3 = marker_values.get("reverse_t3")
        ft3 = marker_values.get("free_t3")
        if rt3 is not None and ft3 is not None and ft3 > 0:
            rt3_ft3_ratio = (rt3 / ft3) * 100  # Scaled for interpretation
            computed.append(ComputedMarker(
                code="rt3_ft3_ratio",
                name="Reverse T3:Free T3 Ratio",
                value=round(rt3_ft3_ratio, 1),
                unit="ratio (x100)",
                formula="(reverse_t3 / free_t3) * 100",
                source_markers=["reverse_t3", "free_t3"]
            ))
        
        # Estradiol:Progesterone ratio (female)
        if sex == "female":
            e2 = marker_values.get("estradiol")
            p4 = marker_values.get("progesterone")
            if e2 is not None and p4 is not None and p4 > 0:
                e2_p4_ratio = e2 / p4
                computed.append(ComputedMarker(
                    code="estradiol_progesterone_ratio",
                    name="Estradiol:Progesterone Ratio",
                    value=round(e2_p4_ratio, 1),
                    unit="ratio (pg/mL : ng/mL)",
                    formula="estradiol / progesterone",
                    source_markers=["estradiol", "progesterone"]
                ))
        
        return computed
    
    def _evaluate_all_safety_gates(
        self,
        processed: List[ProcessedMarker],
        computed: List[ComputedMarker],
        sex: Optional[str],
        age: Optional[int]
    ) -> List[SafetyGate]:
        """Evaluate all safety gates across all tiers."""
        gates = []
        
        # Build marker values lookup
        marker_values = {}
        marker_limits = {}
        marker_flags = {}
        
        for m in processed:
            if m.canonical_code:
                marker_values[m.canonical_code] = m.canonical_value
                if m.decision_limits:
                    marker_limits[m.canonical_code] = m.decision_limits
                marker_flags[m.canonical_code] = m.flags
        
        # Add computed markers
        computed_values = {c.code: c.value for c in computed}
        marker_values.update(computed_values)
        
        # Get CRP for exception checking
        crp_value = marker_values.get("hs_crp")
        crp_acute = crp_value is not None and crp_value > 3.0
        
        # TIER 1: Safety Gates
        gates.extend(self._evaluate_tier1_gates(marker_values, marker_limits, sex, crp_acute))
        
        # TIER 2: Optimization Gates  
        gates.extend(self._evaluate_tier2_gates(marker_values, marker_limits, computed_values, sex))
        
        # TIER 3: Genetic/Hormonal Gates
        gates.extend(self._evaluate_tier3_gates(marker_values, marker_limits, marker_flags, computed_values, sex, age))
        
        return gates
    
    def _evaluate_tier1_gates(
        self,
        values: Dict,
        limits: Dict,
        sex: Optional[str],
        crp_acute: bool
    ) -> List[SafetyGate]:
        """Evaluate Tier 1 safety gates."""
        gates = []
        
        # GATE_001: Iron Block
        ferritin = values.get("ferritin")
        if ferritin is not None:
            threshold = 300 if sex == "male" else 200
            if ferritin > threshold:
                gates.append(SafetyGate(
                    gate_id="GATE_001",
                    name="iron_block",
                    tier=GateTier.TIER1_SAFETY,
                    description="Block iron supplementation when ferritin elevated",
                    trigger_marker="ferritin",
                    trigger_value=ferritin,
                    threshold=threshold,
                    action=GateAction.BLOCK,
                    routing_constraint="BLOCK_IRON",
                    blocked_ingredients=["iron", "iron_bisglycinate", "ferrous_sulfate", "ferrous_fumarate", "heme_iron"],
                    exception_active=crp_acute,
                    exception_reason="Acute inflammation (CRP > 3) - defer ferritin interpretation" if crp_acute else None
                ))
        
        # GATE_002: Vitamin D Caution
        calcium = values.get("calcium_serum")
        if calcium is not None and calcium > 10.5:
            gates.append(SafetyGate(
                gate_id="GATE_002",
                name="vitamin_d_caution",
                tier=GateTier.TIER1_SAFETY,
                description="Caution vitamin D when calcium elevated",
                trigger_marker="calcium_serum",
                trigger_value=calcium,
                threshold=10.5,
                action=GateAction.CAUTION,
                routing_constraint="CAUTION_VITAMIN_D",
                caution_ingredients=["vitamin_d3", "vitamin_d2", "cholecalciferol"]
            ))
        
        # GATE_003: Hepatic Caution
        alt = values.get("alt")
        ast = values.get("ast")
        alt_threshold = 50 if sex == "male" else 40
        ast_threshold = 50 if sex == "male" else 40
        
        if (alt is not None and alt > alt_threshold) or (ast is not None and ast > ast_threshold):
            trigger_marker = "alt" if alt and alt > alt_threshold else "ast"
            trigger_value = alt if trigger_marker == "alt" else ast
            threshold = alt_threshold if trigger_marker == "alt" else ast_threshold
            gates.append(SafetyGate(
                gate_id="GATE_003",
                name="hepatic_caution",
                tier=GateTier.TIER1_SAFETY,
                description="Caution hepatotoxic supplements when liver enzymes elevated",
                trigger_marker=trigger_marker,
                trigger_value=trigger_value,
                threshold=threshold,
                action=GateAction.CAUTION,
                routing_constraint="CAUTION_HEPATOTOXIC",
                caution_ingredients=["niacin_ir", "kava", "high_dose_vitamin_a", "green_tea_extract_egcg"],
                blocked_ingredients=["ashwagandha"]
            ))
        
        # GATE_004: Renal Caution
        egfr = values.get("egfr")
        creatinine = values.get("creatinine")
        creat_threshold = 1.3 if sex == "male" else 1.1
        
        if (egfr is not None and egfr < 60) or (creatinine is not None and creatinine > creat_threshold):
            trigger_marker = "egfr" if egfr and egfr < 60 else "creatinine"
            trigger_value = egfr if trigger_marker == "egfr" else creatinine
            threshold = 60 if trigger_marker == "egfr" else creat_threshold
            gates.append(SafetyGate(
                gate_id="GATE_004",
                name="renal_caution",
                tier=GateTier.TIER1_SAFETY,
                description="Caution renally-cleared supplements with impaired kidney function",
                trigger_marker=trigger_marker,
                trigger_value=trigger_value,
                threshold=threshold,
                action=GateAction.CAUTION,
                routing_constraint="CAUTION_RENAL",
                caution_ingredients=["creatine", "high_dose_protein"]
            ))
        
        # GATE_005: Acute Inflammation
        if crp_acute:
            gates.append(SafetyGate(
                gate_id="GATE_005",
                name="acute_inflammation",
                tier=GateTier.TIER1_SAFETY,
                description="Flag acute inflammation - defer iron interpretation",
                trigger_marker="hs_crp",
                trigger_value=values.get("hs_crp"),
                threshold=3.0,
                action=GateAction.FLAG,
                routing_constraint="FLAG_ACUTE_INFLAMMATION"
            ))
        
        # GATE_006: Potassium Block
        potassium = values.get("potassium")
        if potassium is not None and potassium > 5.0:
            gates.append(SafetyGate(
                gate_id="GATE_006",
                name="potassium_block",
                tier=GateTier.TIER1_SAFETY,
                description="Block potassium with hyperkalemia",
                trigger_marker="potassium",
                trigger_value=potassium,
                threshold=5.0,
                action=GateAction.BLOCK,
                routing_constraint="BLOCK_POTASSIUM",
                blocked_ingredients=["potassium", "potassium_citrate", "potassium_chloride"]
            ))
        
        # GATE_007: Hypokalemia Support
        if potassium is not None and potassium < 3.5:
            gates.append(SafetyGate(
                gate_id="GATE_007",
                name="hypokalemia_support",
                tier=GateTier.TIER1_SAFETY,
                description="Flag low potassium for electrolyte support",
                trigger_marker="potassium",
                trigger_value=potassium,
                threshold=3.5,
                action=GateAction.FLAG,
                routing_constraint="FLAG_LOW_POTASSIUM",
                recommended_ingredients=["potassium_citrate", "electrolyte_blend"]
            ))
        
        # GATE_008: Thyroid Iodine Block
        tsh = values.get("tsh")
        if tsh is not None and tsh < 0.4:
            gates.append(SafetyGate(
                gate_id="GATE_008",
                name="thyroid_iodine_block",
                tier=GateTier.TIER1_SAFETY,
                description="Block iodine with hyperthyroidism",
                trigger_marker="tsh",
                trigger_value=tsh,
                threshold=0.4,
                action=GateAction.BLOCK,
                routing_constraint="BLOCK_IODINE",
                blocked_ingredients=["iodine", "kelp", "iodine_potassium"]
            ))
        
        # GATE_009: Thyroid Support Flag
        if tsh is not None and tsh > 4.5:
            gates.append(SafetyGate(
                gate_id="GATE_009",
                name="thyroid_support_flag",
                tier=GateTier.TIER1_SAFETY,
                description="Flag elevated TSH for thyroid support",
                trigger_marker="tsh",
                trigger_value=tsh,
                threshold=4.5,
                action=GateAction.FLAG,
                routing_constraint="FLAG_THYROID_SUPPORT",
                recommended_ingredients=["selenium", "zinc", "iodine"]
            ))
        
        # GATE_010: B12 Deficiency
        b12 = values.get("vitamin_b12")
        if b12 is not None and b12 < 300:
            gates.append(SafetyGate(
                gate_id="GATE_010",
                name="b12_deficiency",
                tier=GateTier.TIER1_SAFETY,
                description="Flag B12 deficiency",
                trigger_marker="vitamin_b12",
                trigger_value=b12,
                threshold=300,
                action=GateAction.FLAG,
                routing_constraint="FLAG_B12_DEFICIENCY",
                recommended_ingredients=["methylcobalamin", "adenosylcobalamin", "hydroxocobalamin"]
            ))
        
        # GATE_011: Folate Deficiency
        folate = values.get("folate_serum")
        if folate is not None and folate < 3.0:
            gates.append(SafetyGate(
                gate_id="GATE_011",
                name="folate_deficiency",
                tier=GateTier.TIER1_SAFETY,
                description="Flag folate deficiency",
                trigger_marker="folate_serum",
                trigger_value=folate,
                threshold=3.0,
                action=GateAction.FLAG,
                routing_constraint="FLAG_FOLATE_DEFICIENCY",
                recommended_ingredients=["methylfolate", "folinic_acid"]
            ))
        
        # GATE_012: Homocysteine Elevated
        hcy = values.get("homocysteine")
        if hcy is not None and hcy > 10:
            gates.append(SafetyGate(
                gate_id="GATE_012",
                name="homocysteine_elevated",
                tier=GateTier.TIER1_SAFETY,
                description="Flag elevated homocysteine for methylation support",
                trigger_marker="homocysteine",
                trigger_value=hcy,
                threshold=10,
                action=GateAction.FLAG,
                routing_constraint="FLAG_METHYLATION_SUPPORT",
                recommended_ingredients=["methylfolate", "methylcobalamin", "b6_p5p", "riboflavin", "tmg"]
            ))
        
        # GATE_013: Uric Acid High
        uric = values.get("uric_acid")
        if uric is not None:
            threshold = 7.0 if sex == "male" else 6.0
            if uric > threshold:
                gates.append(SafetyGate(
                    gate_id="GATE_013",
                    name="uric_acid_high",
                    tier=GateTier.TIER1_SAFETY,
                    description="Caution purine supplements with elevated uric acid",
                    trigger_marker="uric_acid",
                    trigger_value=uric,
                    threshold=threshold,
                    action=GateAction.CAUTION,
                    routing_constraint="CAUTION_PURINE",
                    caution_ingredients=["high_dose_niacin", "fructose", "purine_supplements"]
                ))
        
        # GATE_014: Coagulation Caution
        platelets = values.get("platelet_count")
        if platelets is not None and platelets < 100:
            gates.append(SafetyGate(
                gate_id="GATE_014",
                name="coagulation_caution",
                tier=GateTier.TIER1_SAFETY,
                description="Caution blood-thinning supplements with low platelets",
                trigger_marker="platelet_count",
                trigger_value=platelets,
                threshold=100,
                action=GateAction.CAUTION,
                routing_constraint="CAUTION_BLOOD_THINNING",
                caution_ingredients=["fish_oil_high_dose", "vitamin_e_high_dose", "ginkgo", "garlic_extract"]
            ))
        
        return gates
    
    def _evaluate_tier2_gates(
        self,
        values: Dict,
        limits: Dict,
        computed: Dict,
        sex: Optional[str]
    ) -> List[SafetyGate]:
        """Evaluate Tier 2 optimization gates."""
        gates = []
        
        # GATE_015: Omega-3 Deficiency
        o3_index = values.get("omega3_index")
        if o3_index is not None and o3_index < 4.0:
            gates.append(SafetyGate(
                gate_id="GATE_015",
                name="omega3_deficiency",
                tier=GateTier.TIER2_OPTIMIZATION,
                description="Flag omega-3 deficiency - high priority",
                trigger_marker="omega3_index",
                trigger_value=o3_index,
                threshold=4.0,
                action=GateAction.FLAG,
                routing_constraint="FLAG_OMEGA3_PRIORITY",
                recommended_ingredients=["epa_dha", "fish_oil", "algae_omega3"]
            ))
        
        # GATE_016: Omega-3 Sufficient
        if o3_index is not None and o3_index > 8.0:
            gates.append(SafetyGate(
                gate_id="GATE_016",
                name="omega3_optimal",
                tier=GateTier.TIER2_OPTIMIZATION,
                description="Omega-3 sufficient - reduce dosing",
                trigger_marker="omega3_index",
                trigger_value=o3_index,
                threshold=8.0,
                action=GateAction.FLAG,
                routing_constraint="FLAG_OMEGA3_SUFFICIENT"
            ))
        
        # GATE_017: Zinc:Copper Imbalance
        zn_cu_ratio = computed.get("zinc_copper_ratio")
        if zn_cu_ratio is not None and zn_cu_ratio > 1.5:
            gates.append(SafetyGate(
                gate_id="GATE_017",
                name="zinc_copper_imbalance",
                tier=GateTier.TIER2_OPTIMIZATION,
                description="Caution zinc excess - reduce zinc dosing",
                trigger_marker="zinc_copper_ratio",
                trigger_value=zn_cu_ratio,
                threshold=1.5,
                action=GateAction.CAUTION,
                routing_constraint="CAUTION_ZINC_EXCESS"
            ))
        
        # GATE_018: Insulin Resistance
        homa_ir = computed.get("homa_ir")
        insulin = values.get("fasting_insulin")
        if (homa_ir is not None and homa_ir > 2.5) or (insulin is not None and insulin > 10):
            trigger_value = homa_ir if homa_ir and homa_ir > 2.5 else insulin
            threshold = 2.5 if homa_ir and homa_ir > 2.5 else 10
            gates.append(SafetyGate(
                gate_id="GATE_018",
                name="insulin_resistance",
                tier=GateTier.TIER2_OPTIMIZATION,
                description="Flag insulin resistance for metabolic support",
                trigger_marker="homa_ir" if homa_ir and homa_ir > 2.5 else "fasting_insulin",
                trigger_value=trigger_value,
                threshold=threshold,
                action=GateAction.FLAG,
                routing_constraint="FLAG_INSULIN_SUPPORT",
                recommended_ingredients=["berberine", "chromium", "alpha_lipoic_acid", "inositol"]
            ))
        
        # GATE_019: Iron Deficiency Anemia
        hgb = values.get("hemoglobin")
        ferritin = values.get("ferritin")
        hgb_threshold = 13.0 if sex == "male" else 11.0
        
        if hgb is not None and ferritin is not None:
            if hgb < hgb_threshold and ferritin < 30:
                gates.append(SafetyGate(
                    gate_id="GATE_019",
                    name="iron_deficiency_anemia",
                    tier=GateTier.TIER2_OPTIMIZATION,
                    description="Flag IDA - override iron block",
                    trigger_marker="hemoglobin",
                    trigger_value=hgb,
                    threshold=hgb_threshold,
                    action=GateAction.FLAG,
                    routing_constraint="FLAG_IRON_DEFICIENCY_ANEMIA",
                    recommended_ingredients=["iron_bisglycinate", "vitamin_c"]
                ))
        
        # GATE_020: Triglyceride Caution
        tg = values.get("triglycerides")
        if tg is not None and tg > 500:
            gates.append(SafetyGate(
                gate_id="GATE_020",
                name="triglyceride_caution",
                tier=GateTier.TIER2_OPTIMIZATION,
                description="Caution fish oil dose with very high triglycerides",
                trigger_marker="triglycerides",
                trigger_value=tg,
                threshold=500,
                action=GateAction.CAUTION,
                routing_constraint="CAUTION_FISH_OIL_DOSE"
            ))
        
        return gates
    
    def _evaluate_tier3_gates(
        self,
        values: Dict,
        limits: Dict,
        marker_flags: Dict,
        computed: Dict,
        sex: Optional[str],
        age: Optional[int]
    ) -> List[SafetyGate]:
        """Evaluate Tier 3 genetic/hormonal gates."""
        gates = []
        
        # GATE_021: MTHFR Methylfolate Required
        c677t = values.get("mthfr_c677t")
        a1298c = values.get("mthfr_a1298c")
        
        mthfr_required = False
        if c677t == "TT":
            mthfr_required = True
        elif c677t == "CT" and a1298c == "AC":
            mthfr_required = True  # Compound heterozygous
        
        if mthfr_required:
            gates.append(SafetyGate(
                gate_id="GATE_021",
                name="mthfr_methylfolate_required",
                tier=GateTier.TIER3_GENETIC_HORMONAL,
                description="MTHFR variant - methylfolate required",
                trigger_marker="mthfr_c677t",
                trigger_value=c677t,
                threshold="TT or compound",
                action=GateAction.FLAG,
                routing_constraint="FLAG_METHYLFOLATE_REQUIRED",
                blocked_ingredients=["folic_acid"],
                recommended_ingredients=["methylfolate", "folinic_acid"]
            ))
        
        # GATE_022: Cortisol High
        cortisol = values.get("cortisol_am")
        if cortisol is not None and cortisol > 20:
            gates.append(SafetyGate(
                gate_id="GATE_022",
                name="cortisol_high",
                tier=GateTier.TIER3_GENETIC_HORMONAL,
                description="Flag elevated morning cortisol",
                trigger_marker="cortisol_am",
                trigger_value=cortisol,
                threshold=20,
                action=GateAction.FLAG,
                routing_constraint="FLAG_CORTISOL_HIGH",
                recommended_ingredients=["phosphatidylserine", "ashwagandha"]
            ))
        
        # GATE_023: Cortisol Low
        if cortisol is not None and cortisol < 5:
            gates.append(SafetyGate(
                gate_id="GATE_023",
                name="cortisol_low",
                tier=GateTier.TIER3_GENETIC_HORMONAL,
                description="Flag low cortisol for adrenal support",
                trigger_marker="cortisol_am",
                trigger_value=cortisol,
                threshold=5,
                action=GateAction.FLAG,
                routing_constraint="FLAG_ADRENAL_SUPPORT",
                recommended_ingredients=["licorice_root", "rhodiola", "vitamin_c", "b5_pantothenic"]
            ))
        
        # GATE_024: DHEA Low
        dhea = values.get("dhea_s")
        if dhea is not None:
            age_bucket = "under_40" if age and age < 40 else "over_40"
            if sex == "male":
                threshold = 200 if age_bucket == "under_40" else 150
            else:
                threshold = 150 if age_bucket == "under_40" else 100
            
            if dhea < threshold:
                gates.append(SafetyGate(
                    gate_id="GATE_024",
                    name="dhea_low",
                    tier=GateTier.TIER3_GENETIC_HORMONAL,
                    description="Flag low DHEA-S for hormonal support",
                    trigger_marker="dhea_s",
                    trigger_value=dhea,
                    threshold=threshold,
                    action=GateAction.FLAG,
                    routing_constraint="FLAG_DHEA_SUPPORT",
                    recommended_ingredients=["dhea", "pregnenolone"]
                ))
        
        # GATE_025: Thyroid Conversion Poor
        rt3_ft3_ratio = computed.get("rt3_ft3_ratio")
        if rt3_ft3_ratio is not None and rt3_ft3_ratio > 10:
            gates.append(SafetyGate(
                gate_id="GATE_025",
                name="thyroid_conversion_poor",
                tier=GateTier.TIER3_GENETIC_HORMONAL,
                description="Flag poor T4-to-T3 conversion",
                trigger_marker="rt3_ft3_ratio",
                trigger_value=rt3_ft3_ratio,
                threshold=10,
                action=GateAction.FLAG,
                routing_constraint="FLAG_T4_T3_CONVERSION_SUPPORT",
                recommended_ingredients=["selenium", "zinc", "iron", "vitamin_a"]
            ))
        
        # GATE_026: Low Free T3
        ft3 = values.get("free_t3")
        if ft3 is not None and ft3 < 2.0:
            gates.append(SafetyGate(
                gate_id="GATE_026",
                name="low_free_t3",
                tier=GateTier.TIER3_GENETIC_HORMONAL,
                description="Flag low fT3 for thyroid support",
                trigger_marker="free_t3",
                trigger_value=ft3,
                threshold=2.0,
                action=GateAction.FLAG,
                routing_constraint="FLAG_THYROID_SUPPORT",
                recommended_ingredients=["selenium", "zinc", "tyrosine", "iodine"]
            ))
        
        # GATE_027: Testosterone Low (Male)
        if sex == "male":
            total_t = values.get("total_testosterone")
            free_t = values.get("free_testosterone")
            
            if (total_t is not None and total_t < 300) or (free_t is not None and free_t < 6.5):
                trigger = "total_testosterone" if total_t and total_t < 300 else "free_testosterone"
                trigger_val = total_t if trigger == "total_testosterone" else free_t
                threshold = 300 if trigger == "total_testosterone" else 6.5
                
                gates.append(SafetyGate(
                    gate_id="GATE_027",
                    name="testosterone_low_male",
                    tier=GateTier.TIER3_GENETIC_HORMONAL,
                    description="Flag low testosterone for support",
                    trigger_marker=trigger,
                    trigger_value=trigger_val,
                    threshold=threshold,
                    action=GateAction.FLAG,
                    routing_constraint="FLAG_TESTOSTERONE_SUPPORT",
                    recommended_ingredients=["zinc", "vitamin_d", "ashwagandha", "tongkat_ali", "boron"]
                ))
        
        # GATE_028: Estrogen:Progesterone Imbalance (Female)
        if sex == "female":
            e2_p4_ratio = computed.get("estradiol_progesterone_ratio")
            if e2_p4_ratio is not None and e2_p4_ratio > 20:
                gates.append(SafetyGate(
                    gate_id="GATE_028",
                    name="estrogen_progesterone_imbalance",
                    tier=GateTier.TIER3_GENETIC_HORMONAL,
                    description="Flag estrogen dominance",
                    trigger_marker="estradiol_progesterone_ratio",
                    trigger_value=e2_p4_ratio,
                    threshold=20,
                    action=GateAction.FLAG,
                    routing_constraint="FLAG_ESTROGEN_DOMINANCE",
                    recommended_ingredients=["dim", "calcium_d_glucarate", "vitex"]
                ))
        
        # GATE_029: ApoB Elevated
        apob = values.get("apolipoprotein_b")
        if apob is not None and apob > 100:
            gates.append(SafetyGate(
                gate_id="GATE_029",
                name="apob_elevated",
                tier=GateTier.TIER3_GENETIC_HORMONAL,
                description="Flag elevated ApoB for cardiovascular support",
                trigger_marker="apolipoprotein_b",
                trigger_value=apob,
                threshold=100,
                action=GateAction.FLAG,
                routing_constraint="FLAG_CARDIOVASCULAR_SUPPORT",
                recommended_ingredients=["berberine", "omega3", "niacin", "plant_sterols"]
            ))
        
        # GATE_030: Lp(a) Elevated
        lpa = values.get("lp_a")
        if lpa is not None and lpa > 75:
            gates.append(SafetyGate(
                gate_id="GATE_030",
                name="lpa_elevated",
                tier=GateTier.TIER3_GENETIC_HORMONAL,
                description="Flag elevated Lp(a) - genetic CVD risk",
                trigger_marker="lp_a",
                trigger_value=lpa,
                threshold=75,
                action=GateAction.FLAG,
                routing_constraint="FLAG_LPA_ELEVATED",
                recommended_ingredients=["niacin", "omega3", "coq10"]
            ))
        
        # GATE_031: GGT Elevated
        ggt = values.get("ggt")
        if ggt is not None:
            threshold = 50 if sex == "male" else 40
            if ggt > threshold:
                gates.append(SafetyGate(
                    gate_id="GATE_031",
                    name="ggt_elevated",
                    tier=GateTier.TIER3_GENETIC_HORMONAL,
                    description="Flag elevated GGT for oxidative stress support",
                    trigger_marker="ggt",
                    trigger_value=ggt,
                    threshold=threshold,
                    action=GateAction.FLAG,
                    routing_constraint="FLAG_OXIDATIVE_STRESS",
                    recommended_ingredients=["nac", "glutathione", "milk_thistle", "alpha_lipoic_acid"]
                ))
        
        return gates
    
    def _compute_hash(self, data: Any) -> str:
        """Compute deterministic hash of input data."""
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]
    
    def _compute_result_hash(self, result: BloodworkResult) -> str:
        """Compute hash of result for determinism verification."""
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
            "computed": [{"code": c.code, "value": c.value} for c in result.computed_markers],
            "routing_constraints": sorted(result.routing_constraints),
            "safety_gates": [g.gate_id for g in result.safety_gates],
            "ruleset_version": result.ruleset_version
        }
        return self._compute_hash(hash_data)


# ============================================================
# MODULE-LEVEL ACCESSORS
# ============================================================

def get_engine(lab_profile: str = "GLOBAL_CONSERVATIVE") -> BloodworkEngineV2:
    """Get a BloodworkEngineV2 instance."""
    return BloodworkEngineV2(lab_profile=lab_profile)


def get_loader() -> BloodworkDataLoader:
    """Get the singleton data loader."""
    return BloodworkDataLoader()
