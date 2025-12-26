"""
GenoMAX² Bloodwork Engine v1.0
==============================
Safety and routing layer for biomarker normalization.

This module:
- Normalizes biomarkers to canonical units
- Flags out-of-range or missing data
- Emits RoutingConstraints

This module MUST NOT:
- Diagnose conditions
- Generate recommendations
- Infer missing values
- Expand biomarker scope beyond allowed markers
"""

import json
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field
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
    lab_profile_used: Optional[str] = None
    fallback_used: bool = False
    conversion_applied: bool = False
    conversion_multiplier: Optional[float] = None
    flags: List[str] = field(default_factory=list)
    log_entries: List[str] = field(default_factory=list)


@dataclass
class BloodworkResult:
    """Complete result of bloodwork processing."""
    processed_at: str
    lab_profile: str
    markers: List[ProcessedMarker]
    routing_constraints: List[str]
    require_review: bool
    summary: Dict[str, int]


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
            logger.info(f"Loaded reference ranges v{self._reference_ranges.get('version', '?')}")
        else:
            logger.warning(f"Reference ranges not found: {ranges_path}")
            self._reference_ranges = {"ranges": [], "lab_profiles": [], "policy": {}}
        
        # Build lookup indexes
        self._build_indexes()
    
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
        
        # First try: exact lab profile match
        for r in ranges:
            if r.get("marker_code") == marker_code and r.get("lab_profile") == lab_profile:
                if self._matches_demographics(r, sex, age):
                    return (r, lab_profile, False)
        
        # Fallback: GLOBAL_CONSERVATIVE
        if lab_profile != "GLOBAL_CONSERVATIVE":
            for r in ranges:
                if r.get("marker_code") == marker_code and r.get("lab_profile") == "GLOBAL_CONSERVATIVE":
                    if self._matches_demographics(r, sex, age):
                        return (r, "GLOBAL_CONSERVATIVE", True)
        
        # No range found
        return (None, None, False)
    
    def _matches_demographics(self, range_def: Dict, sex: Optional[str], age: Optional[int]) -> bool:
        """Check if range definition matches demographics."""
        # Sex filter
        range_sex = range_def.get("sex")
        if range_sex and range_sex != "both" and sex and range_sex != sex:
            return False
        
        # Age filter
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
            BloodworkResult with processed markers and constraints
        """
        processed = []
        routing_constraints = []
        require_review = False
        
        for marker_input in markers:
            result = self._process_single_marker(
                code=marker_input.get("code", ""),
                value=marker_input.get("value"),
                unit=marker_input.get("unit", ""),
                sex=sex,
                age=age
            )
            processed.append(result)
            
            # Collect routing constraints
            if result.range_status == RangeStatus.REQUIRE_REVIEW:
                require_review = True
                routing_constraints.append(f"REQUIRE_REVIEW:{result.canonical_code or result.original_code}")
            
            if result.status == MarkerStatus.UNKNOWN:
                routing_constraints.append(f"UNKNOWN_MARKER:{result.original_code}")
            
            if result.range_status in [RangeStatus.CRITICAL_LOW, RangeStatus.CRITICAL_HIGH]:
                routing_constraints.append(f"CRITICAL:{result.canonical_code}:{result.range_status.value}")
        
        # Build summary
        summary = {
            "total": len(processed),
            "valid": sum(1 for p in processed if p.status == MarkerStatus.VALID),
            "unknown": sum(1 for p in processed if p.status == MarkerStatus.UNKNOWN),
            "missing_range": sum(1 for p in processed if p.range_status == RangeStatus.MISSING_RANGE),
            "require_review": sum(1 for p in processed if p.range_status == RangeStatus.REQUIRE_REVIEW),
            "conversions_applied": sum(1 for p in processed if p.conversion_applied)
        }
        
        return BloodworkResult(
            processed_at=datetime.utcnow().isoformat() + "Z",
            lab_profile=self.lab_profile,
            markers=processed,
            routing_constraints=routing_constraints,
            require_review=require_review,
            summary=summary
        )
    
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
                lab_profile_used=profile_used,
                fallback_used=fallback_used,
                conversion_applied=conversion_applied,
                conversion_multiplier=conversion_multiplier,
                flags=flags,
                log_entries=log_entries
            )
        
        # Step 7: Evaluate value against range
        range_status = self._evaluate_range(canonical_value, range_def)
        
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


# ============================================================
# MODULE-LEVEL ACCESSOR
# ============================================================

def get_engine(lab_profile: str = "GLOBAL_CONSERVATIVE") -> BloodworkEngine:
    """Get a BloodworkEngine instance."""
    return BloodworkEngine(lab_profile=lab_profile)


def get_loader() -> BloodworkDataLoader:
    """Get the singleton data loader."""
    return BloodworkDataLoader()
