"""
GenoMAX² Telemetry Data Models (Issue #9)
Pydantic models for telemetry events and runs.
"""

from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime
import uuid


class AgeBucket(str, Enum):
    """Age bucket for demographic aggregation without revealing exact age."""
    AGE_18_29 = "18_29"
    AGE_30_39 = "30_39"
    AGE_40_49 = "40_49"
    AGE_50_59 = "50_59"
    AGE_60_PLUS = "60_plus"
    UNKNOWN = "unknown"
    
    @classmethod
    def from_age(cls, age: Optional[int]) -> "AgeBucket":
        """Convert exact age to bucket."""
        if age is None:
            return cls.UNKNOWN
        if 18 <= age <= 29:
            return cls.AGE_18_29
        elif 30 <= age <= 39:
            return cls.AGE_30_39
        elif 40 <= age <= 49:
            return cls.AGE_40_49
        elif 50 <= age <= 59:
            return cls.AGE_50_59
        elif age >= 60:
            return cls.AGE_60_PLUS
        return cls.UNKNOWN


class ConfidenceLevel(str, Enum):
    """Confidence level from explainability layer."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class TelemetryEventType(str, Enum):
    """Event types for telemetry tracking."""
    CATALOG_AUTO_BLOCK = "CATALOG_AUTO_BLOCK"
    ROUTING_BLOCK = "ROUTING_BLOCK"
    MATCHING_UNMATCHED_INTENT = "MATCHING_UNMATCHED_INTENT"
    MATCHING_REQUIREMENT_UNFULFILLED = "MATCHING_REQUIREMENT_UNFULFILLED"
    UNKNOWN_INGREDIENT = "UNKNOWN_INGREDIENT"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    SAFETY_GATE_TRIGGERED = "SAFETY_GATE_TRIGGERED"
    BLOODWORK_MARKER_MISSING = "BLOODWORK_MARKER_MISSING"
    UNIT_CONVERSION_APPLIED = "UNIT_CONVERSION_APPLIED"


class TelemetryRun(BaseModel):
    """
    Aggregate telemetry for a single protocol run.
    NO PII - only counts and categorical data.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str  # FK to protocol_runs / brain_runs
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Version tracking
    api_version: str = "3.16.0"
    bloodwork_version: str = "1.0"
    catalog_version: str = "catalog_governance_v1"
    routing_version: str = "routing_layer_v1"
    matching_version: str = "matching_layer_v1"
    explainability_version: str = "explainability_v1"
    
    # Demographics (bucketed, no PII)
    sex: Optional[str] = None  # male/female
    age_bucket: AgeBucket = AgeBucket.UNKNOWN
    
    # Run characteristics
    has_bloodwork: bool = False
    intents_count: int = 0
    matched_items_count: int = 0
    unmatched_intents_count: int = 0
    blocked_skus_count: int = 0
    auto_blocked_skus_count: int = 0
    caution_flags_count: int = 0
    confidence_level: ConfidenceLevel = ConfidenceLevel.UNKNOWN
    
    class Config:
        use_enum_values = True


class TelemetryEvent(BaseModel):
    """
    Individual telemetry event.
    Stores COUNTS, not raw data.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    event_type: TelemetryEventType
    code: str  # reason_code / intent_id / missing_field / ingredient
    count: int = 1
    
    # NON-PII metadata only
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class TelemetryDailyRollup(BaseModel):
    """
    Daily aggregation for dashboards.
    """
    day: str  # YYYY-MM-DD format
    total_runs: int = 0
    pct_has_bloodwork: float = 0.0
    pct_low_confidence: float = 0.0
    avg_unmatched_intents: float = 0.0
    avg_blocked_skus: float = 0.0
    
    # Top issues (limit to top 10)
    top_block_reasons: List[Dict[str, Any]] = Field(default_factory=list)
    top_missing_fields: List[Dict[str, Any]] = Field(default_factory=list)
    top_unknown_ingredients: List[Dict[str, Any]] = Field(default_factory=list)


class TelemetrySummary(BaseModel):
    """Summary response for admin dashboard."""
    period_start: datetime
    period_end: datetime
    total_runs: int = 0
    runs_with_bloodwork: int = 0
    runs_low_confidence: int = 0
    
    # Aggregates
    total_blocks: int = 0
    total_unmatched_intents: int = 0
    total_caution_flags: int = 0
    
    # Top issues
    top_block_reasons: List[Dict[str, int]] = Field(default_factory=list)
    top_unmatched_intents: List[Dict[str, int]] = Field(default_factory=list)
    
    # By demographic
    by_sex: Dict[str, int] = Field(default_factory=dict)
    by_age_bucket: Dict[str, int] = Field(default_factory=dict)
    by_confidence: Dict[str, int] = Field(default_factory=dict)


class TelemetryHealthResponse(BaseModel):
    """Health check response for telemetry system."""
    status: str = "healthy"
    telemetry_enabled: bool = True
    tables_exist: bool = False
    last_event_at: Optional[datetime] = None
    total_runs_24h: int = 0
    total_events_24h: int = 0
