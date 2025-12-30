"""
GenoMAX² Telemetry Derive Module (Issue #9 - Stage 2)
Extracts telemetry data from Brain pipeline outputs.

NO PII STORAGE:
- No raw biomarkers
- No raw lifestyle answers  
- No user identifiers beyond run_id
- Only: counts, reason codes, age_bucket, sex

Usage:
    from app.telemetry.derive import derive_run_summary, derive_events
    
    summary = derive_run_summary(request_ctx, response_data, phase="resolve")
    events = derive_events(response_data, phase="resolve")
"""

from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum

from .models import (
    AgeBucket,
    ConfidenceLevel,
    TelemetryEventType,
)


@dataclass
class RunSummary:
    """Telemetry run summary - NO PII."""
    run_id: str
    phase: str  # resolve, orchestrate, compose, route
    sex: Optional[str] = None
    age_bucket: str = "unknown"
    has_bloodwork: bool = False
    has_lifestyle: bool = False
    goals_count: int = 0
    painpoints_count: int = 0
    intents_count: int = 0
    matched_items_count: int = 0
    unmatched_intents_count: int = 0
    blocked_skus_count: int = 0
    auto_blocked_skus_count: int = 0
    caution_flags_count: int = 0
    confidence_level: str = "unknown"
    status: str = "success"  # success, error
    error_code: Optional[str] = None


@dataclass
class EventRecord:
    """Single telemetry event - NO PII."""
    event_type: str  # TelemetryEventType value
    code: str
    count: int = 1
    metadata: Optional[Dict[str, Any]] = None


def _safe_get(data: Any, *keys, default=None):
    """Safely navigate nested dicts/objects."""
    current = data
    for key in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(key)
        elif hasattr(current, key):
            current = getattr(current, key, None)
        else:
            return default
    return current if current is not None else default


def _age_to_bucket(age: Optional[int]) -> str:
    """Convert age to bucket string."""
    return AgeBucket.from_age(age).value


def _extract_sex(ctx: Dict[str, Any]) -> Optional[str]:
    """Extract sex/gender from context, normalize to male/female."""
    sex = ctx.get("sex") or ctx.get("gender")
    if sex:
        sex_lower = str(sex).lower()
        if sex_lower in ("male", "m"):
            return "male"
        elif sex_lower in ("female", "f"):
            return "female"
    return None


def _count_blocked_targets(constraints: Dict[str, Any]) -> int:
    """Count blocked targets from routing constraints."""
    blocked = constraints.get("blocked_targets", [])
    return len(blocked) if isinstance(blocked, list) else 0


def _count_caution_targets(constraints: Dict[str, Any]) -> int:
    """Count caution targets from routing constraints."""
    caution = constraints.get("caution_targets", [])
    return len(caution) if isinstance(caution, list) else 0


def _extract_confidence_level(response_data: Dict[str, Any]) -> str:
    """Extract confidence level from various response formats."""
    # Direct confidence field
    confidence = response_data.get("confidence_level")
    if confidence:
        return str(confidence).lower()
    
    # From audit
    audit = response_data.get("audit", {})
    if audit.get("confidence_level"):
        return str(audit["confidence_level"]).lower()
    
    # From explainability
    explain = response_data.get("explainability", {})
    if explain.get("confidence_level"):
        return str(explain["confidence_level"]).lower()
    
    return "unknown"


def derive_run_summary(
    request_context: Dict[str, Any],
    response_data: Dict[str, Any],
    phase: str,
    error_code: Optional[str] = None,
) -> RunSummary:
    """
    Derive telemetry run summary from Brain request/response.
    
    Args:
        request_context: The original request (for extracting sex, age, etc.)
        response_data: The Brain phase response
        phase: Which Brain phase (resolve, orchestrate, compose, route)
        error_code: If request failed, the error code
        
    Returns:
        RunSummary with NO PII
    """
    # Extract run_id
    run_id = (
        response_data.get("run_id") or 
        response_data.get("protocol_id") or
        _safe_get(response_data, "audit", "run_id") or
        request_context.get("run_id") or
        "unknown"
    )
    
    # Extract assessment context (might be in request or response)
    ctx = (
        response_data.get("assessment_context") or
        request_context.get("assessment_context") or
        request_context
    )
    if isinstance(ctx, dict):
        assessment = ctx
    elif hasattr(ctx, "model_dump"):
        assessment = ctx.model_dump()
    else:
        assessment = {}
    
    # Extract sex and age_bucket (NO raw age stored)
    sex = _extract_sex(assessment)
    age = assessment.get("age")
    age_bucket = _age_to_bucket(age)
    
    # Determine has_bloodwork
    has_bloodwork = False
    if request_context.get("bloodwork_signal"):
        has_bloodwork = True
    elif request_context.get("bloodwork_constraints"):
        bw = request_context.get("bloodwork_constraints", {})
        has_bloodwork = bool(bw.get("blocked_targets") or bw.get("caution_targets"))
    elif response_data.get("routing_constraints"):
        rc = response_data.get("routing_constraints", {})
        has_bloodwork = bool(rc.get("blocked_targets") or rc.get("caution_targets"))
    
    # Determine has_lifestyle
    has_lifestyle = bool(request_context.get("lifestyle_constraints"))
    
    # Count goals and painpoints
    goals_count = len(request_context.get("raw_goals", []) or request_context.get("selected_goals", []))
    painpoints_count = len(request_context.get("raw_painpoints", []))
    
    # Count intents
    resolved_intents = response_data.get("resolved_intents", {})
    protocol_intents = response_data.get("protocol_intents", {})
    
    if resolved_intents:
        supplements = resolved_intents.get("supplements", [])
        intents_count = len(supplements) if isinstance(supplements, list) else 0
    elif protocol_intents:
        supplements = protocol_intents.get("supplements", [])
        intents_count = len(supplements) if isinstance(supplements, list) else 0
    else:
        intents_count = 0
    
    # Count matched and unmatched (from route phase)
    sku_plan = response_data.get("sku_plan", {})
    matched_items_count = len(sku_plan.get("items", []))
    
    skipped = response_data.get("skipped_intents", [])
    unmatched_intents_count = len(skipped) if isinstance(skipped, list) else 0
    
    # Count blocks from routing constraints
    constraints = (
        response_data.get("resolved_constraints") or
        response_data.get("routing_constraints") or
        {}
    )
    blocked_skus_count = _count_blocked_targets(constraints)
    caution_flags_count = _count_caution_targets(constraints)
    
    # Auto-blocked from catalog governance (if present)
    auto_blocked_skus_count = 0
    
    # Confidence level
    confidence_level = _extract_confidence_level(response_data)
    
    return RunSummary(
        run_id=run_id,
        phase=phase,
        sex=sex,
        age_bucket=age_bucket,
        has_bloodwork=has_bloodwork,
        has_lifestyle=has_lifestyle,
        goals_count=goals_count,
        painpoints_count=painpoints_count,
        intents_count=intents_count,
        matched_items_count=matched_items_count,
        unmatched_intents_count=unmatched_intents_count,
        blocked_skus_count=blocked_skus_count,
        auto_blocked_skus_count=auto_blocked_skus_count,
        caution_flags_count=caution_flags_count,
        confidence_level=confidence_level,
        status="error" if error_code else "success",
        error_code=error_code,
    )


def derive_events(
    response_data: Dict[str, Any],
    phase: str,
) -> List[EventRecord]:
    """
    Derive telemetry events from Brain response.
    
    Returns list of EventRecord with NO PII - only codes and counts.
    """
    events: List[EventRecord] = []
    
    # Extract routing constraints for block events
    constraints = (
        response_data.get("resolved_constraints") or
        response_data.get("routing_constraints") or
        {}
    )
    
    # A) ROUTING_BLOCK events
    blocked_targets = constraints.get("blocked_targets", [])
    for target in blocked_targets:
        events.append(EventRecord(
            event_type=TelemetryEventType.ROUTING_BLOCK.value,
            code=str(target),
            count=1,
            metadata={"layer": "routing"},
        ))
    
    # Global flags as routing blocks
    global_flags = constraints.get("global_flags", [])
    for flag in global_flags:
        flag_id = flag.get("flag_id") if isinstance(flag, dict) else str(flag)
        events.append(EventRecord(
            event_type=TelemetryEventType.ROUTING_BLOCK.value,
            code=f"FLAG_{flag_id}",
            count=1,
            metadata={"layer": "routing", "severity": flag.get("severity") if isinstance(flag, dict) else None},
        ))
    
    # B) MATCHING_UNMATCHED_INTENT events
    skipped_intents = response_data.get("skipped_intents", [])
    for skipped in skipped_intents:
        if isinstance(skipped, dict):
            intent_id = skipped.get("intent_id", "unknown")
            reason = skipped.get("reason", "NO_MATCH")
        else:
            intent_id = str(skipped)
            reason = "NO_MATCH"
        
        events.append(EventRecord(
            event_type=TelemetryEventType.MATCHING_UNMATCHED_INTENT.value,
            code=intent_id,
            count=1,
            metadata={"layer": "matching", "reason": reason},
        ))
    
    # C) LOW_CONFIDENCE event
    confidence = _extract_confidence_level(response_data)
    if confidence == "low":
        events.append(EventRecord(
            event_type=TelemetryEventType.LOW_CONFIDENCE.value,
            code="LOW_CONFIDENCE",
            count=1,
            metadata={"layer": "explainability"},
        ))
    
    # D) Check for blocked ingredients in intents
    resolved_intents = response_data.get("resolved_intents", {})
    supplements = resolved_intents.get("supplements", [])
    for supp in supplements:
        if isinstance(supp, dict) and supp.get("blocked"):
            events.append(EventRecord(
                event_type=TelemetryEventType.ROUTING_BLOCK.value,
                code=supp.get("intent_id", "unknown"),
                count=1,
                metadata={"layer": "intent_resolution"},
            ))
    
    return events


def derive_error_event(
    run_id: str,
    error_code: str,
    phase: str,
) -> EventRecord:
    """Create error event for failed requests."""
    return EventRecord(
        event_type="ERROR",
        code=error_code,
        count=1,
        metadata={"layer": phase},
    )
