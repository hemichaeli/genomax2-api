"""
GenoMAX² Brain Resolver v1.0
Deterministic Merge Logic for Constraints and Intents

This module provides the core resolver functions that:
1. Merge bloodwork + lifestyle constraints -> unified RoutingConstraints
2. Merge goals + painpoint intents -> unified ProtocolIntents
3. Orchestrate the full resolution pipeline

IMPORTANT: All functions are DETERMINISTIC.
Same inputs ALWAYS produce same outputs.
No randomness, no time-based conditions (except audit timestamps).

Usage:
    from app.brain.resolver import resolve_all, merge_constraints, merge_intents
    
    output = resolve_all(resolver_input)
"""

from __future__ import annotations
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional, Any

from app.brain.contracts import (
    CONTRACT_VERSION,
    AssessmentContext,
    RoutingConstraints,
    ProtocolIntents,
    ProtocolIntentItem,
    ResolverInput,
    ResolverOutput,
    ResolverAudit,
    TargetDetail,
    empty_routing_constraints,
    empty_protocol_intents,
)


# =============================================================================
# RESOLVER VERSION
# =============================================================================
RESOLVER_VERSION = "1.0.0"


# =============================================================================
# HASH UTILITIES
# =============================================================================

def compute_hash(data: Any) -> str:
    """Compute deterministic SHA256 hash of any data structure."""
    json_str = json.dumps(data, sort_keys=True, default=str)
    return f"sha256:{hashlib.sha256(json_str.encode()).hexdigest()}"


def now_iso() -> str:
    """Current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# CONSTRAINT MERGE FUNCTIONS
# =============================================================================

def merge_constraints(
    bloodwork: RoutingConstraints,
    lifestyle: RoutingConstraints
) -> Tuple[RoutingConstraints, Dict[str, int]]:
    """
    Merge bloodwork and lifestyle constraints into unified RoutingConstraints.
    
    MERGE RULES (strict, deterministic):
    - blocked_targets = UNION(bloodwork.blocked_targets, lifestyle.blocked_targets)
    - caution_targets = UNION(...)
    - blocked_ingredients = UNION(...)
    - allowed_targets = INTERSECTION if both provided, else whichever is non-empty
    - has_critical_flags = bloodwork.has_critical_flags OR lifestyle.has_critical_flags
    - global_flags = UNION(...)
    - target_details = MERGE with bloodwork taking precedence on conflicts
    
    Args:
        bloodwork: Constraints from bloodwork engine
        lifestyle: Constraints from lifestyle engine
        
    Returns:
        Tuple of (merged RoutingConstraints, audit stats dict)
    """
    # UNION operations for blocked/caution targets
    merged_blocked = sorted(set(bloodwork.blocked_targets) | set(lifestyle.blocked_targets))
    merged_caution = sorted(set(bloodwork.caution_targets) | set(lifestyle.caution_targets))
    merged_blocked_ingredients = sorted(set(bloodwork.blocked_ingredients) | set(lifestyle.blocked_ingredients))
    
    # INTERSECTION for allowed targets (if both have entries)
    bw_allowed = set(bloodwork.allowed_targets)
    ls_allowed = set(lifestyle.allowed_targets)
    
    if bw_allowed and ls_allowed:
        # Both have allowed targets: intersection
        merged_allowed = sorted(bw_allowed & ls_allowed)
    elif bw_allowed:
        merged_allowed = sorted(bw_allowed)
    elif ls_allowed:
        merged_allowed = sorted(ls_allowed)
    else:
        merged_allowed = []
    
    # OR for critical flags
    merged_has_critical = bloodwork.has_critical_flags or lifestyle.has_critical_flags
    
    # UNION for global flags
    merged_global_flags = sorted(set(bloodwork.global_flags) | set(lifestyle.global_flags))
    
    # MERGE target_details (bloodwork takes precedence)
    merged_details: Dict[str, TargetDetail] = {}
    
    # First add all lifestyle details
    for target_id, detail in lifestyle.target_details.items():
        merged_details[target_id] = detail
    
    # Then override/add bloodwork details (higher priority)
    for target_id, detail in bloodwork.target_details.items():
        if target_id in merged_details:
            # Bloodwork overrides lifestyle
            existing = merged_details[target_id]
            # Merge blocking biomarkers
            merged_blocking = sorted(set(existing.blocking_biomarkers) | set(detail.blocking_biomarkers))
            merged_caution_bm = sorted(set(existing.caution_biomarkers) | set(detail.caution_biomarkers))
            
            merged_details[target_id] = TargetDetail(
                gate_status=detail.gate_status,  # Bloodwork wins
                reason=f"{detail.reason}; {existing.reason}" if existing.reason else detail.reason,
                blocking_biomarkers=merged_blocking,
                caution_biomarkers=merged_caution_bm,
                source="merged"
            )
        else:
            merged_details[target_id] = detail
    
    # Build merged constraints
    merged = RoutingConstraints(
        contract_version=CONTRACT_VERSION,
        blocked_targets=merged_blocked,
        caution_targets=merged_caution,
        allowed_targets=merged_allowed,
        blocked_ingredients=merged_blocked_ingredients,
        has_critical_flags=merged_has_critical,
        global_flags=merged_global_flags,
        target_details=merged_details,
    )
    
    # Audit stats
    stats = {
        "bloodwork_blocked_count": len(bloodwork.blocked_targets),
        "lifestyle_blocked_count": len(lifestyle.blocked_targets),
        "merged_blocked_count": len(merged_blocked),
        "bloodwork_caution_count": len(bloodwork.caution_targets),
        "lifestyle_caution_count": len(lifestyle.caution_targets),
        "merged_caution_count": len(merged_caution),
    }
    
    return merged, stats


# =============================================================================
# INTENT MERGE FUNCTIONS
# =============================================================================

def merge_intents(
    goals_intents: ProtocolIntents,
    painpoint_intents: ProtocolIntents
) -> Tuple[ProtocolIntents, Dict[str, int]]:
    """
    Merge goals and painpoint intents into unified ProtocolIntents.
    
    MERGE RULES (strict, deterministic):
    - supplements: UNION by intent_id
      - On conflict: keep MAX(priority), preserve target_id from higher priority
    - lifestyle: CONCATENATE, dedupe by intent_id, stable sort by priority desc
    - nutrition: CONCATENATE, dedupe by intent_id, stable sort by priority desc
    
    Args:
        goals_intents: Intents generated from user goals
        painpoint_intents: Intents generated from user painpoints
        
    Returns:
        Tuple of (merged ProtocolIntents, audit stats dict)
    """
    # SUPPLEMENTS: Union by intent_id with max priority
    supplement_map: Dict[str, ProtocolIntentItem] = {}
    
    # Add goals intents first
    for intent in goals_intents.supplements:
        supplement_map[intent.intent_id] = intent
    
    # Merge painpoint intents (keep max priority)
    conflicts_resolved = 0
    for intent in painpoint_intents.supplements:
        if intent.intent_id in supplement_map:
            existing = supplement_map[intent.intent_id]
            if intent.priority > existing.priority:
                # Painpoint has higher priority, use it
                supplement_map[intent.intent_id] = intent
            # else: keep existing (goals version)
            conflicts_resolved += 1
        else:
            supplement_map[intent.intent_id] = intent
    
    # Sort by priority desc, then intent_id for stability
    merged_supplements = sorted(
        supplement_map.values(),
        key=lambda x: (-x.priority, x.intent_id)
    )
    
    # LIFESTYLE: Concatenate and dedupe
    lifestyle_map: Dict[str, Dict[str, Any]] = {}
    
    for item in goals_intents.lifestyle:
        intent_id = item.get("intent_id")
        if intent_id:
            lifestyle_map[intent_id] = item
    
    for item in painpoint_intents.lifestyle:
        intent_id = item.get("intent_id")
        if intent_id:
            if intent_id in lifestyle_map:
                # Keep higher priority
                existing_priority = lifestyle_map[intent_id].get("priority", 0)
                new_priority = item.get("priority", 0)
                if new_priority > existing_priority:
                    lifestyle_map[intent_id] = item
            else:
                lifestyle_map[intent_id] = item
    
    merged_lifestyle = sorted(
        lifestyle_map.values(),
        key=lambda x: (-x.get("priority", 0), x.get("intent_id", ""))
    )
    
    # NUTRITION: Concatenate and dedupe
    nutrition_map: Dict[str, Dict[str, Any]] = {}
    
    for item in goals_intents.nutrition:
        intent_id = item.get("intent_id")
        if intent_id:
            nutrition_map[intent_id] = item
    
    for item in painpoint_intents.nutrition:
        intent_id = item.get("intent_id")
        if intent_id:
            if intent_id in nutrition_map:
                existing_priority = nutrition_map[intent_id].get("priority", 0)
                new_priority = item.get("priority", 0)
                if new_priority > existing_priority:
                    nutrition_map[intent_id] = item
            else:
                nutrition_map[intent_id] = item
    
    merged_nutrition = sorted(
        nutrition_map.values(),
        key=lambda x: (-x.get("priority", 0), x.get("intent_id", ""))
    )
    
    # Build merged intents
    merged = ProtocolIntents(
        contract_version=CONTRACT_VERSION,
        lifestyle=merged_lifestyle,
        nutrition=merged_nutrition,
        supplements=merged_supplements,
    )
    
    # Dedup count
    goals_supp_count = len(goals_intents.supplements)
    painpoint_supp_count = len(painpoint_intents.supplements)
    total_before = goals_supp_count + painpoint_supp_count
    total_after = len(merged_supplements)
    intents_deduplicated = total_before - total_after
    
    # Audit stats
    stats = {
        "goals_intents_count": goals_supp_count,
        "painpoint_intents_count": painpoint_supp_count,
        "merged_intents_count": total_after,
        "intents_deduplicated": intents_deduplicated,
        "priority_conflicts_resolved": conflicts_resolved,
        "lifestyle_merged_count": len(merged_lifestyle),
        "nutrition_merged_count": len(merged_nutrition),
    }
    
    return merged, stats


# =============================================================================
# MAIN RESOLVER FUNCTION
# =============================================================================

def resolve_all(input_data: ResolverInput) -> ResolverOutput:
    """
    Full resolver pipeline: merge constraints + merge intents.
    
    This is the main entry point for the resolver.
    
    Args:
        input_data: ResolverInput containing all inputs
        
    Returns:
        ResolverOutput with merged constraints, intents, and audit trail
    """
    # Compute input hash for audit
    input_hash = compute_hash(input_data.model_dump())
    
    # STEP 1: Merge constraints
    merged_constraints, constraint_stats = merge_constraints(
        input_data.bloodwork_constraints,
        input_data.lifestyle_constraints
    )
    
    # STEP 2: Get or create intents
    goals_intents = input_data.goals_intents or empty_protocol_intents()
    painpoint_intents = input_data.painpoint_intents or empty_protocol_intents()
    
    # STEP 3: Merge intents
    merged_intents, intent_stats = merge_intents(goals_intents, painpoint_intents)
    
    # Build output (without hash first)
    output_data = {
        "contract_version": CONTRACT_VERSION,
        "protocol_id": input_data.assessment_context.protocol_id,
        "run_id": input_data.assessment_context.run_id,
        "resolved_constraints": merged_constraints.model_dump(),
        "resolved_intents": merged_intents.model_dump(),
        "assessment_context": input_data.assessment_context.model_dump(),
    }
    
    # Compute output hash
    output_hash = compute_hash(output_data)
    
    # Build audit
    audit = ResolverAudit(
        resolver_version=RESOLVER_VERSION,
        resolved_at=now_iso(),
        input_hash=input_hash,
        output_hash=output_hash,
        bloodwork_blocked_count=constraint_stats["bloodwork_blocked_count"],
        lifestyle_blocked_count=constraint_stats["lifestyle_blocked_count"],
        merged_blocked_count=constraint_stats["merged_blocked_count"],
        goals_intents_count=intent_stats["goals_intents_count"],
        painpoint_intents_count=intent_stats["painpoint_intents_count"],
        merged_intents_count=intent_stats["merged_intents_count"],
        intents_deduplicated=intent_stats["intents_deduplicated"],
        priority_conflicts_resolved=intent_stats["priority_conflicts_resolved"],
    )
    
    # Final output
    return ResolverOutput(
        contract_version=CONTRACT_VERSION,
        protocol_id=input_data.assessment_context.protocol_id,
        run_id=input_data.assessment_context.run_id,
        resolved_constraints=merged_constraints,
        resolved_intents=merged_intents,
        assessment_context=input_data.assessment_context,
        audit=audit,
    )


# =============================================================================
# HELPER FUNCTIONS FOR EXTERNAL USE
# =============================================================================

def filter_blocked_intents(
    intents: ProtocolIntents,
    constraints: RoutingConstraints
) -> ProtocolIntents:
    """
    Filter intents based on constraints.
    
    Marks supplement intents as blocked if their target_id
    is in the blocked_targets list.
    
    Args:
        intents: Protocol intents to filter
        constraints: Routing constraints
        
    Returns:
        ProtocolIntents with blocked flags set
    """
    blocked_set = set(constraints.blocked_targets)
    
    filtered_supplements = []
    for intent in intents.supplements:
        if intent.target_id in blocked_set:
            # Create new intent with blocked=True
            filtered_supplements.append(ProtocolIntentItem(
                intent_id=intent.intent_id,
                target_id=intent.target_id,
                priority=intent.priority,
                source_goal=intent.source_goal,
                source_painpoint=intent.source_painpoint,
                blocked=True,
            ))
        else:
            filtered_supplements.append(intent)
    
    return ProtocolIntents(
        contract_version=intents.contract_version,
        lifestyle=intents.lifestyle,
        nutrition=intents.nutrition,
        supplements=filtered_supplements,
    )


def get_active_intents(intents: ProtocolIntents) -> List[ProtocolIntentItem]:
    """
    Get only non-blocked supplement intents.
    
    Args:
        intents: Protocol intents
        
    Returns:
        List of active (non-blocked) supplement intents
    """
    return [i for i in intents.supplements if not i.blocked]


def validate_resolver_output(output: ResolverOutput) -> Tuple[bool, List[str]]:
    """
    Validate resolver output for consistency.
    
    Args:
        output: ResolverOutput to validate
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    # Check version match
    if output.contract_version != CONTRACT_VERSION:
        errors.append(f"Version mismatch: expected {CONTRACT_VERSION}, got {output.contract_version}")
    
    # Check protocol_id presence
    if not output.protocol_id:
        errors.append("Missing protocol_id")
    
    # Check audit completeness
    if not output.audit.input_hash:
        errors.append("Missing input_hash in audit")
    if not output.audit.output_hash:
        errors.append("Missing output_hash in audit")
    
    # Check for logical consistency
    # No blocked targets should appear in allowed_targets
    blocked_set = set(output.resolved_constraints.blocked_targets)
    allowed_set = set(output.resolved_constraints.allowed_targets)
    overlap = blocked_set & allowed_set
    if overlap:
        errors.append(f"Targets appear in both blocked and allowed: {overlap}")
    
    return len(errors) == 0, errors
