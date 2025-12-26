"""
GenoMAX² Brain Pipeline - Full Integration

Coordinates all brain phases:
1. Orchestrate - Bloodwork → Routing Constraints
2. Compose - Painpoints + Lifestyle + Goals → Intents (with blood constraints)
3. Route - Intents → SKU Selection (future)

This module provides the unified entry point for the brain pipeline.
"""

import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

from .orchestrate import run_orchestrate, OrchestrateResult, OrchestrateStatus
from .compose import (
    PainpointInput,
    LifestyleInput,
    Intent,
    compose,
    ComposeResult,
    compose_result_to_dict
)


@dataclass
class BrainPipelineInput:
    """Full input for brain pipeline."""
    # Required: bloodwork signal
    signal_data: Dict[str, Any]
    
    # Optional: painpoints from user
    painpoints: Optional[List[Dict[str, Any]]] = None  # [{"id": "fatigue", "severity": 2}]
    
    # Optional: lifestyle questionnaire
    lifestyle: Optional[Dict[str, Any]] = None
    
    # Optional: user goals
    goals: Optional[List[str]] = None
    
    # Optional: signal hash for immutability verification
    signal_hash: Optional[str] = None


@dataclass 
class BrainPipelineResult:
    """Full result from brain pipeline."""
    run_id: str
    status: str
    
    # Orchestrate outputs
    signal_hash: str
    routing_constraints: List[Dict]
    assessment_context: Dict
    
    # Compose outputs
    intents: List[Dict]
    painpoints_applied: List[str]
    lifestyle_rules_applied: List[str]
    confidence_adjustments: Dict[str, float]
    
    # Audit
    audit: Dict
    error: Optional[str] = None


def convert_routing_to_blood_blocks(routing_constraints: List[Dict]) -> Dict[str, Any]:
    """
    Convert orchestrate routing constraints to blood_blocks format for compose.
    
    Routing constraints have: ingredient_class, constraint_type, reason, severity
    Blood blocks need: blocked_intents, required_intents
    """
    blocked = []
    required = []
    caution = []
    
    # Map ingredient classes to intents
    # This mapping connects the orchestrate output to compose input
    ingredient_to_intent = {
        "iron": ["iron_supplementation", "nutritional_support"],
        "potassium": ["electrolyte_balance"],
        "magnesium": ["magnesium_support", "stress_management"],
        "calcium": ["calcium_supplementation", "bone_health"],
        "vitamin_d_high": ["high_dose_vitamin_d"],
        "iodine": ["thyroid_support"],
        "hepatotoxic": ["liver_support"],
        "kava": ["relaxation_techniques"],
        "high_dose_niacin": ["cholesterol_support"],
        "green_tea_extract_high": ["weight_management"],
        "protein_supplements": ["protein_supplementation"],
        "high_glycemic": ["energy_boost"],
        "blood_sugar_support": ["blood_sugar_management"],
        "immune_stimulants": ["immune_support"]
    }
    
    for constraint in routing_constraints:
        ingredient_class = constraint.get("ingredient_class", "")
        constraint_type = constraint.get("constraint_type", "")
        
        # Get mapped intents for this ingredient class
        mapped_intents = ingredient_to_intent.get(ingredient_class, [ingredient_class])
        
        if constraint_type == "blocked":
            blocked.extend(mapped_intents)
        elif constraint_type == "required":
            required.extend(mapped_intents)
        elif constraint_type == "caution":
            caution.extend(mapped_intents)
    
    return {
        "blocked_intents": list(set(blocked)),
        "required_intents": list(set(required)),
        "caution_intents": list(set(caution))
    }


def parse_painpoints_input(raw: Optional[List[Dict]]) -> List[PainpointInput]:
    """Convert raw painpoints dict list to PainpointInput objects."""
    if not raw:
        return []
    
    result = []
    for item in raw:
        if isinstance(item, dict) and "id" in item:
            severity = item.get("severity", 2)
            # Clamp severity to 1-3
            severity = max(1, min(3, int(severity)))
            result.append(PainpointInput(id=item["id"], severity=severity))
    
    return result


def parse_lifestyle_input(raw: Optional[Dict]) -> Optional[LifestyleInput]:
    """Convert raw lifestyle dict to LifestyleInput object."""
    if not raw:
        return None
    
    try:
        return LifestyleInput(
            sleep_hours=float(raw.get("sleep_hours", 7)),
            sleep_quality=int(raw.get("sleep_quality", 7)),
            stress_level=int(raw.get("stress_level", 5)),
            activity_level=str(raw.get("activity_level", "moderate")),
            caffeine_intake=str(raw.get("caffeine_intake", "low")),
            alcohol_intake=str(raw.get("alcohol_intake", "none")),
            work_schedule=str(raw.get("work_schedule", "day")),
            meals_per_day=int(raw.get("meals_per_day", 3)),
            sugar_intake=str(raw.get("sugar_intake", "low")),
            smoking=bool(raw.get("smoking", False))
        )
    except (ValueError, TypeError) as e:
        # Return None if parsing fails
        return None


def parse_goals_to_intents(goals: Optional[List[str]]) -> List[Intent]:
    """Convert goal strings to Intent objects with base priority."""
    if not goals:
        return []
    
    # Goal string to intent mapping
    goal_intent_map = {
        "energy": ("increase_energy", 0.7),
        "sleep": ("improve_sleep", 0.7),
        "stress": ("stress_management", 0.7),
        "focus": ("cognitive_support", 0.7),
        "weight_loss": ("weight_management", 0.7),
        "muscle": ("muscle_support", 0.7),
        "recovery": ("recovery_support", 0.7),
        "immunity": ("immune_support", 0.6),
        "longevity": ("longevity_support", 0.6),
        "heart": ("cardiovascular_health", 0.6),
        "gut": ("gut_health_support", 0.6),
        "skin": ("skin_health", 0.5),
        "hair": ("nutritional_support", 0.5)
    }
    
    intents = []
    for goal in goals:
        goal_lower = goal.lower().strip()
        if goal_lower in goal_intent_map:
            intent_id, priority = goal_intent_map[goal_lower]
            intents.append(Intent(
                id=intent_id,
                priority=priority,
                source="goal",
                max_priority_cap=0.9
            ))
        else:
            # Create intent from goal name directly
            intents.append(Intent(
                id=goal_lower.replace(" ", "_"),
                priority=0.5,
                source="goal",
                max_priority_cap=0.8
            ))
    
    return intents


def run_brain_pipeline(
    pipeline_input: BrainPipelineInput,
    db_conn=None
) -> BrainPipelineResult:
    """
    Execute full brain pipeline.
    
    Pipeline flow:
    1. Orchestrate: bloodwork → routing constraints
    2. Convert: routing constraints → blood blocks
    3. Parse: painpoints, lifestyle, goals → typed inputs
    4. Compose: all inputs → prioritized intents
    5. (Future) Route: intents → SKU selection
    
    Args:
        pipeline_input: Complete pipeline input
        db_conn: Optional database connection for persistence
    
    Returns:
        BrainPipelineResult with complete audit trail
    """
    now = datetime.utcnow().isoformat() + "Z"
    audit = {"started_at": now, "phases": {}}
    
    # =========================================
    # Phase 1: Orchestrate
    # =========================================
    orchestrate_result = run_orchestrate(
        signal_data=pipeline_input.signal_data,
        provided_hash=pipeline_input.signal_hash,
        db_conn=db_conn
    )
    
    audit["phases"]["orchestrate"] = {
        "status": orchestrate_result.status.value,
        "constraints_count": len(orchestrate_result.routing_constraints)
    }
    
    # Check for orchestrate failure
    if orchestrate_result.status != OrchestrateStatus.SUCCESS:
        return BrainPipelineResult(
            run_id=orchestrate_result.run_id,
            status=f"orchestrate_{orchestrate_result.status.value}",
            signal_hash=orchestrate_result.signal_hash,
            routing_constraints=orchestrate_result.routing_constraints,
            assessment_context=orchestrate_result.assessment_context,
            intents=[],
            painpoints_applied=[],
            lifestyle_rules_applied=[],
            confidence_adjustments={},
            audit=audit,
            error=orchestrate_result.error
        )
    
    # =========================================
    # Phase 2: Convert routing → blood blocks
    # =========================================
    blood_blocks = convert_routing_to_blood_blocks(orchestrate_result.routing_constraints)
    audit["phases"]["convert"] = {
        "blocked_count": len(blood_blocks.get("blocked_intents", [])),
        "required_count": len(blood_blocks.get("required_intents", []))
    }
    
    # =========================================
    # Phase 3: Parse user inputs
    # =========================================
    painpoints = parse_painpoints_input(pipeline_input.painpoints)
    lifestyle = parse_lifestyle_input(pipeline_input.lifestyle)
    goal_intents = parse_goals_to_intents(pipeline_input.goals)
    
    audit["phases"]["parse"] = {
        "painpoints_count": len(painpoints),
        "lifestyle_provided": lifestyle is not None,
        "goals_count": len(goal_intents)
    }
    
    # =========================================
    # Phase 4: Compose
    # =========================================
    compose_result = compose(
        painpoints_input=painpoints if painpoints else None,
        lifestyle_input=lifestyle,
        goal_intents=goal_intents if goal_intents else None,
        blood_blocks=blood_blocks
    )
    
    audit["phases"]["compose"] = {
        "intents_generated": len(compose_result.intents),
        "painpoints_applied": compose_result.painpoints_applied,
        "lifestyle_rules_applied": compose_result.lifestyle_rules_applied,
        "audit_entries": len(compose_result.audit_log)
    }
    
    # Store compose audit in detail
    audit["compose_audit_log"] = compose_result.audit_log
    
    # =========================================
    # Persist compose results (if db provided)
    # =========================================
    if db_conn:
        try:
            _persist_compose_run(
                db_conn,
                run_id=orchestrate_result.run_id,
                painpoints_input=pipeline_input.painpoints,
                lifestyle_input=pipeline_input.lifestyle,
                compose_result=compose_result
            )
            audit["persisted"] = True
        except Exception as e:
            audit["persist_error"] = str(e)
    
    audit["completed_at"] = datetime.utcnow().isoformat() + "Z"
    
    # =========================================
    # Return Complete Result
    # =========================================
    return BrainPipelineResult(
        run_id=orchestrate_result.run_id,
        status="success",
        signal_hash=orchestrate_result.signal_hash,
        routing_constraints=orchestrate_result.routing_constraints,
        assessment_context=orchestrate_result.assessment_context,
        intents=[asdict(i) if hasattr(i, '__dataclass_fields__') else i.__dict__ 
                 for i in compose_result.intents],
        painpoints_applied=compose_result.painpoints_applied,
        lifestyle_rules_applied=compose_result.lifestyle_rules_applied,
        confidence_adjustments=compose_result.confidence_adjustments,
        audit=audit
    )


def _persist_compose_run(
    conn,
    run_id: str,
    painpoints_input: Optional[List[Dict]],
    lifestyle_input: Optional[Dict],
    compose_result: ComposeResult
):
    """Persist compose phase results to database."""
    cur = conn.cursor()
    
    # Get compose result as dict
    compose_dict = compose_result_to_dict(compose_result)
    
    # Store inputs and outputs in decision_outputs
    cur.execute("""
        INSERT INTO decision_outputs (run_id, phase, output_json, output_hash, created_at)
        VALUES (%s, %s, %s, %s, NOW())
    """, (
        run_id, 
        "compose",
        json.dumps({
            "painpoints_input": painpoints_input,
            "lifestyle_input": lifestyle_input,
            "result": compose_dict
        }),
        ""  # Hash computed separately if needed
    ))
    
    # Add audit log entry
    cur.execute("""
        INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
        VALUES (%s, %s, %s, %s, NOW())
    """, (
        "brain_run",
        run_id,
        "compose_completed",
        json.dumps({
            "intents_count": len(compose_result.intents),
            "painpoints_applied": compose_result.painpoints_applied,
            "lifestyle_rules_applied": compose_result.lifestyle_rules_applied
        })
    ))
    
    conn.commit()
    cur.close()


# =========================================
# Convenience Functions
# =========================================

def run_brain(
    signal_data: Dict[str, Any],
    painpoints: Optional[List[Dict]] = None,
    lifestyle: Optional[Dict] = None,
    goals: Optional[List[str]] = None,
    signal_hash: Optional[str] = None,
    db_conn=None
) -> BrainPipelineResult:
    """
    Convenience wrapper for run_brain_pipeline.
    
    Example usage:
        result = run_brain(
            signal_data={"user_id": "123", "markers": {"vitamin_d": 25}},
            painpoints=[{"id": "fatigue", "severity": 2}],
            lifestyle={"sleep_hours": 5, "stress_level": 8},
            goals=["energy", "sleep"]
        )
    """
    return run_brain_pipeline(
        BrainPipelineInput(
            signal_data=signal_data,
            painpoints=painpoints,
            lifestyle=lifestyle,
            goals=goals,
            signal_hash=signal_hash
        ),
        db_conn=db_conn
    )


def brain_result_to_dict(result: BrainPipelineResult) -> Dict[str, Any]:
    """Convert BrainPipelineResult to JSON-serializable dict."""
    return {
        "run_id": result.run_id,
        "status": result.status,
        "signal_hash": result.signal_hash,
        "routing_constraints": result.routing_constraints,
        "assessment_context": result.assessment_context,
        "intents": result.intents,
        "painpoints_applied": result.painpoints_applied,
        "lifestyle_rules_applied": result.lifestyle_rules_applied,
        "confidence_adjustments": result.confidence_adjustments,
        "audit": result.audit,
        "error": result.error
    }
