"""
GenoMAX² Compose Phase - Intent Generation and Priority Modulation

This module handles:
1. Painpoints → Intent generation (creates intents with base priority)
2. Lifestyle → Priority modification (adjusts existing intent priorities)
3. Blood constraints enforcement (always overrides)

SCOPE RULES:
- Painpoints MAY create intents and set base priority
- Lifestyle MAY ONLY modify priority (never creates intents)
- Blood constraints ALWAYS override everything
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class PainpointInput:
    """Single painpoint with severity rating."""
    id: str
    severity: int  # 1-3

@dataclass
class LifestyleInput:
    """User lifestyle questionnaire responses."""
    sleep_hours: float
    sleep_quality: int  # 1-10
    stress_level: int  # 1-10
    activity_level: str  # sedentary|light|moderate|high
    caffeine_intake: str  # none|low|medium|high
    alcohol_intake: str  # none|low|medium|high
    work_schedule: str  # day|night|rotating
    meals_per_day: int
    sugar_intake: str  # low|medium|high
    smoking: bool

@dataclass
class Intent:
    """Generated protocol intent with priority."""
    id: str
    priority: float
    source: str  # "painpoint", "goal", "blood"
    confidence: float = 1.0
    max_priority_cap: float = 1.0
    applied_modifiers: Dict[str, float] = field(default_factory=dict)

@dataclass
class ComposeResult:
    """Result of compose phase - merged intents with audit trail."""
    intents: List[Intent]
    painpoints_applied: List[str]
    lifestyle_rules_applied: List[str]
    confidence_adjustments: Dict[str, float]
    audit_log: List[str]


# ---------------------------------------------------------------------------
# Config Loaders
# ---------------------------------------------------------------------------

def get_config_path(filename: str) -> Path:
    """Get absolute path to config file."""
    # Try relative to this file first
    base = Path(__file__).parent.parent.parent / "config"
    if (base / filename).exists():
        return base / filename
    # Fall back to project root
    return Path(os.getcwd()) / "config" / filename


def load_painpoints_dictionary() -> Dict[str, Any]:
    """Load painpoints dictionary config."""
    path = get_config_path("painpoints/painpoints_dictionary.v1.json")
    with open(path, 'r') as f:
        data = json.load(f)
    return data.get("painpoints", {})


def load_lifestyle_ruleset() -> Tuple[List[Dict], Dict[str, Any]]:
    """Load lifestyle ruleset config. Returns (rules, constraints)."""
    path = get_config_path("lifestyle/lifestyle_ruleset.v1.json")
    with open(path, 'r') as f:
        data = json.load(f)
    return data.get("rules", []), data.get("global_constraints", {})


# ---------------------------------------------------------------------------
# Painpoints → Intents
# ---------------------------------------------------------------------------

def generate_intents_from_painpoints(
    painpoints: List[PainpointInput]
) -> Tuple[List[Intent], List[str]]:
    """
    Map painpoints to intents with base priority.
    
    Priority calculation: severity × mapping_weight
    Capped by max_priority_cap
    
    Returns: (intents, audit_log)
    """
    dictionary = load_painpoints_dictionary()
    intents: Dict[str, Intent] = {}
    audit_log: List[str] = []
    applied: List[str] = []
    
    for pp in painpoints:
        if pp.id not in dictionary:
            audit_log.append(f"[WARN] Unknown painpoint: {pp.id}")
            continue
            
        config = dictionary[pp.id]
        max_cap = config.get("max_priority_cap", 0.85)
        mapped_intents = config.get("mapped_intents", {})
        
        for intent_id, weight in mapped_intents.items():
            # Calculate priority: severity × weight
            raw_priority = pp.severity * weight
            capped_priority = min(raw_priority, max_cap)
            
            if intent_id in intents:
                # Merge: take max priority
                existing = intents[intent_id]
                if capped_priority > existing.priority:
                    existing.priority = capped_priority
                    existing.applied_modifiers[f"painpoint:{pp.id}"] = weight
                    audit_log.append(
                        f"[MERGE] {intent_id}: updated priority to {capped_priority:.2f} from {pp.id}"
                    )
            else:
                intents[intent_id] = Intent(
                    id=intent_id,
                    priority=capped_priority,
                    source="painpoint",
                    max_priority_cap=max_cap,
                    applied_modifiers={f"painpoint:{pp.id}": weight}
                )
                audit_log.append(
                    f"[NEW] {intent_id}: priority={capped_priority:.2f} from {pp.id}"
                )
        
        applied.append(pp.id)
    
    return list(intents.values()), applied, audit_log


# ---------------------------------------------------------------------------
# Lifestyle → Priority Modifiers
# ---------------------------------------------------------------------------

def evaluate_rule_condition(
    condition: Dict[str, Any],
    lifestyle: LifestyleInput
) -> bool:
    """Check if a single rule condition matches the lifestyle input."""
    for field, check in condition.items():
        value = getattr(lifestyle, field, None)
        if value is None:
            return False
            
        if isinstance(check, dict):
            if "min" in check and value < check["min"]:
                return False
            if "max" in check and value > check["max"]:
                return False
            if "equals" in check and value != check["equals"]:
                return False
        elif value != check:
            return False
    
    return True


def apply_lifestyle_modifiers(
    intents: List[Intent],
    lifestyle: LifestyleInput
) -> Tuple[List[Intent], List[str], Dict[str, float], List[str]]:
    """
    Apply lifestyle rules to modify intent priorities.
    
    CRITICAL: Lifestyle CANNOT create new intents.
    It can only modify priority of EXISTING intents.
    
    Returns: (modified_intents, applied_rules, confidence_adjustments, audit_log)
    """
    rules, constraints = load_lifestyle_ruleset()
    audit_log: List[str] = []
    applied_rules: List[str] = []
    confidence_adjustments: Dict[str, float] = {}
    
    max_modifier = constraints.get("max_modifier_value", 1.0)
    min_modifier = constraints.get("min_modifier_value", -1.0)
    
    # Build intent lookup
    intent_map = {i.id: i for i in intents}
    
    for rule in rules:
        conditions = rule.get("input_conditions", {})
        
        if not evaluate_rule_condition(conditions, lifestyle):
            continue
            
        rule_id = rule.get("id", "unknown")
        effects = rule.get("effects", {})
        applied_rules.append(rule_id)
        
        # Apply intent modifiers
        for intent_id, modifier in effects.get("intent_modifiers", {}).items():
            # Clamp modifier
            modifier = max(min_modifier, min(max_modifier, modifier))
            
            if intent_id in intent_map:
                intent = intent_map[intent_id]
                old_priority = intent.priority
                
                # Apply modifier: priority = priority × (1 + modifier)
                new_priority = intent.priority * (1 + modifier)
                new_priority = max(0, min(intent.max_priority_cap, new_priority))
                
                intent.priority = new_priority
                intent.applied_modifiers[f"lifestyle:{rule_id}"] = modifier
                
                audit_log.append(
                    f"[MOD] {intent_id}: {old_priority:.2f} → {new_priority:.2f} "
                    f"(rule: {rule_id}, modifier: {modifier:+.2f})"
                )
            else:
                # CRITICAL: Do NOT create new intents from lifestyle
                audit_log.append(
                    f"[SKIP] Cannot modify {intent_id} - intent does not exist "
                    f"(lifestyle cannot create intents)"
                )
        
        # Apply confidence penalty
        penalty = effects.get("confidence_penalty", 0.0)
        if penalty > 0:
            for intent in intents:
                intent.confidence *= (1 - penalty)
                if intent.id not in confidence_adjustments:
                    confidence_adjustments[intent.id] = 0
                confidence_adjustments[intent.id] += penalty
            
            audit_log.append(
                f"[CONF] Applied {penalty:.1%} confidence penalty from {rule_id}"
            )
    
    return intents, applied_rules, confidence_adjustments, audit_log


# ---------------------------------------------------------------------------
# Blood Constraints (Interface)
# ---------------------------------------------------------------------------

def apply_blood_constraints(
    intents: List[Intent],
    blood_blocks: Optional[Dict[str, Any]] = None
) -> Tuple[List[Intent], List[str]]:
    """
    Apply blood-based constraints to intents.
    
    Blood constraints ALWAYS override:
    - If blood says block, intent is removed
    - If blood says require, priority is maximized
    
    This is a stub - actual implementation calls existing Bloodwork Engine.
    """
    if not blood_blocks:
        return intents, []
    
    audit_log: List[str] = []
    filtered: List[Intent] = []
    
    for intent in intents:
        if intent.id in blood_blocks.get("blocked_intents", []):
            audit_log.append(
                f"[BLOCK] {intent.id}: removed by blood constraint"
            )
            continue
        
        if intent.id in blood_blocks.get("required_intents", []):
            intent.priority = intent.max_priority_cap
            intent.source = "blood"
            audit_log.append(
                f"[FORCE] {intent.id}: priority maxed by blood requirement"
            )
        
        filtered.append(intent)
    
    return filtered, audit_log


# ---------------------------------------------------------------------------
# Main Compose Function
# ---------------------------------------------------------------------------

def compose(
    painpoints_input: Optional[List[PainpointInput]] = None,
    lifestyle_input: Optional[LifestyleInput] = None,
    goal_intents: Optional[List[Intent]] = None,
    blood_blocks: Optional[Dict[str, Any]] = None
) -> ComposeResult:
    """
    Main compose phase - generates and modifies intents.
    
    Pipeline:
    1. Generate intents from painpoints (creates intents)
    2. Merge with goal-based intents
    3. Apply lifestyle modifiers (adjusts priority)
    4. Apply blood constraints (overrides everything)
    
    Returns: ComposeResult with full audit trail
    """
    all_intents: List[Intent] = []
    full_audit: List[str] = []
    painpoints_applied: List[str] = []
    lifestyle_rules_applied: List[str] = []
    confidence_adjustments: Dict[str, float] = {}
    
    # Step 1: Painpoints → Intents
    if painpoints_input:
        pp_intents, pp_applied, pp_audit = generate_intents_from_painpoints(painpoints_input)
        all_intents.extend(pp_intents)
        painpoints_applied = pp_applied
        full_audit.extend(pp_audit)
    
    # Step 2: Merge goal-based intents
    if goal_intents:
        intent_map = {i.id: i for i in all_intents}
        for goal_intent in goal_intents:
            if goal_intent.id in intent_map:
                existing = intent_map[goal_intent.id]
                if goal_intent.priority > existing.priority:
                    existing.priority = goal_intent.priority
                    full_audit.append(
                        f"[MERGE] {goal_intent.id}: goal priority wins"
                    )
            else:
                all_intents.append(goal_intent)
                full_audit.append(
                    f"[NEW] {goal_intent.id}: from goal"
                )
    
    # Step 3: Lifestyle → Priority modifiers
    if lifestyle_input and all_intents:
        all_intents, ls_applied, conf_adj, ls_audit = apply_lifestyle_modifiers(
            all_intents, lifestyle_input
        )
        lifestyle_rules_applied = ls_applied
        confidence_adjustments = conf_adj
        full_audit.extend(ls_audit)
    
    # Step 4: Blood constraints override
    if blood_blocks:
        all_intents, blood_audit = apply_blood_constraints(all_intents, blood_blocks)
        full_audit.extend(blood_audit)
    
    # Sort by priority descending
    all_intents.sort(key=lambda x: x.priority, reverse=True)
    
    return ComposeResult(
        intents=all_intents,
        painpoints_applied=painpoints_applied,
        lifestyle_rules_applied=lifestyle_rules_applied,
        confidence_adjustments=confidence_adjustments,
        audit_log=full_audit
    )


# ---------------------------------------------------------------------------
# Serialization Helpers
# ---------------------------------------------------------------------------

def compose_result_to_dict(result: ComposeResult) -> Dict[str, Any]:
    """Convert ComposeResult to JSON-serializable dict for storage."""
    return {
        "intents": [
            {
                "id": i.id,
                "priority": i.priority,
                "source": i.source,
                "confidence": i.confidence,
                "max_priority_cap": i.max_priority_cap,
                "applied_modifiers": i.applied_modifiers
            }
            for i in result.intents
        ],
        "painpoints_applied": result.painpoints_applied,
        "lifestyle_rules_applied": result.lifestyle_rules_applied,
        "confidence_adjustments": result.confidence_adjustments,
        "audit_log": result.audit_log
    }
