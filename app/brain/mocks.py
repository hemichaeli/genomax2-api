"""
GenoMAX² Brain Mock Engines v1.0
Replaceable Mock Implementations for Testing

These mocks allow the full pipeline to run end-to-end without
real Bloodwork, Lifestyle, or Goals engines.

IMPORTANT: These mocks produce VALID schemas that match contracts.
They enable testing and development until real engines are built.

Usage:
    from app.brain.mocks import bloodwork_mock, lifestyle_mock, goals_mock
    
    constraints = bloodwork_mock(assessment_context)
    intents = goals_mock(raw_goals, raw_painpoints)

To replace with real engines:
    1. Implement real engine with same function signature
    2. Import real engine instead of mock
    3. No other code changes needed (contract is stable)
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any

from app.brain.contracts import (
    CONTRACT_VERSION,
    AssessmentContext,
    RoutingConstraints,
    ProtocolIntents,
    ProtocolIntentItem,
    TargetDetail,
    empty_routing_constraints,
    empty_protocol_intents,
)


# =============================================================================
# MOCK CONFIGURATION
# =============================================================================

# Known medications that trigger constraints
MEDICATION_CONSTRAINTS = {
    "warfarin": {
        "caution_targets": ["vitamin_k", "omega3_high_dose"],
        "caution_ingredients": ["vitamin-k", "fish-oil-high-dose"],
        "reason": "Warfarin interaction risk",
    },
    "metformin": {
        "caution_targets": ["berberine_glucose"],
        "caution_ingredients": ["berberine"],
        "reason": "Metformin + Berberine may cause hypoglycemia",
    },
    "lithium": {
        "blocked_targets": ["iodine_supplement"],
        "blocked_ingredients": ["iodine"],
        "reason": "Lithium + Iodine may affect thyroid",
    },
    "blood_thinners": {
        "caution_targets": ["omega3_high_dose", "vitamin_e_high"],
        "caution_ingredients": ["fish-oil-high-dose", "vitamin-e-high"],
        "reason": "Bleeding risk with blood thinners",
    },
    "statins": {
        "caution_targets": ["coq10_depletion"],
        "caution_ingredients": [],
        "reason": "Statins may deplete CoQ10; supplementation often recommended",
    },
}

# Known conditions that trigger constraints
CONDITION_CONSTRAINTS = {
    "hemochromatosis": {
        "blocked_targets": ["iron_boost", "iron_support"],
        "blocked_ingredients": ["iron", "iron-bisglycinate"],
        "reason": "Iron overload disorder",
    },
    "kidney_disease": {
        "blocked_targets": ["creatine_performance", "potassium_supplement"],
        "blocked_ingredients": ["creatine", "potassium"],
        "caution_targets": ["magnesium_high_dose"],
        "reason": "Kidney function impairment",
    },
    "liver_disease": {
        "blocked_targets": ["hepatotoxic_supplements"],
        "blocked_ingredients": ["ashwagandha", "kava", "green-tea-extract-high"],
        "reason": "Hepatotoxicity risk",
    },
    "pregnancy": {
        "blocked_targets": ["retinol_high_dose", "certain_herbs"],
        "blocked_ingredients": ["retinol-high", "dong-quai", "black-cohosh"],
        "caution_targets": ["vitamin_a"],
        "reason": "Pregnancy contraindications",
    },
    "hyperthyroidism": {
        "blocked_targets": ["iodine_supplement"],
        "blocked_ingredients": ["iodine", "kelp"],
        "reason": "Thyroid hormone excess",
    },
    "hypercalcemia": {
        "blocked_targets": ["vitamin_d_high", "calcium_supplement"],
        "blocked_ingredients": ["vitamin-d3-high", "calcium"],
        "reason": "Elevated calcium levels",
    },
}

# Goal to intent mapping (minimal for testing)
GOAL_INTENT_MOCK_MAP = {
    "sleep": [
        {"intent_id": "magnesium_for_sleep", "target_id": "sleep_quality", "priority": 0.85},
        {"intent_id": "glycine_for_sleep", "target_id": "sleep_quality", "priority": 0.70},
        {"intent_id": "melatonin_sleep", "target_id": "sleep_onset", "priority": 0.65},
    ],
    "energy": [
        {"intent_id": "b12_energy_support", "target_id": "energy_metabolism", "priority": 0.80},
        {"intent_id": "iron_energy_support", "target_id": "oxygen_transport", "priority": 0.75},
        {"intent_id": "coq10_cellular_energy", "target_id": "mitochondrial_function", "priority": 0.70},
    ],
    "stress": [
        {"intent_id": "magnesium_stress_support", "target_id": "stress_response", "priority": 0.80},
        {"intent_id": "adaptogen_support", "target_id": "hpa_axis", "priority": 0.70},
    ],
    "focus": [
        {"intent_id": "omega3_brain_support", "target_id": "cognitive_function", "priority": 0.85},
        {"intent_id": "lions_mane_cognition", "target_id": "neuroplasticity", "priority": 0.70},
    ],
    "immunity": [
        {"intent_id": "vitamin_d_immune", "target_id": "immune_function", "priority": 0.85},
        {"intent_id": "zinc_immune_support", "target_id": "immune_function", "priority": 0.75},
        {"intent_id": "vitamin_c_immune", "target_id": "antioxidant_defense", "priority": 0.70},
    ],
    "heart": [
        {"intent_id": "omega3_cardiovascular", "target_id": "cardiovascular_health", "priority": 0.85},
        {"intent_id": "coq10_heart_support", "target_id": "heart_muscle", "priority": 0.75},
        {"intent_id": "magnesium_heart", "target_id": "blood_pressure", "priority": 0.70},
    ],
    "gut": [
        {"intent_id": "probiotic_support", "target_id": "gut_microbiome", "priority": 0.80},
        {"intent_id": "digestive_enzyme_support", "target_id": "digestion", "priority": 0.65},
        {"intent_id": "fiber_gut_health", "target_id": "gut_motility", "priority": 0.60},
    ],
    "inflammation": [
        {"intent_id": "omega3_antiinflammatory", "target_id": "inflammation", "priority": 0.85},
        {"intent_id": "curcumin_inflammation", "target_id": "inflammation", "priority": 0.75},
    ],
    "liver": [
        {"intent_id": "milk_thistle_liver", "target_id": "liver_function", "priority": 0.75},
        {"intent_id": "nac_liver_support", "target_id": "glutathione", "priority": 0.70},
    ],
    "muscle": [
        {"intent_id": "creatine_performance", "target_id": "muscle_strength", "priority": 0.85},
        {"intent_id": "protein_muscle_support", "target_id": "muscle_synthesis", "priority": 0.80},
    ],
    "joints": [
        {"intent_id": "collagen_joint_support", "target_id": "joint_health", "priority": 0.80},
        {"intent_id": "omega3_joint", "target_id": "joint_inflammation", "priority": 0.70},
    ],
    "blood_sugar": [
        {"intent_id": "berberine_glucose", "target_id": "glucose_metabolism", "priority": 0.80},
        {"intent_id": "chromium_glucose", "target_id": "insulin_sensitivity", "priority": 0.65},
    ],
}

# Painpoint to intent mapping
PAINPOINT_INTENT_MOCK_MAP = {
    "fatigue": [
        {"intent_id": "b12_energy_support", "target_id": "energy_metabolism", "priority": 0.90},
        {"intent_id": "iron_energy_support", "target_id": "oxygen_transport", "priority": 0.85},
        {"intent_id": "coq10_cellular_energy", "target_id": "mitochondrial_function", "priority": 0.75},
    ],
    "brain_fog": [
        {"intent_id": "omega3_brain_support", "target_id": "cognitive_function", "priority": 0.90},
        {"intent_id": "b12_energy_support", "target_id": "neurological_function", "priority": 0.80},
        {"intent_id": "lions_mane_cognition", "target_id": "neuroplasticity", "priority": 0.75},
    ],
    "poor_sleep": [
        {"intent_id": "magnesium_for_sleep", "target_id": "sleep_quality", "priority": 0.90},
        {"intent_id": "glycine_for_sleep", "target_id": "sleep_quality", "priority": 0.80},
    ],
    "anxiety": [
        {"intent_id": "magnesium_stress_support", "target_id": "stress_response", "priority": 0.85},
        {"intent_id": "adaptogen_support", "target_id": "hpa_axis", "priority": 0.75},
    ],
    "joint_pain": [
        {"intent_id": "omega3_antiinflammatory", "target_id": "inflammation", "priority": 0.85},
        {"intent_id": "collagen_joint_support", "target_id": "joint_health", "priority": 0.80},
        {"intent_id": "curcumin_inflammation", "target_id": "inflammation", "priority": 0.75},
    ],
    "frequent_illness": [
        {"intent_id": "vitamin_d_immune", "target_id": "immune_function", "priority": 0.90},
        {"intent_id": "zinc_immune_support", "target_id": "immune_function", "priority": 0.85},
    ],
    "digestive_issues": [
        {"intent_id": "probiotic_support", "target_id": "gut_microbiome", "priority": 0.85},
        {"intent_id": "digestive_enzyme_support", "target_id": "digestion", "priority": 0.75},
    ],
    "muscle_weakness": [
        {"intent_id": "creatine_performance", "target_id": "muscle_strength", "priority": 0.85},
        {"intent_id": "vitamin_d_immune", "target_id": "muscle_function", "priority": 0.75},
    ],
}

# Lifestyle intent mapping
LIFESTYLE_INTENT_MOCK_MAP = {
    "sleep": [
        {"intent_id": "improve_sleep_quality", "priority": 0.85, "category": "sleep"},
        {"intent_id": "regulate_circadian_rhythm", "priority": 0.70, "category": "sleep"},
    ],
    "energy": [
        {"intent_id": "optimize_energy_levels", "priority": 0.85, "category": "energy"},
        {"intent_id": "morning_light_exposure", "priority": 0.70, "category": "circadian"},
    ],
    "stress": [
        {"intent_id": "reduce_stress_response", "priority": 0.85, "category": "stress"},
        {"intent_id": "breathwork_practice", "priority": 0.70, "category": "stress"},
    ],
    "focus": [
        {"intent_id": "enhance_cognitive_function", "priority": 0.85, "category": "cognitive"},
        {"intent_id": "attention_training", "priority": 0.65, "category": "cognitive"},
    ],
}

# Nutrition intent mapping
NUTRITION_INTENT_MOCK_MAP = {
    "sleep": [
        {"intent_id": "evening_carb_timing", "priority": 0.60, "category": "timing"},
    ],
    "energy": [
        {"intent_id": "blood_sugar_stability", "priority": 0.75, "category": "macros"},
    ],
    "stress": [
        {"intent_id": "anti_stress_nutrition", "priority": 0.65, "category": "nutrition"},
    ],
    "focus": [
        {"intent_id": "brain_fuel_optimization", "priority": 0.70, "category": "nutrition"},
    ],
    "immunity": [
        {"intent_id": "immune_nutrition", "priority": 0.70, "category": "nutrition"},
    ],
    "heart": [
        {"intent_id": "heart_healthy_diet", "priority": 0.80, "category": "nutrition"},
    ],
    "gut": [
        {"intent_id": "fiber_diversity", "priority": 0.75, "category": "fiber"},
        {"intent_id": "fermented_foods", "priority": 0.70, "category": "probiotics"},
    ],
    "inflammation": [
        {"intent_id": "anti_inflammatory_diet", "priority": 0.80, "category": "nutrition"},
    ],
    "liver": [
        {"intent_id": "liver_supportive_diet", "priority": 0.80, "category": "nutrition"},
    ],
}


# =============================================================================
# MOCK ENGINE FUNCTIONS
# =============================================================================

def bloodwork_mock(
    assessment_context: AssessmentContext,
    simulated_markers: Optional[Dict[str, Any]] = None
) -> RoutingConstraints:
    """
    Mock Bloodwork Engine.
    
    Returns empty constraints by default.
    Can be toggled to return constraints based on:
    - assessment_context.meds (medication interactions)
    - assessment_context.conditions (condition contraindications)
    - simulated_markers (for testing specific biomarker scenarios)
    
    Args:
        assessment_context: User context from assessment
        simulated_markers: Optional dict of marker -> value for testing
        
    Returns:
        RoutingConstraints (valid schema)
    """
    blocked_targets: List[str] = []
    caution_targets: List[str] = []
    blocked_ingredients: List[str] = []
    target_details: Dict[str, TargetDetail] = {}
    global_flags: List[str] = []
    has_critical = False
    
    # Check medications
    meds = [m.lower().strip() for m in (assessment_context.meds or [])]
    for med in meds:
        if med in MEDICATION_CONSTRAINTS:
            rule = MEDICATION_CONSTRAINTS[med]
            
            # Add caution targets
            for target in rule.get("caution_targets", []):
                if target not in caution_targets:
                    caution_targets.append(target)
                    target_details[target] = TargetDetail(
                        gate_status="caution",
                        reason=rule["reason"],
                        blocking_biomarkers=[],
                        caution_biomarkers=[],
                        source="bloodwork_mock:medication"
                    )
            
            # Add blocked targets
            for target in rule.get("blocked_targets", []):
                if target not in blocked_targets:
                    blocked_targets.append(target)
                    target_details[target] = TargetDetail(
                        gate_status="blocked",
                        reason=rule["reason"],
                        blocking_biomarkers=[],
                        caution_biomarkers=[],
                        source="bloodwork_mock:medication"
                    )
            
            # Add blocked ingredients
            for ing in rule.get("blocked_ingredients", []):
                if ing not in blocked_ingredients:
                    blocked_ingredients.append(ing)
            
            # Add caution ingredients as blocked (conservative)
            for ing in rule.get("caution_ingredients", []):
                if ing not in blocked_ingredients:
                    blocked_ingredients.append(ing)
    
    # Check conditions
    conditions = [c.lower().strip() for c in (assessment_context.conditions or [])]
    for condition in conditions:
        if condition in CONDITION_CONSTRAINTS:
            rule = CONDITION_CONSTRAINTS[condition]
            
            for target in rule.get("blocked_targets", []):
                if target not in blocked_targets:
                    blocked_targets.append(target)
                    target_details[target] = TargetDetail(
                        gate_status="blocked",
                        reason=rule["reason"],
                        blocking_biomarkers=[],
                        caution_biomarkers=[],
                        source="bloodwork_mock:condition"
                    )
                    
            for target in rule.get("caution_targets", []):
                if target not in caution_targets and target not in blocked_targets:
                    caution_targets.append(target)
                    target_details[target] = TargetDetail(
                        gate_status="caution",
                        reason=rule["reason"],
                        blocking_biomarkers=[],
                        caution_biomarkers=[],
                        source="bloodwork_mock:condition"
                    )
            
            for ing in rule.get("blocked_ingredients", []):
                if ing not in blocked_ingredients:
                    blocked_ingredients.append(ing)
            
            # Flag critical conditions
            if condition in ("hemochromatosis", "kidney_disease", "liver_disease"):
                has_critical = True
                global_flags.append(f"CRITICAL_CONDITION:{condition.upper()}")
    
    # Sort for determinism
    blocked_targets = sorted(blocked_targets)
    caution_targets = sorted(caution_targets)
    blocked_ingredients = sorted(blocked_ingredients)
    global_flags = sorted(global_flags)
    
    return RoutingConstraints(
        contract_version=CONTRACT_VERSION,
        blocked_targets=blocked_targets,
        caution_targets=caution_targets,
        allowed_targets=[],  # Mock doesn't populate allowed
        blocked_ingredients=blocked_ingredients,
        has_critical_flags=has_critical,
        global_flags=global_flags,
        target_details=target_details,
    )


def lifestyle_mock(
    assessment_context: AssessmentContext,
    lifestyle_factors: Optional[Dict[str, Any]] = None
) -> RoutingConstraints:
    """
    Mock Lifestyle Engine.
    
    Returns empty constraints by default.
    Can be extended to return constraints based on lifestyle factors.
    
    Args:
        assessment_context: User context
        lifestyle_factors: Optional dict for testing (e.g., {"fasting": True})
        
    Returns:
        RoutingConstraints (valid schema, typically empty)
    """
    # Default: return empty constraints
    # Future: Add lifestyle-based constraints (fasting, shift work, etc.)
    
    blocked_targets: List[str] = []
    caution_targets: List[str] = []
    
    if lifestyle_factors:
        # Example: fasting might caution certain supplements
        if lifestyle_factors.get("fasting"):
            caution_targets.append("high_dose_fat_soluble")
        
        # Example: shift work might affect melatonin timing
        if lifestyle_factors.get("shift_work"):
            caution_targets.append("melatonin_fixed_timing")
    
    return RoutingConstraints(
        contract_version=CONTRACT_VERSION,
        blocked_targets=sorted(blocked_targets),
        caution_targets=sorted(caution_targets),
        allowed_targets=[],
        blocked_ingredients=[],
        has_critical_flags=False,
        global_flags=[],
        target_details={},
    )


def goals_mock(
    raw_goals: List[str],
    raw_painpoints: Optional[List[str]] = None
) -> ProtocolIntents:
    """
    Mock Goals/Painpoints Engine.
    
    Returns deterministic intents for known goal strings.
    
    Args:
        raw_goals: List of goal strings (e.g., ["sleep", "energy"])
        raw_painpoints: Optional list of painpoint strings
        
    Returns:
        ProtocolIntents (valid schema)
    """
    supplement_intents: List[ProtocolIntentItem] = []
    lifestyle_intents: List[Dict[str, Any]] = []
    nutrition_intents: List[Dict[str, Any]] = []
    seen_intent_ids: set = set()
    
    # Process goals
    for goal in (raw_goals or []):
        goal_lower = goal.lower().strip()
        
        # Supplement intents from goals
        if goal_lower in GOAL_INTENT_MOCK_MAP:
            for intent_data in GOAL_INTENT_MOCK_MAP[goal_lower]:
                intent_id = intent_data["intent_id"]
                if intent_id not in seen_intent_ids:
                    supplement_intents.append(ProtocolIntentItem(
                        intent_id=intent_id,
                        target_id=intent_data["target_id"],
                        priority=intent_data["priority"],
                        source_goal=goal_lower,
                        source_painpoint=None,
                        blocked=False,
                    ))
                    seen_intent_ids.add(intent_id)
        
        # Lifestyle intents from goals
        if goal_lower in LIFESTYLE_INTENT_MOCK_MAP:
            for intent_data in LIFESTYLE_INTENT_MOCK_MAP[goal_lower]:
                lifestyle_intents.append({
                    "intent_id": intent_data["intent_id"],
                    "priority": intent_data["priority"],
                    "category": intent_data.get("category"),
                    "source_goal": goal_lower,
                })
        
        # Nutrition intents from goals
        if goal_lower in NUTRITION_INTENT_MOCK_MAP:
            for intent_data in NUTRITION_INTENT_MOCK_MAP[goal_lower]:
                nutrition_intents.append({
                    "intent_id": intent_data["intent_id"],
                    "priority": intent_data["priority"],
                    "category": intent_data.get("category"),
                    "source_goal": goal_lower,
                })
    
    # Process painpoints (with higher priority)
    for painpoint in (raw_painpoints or []):
        painpoint_lower = painpoint.lower().strip()
        
        if painpoint_lower in PAINPOINT_INTENT_MOCK_MAP:
            for intent_data in PAINPOINT_INTENT_MOCK_MAP[painpoint_lower]:
                intent_id = intent_data["intent_id"]
                if intent_id not in seen_intent_ids:
                    supplement_intents.append(ProtocolIntentItem(
                        intent_id=intent_id,
                        target_id=intent_data["target_id"],
                        priority=intent_data["priority"],
                        source_goal=None,
                        source_painpoint=painpoint_lower,
                        blocked=False,
                    ))
                    seen_intent_ids.add(intent_id)
                else:
                    # Update priority if painpoint has higher priority
                    for i, existing in enumerate(supplement_intents):
                        if existing.intent_id == intent_id:
                            if intent_data["priority"] > existing.priority:
                                supplement_intents[i] = ProtocolIntentItem(
                                    intent_id=intent_id,
                                    target_id=intent_data["target_id"],
                                    priority=intent_data["priority"],
                                    source_goal=existing.source_goal,
                                    source_painpoint=painpoint_lower,
                                    blocked=False,
                                )
                            break
    
    # Sort by priority desc for determinism
    supplement_intents.sort(key=lambda x: (-x.priority, x.intent_id))
    lifestyle_intents.sort(key=lambda x: (-x.get("priority", 0), x.get("intent_id", "")))
    nutrition_intents.sort(key=lambda x: (-x.get("priority", 0), x.get("intent_id", "")))
    
    return ProtocolIntents(
        contract_version=CONTRACT_VERSION,
        lifestyle=lifestyle_intents,
        nutrition=nutrition_intents,
        supplements=supplement_intents,
    )


def create_test_assessment_context(
    protocol_id: str = "test-protocol-001",
    run_id: str = "test-run-001",
    gender: str = "male",
    age: int = 35,
    meds: Optional[List[str]] = None,
    conditions: Optional[List[str]] = None,
) -> AssessmentContext:
    """
    Helper to create AssessmentContext for testing.
    
    Args:
        protocol_id: Protocol ID
        run_id: Run ID
        gender: Gender ("male" or "female")
        age: Age in years
        meds: List of medications
        conditions: List of health conditions
        
    Returns:
        AssessmentContext
    """
    return AssessmentContext(
        contract_version=CONTRACT_VERSION,
        protocol_id=protocol_id,
        run_id=run_id,
        gender=gender,
        age=age,
        height_cm=None,
        weight_kg=None,
        meds=meds or [],
        conditions=conditions or [],
        allergies=[],
        flags={},
    )
