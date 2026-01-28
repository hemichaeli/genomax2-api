"""
GenoMAX² Constraint Translator
==============================
Translates bloodwork signals into routing constraints for supplement selection.

"Blood does not negotiate" - deterministic constraint enforcement.

Version: 1.0.0
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field

# =============================================================================
# MODELS
# =============================================================================

class ConstraintType(str, Enum):
    BLOCK = "block"           # Hard block - never include
    BOOST = "boost"           # Prioritize - deficiency detected
    REDUCE = "reduce"         # Lower dosage recommended
    CAUTION = "caution"       # Include with warning
    REQUIRE = "require"       # Must include - critical deficiency

class ConstraintSource(str, Enum):
    BLOODWORK = "bloodwork"
    SAFETY_GATE = "safety_gate"
    LIFECYCLE = "lifecycle"
    USER_PREFERENCE = "user_preference"
    DRUG_INTERACTION = "drug_interaction"

class RoutingConstraint(BaseModel):
    """Single routing constraint for supplement selection."""
    constraint_id: str
    constraint_type: ConstraintType
    source: ConstraintSource
    target_type: str  # ingredient, module, category
    target_ids: List[str]
    reason: str
    marker_code: Optional[str] = None
    marker_value: Optional[float] = None
    threshold: Optional[float] = None
    dosage_modifier: Optional[float] = None  # 0.5 = half dose, 2.0 = double
    priority: int = 5  # 1=highest, 10=lowest
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ConstraintSet(BaseModel):
    """Complete set of routing constraints for a user."""
    user_id: str
    submission_id: str
    constraints: List[RoutingConstraint]
    blocked_ingredients: Set[str] = set()
    boosted_ingredients: Set[str] = set()
    required_ingredients: Set[str] = set()
    caution_ingredients: Set[str] = set()
    dosage_modifiers: Dict[str, float] = {}
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        arbitrary_types_allowed = True

# =============================================================================
# BIOMARKER-TO-INGREDIENT MAPPING
# =============================================================================

DEFICIENCY_RECOMMENDATIONS = {
    # Iron markers
    "ferritin": {
        "deficient": {"threshold": 30, "ingredients": ["iron_bisglycinate"], "dosage_modifier": 1.0},
        "critical": {"threshold": 10, "ingredients": ["iron_bisglycinate"], "dosage_modifier": 1.5},
    },
    "serum_iron": {
        "deficient": {"threshold": 60, "ingredients": ["iron_bisglycinate"], "dosage_modifier": 1.0},
    },
    
    # Vitamin D
    "vitamin_d_25oh": {
        "deficient": {"threshold": 20, "ingredients": ["vitamin_d3"], "dosage_modifier": 2.0},
        "insufficient": {"threshold": 30, "ingredients": ["vitamin_d3"], "dosage_modifier": 1.0},
    },
    
    # B Vitamins
    "vitamin_b12": {
        "deficient": {"threshold": 200, "ingredients": ["methylcobalamin", "vitamin_b12"], "dosage_modifier": 1.5},
        "suboptimal": {"threshold": 400, "ingredients": ["methylcobalamin"], "dosage_modifier": 1.0},
    },
    "folate": {
        "deficient": {"threshold": 3, "ingredients": ["methylfolate", "folate"], "dosage_modifier": 1.5},
        "suboptimal": {"threshold": 7, "ingredients": ["methylfolate"], "dosage_modifier": 1.0},
    },
    
    # Minerals
    "magnesium_rbc": {
        "deficient": {"threshold": 4.2, "ingredients": ["magnesium_glycinate", "magnesium_threonate"], "dosage_modifier": 1.5},
        "suboptimal": {"threshold": 5.0, "ingredients": ["magnesium_glycinate"], "dosage_modifier": 1.0},
    },
    "zinc": {
        "deficient": {"threshold": 60, "ingredients": ["zinc_picolinate", "zinc"], "dosage_modifier": 1.0},
    },
    
    # Cardiovascular/Inflammation
    "omega3_index": {
        "deficient": {"threshold": 4, "ingredients": ["omega3_fish_oil", "epa", "dha"], "dosage_modifier": 1.5},
        "suboptimal": {"threshold": 8, "ingredients": ["omega3_fish_oil"], "dosage_modifier": 1.0},
    },
    "homocysteine": {
        "elevated": {"threshold": 15, "compare": ">", "ingredients": ["methylfolate", "methylcobalamin", "b6_p5p"], "dosage_modifier": 1.0},
    },
    "hscrp": {
        "elevated": {"threshold": 3, "compare": ">", "ingredients": ["omega3_fish_oil", "curcumin"], "dosage_modifier": 1.0},
        "high_risk": {"threshold": 10, "compare": ">", "ingredients": ["omega3_fish_oil", "curcumin"], "dosage_modifier": 1.5},
    },
}

EXCESS_BLOCKS = {
    # Iron overload
    "ferritin": {
        "block": {"threshold": 300, "ingredients": ["iron", "iron_bisglycinate", "iron_glycinate", "ferrous_sulfate"]},
        "caution": {"threshold": 200, "ingredients": ["iron", "iron_bisglycinate"]},
    },
    "transferrin_sat": {
        "block": {"threshold": 45, "ingredients": ["iron", "iron_bisglycinate", "iron_glycinate", "ferrous_sulfate"]},
    },
    
    # Vitamin D toxicity
    "vitamin_d_25oh": {
        "block": {"threshold": 100, "ingredients": ["vitamin_d3", "vitamin_d2", "cholecalciferol"]},
        "reduce": {"threshold": 80, "ingredients": ["vitamin_d3"], "dosage_modifier": 0.5},
    },
    
    # HbA1c - metabolic
    "hba1c": {
        "caution": {"threshold": 6.5, "ingredients": ["sugar", "maltodextrin", "dextrose", "glucose"]},
    },
}

GENDER_ADJUSTMENTS = {
    "female": {
        "ferritin": {"deficient_threshold_modifier": 0.5},  # Women have lower normal range
        "iron": {"default_dosage_modifier": 1.2},  # Higher iron needs
    },
    "male": {
        "ferritin": {"excess_threshold_modifier": 0.8},  # More conservative iron blocking
    },
}

LIFECYCLE_CONSTRAINTS = {
    "pregnant": {
        "required": ["folate", "methylfolate", "iron_bisglycinate", "dha"],
        "blocked": ["vitamin_a_retinol", "high_dose_vitamin_a"],  # Teratogenic
        "caution": ["caffeine", "green_tea_extract"],
        "dosage_modifiers": {"folate": 1.5, "iron_bisglycinate": 1.3, "dha": 1.5},
    },
    "breastfeeding": {
        "required": ["dha", "vitamin_d3"],
        "blocked": [],
        "caution": ["caffeine"],
        "dosage_modifiers": {"dha": 1.3},
    },
    "postmenopausal": {
        "boosted": ["calcium", "vitamin_d3", "magnesium_glycinate", "vitamin_k2"],
        "caution": ["iron"],  # Less iron needed post-menopause
        "dosage_modifiers": {"vitamin_d3": 1.2, "calcium": 1.2},
    },
    "perimenopause": {
        "boosted": ["magnesium_glycinate", "vitamin_b6", "evening_primrose_oil"],
        "dosage_modifiers": {},
    },
}

# =============================================================================
# CONSTRAINT TRANSLATOR
# =============================================================================

class ConstraintTranslator:
    """Translates bloodwork signals into routing constraints."""
    
    def __init__(self):
        self.deficiency_map = DEFICIENCY_RECOMMENDATIONS
        self.excess_map = EXCESS_BLOCKS
        self.gender_adjustments = GENDER_ADJUSTMENTS
        self.lifecycle_constraints = LIFECYCLE_CONSTRAINTS
    
    def translate(
        self,
        markers: List[Dict[str, Any]],
        safety_gates: List[Dict[str, Any]],
        blocked_ingredients: List[str],
        caution_ingredients: List[str],
        user_id: str,
        submission_id: str,
        gender: Optional[str] = None,
        lifecycle_phase: Optional[str] = None,
        excluded_by_user: List[str] = []
    ) -> ConstraintSet:
        """
        Translate bloodwork to routing constraints.
        
        Args:
            markers: Normalized marker list from BloodworkCanonical
            safety_gates: Safety gate results
            blocked_ingredients: Already blocked by safety gates
            caution_ingredients: Already flagged for caution
            user_id: User identifier
            submission_id: Bloodwork submission ID
            gender: male/female for threshold adjustments
            lifecycle_phase: pregnant, postmenopausal, etc.
            excluded_by_user: User-specified exclusions
        
        Returns:
            ConstraintSet with all routing constraints
        """
        constraints = []
        all_blocked = set(blocked_ingredients)
        all_boosted = set()
        all_required = set()
        all_caution = set(caution_ingredients)
        dosage_mods = {}
        
        # Build marker lookup
        marker_lookup = {m.get("code", m.get("name", "")): m for m in markers}
        
        # 1. Process safety gate blocks (highest priority)
        for gate in safety_gates:
            if gate.get("result") == "block":
                gate_blocked = gate.get("blocked_ingredients", [])
                for ing in gate_blocked:
                    constraints.append(RoutingConstraint(
                        constraint_id=f"safety_block_{gate.get('gate_id', 'unknown')}_{ing}",
                        constraint_type=ConstraintType.BLOCK,
                        source=ConstraintSource.SAFETY_GATE,
                        target_type="ingredient",
                        target_ids=[ing],
                        reason=gate.get("message", "Safety gate block"),
                        marker_code=gate.get("triggered_by"),
                        marker_value=gate.get("marker_value"),
                        threshold=gate.get("threshold"),
                        priority=1
                    ))
                    all_blocked.add(ing)
        
        # 2. Process deficiencies -> BOOST/REQUIRE
        for marker_code, marker_data in marker_lookup.items():
            if marker_code not in self.deficiency_map:
                continue
            
            value = marker_data.get("value")
            if value is None:
                continue
            
            deficiency_config = self.deficiency_map[marker_code]
            
            # Apply gender adjustments to thresholds
            threshold_modifier = 1.0
            if gender and gender in self.gender_adjustments:
                gender_config = self.gender_adjustments[gender].get(marker_code, {})
                threshold_modifier = gender_config.get("deficient_threshold_modifier", 1.0)
            
            for level, config in deficiency_config.items():
                threshold = config["threshold"] * threshold_modifier
                compare = config.get("compare", "<")
                
                triggered = False
                if compare == "<" and value < threshold:
                    triggered = True
                elif compare == ">" and value > threshold:
                    triggered = True
                
                if triggered:
                    ingredients = config["ingredients"]
                    dosage_mod = config.get("dosage_modifier", 1.0)
                    
                    # Apply gender dosage modifiers
                    if gender and gender in self.gender_adjustments:
                        for ing in ingredients:
                            if ing in self.gender_adjustments[gender]:
                                dosage_mod *= self.gender_adjustments[gender][ing].get("default_dosage_modifier", 1.0)
                    
                    constraint_type = ConstraintType.REQUIRE if level == "critical" else ConstraintType.BOOST
                    priority = 2 if level == "critical" else 3
                    
                    for ing in ingredients:
                        if ing not in all_blocked:
                            constraints.append(RoutingConstraint(
                                constraint_id=f"deficiency_{marker_code}_{level}_{ing}",
                                constraint_type=constraint_type,
                                source=ConstraintSource.BLOODWORK,
                                target_type="ingredient",
                                target_ids=[ing],
                                reason=f"{marker_code} {level}: {value} {'<' if compare == '<' else '>'} {threshold}",
                                marker_code=marker_code,
                                marker_value=value,
                                threshold=threshold,
                                dosage_modifier=dosage_mod,
                                priority=priority
                            ))
                            
                            if constraint_type == ConstraintType.REQUIRE:
                                all_required.add(ing)
                            else:
                                all_boosted.add(ing)
                            
                            if dosage_mod != 1.0:
                                dosage_mods[ing] = max(dosage_mods.get(ing, 1.0), dosage_mod)
                    
                    break  # Only apply most severe level
        
        # 3. Process excesses -> BLOCK/REDUCE/CAUTION
        for marker_code, marker_data in marker_lookup.items():
            if marker_code not in self.excess_map:
                continue
            
            value = marker_data.get("value")
            if value is None:
                continue
            
            excess_config = self.excess_map[marker_code]
            
            # Apply gender adjustments
            threshold_modifier = 1.0
            if gender and gender in self.gender_adjustments:
                gender_config = self.gender_adjustments[gender].get(marker_code, {})
                threshold_modifier = gender_config.get("excess_threshold_modifier", 1.0)
            
            for level, config in excess_config.items():
                threshold = config["threshold"] * threshold_modifier
                
                if value > threshold:
                    ingredients = config["ingredients"]
                    dosage_mod = config.get("dosage_modifier")
                    
                    if level == "block":
                        for ing in ingredients:
                            constraints.append(RoutingConstraint(
                                constraint_id=f"excess_block_{marker_code}_{ing}",
                                constraint_type=ConstraintType.BLOCK,
                                source=ConstraintSource.BLOODWORK,
                                target_type="ingredient",
                                target_ids=[ing],
                                reason=f"{marker_code} excess: {value} > {threshold}",
                                marker_code=marker_code,
                                marker_value=value,
                                threshold=threshold,
                                priority=1
                            ))
                            all_blocked.add(ing)
                    
                    elif level == "reduce":
                        for ing in ingredients:
                            if ing not in all_blocked:
                                constraints.append(RoutingConstraint(
                                    constraint_id=f"excess_reduce_{marker_code}_{ing}",
                                    constraint_type=ConstraintType.REDUCE,
                                    source=ConstraintSource.BLOODWORK,
                                    target_type="ingredient",
                                    target_ids=[ing],
                                    reason=f"{marker_code} elevated: {value} > {threshold} - reduce dosage",
                                    marker_code=marker_code,
                                    marker_value=value,
                                    threshold=threshold,
                                    dosage_modifier=dosage_mod,
                                    priority=4
                                ))
                                if dosage_mod:
                                    dosage_mods[ing] = min(dosage_mods.get(ing, 1.0), dosage_mod)
                    
                    elif level == "caution":
                        for ing in ingredients:
                            if ing not in all_blocked:
                                constraints.append(RoutingConstraint(
                                    constraint_id=f"excess_caution_{marker_code}_{ing}",
                                    constraint_type=ConstraintType.CAUTION,
                                    source=ConstraintSource.BLOODWORK,
                                    target_type="ingredient",
                                    target_ids=[ing],
                                    reason=f"{marker_code} elevated: {value} > {threshold}",
                                    marker_code=marker_code,
                                    marker_value=value,
                                    threshold=threshold,
                                    priority=5
                                ))
                                all_caution.add(ing)
                    
                    break  # Only apply most severe level
        
        # 4. Apply lifecycle constraints
        if lifecycle_phase and lifecycle_phase in self.lifecycle_constraints:
            lc_config = self.lifecycle_constraints[lifecycle_phase]
            
            # Required ingredients for lifecycle
            for ing in lc_config.get("required", []):
                if ing not in all_blocked:
                    constraints.append(RoutingConstraint(
                        constraint_id=f"lifecycle_require_{lifecycle_phase}_{ing}",
                        constraint_type=ConstraintType.REQUIRE,
                        source=ConstraintSource.LIFECYCLE,
                        target_type="ingredient",
                        target_ids=[ing],
                        reason=f"Required for {lifecycle_phase}",
                        priority=2
                    ))
                    all_required.add(ing)
            
            # Blocked for lifecycle
            for ing in lc_config.get("blocked", []):
                constraints.append(RoutingConstraint(
                    constraint_id=f"lifecycle_block_{lifecycle_phase}_{ing}",
                    constraint_type=ConstraintType.BLOCK,
                    source=ConstraintSource.LIFECYCLE,
                    target_type="ingredient",
                    target_ids=[ing],
                    reason=f"Contraindicated during {lifecycle_phase}",
                    priority=1
                ))
                all_blocked.add(ing)
            
            # Caution for lifecycle
            for ing in lc_config.get("caution", []):
                if ing not in all_blocked:
                    constraints.append(RoutingConstraint(
                        constraint_id=f"lifecycle_caution_{lifecycle_phase}_{ing}",
                        constraint_type=ConstraintType.CAUTION,
                        source=ConstraintSource.LIFECYCLE,
                        target_type="ingredient",
                        target_ids=[ing],
                        reason=f"Use caution during {lifecycle_phase}",
                        priority=5
                    ))
                    all_caution.add(ing)
            
            # Boosted for lifecycle
            for ing in lc_config.get("boosted", []):
                if ing not in all_blocked:
                    constraints.append(RoutingConstraint(
                        constraint_id=f"lifecycle_boost_{lifecycle_phase}_{ing}",
                        constraint_type=ConstraintType.BOOST,
                        source=ConstraintSource.LIFECYCLE,
                        target_type="ingredient",
                        target_ids=[ing],
                        reason=f"Beneficial during {lifecycle_phase}",
                        priority=4
                    ))
                    all_boosted.add(ing)
            
            # Lifecycle dosage modifiers
            for ing, mod in lc_config.get("dosage_modifiers", {}).items():
                dosage_mods[ing] = dosage_mods.get(ing, 1.0) * mod
        
        # 5. Apply user exclusions (lowest priority but hard block)
        for ing in excluded_by_user:
            constraints.append(RoutingConstraint(
                constraint_id=f"user_exclude_{ing}",
                constraint_type=ConstraintType.BLOCK,
                source=ConstraintSource.USER_PREFERENCE,
                target_type="ingredient",
                target_ids=[ing],
                reason="User excluded",
                priority=1
            ))
            all_blocked.add(ing)
        
        # Sort constraints by priority
        constraints.sort(key=lambda c: c.priority)
        
        return ConstraintSet(
            user_id=user_id,
            submission_id=submission_id,
            constraints=constraints,
            blocked_ingredients=all_blocked,
            boosted_ingredients=all_boosted - all_blocked,
            required_ingredients=all_required - all_blocked,
            caution_ingredients=all_caution - all_blocked,
            dosage_modifiers=dosage_mods
        )
    
    def get_effective_dosage(
        self,
        ingredient_id: str,
        base_dosage: float,
        constraint_set: ConstraintSet
    ) -> Tuple[float, str]:
        """
        Calculate effective dosage after applying constraints.
        
        Returns:
            Tuple of (effective_dosage, reason)
        """
        if ingredient_id in constraint_set.blocked_ingredients:
            return 0.0, "Blocked by constraint"
        
        modifier = constraint_set.dosage_modifiers.get(ingredient_id, 1.0)
        effective = base_dosage * modifier
        
        if modifier > 1.0:
            reason = f"Increased {(modifier - 1) * 100:.0f}% due to deficiency"
        elif modifier < 1.0:
            reason = f"Reduced {(1 - modifier) * 100:.0f}% due to elevated levels"
        else:
            reason = "Standard dosage"
        
        return effective, reason


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_constraint_translator() -> ConstraintTranslator:
    """Factory function to create constraint translator."""
    return ConstraintTranslator()


def translate_bloodwork_to_constraints(
    canonical_handoff: Dict[str, Any],
    gender: Optional[str] = None,
    lifecycle_phase: Optional[str] = None,
    excluded_by_user: List[str] = []
) -> ConstraintSet:
    """
    Convenience function to translate BloodworkCanonical to constraints.
    
    Args:
        canonical_handoff: BloodworkCanonical dict
        gender: male/female
        lifecycle_phase: pregnant, postmenopausal, etc.
        excluded_by_user: User-specified exclusions
    
    Returns:
        ConstraintSet ready for routing
    """
    translator = create_constraint_translator()
    
    return translator.translate(
        markers=[m if isinstance(m, dict) else m.dict() for m in canonical_handoff.get("markers", [])],
        safety_gates=[g if isinstance(g, dict) else g.dict() for g in canonical_handoff.get("safety_gates", [])],
        blocked_ingredients=canonical_handoff.get("blocked_ingredients", []),
        caution_ingredients=canonical_handoff.get("caution_ingredients", []),
        user_id=canonical_handoff.get("user_id", ""),
        submission_id=canonical_handoff.get("submission_id", ""),
        gender=gender,
        lifecycle_phase=lifecycle_phase,
        excluded_by_user=excluded_by_user
    )
