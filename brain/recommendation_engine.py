"""
GenoMAX² Recommendation Engine
==============================
Scores and ranks supplement modules based on bloodwork markers and constraints.

The engine connects biomarker deficiencies to specific supplement recommendations,
respecting all safety gates and user preferences.

Version: 1.0.0
"""

from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum
import math

# =============================================================================
# MODELS
# =============================================================================

class RecommendationTier(str, Enum):
    CRITICAL = "critical"     # Required - critical deficiency
    PRIMARY = "primary"       # Highly recommended
    SECONDARY = "secondary"   # Beneficial
    OPTIONAL = "optional"     # Nice to have
    BLOCKED = "blocked"       # Cannot include

class ModuleRecommendation(BaseModel):
    """Single supplement module recommendation."""
    module_id: str
    module_name: str
    tier: RecommendationTier
    score: float  # 0-100
    reasons: List[str]
    addressed_deficiencies: List[str]
    key_ingredients: List[str]
    dosage_adjustments: Dict[str, float] = {}
    contraindications: List[str] = []
    evidence_grade: str = "B"  # A, B, C
    
class RecommendationResult(BaseModel):
    """Complete recommendation result from engine."""
    user_id: str
    submission_id: str
    gender: str
    recommendations: List[ModuleRecommendation]
    blocked_modules: List[Dict[str, Any]]
    summary: Dict[str, Any]
    created_at: datetime = Field(default_factory=datetime.utcnow)

# =============================================================================
# MODULE CATALOG (Simplified - Production pulls from database)
# =============================================================================

MODULE_CATALOG = {
    # Iron Support
    "iron_support": {
        "name": "Iron Support Complex",
        "category": "mineral",
        "key_ingredients": ["iron_bisglycinate", "vitamin_c"],
        "addresses_markers": ["ferritin", "serum_iron", "tibc"],
        "evidence_grade": "A",
        "gender_specific": None,
        "contraindicated_markers": {"ferritin": {"operator": ">", "value": 200}},
        "base_score": 70,
    },
    
    # Vitamin D
    "vitamin_d3_k2": {
        "name": "Vitamin D3 + K2",
        "category": "vitamin",
        "key_ingredients": ["vitamin_d3", "vitamin_k2_mk7"],
        "addresses_markers": ["vitamin_d_25oh"],
        "evidence_grade": "A",
        "gender_specific": None,
        "contraindicated_markers": {"vitamin_d_25oh": {"operator": ">", "value": 80}},
        "base_score": 75,
    },
    
    # B-Complex
    "methylated_b_complex": {
        "name": "Methylated B-Complex",
        "category": "vitamin",
        "key_ingredients": ["methylcobalamin", "methylfolate", "b6_p5p", "riboflavin_5p"],
        "addresses_markers": ["vitamin_b12", "folate", "homocysteine"],
        "evidence_grade": "A",
        "gender_specific": None,
        "contraindicated_markers": {},
        "base_score": 72,
    },
    
    # Omega-3
    "omega3_premium": {
        "name": "Omega-3 Premium (EPA/DHA)",
        "category": "fatty_acid",
        "key_ingredients": ["omega3_fish_oil", "epa", "dha"],
        "addresses_markers": ["omega3_index", "hscrp", "homocysteine"],
        "evidence_grade": "A",
        "gender_specific": None,
        "contraindicated_markers": {},
        "base_score": 78,
    },
    
    # Magnesium
    "magnesium_complex": {
        "name": "Magnesium Complex",
        "category": "mineral",
        "key_ingredients": ["magnesium_glycinate", "magnesium_threonate"],
        "addresses_markers": ["magnesium_rbc"],
        "evidence_grade": "A",
        "gender_specific": None,
        "contraindicated_markers": {},
        "base_score": 74,
    },
    
    # Zinc
    "zinc_copper_balance": {
        "name": "Zinc + Copper Balance",
        "category": "mineral",
        "key_ingredients": ["zinc_picolinate", "copper_bisglycinate"],
        "addresses_markers": ["zinc"],
        "evidence_grade": "A",
        "gender_specific": None,
        "contraindicated_markers": {},
        "base_score": 68,
    },
    
    # Anti-Inflammatory
    "anti_inflammatory_support": {
        "name": "Anti-Inflammatory Support",
        "category": "specialty",
        "key_ingredients": ["curcumin", "quercetin", "bromelain"],
        "addresses_markers": ["hscrp", "homocysteine"],
        "evidence_grade": "B",
        "gender_specific": None,
        "contraindicated_markers": {},
        "base_score": 65,
    },
    
    # Metabolic Support
    "metabolic_support": {
        "name": "Metabolic Support",
        "category": "specialty",
        "key_ingredients": ["berberine", "chromium", "alpha_lipoic_acid"],
        "addresses_markers": ["hba1c"],
        "evidence_grade": "B",
        "gender_specific": None,
        "contraindicated_markers": {},
        "base_score": 62,
    },
    
    # Women's Prenatal
    "prenatal_complete": {
        "name": "Prenatal Complete",
        "category": "lifecycle",
        "key_ingredients": ["methylfolate", "iron_bisglycinate", "dha", "choline", "vitamin_d3"],
        "addresses_markers": ["folate", "ferritin", "vitamin_d_25oh", "omega3_index"],
        "evidence_grade": "A",
        "gender_specific": "female",
        "lifecycle_phase": "pregnant",
        "contraindicated_markers": {},
        "base_score": 85,
    },
    
    # Women's Menopause
    "menopause_support": {
        "name": "Menopause Support",
        "category": "lifecycle",
        "key_ingredients": ["calcium", "vitamin_d3", "magnesium_glycinate", "vitamin_k2_mk7"],
        "addresses_markers": ["vitamin_d_25oh", "magnesium_rbc"],
        "evidence_grade": "A",
        "gender_specific": "female",
        "lifecycle_phase": "postmenopausal",
        "contraindicated_markers": {},
        "base_score": 80,
    },
    
    # Men's Vitality
    "mens_vitality": {
        "name": "Men's Vitality Complex",
        "category": "lifecycle",
        "key_ingredients": ["zinc_picolinate", "vitamin_d3", "magnesium_glycinate", "boron"],
        "addresses_markers": ["zinc", "vitamin_d_25oh", "magnesium_rbc"],
        "evidence_grade": "B",
        "gender_specific": "male",
        "contraindicated_markers": {},
        "base_score": 72,
    },
}

# =============================================================================
# SCORING WEIGHTS
# =============================================================================

SCORING_WEIGHTS = {
    "deficiency_match": 25,       # Module addresses a detected deficiency
    "critical_deficiency": 20,    # Deficiency is critical level
    "boosted_ingredient": 10,     # Contains boosted ingredient
    "required_ingredient": 15,    # Contains required ingredient
    "evidence_grade_a": 10,       # A-grade evidence
    "evidence_grade_b": 5,        # B-grade evidence
    "gender_match": 8,            # Gender-specific match
    "lifecycle_match": 12,        # Lifecycle phase match
    "goal_alignment": 5,          # Aligns with user goals
}

SCORING_PENALTIES = {
    "caution_ingredient": -10,    # Contains caution ingredient
    "partial_block": -15,         # Some ingredients blocked
    "low_priority_marker": -5,    # Addresses non-priority markers
}

# =============================================================================
# RECOMMENDATION ENGINE
# =============================================================================

class RecommendationEngine:
    """Scores and ranks supplement modules based on constraints."""
    
    def __init__(self, module_catalog: Dict[str, Dict] = None):
        self.catalog = module_catalog or MODULE_CATALOG
    
    def generate_recommendations(
        self,
        constraint_set: Dict[str, Any],
        markers: List[Dict[str, Any]],
        gender: str,
        lifecycle_phase: Optional[str] = None,
        goals: List[str] = [],
        max_recommendations: int = 8
    ) -> RecommendationResult:
        """
        Generate scored recommendations based on constraints.
        
        Args:
            constraint_set: ConstraintSet from translator
            markers: Normalized markers
            gender: male/female
            lifecycle_phase: pregnant, postmenopausal, etc.
            goals: User health goals
            max_recommendations: Max modules to recommend
        
        Returns:
            RecommendationResult with scored modules
        """
        blocked_ingredients = set(constraint_set.get("blocked_ingredients", []))
        boosted_ingredients = set(constraint_set.get("boosted_ingredients", []))
        required_ingredients = set(constraint_set.get("required_ingredients", []))
        caution_ingredients = set(constraint_set.get("caution_ingredients", []))
        dosage_modifiers = constraint_set.get("dosage_modifiers", {})
        
        # Build marker lookup
        marker_lookup = self._build_marker_lookup(markers)
        
        # Identify deficiencies
        deficiencies = self._identify_deficiencies(markers)
        critical_deficiencies = self._identify_critical_deficiencies(markers)
        
        recommendations = []
        blocked_modules = []
        
        for module_id, module_config in self.catalog.items():
            # Check gender match
            if module_config.get("gender_specific") and module_config["gender_specific"] != gender:
                continue
            
            # Check lifecycle match
            if module_config.get("lifecycle_phase"):
                if lifecycle_phase != module_config["lifecycle_phase"]:
                    continue
            
            # Check if module is completely blocked
            module_ingredients = set(module_config["key_ingredients"])
            blocked_in_module = module_ingredients & blocked_ingredients
            
            if blocked_in_module == module_ingredients:
                blocked_modules.append({
                    "module_id": module_id,
                    "name": module_config["name"],
                    "reason": f"All ingredients blocked: {list(blocked_in_module)}"
                })
                continue
            
            # Check contraindicated markers
            if self._check_contraindications(module_config, marker_lookup):
                blocked_modules.append({
                    "module_id": module_id,
                    "name": module_config["name"],
                    "reason": "Contraindicated based on biomarker levels"
                })
                continue
            
            # Score the module
            score, reasons, tier = self._score_module(
                module_id=module_id,
                module_config=module_config,
                deficiencies=deficiencies,
                critical_deficiencies=critical_deficiencies,
                blocked_ingredients=blocked_ingredients,
                boosted_ingredients=boosted_ingredients,
                required_ingredients=required_ingredients,
                caution_ingredients=caution_ingredients,
                gender=gender,
                lifecycle_phase=lifecycle_phase,
                goals=goals
            )
            
            # Calculate dosage adjustments
            dosage_adj = {}
            for ing in module_ingredients:
                if ing in dosage_modifiers:
                    dosage_adj[ing] = dosage_modifiers[ing]
            
            # Get addressed deficiencies
            addressed = [d for d in deficiencies if d in module_config.get("addresses_markers", [])]
            
            # Get contraindications
            contras = list(blocked_in_module) if blocked_in_module else []
            
            recommendations.append(ModuleRecommendation(
                module_id=module_id,
                module_name=module_config["name"],
                tier=tier,
                score=score,
                reasons=reasons,
                addressed_deficiencies=addressed,
                key_ingredients=list(module_ingredients - blocked_ingredients),
                dosage_adjustments=dosage_adj,
                contraindications=contras,
                evidence_grade=module_config.get("evidence_grade", "B")
            ))
        
        # Sort by score (descending) and tier
        tier_order = {
            RecommendationTier.CRITICAL: 0,
            RecommendationTier.PRIMARY: 1,
            RecommendationTier.SECONDARY: 2,
            RecommendationTier.OPTIONAL: 3,
        }
        recommendations.sort(key=lambda r: (tier_order.get(r.tier, 4), -r.score))
        
        # Limit recommendations
        top_recommendations = recommendations[:max_recommendations]
        
        # Build summary
        summary = {
            "total_modules_evaluated": len(self.catalog),
            "modules_recommended": len(top_recommendations),
            "modules_blocked": len(blocked_modules),
            "deficiencies_detected": len(deficiencies),
            "critical_deficiencies": len(critical_deficiencies),
            "tier_breakdown": self._count_tiers(top_recommendations),
            "categories_covered": list(set(
                self.catalog[r.module_id].get("category", "other") 
                for r in top_recommendations
            )),
        }
        
        return RecommendationResult(
            user_id=constraint_set.get("user_id", ""),
            submission_id=constraint_set.get("submission_id", ""),
            gender=gender,
            recommendations=top_recommendations,
            blocked_modules=blocked_modules,
            summary=summary
        )
    
    def _build_marker_lookup(self, markers: List[Dict]) -> Dict[str, Dict]:
        """Build marker code to marker data lookup."""
        return {m.get("code", m.get("name", "")): m for m in markers}
    
    def _identify_deficiencies(self, markers: List[Dict]) -> List[str]:
        """Identify marker codes with deficiencies (flag = L or below reference)."""
        deficiencies = []
        for m in markers:
            flag = m.get("flag", "N")
            if flag in ["L", "C"]:  # Low or Critical
                deficiencies.append(m.get("code", ""))
        return deficiencies
    
    def _identify_critical_deficiencies(self, markers: List[Dict]) -> List[str]:
        """Identify critical level deficiencies."""
        critical = []
        for m in markers:
            if m.get("flag") == "C":
                critical.append(m.get("code", ""))
        return critical
    
    def _check_contraindications(
        self, 
        module_config: Dict, 
        marker_lookup: Dict[str, Dict]
    ) -> bool:
        """Check if module is contraindicated based on marker levels."""
        contras = module_config.get("contraindicated_markers", {})
        
        for marker_code, condition in contras.items():
            if marker_code not in marker_lookup:
                continue
            
            value = marker_lookup[marker_code].get("value")
            if value is None:
                continue
            
            operator = condition.get("operator", ">")
            threshold = condition.get("value", 0)
            
            if operator == ">" and value > threshold:
                return True
            elif operator == "<" and value < threshold:
                return True
            elif operator == ">=" and value >= threshold:
                return True
            elif operator == "<=" and value <= threshold:
                return True
        
        return False
    
    def _score_module(
        self,
        module_id: str,
        module_config: Dict,
        deficiencies: List[str],
        critical_deficiencies: List[str],
        blocked_ingredients: Set[str],
        boosted_ingredients: Set[str],
        required_ingredients: Set[str],
        caution_ingredients: Set[str],
        gender: str,
        lifecycle_phase: Optional[str],
        goals: List[str]
    ) -> Tuple[float, List[str], RecommendationTier]:
        """
        Score a module based on all factors.
        
        Returns:
            Tuple of (score, reasons, tier)
        """
        base_score = module_config.get("base_score", 50)
        score = base_score
        reasons = []
        
        module_ingredients = set(module_config["key_ingredients"])
        addresses_markers = set(module_config.get("addresses_markers", []))
        
        # Deficiency matching
        matched_deficiencies = addresses_markers & set(deficiencies)
        if matched_deficiencies:
            score += SCORING_WEIGHTS["deficiency_match"] * len(matched_deficiencies)
            reasons.append(f"Addresses deficiencies: {list(matched_deficiencies)}")
        
        # Critical deficiency bonus
        matched_critical = addresses_markers & set(critical_deficiencies)
        if matched_critical:
            score += SCORING_WEIGHTS["critical_deficiency"] * len(matched_critical)
            reasons.append(f"Addresses critical deficiencies: {list(matched_critical)}")
        
        # Boosted ingredient bonus
        boosted_matches = module_ingredients & boosted_ingredients
        if boosted_matches:
            score += SCORING_WEIGHTS["boosted_ingredient"] * len(boosted_matches)
            reasons.append(f"Contains boosted ingredients: {list(boosted_matches)}")
        
        # Required ingredient bonus
        required_matches = module_ingredients & required_ingredients
        if required_matches:
            score += SCORING_WEIGHTS["required_ingredient"] * len(required_matches)
            reasons.append(f"Contains required ingredients: {list(required_matches)}")
        
        # Evidence grade bonus
        evidence = module_config.get("evidence_grade", "B")
        if evidence == "A":
            score += SCORING_WEIGHTS["evidence_grade_a"]
            reasons.append("Strong clinical evidence (Grade A)")
        elif evidence == "B":
            score += SCORING_WEIGHTS["evidence_grade_b"]
        
        # Gender match bonus
        if module_config.get("gender_specific") == gender:
            score += SCORING_WEIGHTS["gender_match"]
            reasons.append(f"Optimized for {gender}")
        
        # Lifecycle match bonus
        if lifecycle_phase and module_config.get("lifecycle_phase") == lifecycle_phase:
            score += SCORING_WEIGHTS["lifecycle_match"]
            reasons.append(f"Optimized for {lifecycle_phase}")
        
        # Caution ingredient penalty
        caution_matches = module_ingredients & caution_ingredients
        if caution_matches:
            score += SCORING_PENALTIES["caution_ingredient"] * len(caution_matches)
            reasons.append(f"Contains caution ingredients: {list(caution_matches)}")
        
        # Partial block penalty
        blocked_matches = module_ingredients & blocked_ingredients
        if blocked_matches and blocked_matches != module_ingredients:
            score += SCORING_PENALTIES["partial_block"]
            reasons.append(f"Some ingredients blocked: {list(blocked_matches)}")
        
        # Clamp score
        score = max(0, min(100, score))
        
        # Determine tier
        tier = self._determine_tier(
            score=score,
            has_critical=bool(matched_critical),
            has_required=bool(required_matches),
            has_deficiency=bool(matched_deficiencies)
        )
        
        return score, reasons, tier
    
    def _determine_tier(
        self,
        score: float,
        has_critical: bool,
        has_required: bool,
        has_deficiency: bool
    ) -> RecommendationTier:
        """Determine recommendation tier based on scoring factors."""
        if has_critical or has_required:
            return RecommendationTier.CRITICAL
        
        if has_deficiency and score >= 70:
            return RecommendationTier.PRIMARY
        
        if score >= 65:
            return RecommendationTier.SECONDARY
        
        return RecommendationTier.OPTIONAL
    
    def _count_tiers(self, recommendations: List[ModuleRecommendation]) -> Dict[str, int]:
        """Count recommendations by tier."""
        counts = {tier.value: 0 for tier in RecommendationTier if tier != RecommendationTier.BLOCKED}
        for r in recommendations:
            if r.tier.value in counts:
                counts[r.tier.value] += 1
        return counts


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_recommendation_engine(
    custom_catalog: Dict[str, Dict] = None
) -> RecommendationEngine:
    """Factory function to create recommendation engine."""
    return RecommendationEngine(custom_catalog)


def generate_recommendations(
    constraint_set: Dict[str, Any],
    markers: List[Dict[str, Any]],
    gender: str,
    lifecycle_phase: Optional[str] = None,
    goals: List[str] = [],
    max_recommendations: int = 8
) -> RecommendationResult:
    """
    Convenience function for recommendation generation.
    
    Args:
        constraint_set: ConstraintSet dict
        markers: Normalized markers
        gender: male/female
        lifecycle_phase: Optional lifecycle
        goals: User goals
        max_recommendations: Max modules
    
    Returns:
        RecommendationResult
    """
    engine = create_recommendation_engine()
    return engine.generate_recommendations(
        constraint_set=constraint_set,
        markers=markers,
        gender=gender,
        lifecycle_phase=lifecycle_phase,
        goals=goals,
        max_recommendations=max_recommendations
    )
