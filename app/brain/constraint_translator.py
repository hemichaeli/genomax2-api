"""
GenoMAX² Constraint Translator v1.0.0
=====================================
Pure, deterministic translator that converts Bloodwork Engine constraint codes
into canonical enforcement semantics for routing and matching layers.

Principle: "Blood does not negotiate"
- Bloodwork constraints are non-negotiable
- No downstream layer may override blood-based restrictions
- All translations are deterministic and auditable

Usage:
    from app.brain.constraint_translator import ConstraintTranslator, translate_constraints
    
    # Get translated constraints from constraint codes
    result = translate_constraints(
        constraint_codes=["BLOCK_IRON", "CAUTION_RENAL"],
        sex="male"
    )
    
    # Access enforcement fields
    print(result.blocked_ingredients)  # ["iron", "iron_bisglycinate", ...]
    print(result.caution_flags)        # ["renal_sensitive"]
    print(result.reason_codes)         # ["BLOOD_BLOCK_IRON", "BLOOD_CAUTION_RENAL"]

Scope (Non-negotiable):
- Pure + deterministic (same input → same output)
- No diagnosis, dosing, or recommendation logic
- Only translates constraint codes into canonical enforcement fields
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set, Any, Optional
import hashlib
import json
from datetime import datetime

__version__ = "1.0.0"


@dataclass
class TranslatedConstraints:
    """
    Canonical enforcement object produced by ConstraintTranslator.
    
    Fields are alphabetically sorted for deterministic hashing.
    All lists/sets are converted to sorted lists in to_dict().
    """
    # Hard blocks - ingredients that MUST NOT be included
    blocked_ingredients: List[str] = field(default_factory=list)
    
    # Blocked categories - entire categories to exclude
    blocked_categories: List[str] = field(default_factory=list)
    
    # Blocked targets - specific biological targets to avoid
    blocked_targets: List[str] = field(default_factory=list)
    
    # Caution flags - warnings that don't block but require attention
    caution_flags: List[str] = field(default_factory=list)
    
    # Reason codes - audit trail of why constraints were applied
    reason_codes: List[str] = field(default_factory=list)
    
    # Recommended ingredients - prioritize these based on bloodwork
    recommended_ingredients: List[str] = field(default_factory=list)
    
    # Metadata
    translator_version: str = __version__
    input_hash: str = ""
    output_hash: str = ""
    translated_at: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary with sorted fields for determinism."""
        return {
            "blocked_categories": sorted(self.blocked_categories),
            "blocked_ingredients": sorted(self.blocked_ingredients),
            "blocked_targets": sorted(self.blocked_targets),
            "caution_flags": sorted(self.caution_flags),
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "reason_codes": sorted(self.reason_codes),
            "recommended_ingredients": sorted(self.recommended_ingredients),
            "translated_at": self.translated_at,
            "translator_version": self.translator_version,
        }
    
    def is_empty(self) -> bool:
        """Check if no constraints were generated."""
        return (
            len(self.blocked_ingredients) == 0 and
            len(self.blocked_categories) == 0 and
            len(self.blocked_targets) == 0 and
            len(self.caution_flags) == 0 and
            len(self.recommended_ingredients) == 0
        )


# ============================================
# Constraint Mapping Rules (v1)
# ============================================

CONSTRAINT_MAPPINGS: Dict[str, Dict[str, Any]] = {
    # === IRON CONSTRAINTS ===
    "BLOCK_IRON": {
        "blocked_ingredients": [
            "iron",
            "iron_bisglycinate", 
            "iron_glycinate",
            "ferrous_sulfate",
            "ferrous_fumarate",
            "ferrous_gluconate",
            "carbonyl_iron",
            "heme_iron",
        ],
        "blocked_categories": [],
        "blocked_targets": ["iron_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_IRON"],
    },
    
    # === POTASSIUM CONSTRAINTS ===
    "BLOCK_POTASSIUM": {
        "blocked_ingredients": [
            "potassium",
            "potassium_citrate",
            "potassium_chloride",
            "potassium_gluconate",
            "potassium_bicarbonate",
        ],
        "blocked_categories": [],
        "blocked_targets": ["potassium_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_POTASSIUM"],
    },
    
    # === IODINE CONSTRAINTS ===
    "BLOCK_IODINE": {
        "blocked_ingredients": [
            "iodine",
            "potassium_iodide",
            "kelp",
            "bladderwrack",
            "sea_vegetables",
        ],
        "blocked_categories": [],
        "blocked_targets": ["iodine_supplementation", "thyroid_stimulation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_IODINE"],
    },
    
    # === VITAMIN D CONSTRAINTS ===
    "CAUTION_VITAMIN_D": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["vitamin_d_caution", "hypercalcemia_risk"],
        "reason_codes": ["BLOOD_CAUTION_VITAMIN_D"],
    },
    
    "BLOCK_VITAMIN_D": {
        "blocked_ingredients": [
            "vitamin_d3",
            "vitamin_d2",
            "cholecalciferol",
            "ergocalciferol",
        ],
        "blocked_categories": [],
        "blocked_targets": ["vitamin_d_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_VITAMIN_D"],
    },
    
    # === RENAL CONSTRAINTS ===
    "CAUTION_RENAL": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["renal_sensitive", "kidney_function_impaired"],
        "reason_codes": ["BLOOD_CAUTION_RENAL"],
        "dose_adjustments": {
            "magnesium": 0.5,  # 50% dose
            "potassium": 0.0,  # block
            "creatine": 0.5,
        },
    },
    
    # === HEPATIC CONSTRAINTS ===
    "CAUTION_HEPATOTOXIC": {
        "blocked_ingredients": [
            "ashwagandha",  # PERMANENTLY BLOCKED - documented hepatotoxicity
        ],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["hepatic_sensitive", "liver_function_impaired"],
        "reason_codes": ["BLOOD_CAUTION_HEPATOTOXIC"],
    },
    
    "BLOCK_HEPATOTOXIC": {
        "blocked_ingredients": [
            "ashwagandha",
            "kava",
            "black_cohosh",
            "green_tea_extract_high_dose",
            "pyrrolizidine_alkaloids",
            "germander",
            "comfrey",
        ],
        "blocked_categories": ["hepatotoxic_herbs"],
        "blocked_targets": [],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_HEPATOTOXIC"],
    },
    
    # === INFLAMMATION CONSTRAINTS ===
    "FLAG_ACUTE_INFLAMMATION": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["acute_inflammation", "crp_elevated"],
        "reason_codes": ["BLOOD_FLAG_INFLAMMATION"],
        "recommended_ingredients": [
            "omega3",
            "fish_oil",
            "epa_dha",
            "curcumin",
            "boswellia",
        ],
    },
    
    "FLAG_CHRONIC_INFLAMMATION": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["chronic_inflammation"],
        "reason_codes": ["BLOOD_FLAG_CHRONIC_INFLAMMATION"],
        "recommended_ingredients": [
            "omega3",
            "curcumin",
            "resveratrol",
            "quercetin",
        ],
    },
    
    # === BLOOD SUGAR CONSTRAINTS ===
    "FLAG_INSULIN_RESISTANCE": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["insulin_resistance", "metabolic_support_needed"],
        "reason_codes": ["BLOOD_FLAG_INSULIN_RESISTANCE"],
        "recommended_ingredients": [
            "berberine",
            "chromium",
            "alpha_lipoic_acid",
            "cinnamon_extract",
            "magnesium",
        ],
    },
    
    "FLAG_HYPERGLYCEMIA": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["hyperglycemia", "blood_sugar_elevated"],
        "reason_codes": ["BLOOD_FLAG_HYPERGLYCEMIA"],
    },
    
    # === METHYLATION CONSTRAINTS ===
    "FLAG_METHYLFOLATE_REQUIRED": {
        "blocked_ingredients": [
            "folic_acid",  # Synthetic form blocked for MTHFR
        ],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["mthfr_variant"],
        "reason_codes": ["BLOOD_FLAG_MTHFR"],
        "recommended_ingredients": [
            "methylfolate",
            "5_mthf",
            "folinic_acid",
        ],
    },
    
    "FLAG_METHYLATION_SUPPORT": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["methylation_impaired", "homocysteine_elevated"],
        "reason_codes": ["BLOOD_FLAG_METHYLATION"],
        "recommended_ingredients": [
            "methylcobalamin",
            "methylfolate",
            "pyridoxal_5_phosphate",
            "betaine_tmg",
            "riboflavin_5_phosphate",
        ],
    },
    
    # === B12 CONSTRAINTS ===
    "FLAG_B12_DEFICIENCY": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["b12_deficiency"],
        "reason_codes": ["BLOOD_FLAG_B12_DEFICIENCY"],
        "recommended_ingredients": [
            "methylcobalamin",
            "adenosylcobalamin",
            "hydroxocobalamin",
        ],
    },
    
    # === THYROID CONSTRAINTS ===
    "FLAG_THYROID_SUPPORT": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["thyroid_support_needed"],
        "reason_codes": ["BLOOD_FLAG_THYROID_SUPPORT"],
        "recommended_ingredients": [
            "selenium",
            "zinc",
            "l_tyrosine",
        ],
    },
    
    "FLAG_HYPOTHYROID": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["hypothyroid", "tsh_elevated"],
        "reason_codes": ["BLOOD_FLAG_HYPOTHYROID"],
        "recommended_ingredients": [
            "selenium",
            "zinc",
            "iodine",  # Only if not blocked
        ],
    },
    
    "FLAG_HYPERTHYROID": {
        "blocked_ingredients": [
            "iodine",
            "kelp",
            "bladderwrack",
        ],
        "blocked_categories": [],
        "blocked_targets": ["thyroid_stimulation"],
        "caution_flags": ["hyperthyroid", "tsh_suppressed"],
        "reason_codes": ["BLOOD_FLAG_HYPERTHYROID"],
    },
    
    # === OMEGA-3 CONSTRAINTS ===
    "FLAG_OMEGA3_PRIORITY": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": [],
        "reason_codes": ["BLOOD_FLAG_OMEGA3_PRIORITY"],
        "recommended_ingredients": [
            "omega3",
            "fish_oil",
            "epa_dha",
            "algal_oil",
        ],
    },
    
    # === OXIDATIVE STRESS CONSTRAINTS ===
    "FLAG_OXIDATIVE_STRESS": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["oxidative_stress", "antioxidant_support_needed"],
        "reason_codes": ["BLOOD_FLAG_OXIDATIVE_STRESS"],
        "recommended_ingredients": [
            "nac",
            "glutathione",
            "alpha_lipoic_acid",
            "vitamin_c",
            "vitamin_e",
            "coq10",
        ],
    },
    
    # === CARDIOVASCULAR CONSTRAINTS ===
    "FLAG_CARDIOVASCULAR_RISK": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["cardiovascular_risk", "lipid_optimization_needed"],
        "reason_codes": ["BLOOD_FLAG_CARDIOVASCULAR_RISK"],
        "recommended_ingredients": [
            "omega3",
            "coq10",
            "plant_sterols",
            "red_yeast_rice",
        ],
    },
    
    "CAUTION_BLOOD_THINNING": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["blood_thinning_caution", "anticoagulant_interaction"],
        "reason_codes": ["BLOOD_CAUTION_BLOOD_THINNING"],
    },
    
    # === ANEMIA CONSTRAINTS ===
    "FLAG_ANEMIA": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["anemia"],
        "reason_codes": ["BLOOD_FLAG_ANEMIA"],
        "recommended_ingredients": [
            "iron_bisglycinate",
            "vitamin_c",
            "b12",
            "folate",
        ],
    },
    
    # === VINTAGE MI TRIAL - L-ARGININE BLOCK ===
    "BLOCK_POST_MI": {
        "blocked_ingredients": [
            "l_arginine",  # 8.6% mortality in VINTAGE MI trial
            "arginine",
        ],
        "blocked_categories": [],
        "blocked_targets": ["no_boosting"],
        "caution_flags": ["post_mi", "recent_cardiac_event"],
        "reason_codes": ["BLOOD_BLOCK_POST_MI", "VINTAGE_MI_TRIAL"],
    },
}


class ConstraintTranslator:
    """
    Pure, deterministic translator for bloodwork constraints.
    
    Converts constraint codes from Bloodwork Engine into canonical
    enforcement semantics for routing and matching layers.
    """
    
    def __init__(self, custom_mappings: Optional[Dict] = None):
        """
        Initialize translator with optional custom mappings.
        
        Args:
            custom_mappings: Additional constraint mappings to merge
        """
        self.mappings = CONSTRAINT_MAPPINGS.copy()
        if custom_mappings:
            self.mappings.update(custom_mappings)
        self.version = __version__
    
    def _compute_hash(self, data: Any) -> str:
        """Compute deterministic SHA-256 hash of data."""
        serialized = json.dumps(data, sort_keys=True, separators=(',', ':'))
        return f"sha256:{hashlib.sha256(serialized.encode()).hexdigest()[:16]}"
    
    def translate(
        self,
        constraint_codes: List[str],
        sex: Optional[str] = None,
        additional_context: Optional[Dict] = None
    ) -> TranslatedConstraints:
        """
        Translate constraint codes into enforcement semantics.
        
        Args:
            constraint_codes: List of constraint codes from Bloodwork Engine
            sex: Optional sex for gender-specific rules
            additional_context: Optional context for custom logic
            
        Returns:
            TranslatedConstraints with all enforcement fields populated
        """
        # Normalize and deduplicate input
        codes = sorted(set(code.upper() for code in constraint_codes))
        
        # Compute input hash for audit
        input_data = {
            "constraint_codes": codes,
            "sex": sex,
            "additional_context": additional_context or {},
        }
        input_hash = self._compute_hash(input_data)
        
        # Initialize collections
        blocked_ingredients: Set[str] = set()
        blocked_categories: Set[str] = set()
        blocked_targets: Set[str] = set()
        caution_flags: Set[str] = set()
        reason_codes: Set[str] = set()
        recommended_ingredients: Set[str] = set()
        
        # Process each constraint code
        for code in codes:
            if code not in self.mappings:
                # Unknown constraint - add to reason codes for audit
                reason_codes.add(f"UNKNOWN_CONSTRAINT_{code}")
                continue
            
            mapping = self.mappings[code]
            
            # Merge blocked ingredients
            blocked_ingredients.update(mapping.get("blocked_ingredients", []))
            
            # Merge blocked categories
            blocked_categories.update(mapping.get("blocked_categories", []))
            
            # Merge blocked targets
            blocked_targets.update(mapping.get("blocked_targets", []))
            
            # Merge caution flags
            caution_flags.update(mapping.get("caution_flags", []))
            
            # Merge reason codes
            reason_codes.update(mapping.get("reason_codes", []))
            
            # Merge recommended ingredients
            recommended_ingredients.update(mapping.get("recommended_ingredients", []))
        
        # Remove any recommended ingredients that are also blocked
        recommended_ingredients -= blocked_ingredients
        
        # Build result with sorted lists (for determinism)
        result = TranslatedConstraints(
            blocked_ingredients=sorted(blocked_ingredients),
            blocked_categories=sorted(blocked_categories),
            blocked_targets=sorted(blocked_targets),
            caution_flags=sorted(caution_flags),
            reason_codes=sorted(reason_codes),
            recommended_ingredients=sorted(recommended_ingredients),
            translator_version=self.version,
            input_hash=input_hash,
            translated_at=datetime.utcnow().isoformat() + "Z",
        )
        
        # Compute output hash
        output_data = {
            "blocked_ingredients": result.blocked_ingredients,
            "blocked_categories": result.blocked_categories,
            "blocked_targets": result.blocked_targets,
            "caution_flags": result.caution_flags,
            "reason_codes": result.reason_codes,
            "recommended_ingredients": result.recommended_ingredients,
        }
        result.output_hash = self._compute_hash(output_data)
        
        return result
    
    def get_mapping_for_code(self, code: str) -> Optional[Dict]:
        """Get the mapping for a specific constraint code."""
        return self.mappings.get(code.upper())
    
    def list_known_constraints(self) -> List[str]:
        """List all known constraint codes."""
        return sorted(self.mappings.keys())


# ============================================
# Module-Level Convenience Functions
# ============================================

_default_translator: Optional[ConstraintTranslator] = None


def get_translator() -> ConstraintTranslator:
    """Get the default translator instance (singleton)."""
    global _default_translator
    if _default_translator is None:
        _default_translator = ConstraintTranslator()
    return _default_translator


def translate_constraints(
    constraint_codes: List[str],
    sex: Optional[str] = None,
    additional_context: Optional[Dict] = None
) -> TranslatedConstraints:
    """
    Convenience function to translate constraints using default translator.
    
    Args:
        constraint_codes: List of constraint codes from Bloodwork Engine
        sex: Optional sex for gender-specific rules
        additional_context: Optional context for custom logic
        
    Returns:
        TranslatedConstraints with all enforcement fields populated
    """
    return get_translator().translate(constraint_codes, sex, additional_context)


def is_ingredient_blocked(ingredient_code: str, constraints: TranslatedConstraints) -> bool:
    """Check if an ingredient is blocked by the given constraints."""
    return ingredient_code.lower() in [i.lower() for i in constraints.blocked_ingredients]


def get_block_reason(ingredient_code: str, constraints: TranslatedConstraints) -> Optional[str]:
    """Get the reason an ingredient is blocked, if any."""
    if is_ingredient_blocked(ingredient_code, constraints):
        # Return first matching reason code
        for code in constraints.reason_codes:
            if "BLOCK" in code:
                return code
    return None


# ============================================
# Integration Helpers
# ============================================

def merge_constraints(
    bloodwork_constraints: TranslatedConstraints,
    other_constraints: Optional[Dict] = None
) -> TranslatedConstraints:
    """
    Merge bloodwork constraints with other sources.
    
    IMPORTANT: Bloodwork blocks CANNOT be removed or overridden.
    This enforces "Blood does not negotiate" principle.
    
    Args:
        bloodwork_constraints: Constraints from bloodwork translation
        other_constraints: Optional dict with additional constraints
        
    Returns:
        Merged TranslatedConstraints (bloodwork always takes precedence)
    """
    if not other_constraints:
        return bloodwork_constraints
    
    # Start with bloodwork constraints
    merged = TranslatedConstraints(
        blocked_ingredients=list(bloodwork_constraints.blocked_ingredients),
        blocked_categories=list(bloodwork_constraints.blocked_categories),
        blocked_targets=list(bloodwork_constraints.blocked_targets),
        caution_flags=list(bloodwork_constraints.caution_flags),
        reason_codes=list(bloodwork_constraints.reason_codes),
        recommended_ingredients=list(bloodwork_constraints.recommended_ingredients),
        translator_version=bloodwork_constraints.translator_version,
        input_hash=bloodwork_constraints.input_hash,
    )
    
    # Add (but never remove) from other constraints
    if "blocked_ingredients" in other_constraints:
        for ing in other_constraints["blocked_ingredients"]:
            if ing not in merged.blocked_ingredients:
                merged.blocked_ingredients.append(ing)
    
    if "caution_flags" in other_constraints:
        for flag in other_constraints["caution_flags"]:
            if flag not in merged.caution_flags:
                merged.caution_flags.append(flag)
    
    if "reason_codes" in other_constraints:
        for code in other_constraints["reason_codes"]:
            if code not in merged.reason_codes:
                merged.reason_codes.append(code)
    
    # Re-sort for determinism
    merged.blocked_ingredients = sorted(merged.blocked_ingredients)
    merged.blocked_categories = sorted(merged.blocked_categories)
    merged.blocked_targets = sorted(merged.blocked_targets)
    merged.caution_flags = sorted(merged.caution_flags)
    merged.reason_codes = sorted(merged.reason_codes)
    merged.recommended_ingredients = sorted(merged.recommended_ingredients)
    
    # Recompute output hash
    translator = get_translator()
    output_data = {
        "blocked_ingredients": merged.blocked_ingredients,
        "blocked_categories": merged.blocked_categories,
        "blocked_targets": merged.blocked_targets,
        "caution_flags": merged.caution_flags,
        "reason_codes": merged.reason_codes,
        "recommended_ingredients": merged.recommended_ingredients,
    }
    merged.output_hash = translator._compute_hash(output_data)
    merged.translated_at = datetime.utcnow().isoformat() + "Z"
    
    return merged
