"""
GenoMAX² Constraint Translator: Mapping Rules v1
=================================================
Deterministic mapping from Bloodwork Engine constraint codes
to enforcement fields for routing and matching layers.

Schema per constraint:
{
    "blocked_ingredients": [],   # Hard blocks - MUST NOT include
    "blocked_categories": [],    # Category-level blocks
    "blocked_targets": [],       # Target pathway blocks
    "caution_flags": [],         # Warnings (reduce dose, flag, acknowledge)
    "reason_codes": [],          # Audit trail codes
    "recommended_ingredients": [] # Priority ingredients (optional)
}

Mapping Version: 1.0.0
Last Updated: 2026-01-26
"""

from typing import Dict, List, Any

MAPPING_VERSION = "1.0.0"

def get_mapping_version() -> str:
    return MAPPING_VERSION


CONSTRAINT_MAPPINGS: Dict[str, Dict[str, List[str]]] = {
    
    # =========================================================================
    # TIER 1 SAFETY GATES: HARD BLOCKS
    # These constraints result in ingredient elimination
    # =========================================================================
    
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
            "iron_picolinate",
            "iron_citrate",
        ],
        "blocked_categories": ["iron_supplements"],
        "blocked_targets": ["iron_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_IRON", "FERRITIN_ELEVATED"],
    },
    
    "BLOCK_POTASSIUM": {
        "blocked_ingredients": [
            "potassium",
            "potassium_citrate",
            "potassium_chloride",
            "potassium_gluconate",
            "potassium_bicarbonate",
            "potassium_aspartate",
        ],
        "blocked_categories": ["potassium_supplements"],
        "blocked_targets": ["potassium_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_POTASSIUM", "HYPERKALEMIA"],
    },
    
    "BLOCK_IODINE": {
        "blocked_ingredients": [
            "iodine",
            "potassium_iodide",
            "kelp",
            "bladderwrack",
            "sea_vegetables",
            "irish_moss",
        ],
        "blocked_categories": ["iodine_supplements", "thyroid_stimulants"],
        "blocked_targets": ["iodine_supplementation", "thyroid_stimulation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_IODINE", "HYPERTHYROIDISM"],
    },
    
    "BLOCK_VITAMIN_D": {
        "blocked_ingredients": [
            "vitamin_d3",
            "vitamin_d2",
            "cholecalciferol",
            "ergocalciferol",
            "calcifediol",
        ],
        "blocked_categories": ["vitamin_d_supplements"],
        "blocked_targets": ["vitamin_d_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_VITAMIN_D", "HYPERVITAMINOSIS_D"],
    },
    
    "BLOCK_CALCIUM": {
        "blocked_ingredients": [
            "calcium",
            "calcium_carbonate",
            "calcium_citrate",
            "calcium_phosphate",
            "calcium_lactate",
            "calcium_gluconate",
            "calcium_orotate",
        ],
        "blocked_categories": ["calcium_supplements"],
        "blocked_targets": ["calcium_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_CALCIUM", "HYPERCALCEMIA"],
    },
    
    "BLOCK_B12": {
        "blocked_ingredients": [
            "vitamin_b12",
            "cyanocobalamin",
            "methylcobalamin",
            "adenosylcobalamin",
            "hydroxocobalamin",
        ],
        "blocked_categories": ["b12_supplements"],
        "blocked_targets": ["b12_supplementation"],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_B12", "B12_ELEVATED"],
    },
    
    # VINTAGE MI Trial: L-Arginine contraindicated post-MI (8.6% mortality)
    "BLOCK_POST_MI": {
        "blocked_ingredients": [
            "l_arginine",
            "arginine",
            "arginine_akg",
            "arginine_hcl",
        ],
        "blocked_categories": ["nitric_oxide_boosters"],
        "blocked_targets": ["no_boosting", "vasodilation"],
        "caution_flags": ["post_mi", "recent_cardiac_event"],
        "reason_codes": ["BLOOD_BLOCK_POST_MI", "VINTAGE_MI_TRIAL"],
    },
    
    # =========================================================================
    # TIER 1 SAFETY GATES: HEPATOTOXIC
    # Ashwagandha is PERMANENTLY BLOCKED due to documented hepatotoxicity
    # =========================================================================
    
    "BLOCK_HEPATOTOXIC": {
        "blocked_ingredients": [
            "ashwagandha",           # PERMANENTLY BLOCKED
            "kava",
            "black_cohosh",
            "germander",
            "comfrey",
            "pennyroyal",
            "pyrrolizidine_alkaloids",
            "green_tea_extract_high_dose",
        ],
        "blocked_categories": ["hepatotoxic_herbs"],
        "blocked_targets": [],
        "caution_flags": [],
        "reason_codes": ["BLOOD_BLOCK_HEPATOTOXIC", "LIVER_ENZYMES_CRITICAL"],
    },
    
    "CAUTION_HEPATOTOXIC": {
        "blocked_ingredients": [
            "ashwagandha",           # PERMANENTLY BLOCKED even at CAUTION level
        ],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["hepatic_sensitive", "liver_function_impaired"],
        "reason_codes": ["BLOOD_CAUTION_HEPATOTOXIC", "ALT_ELEVATED", "AST_ELEVATED"],
    },
    
    # =========================================================================
    # TIER 1 SAFETY GATES: RENAL
    # =========================================================================
    
    "CAUTION_RENAL": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["renal_sensitive", "kidney_function_impaired", "egfr_reduced"],
        "reason_codes": ["BLOOD_CAUTION_RENAL", "CREATININE_ELEVATED", "EGFR_LOW"],
    },
    
    "BLOCK_RENAL": {
        "blocked_ingredients": [
            "potassium",
            "potassium_citrate",
            "potassium_chloride",
            "magnesium_oxide",       # Poor renal clearance
            "phosphorus",
        ],
        "blocked_categories": ["high_potassium", "high_phosphorus"],
        "blocked_targets": ["potassium_supplementation"],
        "caution_flags": ["renal_severe"],
        "reason_codes": ["BLOOD_BLOCK_RENAL", "EGFR_CRITICAL"],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: CAUTIONS (No hard blocks, require attention)
    # =========================================================================
    
    "CAUTION_VITAMIN_D": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["vitamin_d_caution", "hypercalcemia_risk"],
        "reason_codes": ["BLOOD_CAUTION_VITAMIN_D", "VITAMIN_D_ELEVATED"],
    },
    
    "CAUTION_BLOOD_THINNING": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["blood_thinning_caution", "anticoagulant_interaction"],
        "reason_codes": ["BLOOD_CAUTION_BLOOD_THINNING"],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: INFLAMMATION
    # =========================================================================
    
    "FLAG_ACUTE_INFLAMMATION": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["acute_inflammation", "crp_elevated"],
        "reason_codes": ["BLOOD_FLAG_INFLAMMATION", "CRP_HIGH"],
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
            "boswellia",
        ],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: METABOLIC
    # =========================================================================
    
    "FLAG_INSULIN_RESISTANCE": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["insulin_resistance", "metabolic_support_needed"],
        "reason_codes": ["BLOOD_FLAG_INSULIN_RESISTANCE", "HOMA_IR_ELEVATED"],
        "recommended_ingredients": [
            "berberine",
            "chromium",
            "alpha_lipoic_acid",
            "cinnamon_extract",
            "magnesium",
            "inositol",
        ],
    },
    
    "FLAG_HYPERGLYCEMIA": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["hyperglycemia", "blood_sugar_elevated"],
        "reason_codes": ["BLOOD_FLAG_HYPERGLYCEMIA", "GLUCOSE_HIGH", "HBA1C_ELEVATED"],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: METHYLATION / MTHFR
    # =========================================================================
    
    "FLAG_METHYLFOLATE_REQUIRED": {
        "blocked_ingredients": [
            "folic_acid",            # Synthetic form blocked for MTHFR variants
        ],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["mthfr_variant", "methylation_impaired"],
        "reason_codes": ["BLOOD_FLAG_MTHFR", "MTHFR_DETECTED"],
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
        "reason_codes": ["BLOOD_FLAG_METHYLATION", "HOMOCYSTEINE_HIGH"],
        "recommended_ingredients": [
            "methylcobalamin",
            "methylfolate",
            "pyridoxal_5_phosphate",
            "betaine_tmg",
            "riboflavin_5_phosphate",
        ],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: B VITAMINS
    # =========================================================================
    
    "FLAG_B12_DEFICIENCY": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["b12_deficiency"],
        "reason_codes": ["BLOOD_FLAG_B12_DEFICIENCY", "B12_LOW"],
        "recommended_ingredients": [
            "methylcobalamin",
            "adenosylcobalamin",
            "hydroxocobalamin",
        ],
    },
    
    "FLAG_FOLATE_DEFICIENCY": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["folate_deficiency"],
        "reason_codes": ["BLOOD_FLAG_FOLATE_DEFICIENCY", "FOLATE_LOW"],
        "recommended_ingredients": [
            "methylfolate",
            "folinic_acid",
        ],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: THYROID
    # =========================================================================
    
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
        "reason_codes": ["BLOOD_FLAG_HYPOTHYROID", "TSH_HIGH"],
        "recommended_ingredients": [
            "selenium",
            "zinc",
            "iodine",              # Only if not blocked by other constraints
        ],
    },
    
    "FLAG_HYPERTHYROID": {
        "blocked_ingredients": [
            "iodine",
            "kelp",
            "bladderwrack",
        ],
        "blocked_categories": ["thyroid_stimulants"],
        "blocked_targets": ["thyroid_stimulation"],
        "caution_flags": ["hyperthyroid", "tsh_suppressed"],
        "reason_codes": ["BLOOD_FLAG_HYPERTHYROID", "TSH_LOW"],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: LIPIDS / CARDIOVASCULAR
    # =========================================================================
    
    "FLAG_OMEGA3_PRIORITY": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": [],
        "reason_codes": ["BLOOD_FLAG_OMEGA3_PRIORITY", "OMEGA3_INDEX_LOW"],
        "recommended_ingredients": [
            "omega3",
            "fish_oil",
            "epa_dha",
            "algal_oil",
            "krill_oil",
        ],
    },
    
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
            "niacin",
        ],
    },
    
    "FLAG_LDL_ELEVATED": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["ldl_elevated"],
        "reason_codes": ["BLOOD_FLAG_LDL_ELEVATED", "LDL_HIGH"],
        "recommended_ingredients": [
            "plant_sterols",
            "psyllium",
            "red_yeast_rice",
            "berberine",
        ],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: OXIDATIVE STRESS
    # =========================================================================
    
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
            "astaxanthin",
        ],
    },
    
    # =========================================================================
    # TIER 2 FLAGS: ANEMIA / IRON
    # =========================================================================
    
    "FLAG_ANEMIA": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["anemia"],
        "reason_codes": ["BLOOD_FLAG_ANEMIA", "HEMOGLOBIN_LOW", "HEMATOCRIT_LOW"],
        "recommended_ingredients": [
            "iron_bisglycinate",
            "vitamin_c",
            "vitamin_b12",
            "folate",
        ],
    },
    
    "FLAG_IRON_DEFICIENCY": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["iron_deficiency"],
        "reason_codes": ["BLOOD_FLAG_IRON_DEFICIENCY", "FERRITIN_LOW"],
        "recommended_ingredients": [
            "iron_bisglycinate",
            "iron_glycinate",
            "vitamin_c",
        ],
    },
    
    # =========================================================================
    # TIER 3 FLAGS: HORMONAL (Gender-specific handled in translator)
    # =========================================================================
    
    "FLAG_TESTOSTERONE_LOW": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["testosterone_low"],
        "reason_codes": ["BLOOD_FLAG_TESTOSTERONE_LOW"],
        "recommended_ingredients": [
            "zinc",
            "vitamin_d3",
            "magnesium",
            "tongkat_ali",
            "fenugreek",
        ],
    },
    
    "FLAG_ESTROGEN_IMBALANCE": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["estrogen_imbalance"],
        "reason_codes": ["BLOOD_FLAG_ESTROGEN_IMBALANCE"],
        "recommended_ingredients": [
            "dim",
            "calcium_d_glucarate",
            "broccoli_extract",
        ],
    },
    
    "FLAG_CORTISOL_ELEVATED": {
        "blocked_ingredients": [],
        "blocked_categories": [],
        "blocked_targets": [],
        "caution_flags": ["cortisol_elevated", "stress_response"],
        "reason_codes": ["BLOOD_FLAG_CORTISOL_ELEVATED"],
        "recommended_ingredients": [
            "phosphatidylserine",
            "rhodiola",
            "holy_basil",
            "l_theanine",
        ],
    },
}


# =========================================================================
# Validation: Ensure all mappings have required fields
# =========================================================================

REQUIRED_FIELDS = [
    "blocked_ingredients",
    "blocked_categories", 
    "blocked_targets",
    "caution_flags",
    "reason_codes",
]

def validate_mappings() -> Dict[str, Any]:
    """Validate all constraint mappings have required fields."""
    errors = []
    for code, mapping in CONSTRAINT_MAPPINGS.items():
        for field in REQUIRED_FIELDS:
            if field not in mapping:
                errors.append(f"{code} missing field: {field}")
    return {
        "valid": len(errors) == 0,
        "total_mappings": len(CONSTRAINT_MAPPINGS),
        "errors": errors,
    }
