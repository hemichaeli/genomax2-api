"""
GenoMAX² Brain Orchestrator Pipeline
=====================================
Core business logic connecting bloodwork to supplement recommendations.

Pipeline Flow:
1. Receive BloodworkCanonical from Bloodwork Engine
2. Load supplement catalog from catalog wiring
3. Score modules against biomarker deficiencies
4. Apply safety constraints (blocked ingredients)
5. Filter by gender/lifecycle requirements
6. Rank and select optimal module combination
7. Prepare for Route phase (SKU selection)

"Blood does not negotiate" - safety constraints are absolute.

Version: 1.1.0 - Uses catalog wiring instead of supplement_modules table
"""

import os
import json
import httpx
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, Set
from enum import Enum
from dataclasses import dataclass, field
from pydantic import BaseModel, Field
import asyncpg
from asyncpg import Pool

# =============================================================================
# CONFIGURATION
# =============================================================================

DATABASE_URL = os.getenv("DATABASE_URL", "")
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

# 13 Priority Biomarkers with deficiency thresholds
BIOMARKER_DEFICIENCY_THRESHOLDS = {
    "ferritin": {
        "deficient": {"male": 30, "female": 20},
        "suboptimal": {"male": 50, "female": 40},
        "optimal_range": {"male": (50, 200), "female": (40, 150)},
        "target_ingredients": ["iron_bisglycinate", "iron"],
        "priority_weight": 1.2
    },
    "vitamin_d_25oh": {
        "deficient": {"male": 20, "female": 20},
        "suboptimal": {"male": 30, "female": 30},
        "optimal_range": {"male": (40, 60), "female": (40, 60)},
        "target_ingredients": ["vitamin_d3", "cholecalciferol"],
        "priority_weight": 1.3
    },
    "vitamin_b12": {
        "deficient": {"male": 200, "female": 200},
        "suboptimal": {"male": 400, "female": 400},
        "optimal_range": {"male": (500, 900), "female": (500, 900)},
        "target_ingredients": ["methylcobalamin", "vitamin_b12", "cyanocobalamin"],
        "priority_weight": 1.2
    },
    "folate": {
        "deficient": {"male": 3, "female": 3},
        "suboptimal": {"male": 5, "female": 5},
        "optimal_range": {"male": (10, 25), "female": (10, 25)},
        "target_ingredients": ["methylfolate", "folate", "folic_acid"],
        "priority_weight": 1.1
    },
    "hba1c": {
        "elevated": {"male": 5.7, "female": 5.7},
        "diabetic": {"male": 6.5, "female": 6.5},
        "optimal_range": {"male": (4.5, 5.6), "female": (4.5, 5.6)},
        "target_ingredients": ["berberine", "chromium", "alpha_lipoic_acid"],
        "priority_weight": 1.4,
        "inverse": True  # Lower is better
    },
    "hscrp": {
        "elevated": {"male": 1.0, "female": 1.0},
        "high": {"male": 3.0, "female": 3.0},
        "optimal_range": {"male": (0, 1.0), "female": (0, 1.0)},
        "target_ingredients": ["omega_3", "fish_oil", "curcumin", "turmeric"],
        "priority_weight": 1.3,
        "inverse": True
    },
    "homocysteine": {
        "elevated": {"male": 10, "female": 10},
        "high": {"male": 15, "female": 15},
        "optimal_range": {"male": (5, 10), "female": (5, 10)},
        "target_ingredients": ["methylfolate", "methylcobalamin", "b6_p5p"],
        "priority_weight": 1.2,
        "inverse": True
    },
    "omega3_index": {
        "deficient": {"male": 4, "female": 4},
        "suboptimal": {"male": 6, "female": 6},
        "optimal_range": {"male": (8, 12), "female": (8, 12)},
        "target_ingredients": ["omega_3", "fish_oil", "epa", "dha"],
        "priority_weight": 1.2
    },
    "magnesium_rbc": {
        "deficient": {"male": 4.2, "female": 4.2},
        "suboptimal": {"male": 5.0, "female": 5.0},
        "optimal_range": {"male": (5.5, 6.5), "female": (5.5, 6.5)},
        "target_ingredients": ["magnesium_glycinate", "magnesium"],
        "priority_weight": 1.1
    },
    "zinc": {
        "deficient": {"male": 60, "female": 60},
        "suboptimal": {"male": 80, "female": 80},
        "optimal_range": {"male": (90, 120), "female": (80, 110)},
        "target_ingredients": ["zinc_picolinate", "zinc"],
        "priority_weight": 1.0
    },
    "serum_iron": {
        "deficient": {"male": 60, "female": 50},
        "suboptimal": {"male": 80, "female": 70},
        "optimal_range": {"male": (80, 170), "female": (60, 150)},
        "target_ingredients": ["iron_bisglycinate", "iron"],
        "priority_weight": 1.1
    },
    "transferrin_sat": {
        "deficient": {"male": 20, "female": 15},
        "suboptimal": {"male": 25, "female": 20},
        "optimal_range": {"male": (25, 45), "female": (20, 45)},
        "target_ingredients": ["iron_bisglycinate"],
        "priority_weight": 1.0
    },
    "tibc": {
        "elevated": {"male": 400, "female": 400},
        "optimal_range": {"male": (250, 400), "female": (250, 400)},
        "target_ingredients": ["iron_bisglycinate"],
        "priority_weight": 0.9,
        "inverse": True
    }
}

# Ingredient to biomarker mapping for target_biomarkers derivation
INGREDIENT_BIOMARKER_MAP = {
    "iron": ["ferritin", "serum_iron", "transferrin_sat", "tibc"],
    "iron_bisglycinate": ["ferritin", "serum_iron", "transferrin_sat", "tibc"],
    "vitamin_d3": ["vitamin_d_25oh"],
    "cholecalciferol": ["vitamin_d_25oh"],
    "vitamin_k2": ["vitamin_d_25oh"],  # Synergistic
    "methylcobalamin": ["vitamin_b12", "homocysteine"],
    "vitamin_b12": ["vitamin_b12", "homocysteine"],
    "cyanocobalamin": ["vitamin_b12"],
    "methylfolate": ["folate", "homocysteine"],
    "folate": ["folate", "homocysteine"],
    "folic_acid": ["folate"],
    "berberine": ["hba1c"],
    "chromium": ["hba1c"],
    "alpha_lipoic_acid": ["hba1c"],
    "omega_3": ["omega3_index", "hscrp"],
    "fish_oil": ["omega3_index", "hscrp"],
    "epa": ["omega3_index", "hscrp"],
    "dha": ["omega3_index", "hscrp"],
    "curcumin": ["hscrp"],
    "turmeric": ["hscrp"],
    "magnesium": ["magnesium_rbc"],
    "magnesium_glycinate": ["magnesium_rbc"],
    "zinc": ["zinc"],
    "zinc_picolinate": ["zinc"],
    "b6_p5p": ["homocysteine"],
    "coq10": ["hscrp"],
    "ubiquinol": ["hscrp"],
}

# Lifecycle-specific recommendations
LIFECYCLE_RECOMMENDATIONS = {
    "pregnant": {
        "required": ["folate", "iron", "vitamin_d3", "omega_3", "choline"],
        "blocked": ["vitamin_a_retinol", "high_dose_vitamin_a"],
        "increased_dosage": ["folate", "iron", "vitamin_d3"],
        "priority_boost": 1.5
    },
    "breastfeeding": {
        "required": ["vitamin_d3", "omega_3", "choline", "iodine"],
        "blocked": [],
        "increased_dosage": ["vitamin_d3", "omega_3"],
        "priority_boost": 1.3
    },
    "perimenopause": {
        "recommended": ["magnesium", "vitamin_d3", "omega_3", "b_complex"],
        "priority_boost": 1.2
    },
    "postmenopausal": {
        "recommended": ["vitamin_d3", "calcium", "magnesium", "vitamin_k2"],
        "blocked": [],
        "priority_boost": 1.2
    },
    "athletic": {
        "recommended": ["magnesium", "zinc", "vitamin_d3", "omega_3", "coq10"],
        "increased_dosage": ["magnesium", "zinc"],
        "priority_boost": 1.1
    }
}

# =============================================================================
# MODELS
# =============================================================================

class BrainRunStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SCORING = "scoring"
    ROUTING = "routing"
    COMPLETED = "completed"
    FAILED = "failed"

class DeficiencyLevel(str, Enum):
    OPTIMAL = "optimal"
    SUBOPTIMAL = "suboptimal"
    DEFICIENT = "deficient"
    ELEVATED = "elevated"  # For inverse markers
    UNKNOWN = "unknown"

@dataclass
class BiomarkerDeficiency:
    """Represents a detected biomarker deficiency/elevation."""
    marker_code: str
    value: float
    unit: str
    level: DeficiencyLevel
    distance_from_optimal: float  # How far from optimal range
    target_ingredients: List[str]
    priority_weight: float
    score_contribution: float = 0.0

@dataclass
class ModuleScore:
    """Score for a supplement module against user's bloodwork."""
    module_id: str  # Changed to str for SKU
    module_name: str
    category: str
    sku: str = ""
    base_score: float = 0.0
    biomarker_match_score: float = 0.0
    goal_match_score: float = 0.0
    lifecycle_bonus: float = 0.0
    confidence_multiplier: float = 1.0
    final_score: float = 0.0
    matched_deficiencies: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    blocked: bool = False
    block_reason: Optional[str] = None
    caution: bool = False
    caution_reasons: List[str] = field(default_factory=list)

@dataclass
class BrainRunResult:
    """Complete result of a Brain pipeline run."""
    run_id: str
    submission_id: str
    user_id: str
    status: BrainRunStatus
    
    # Input summary
    markers_processed: int
    priority_markers_found: int
    deficiencies_detected: List[BiomarkerDeficiency]
    
    # Module selection
    recommended_modules: List[ModuleScore]
    blocked_modules: List[ModuleScore]
    caution_modules: List[ModuleScore]
    
    # Safety summary
    blocked_ingredients: List[str]
    caution_ingredients: List[str]
    safety_gates_triggered: int
    
    # Metadata
    gender: str
    lifecycle_phase: Optional[str]
    goals: List[str]
    processing_time_ms: int
    created_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

_pool: Optional[Pool] = None

async def get_pool() -> Pool:
    """Get or create database connection pool."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool

async def close_pool():
    """Close database connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

# =============================================================================
# CATALOG LOADING - Uses catalog wiring endpoint
# =============================================================================

# In-memory catalog cache
_catalog_cache: Optional[List[Dict[str, Any]]] = None
_catalog_loaded_at: Optional[datetime] = None
CATALOG_CACHE_TTL_SECONDS = 300  # 5 minutes

async def load_catalog_from_wiring() -> List[Dict[str, Any]]:
    """
    Load supplement catalog from catalog wiring endpoint.
    
    Maps catalog wiring format to Brain orchestrator expected format.
    """
    global _catalog_cache, _catalog_loaded_at
    
    # Check cache
    if _catalog_cache and _catalog_loaded_at:
        age = (datetime.utcnow() - _catalog_loaded_at).total_seconds()
        if age < CATALOG_CACHE_TTL_SECONDS:
            return _catalog_cache
    
    # Fetch from catalog wiring endpoint
    try:
        async with httpx.AsyncClient() as client:
            # Try internal endpoint first
            response = await client.get(
                f"{API_BASE_URL}/api/v1/catalog/wiring/products",
                timeout=10.0
            )
            if response.status_code != 200:
                # Fallback to self-reference
                response = await client.get(
                    "http://localhost:8000/api/v1/catalog/wiring/products",
                    timeout=10.0
                )
            
            data = response.json()
            products = data.get("products", [])
            
    except Exception as e:
        print(f"Error loading catalog from wiring: {e}")
        # Return cached data if available, even if stale
        if _catalog_cache:
            return _catalog_cache
        return []
    
    # Transform to Brain orchestrator format
    modules = []
    for idx, product in enumerate(products):
        # Extract ingredient tags
        ingredient_tags = product.get("ingredient_tags", [])
        
        # Derive target_biomarkers from ingredients
        target_biomarkers = set()
        for ingredient in ingredient_tags:
            ingredient_lower = ingredient.lower()
            if ingredient_lower in INGREDIENT_BIOMARKER_MAP:
                target_biomarkers.update(INGREDIENT_BIOMARKER_MAP[ingredient_lower])
        
        # Map product_line to gender eligibility
        product_line = product.get("product_line", "Universal")
        sex_target = product.get("sex_target", "unisex")
        
        modules.append({
            "id": product.get("sku", f"module_{idx}"),
            "sku": product.get("sku", ""),
            "name": product.get("name", "Unknown"),
            "category": product.get("category", "General"),
            "subcategory": "",  # Not available in wiring
            "description": "",  # Not available in wiring
            "target_biomarkers": list(target_biomarkers),
            "primary_ingredients": ingredient_tags,
            "all_ingredients": ingredient_tags,
            "evidence_tier": product.get("evidence_tier", "TIER_2"),
            "product_line": product_line,
            "sex_target": sex_target,
            "lifecycle_phases": [],  # Derive from category if needed
            "contraindications": [],  # Not available in wiring
            "price_usd": product.get("price_usd", 0)
        })
    
    # Update cache
    _catalog_cache = modules
    _catalog_loaded_at = datetime.utcnow()
    
    return modules

async def load_supplement_catalog(
    gender: str,
    blocked_ingredients: Set[str]
) -> List[Dict[str, Any]]:
    """
    Load supplement modules filtered by gender eligibility.
    
    Returns modules with:
    - Ingredient lists
    - Category/subcategory
    - Target biomarkers (derived from ingredients)
    - Eligibility criteria
    """
    # Load from catalog wiring
    all_modules = await load_catalog_from_wiring()
    
    # Filter by gender eligibility
    gender_lower = gender.lower()
    eligible_modules = []
    
    for module in all_modules:
        # Check product line eligibility
        product_line = module.get("product_line", "Universal").lower()
        sex_target = module.get("sex_target", "unisex").lower()
        
        # Universal products are always eligible
        if product_line == "universal" or sex_target == "unisex":
            is_eligible = True
        # Gender-specific products
        elif gender_lower == "male" and (product_line == "maximo²" or sex_target == "male"):
            is_eligible = True
        elif gender_lower == "female" and (product_line == "maxima²" or sex_target == "female"):
            is_eligible = True
        else:
            is_eligible = False
        
        if not is_eligible:
            continue
        
        # Filter by evidence tier
        evidence_tier = module.get("evidence_tier", "TIER_2")
        if evidence_tier not in ["TIER_1", "TIER_2"]:
            continue
        
        # Check if any ingredient is blocked
        all_ingredients = module.get("all_ingredients", [])
        ingredient_set = set(i.lower() for i in all_ingredients)
        blocked_match = ingredient_set.intersection(blocked_ingredients)
        
        eligible_modules.append({
            **module,
            "is_blocked": len(blocked_match) > 0,
            "blocked_ingredients": list(blocked_match)
        })
    
    return eligible_modules

# =============================================================================
# DEFICIENCY DETECTION
# =============================================================================

def detect_deficiencies(
    markers: List[Dict[str, Any]],
    gender: str
) -> List[BiomarkerDeficiency]:
    """
    Analyze markers and detect deficiencies/elevations.
    
    Args:
        markers: List of normalized markers from BloodworkCanonical
        gender: User gender for reference ranges
    
    Returns:
        List of detected deficiencies with scoring data
    """
    deficiencies = []
    gender_key = "male" if gender.lower() == "male" else "female"
    
    # Build marker lookup
    marker_lookup = {m["code"]: m for m in markers}
    
    for marker_code, config in BIOMARKER_DEFICIENCY_THRESHOLDS.items():
        if marker_code not in marker_lookup:
            continue
        
        marker = marker_lookup[marker_code]
        value = marker["value"]
        optimal_range = config["optimal_range"][gender_key]
        is_inverse = config.get("inverse", False)
        
        # Determine deficiency level
        level = DeficiencyLevel.OPTIMAL
        distance = 0.0
        
        if is_inverse:
            # Higher values are worse (HbA1c, hs-CRP, homocysteine)
            if "diabetic" in config and value >= config["diabetic"][gender_key]:
                level = DeficiencyLevel.ELEVATED
                distance = (value - optimal_range[1]) / optimal_range[1]
            elif "high" in config and value >= config["high"][gender_key]:
                level = DeficiencyLevel.ELEVATED
                distance = (value - optimal_range[1]) / optimal_range[1]
            elif "elevated" in config and value >= config["elevated"][gender_key]:
                level = DeficiencyLevel.SUBOPTIMAL
                distance = (value - optimal_range[1]) / optimal_range[1]
            elif value < optimal_range[0]:
                level = DeficiencyLevel.OPTIMAL
                distance = 0
            elif value > optimal_range[1]:
                level = DeficiencyLevel.SUBOPTIMAL
                distance = (value - optimal_range[1]) / optimal_range[1]
        else:
            # Lower values are worse (most vitamins/minerals)
            deficient_threshold = config.get("deficient", {}).get(gender_key, 0)
            suboptimal_threshold = config.get("suboptimal", {}).get(gender_key, 0)
            
            if deficient_threshold and value < deficient_threshold:
                level = DeficiencyLevel.DEFICIENT
                distance = (deficient_threshold - value) / deficient_threshold
            elif suboptimal_threshold and value < suboptimal_threshold:
                level = DeficiencyLevel.SUBOPTIMAL
                distance = (suboptimal_threshold - value) / suboptimal_threshold
            elif value < optimal_range[0]:
                level = DeficiencyLevel.SUBOPTIMAL
                distance = (optimal_range[0] - value) / optimal_range[0]
        
        # Only include non-optimal markers
        if level != DeficiencyLevel.OPTIMAL:
            deficiencies.append(BiomarkerDeficiency(
                marker_code=marker_code,
                value=value,
                unit=marker.get("unit", ""),
                level=level,
                distance_from_optimal=min(distance, 2.0),  # Cap at 2x
                target_ingredients=config["target_ingredients"],
                priority_weight=config["priority_weight"],
                score_contribution=0.0
            ))
    
    # Sort by severity (deficient > suboptimal, higher distance first)
    level_order = {
        DeficiencyLevel.DEFICIENT: 0,
        DeficiencyLevel.ELEVATED: 1,
        DeficiencyLevel.SUBOPTIMAL: 2
    }
    deficiencies.sort(
        key=lambda d: (level_order.get(d.level, 99), -d.distance_from_optimal)
    )
    
    return deficiencies

# =============================================================================
# MODULE SCORING
# =============================================================================

def score_module(
    module: Dict[str, Any],
    deficiencies: List[BiomarkerDeficiency],
    goals: List[str],
    lifecycle_phase: Optional[str],
    confidence_score: float,
    caution_ingredients: Set[str]
) -> ModuleScore:
    """
    Score a supplement module against user's deficiencies and goals.
    
    Scoring factors:
    1. Biomarker match: Does module target detected deficiencies?
    2. Goal alignment: Does module support user's health goals?
    3. Lifecycle bonus: Is module recommended for lifecycle phase?
    4. Evidence tier: TIER_1 > TIER_2
    5. Confidence multiplier: Based on bloodwork data quality
    """
    score = ModuleScore(
        module_id=module["id"],
        module_name=module["name"],
        category=module["category"],
        sku=module.get("sku", "")
    )
    
    # Check if blocked
    if module.get("is_blocked", False):
        score.blocked = True
        score.block_reason = f"Contains blocked ingredients: {', '.join(module.get('blocked_ingredients', []))}"
        score.final_score = -1000
        return score
    
    # Check for caution ingredients
    module_ingredients = set(i.lower() for i in module.get("all_ingredients", []))
    caution_match = module_ingredients.intersection(caution_ingredients)
    if caution_match:
        score.caution = True
        score.caution_reasons.append(f"Contains caution ingredients: {', '.join(caution_match)}")
    
    # Base score from evidence tier
    tier_scores = {"TIER_1": 30, "TIER_2": 20, "TIER_3": 0}
    score.base_score = tier_scores.get(module.get("evidence_tier", "TIER_2"), 10)
    
    # Biomarker match scoring
    module_ingredients_lower = set(i.lower() for i in module.get("primary_ingredients", []))
    module_biomarkers = set(b.lower() for b in module.get("target_biomarkers", []))
    
    for deficiency in deficiencies:
        # Check ingredient match
        target_set = set(i.lower() for i in deficiency.target_ingredients)
        ingredient_match = module_ingredients_lower.intersection(target_set)
        
        # Check biomarker match
        biomarker_match = deficiency.marker_code.lower() in module_biomarkers
        
        if ingredient_match or biomarker_match:
            # Score based on deficiency severity and priority
            severity_multiplier = {
                DeficiencyLevel.DEFICIENT: 2.0,
                DeficiencyLevel.ELEVATED: 1.8,
                DeficiencyLevel.SUBOPTIMAL: 1.0
            }.get(deficiency.level, 0.5)
            
            contribution = (
                10 * 
                severity_multiplier * 
                deficiency.priority_weight * 
                (1 + deficiency.distance_from_optimal)
            )
            
            score.biomarker_match_score += contribution
            score.matched_deficiencies.append(deficiency.marker_code)
            score.reasons.append(
                f"Targets {deficiency.marker_code} ({deficiency.level.value})"
            )
    
    # Goal alignment scoring
    goal_keywords = {
        "energy": ["b_complex", "b12", "iron", "coq10", "mitochondrial", "cordyceps"],
        "sleep": ["magnesium", "gaba", "melatonin", "glycine", "valerian"],
        "stress": ["magnesium", "rhodiola", "adaptogen", "reishi", "holy_basil"],
        "immunity": ["vitamin_d", "zinc", "vitamin_c", "elderberry", "echinacea"],
        "heart_health": ["omega_3", "coq10", "magnesium", "vitamin_k2", "fish_oil"],
        "brain_health": ["omega_3", "b_complex", "choline", "lions_mane", "alpha_gpc"],
        "bone_health": ["vitamin_d", "calcium", "vitamin_k2", "magnesium"],
        "skin_health": ["collagen", "vitamin_c", "vitamin_e", "biotin"],
        "gut_health": ["probiotic", "fiber", "digestive_enzyme", "glutamine"],
        "muscle_recovery": ["magnesium", "protein", "bcaa", "creatine", "l_glutamine"],
        "inflammation": ["curcumin", "turmeric", "omega_3", "fish_oil", "quercetin"],
        "detox": ["milk_thistle", "chlorella", "spirulina"],
        "longevity": ["nmn", "resveratrol", "coq10", "quercetin"]
    }
    
    for goal in goals:
        goal_key = goal.lower().replace(" ", "_")
        if goal_key in goal_keywords:
            keywords = goal_keywords[goal_key]
            category_match = any(kw in module["category"].lower() for kw in keywords)
            ingredient_match = any(
                kw in i.lower() 
                for i in module.get("primary_ingredients", []) 
                for kw in keywords
            )
            
            if category_match or ingredient_match:
                score.goal_match_score += 15
                score.reasons.append(f"Supports goal: {goal}")
    
    # Lifecycle phase bonus
    if lifecycle_phase:
        lifecycle_config = LIFECYCLE_RECOMMENDATIONS.get(lifecycle_phase.lower(), {})
        
        # Check required supplements
        required = lifecycle_config.get("required", [])
        if any(r.lower() in i.lower() for i in module.get("primary_ingredients", []) for r in required):
            score.lifecycle_bonus += 25
            score.reasons.append(f"Required for {lifecycle_phase}")
        
        # Check recommended supplements
        recommended = lifecycle_config.get("recommended", [])
        if any(r.lower() in i.lower() for i in module.get("primary_ingredients", []) for r in recommended):
            score.lifecycle_bonus += 15
            score.reasons.append(f"Recommended for {lifecycle_phase}")
        
        # Apply priority boost
        priority_boost = lifecycle_config.get("priority_boost", 1.0)
        score.lifecycle_bonus *= priority_boost
    
    # Confidence multiplier
    score.confidence_multiplier = 0.5 + (confidence_score * 0.5)  # Range: 0.5 to 1.0
    
    # Calculate final score
    score.final_score = (
        (score.base_score + score.biomarker_match_score + 
         score.goal_match_score + score.lifecycle_bonus) *
        score.confidence_multiplier
    )
    
    # Apply caution penalty (reduce score but don't block)
    if score.caution:
        score.final_score *= 0.8
    
    return score

# =============================================================================
# BRAIN ORCHESTRATOR
# =============================================================================

class BrainOrchestrator:
    """
    Main Brain pipeline orchestrator.
    
    Connects bloodwork analysis to supplement recommendations
    while enforcing safety constraints.
    """
    
    def __init__(self):
        self.pool: Optional[Pool] = None
    
    async def initialize(self):
        """Initialize database connection."""
        self.pool = await get_pool()
    
    async def run(
        self,
        submission_id: str,
        user_id: str,
        markers: List[Dict[str, Any]],
        blocked_ingredients: List[str],
        caution_ingredients: List[str],
        gender: str,
        age: Optional[int] = None,
        lifecycle_phase: Optional[str] = None,
        goals: List[str] = None,
        excluded_ingredients: List[str] = None,
        confidence_score: float = 1.0
    ) -> BrainRunResult:
        """
        Execute full Brain pipeline.
        
        "Blood does not negotiate" - blocked ingredients are absolute.
        """
        start_time = datetime.utcnow()
        run_id = f"brain_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{user_id[:8]}"
        
        try:
            # Combine blocked ingredients
            all_blocked = set(i.lower() for i in (blocked_ingredients or []))
            all_blocked.update(i.lower() for i in (excluded_ingredients or []))
            
            caution_set = set(i.lower() for i in (caution_ingredients or []))
            
            # Step 1: Detect deficiencies
            deficiencies = detect_deficiencies(markers, gender)
            
            # Step 2: Load supplement catalog from wiring
            modules = await load_supplement_catalog(gender, all_blocked)
            
            # Step 3: Score all modules
            scored_modules = []
            for module in modules:
                score = score_module(
                    module=module,
                    deficiencies=deficiencies,
                    goals=goals or [],
                    lifecycle_phase=lifecycle_phase,
                    confidence_score=confidence_score,
                    caution_ingredients=caution_set
                )
                scored_modules.append(score)
            
            # Step 4: Separate blocked, caution, and recommended
            blocked_modules = [m for m in scored_modules if m.blocked]
            caution_modules = [m for m in scored_modules if m.caution and not m.blocked]
            eligible_modules = [m for m in scored_modules if not m.blocked]
            
            # Step 5: Rank by score
            eligible_modules.sort(key=lambda m: m.final_score, reverse=True)
            
            # Step 6: Select top recommendations (limit to reasonable number)
            max_modules = 8 if lifecycle_phase in ["pregnant", "breastfeeding"] else 6
            recommended = eligible_modules[:max_modules]
            
            # Calculate processing time
            end_time = datetime.utcnow()
            processing_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Build result
            result = BrainRunResult(
                run_id=run_id,
                submission_id=submission_id,
                user_id=user_id,
                status=BrainRunStatus.COMPLETED,
                markers_processed=len(markers),
                priority_markers_found=sum(
                    1 for m in markers 
                    if m["code"] in BIOMARKER_DEFICIENCY_THRESHOLDS
                ),
                deficiencies_detected=deficiencies,
                recommended_modules=recommended,
                blocked_modules=blocked_modules,
                caution_modules=caution_modules,
                blocked_ingredients=list(all_blocked),
                caution_ingredients=list(caution_set),
                safety_gates_triggered=len(all_blocked) + len(caution_set),
                gender=gender,
                lifecycle_phase=lifecycle_phase,
                goals=goals or [],
                processing_time_ms=processing_ms,
                created_at=start_time,
                completed_at=end_time
            )
            
            # Store result in database
            await self._store_run_result(result)
            
            return result
            
        except Exception as e:
            # Return failed result
            end_time = datetime.utcnow()
            return BrainRunResult(
                run_id=run_id,
                submission_id=submission_id,
                user_id=user_id,
                status=BrainRunStatus.FAILED,
                markers_processed=len(markers),
                priority_markers_found=0,
                deficiencies_detected=[],
                recommended_modules=[],
                blocked_modules=[],
                caution_modules=[],
                blocked_ingredients=list(blocked_ingredients or []),
                caution_ingredients=list(caution_ingredients or []),
                safety_gates_triggered=0,
                gender=gender,
                lifecycle_phase=lifecycle_phase,
                goals=goals or [],
                processing_time_ms=int((end_time - start_time).total_seconds() * 1000),
                created_at=start_time,
                completed_at=end_time,
                error_message=str(e)
            )
    
    async def _store_run_result(self, result: BrainRunResult):
        """Store Brain run result in database for audit trail."""
        if not self.pool:
            self.pool = await get_pool()
        
        query = """
        INSERT INTO brain_runs (
            run_id, submission_id, user_id, status,
            markers_processed, priority_markers_found,
            deficiencies_json, recommended_modules_json,
            blocked_modules_json, caution_modules_json,
            blocked_ingredients, caution_ingredients,
            safety_gates_triggered, gender, lifecycle_phase,
            goals, processing_time_ms, created_at, completed_at,
            error_message
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
        )
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            completed_at = EXCLUDED.completed_at,
            error_message = EXCLUDED.error_message
        """
        
        # Serialize dataclasses to JSON
        deficiencies_json = json.dumps([
            {
                "marker_code": d.marker_code,
                "value": d.value,
                "unit": d.unit,
                "level": d.level.value,
                "distance_from_optimal": d.distance_from_optimal,
                "target_ingredients": d.target_ingredients,
                "priority_weight": d.priority_weight
            }
            for d in result.deficiencies_detected
        ])
        
        def module_to_dict(m: ModuleScore) -> dict:
            return {
                "module_id": m.module_id,
                "module_name": m.module_name,
                "category": m.category,
                "sku": m.sku,
                "final_score": m.final_score,
                "matched_deficiencies": m.matched_deficiencies,
                "reasons": m.reasons,
                "blocked": m.blocked,
                "block_reason": m.block_reason,
                "caution": m.caution,
                "caution_reasons": m.caution_reasons
            }
        
        recommended_json = json.dumps([module_to_dict(m) for m in result.recommended_modules])
        blocked_json = json.dumps([module_to_dict(m) for m in result.blocked_modules])
        caution_json = json.dumps([module_to_dict(m) for m in result.caution_modules])
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    query,
                    result.run_id,
                    result.submission_id,
                    result.user_id,
                    result.status.value,
                    result.markers_processed,
                    result.priority_markers_found,
                    deficiencies_json,
                    recommended_json,
                    blocked_json,
                    caution_json,
                    result.blocked_ingredients,
                    result.caution_ingredients,
                    result.safety_gates_triggered,
                    result.gender,
                    result.lifecycle_phase,
                    result.goals,
                    result.processing_time_ms,
                    result.created_at,
                    result.completed_at,
                    result.error_message
                )
        except Exception as e:
            # Log error but don't fail the pipeline
            print(f"Error storing brain run: {e}")
    
    async def get_run_status(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a Brain run."""
        if not self.pool:
            self.pool = await get_pool()
        
        query = """
        SELECT 
            run_id, submission_id, user_id, status,
            markers_processed, priority_markers_found,
            deficiencies_json, recommended_modules_json,
            blocked_modules_json, safety_gates_triggered,
            gender, lifecycle_phase, goals,
            processing_time_ms, created_at, completed_at,
            error_message
        FROM brain_runs
        WHERE run_id = $1
        """
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, run_id)
        
        if not row:
            return None
        
        return {
            "run_id": row["run_id"],
            "submission_id": row["submission_id"],
            "user_id": row["user_id"],
            "status": row["status"],
            "markers_processed": row["markers_processed"],
            "priority_markers_found": row["priority_markers_found"],
            "deficiencies": json.loads(row["deficiencies_json"]) if row["deficiencies_json"] else [],
            "recommended_modules": json.loads(row["recommended_modules_json"]) if row["recommended_modules_json"] else [],
            "blocked_modules": json.loads(row["blocked_modules_json"]) if row["blocked_modules_json"] else [],
            "safety_gates_triggered": row["safety_gates_triggered"],
            "gender": row["gender"],
            "lifecycle_phase": row["lifecycle_phase"],
            "goals": row["goals"],
            "processing_time_ms": row["processing_time_ms"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
            "error_message": row["error_message"]
        }

# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_orchestrator: Optional[BrainOrchestrator] = None

async def get_orchestrator() -> BrainOrchestrator:
    """Get or create Brain orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = BrainOrchestrator()
        await _orchestrator.initialize()
    return _orchestrator

# =============================================================================
# INTEGRATION NOTES
# =============================================================================
"""
INTEGRATION WITH BLOODWORK ENGINE:
1. BloodworkCanonical arrives with markers, safety gates, blocked/caution lists
2. Brain orchestrator receives canonical data
3. Detects deficiencies from markers
4. Loads catalog from wiring endpoint (cached for 5 minutes)
5. Scores modules against deficiencies + goals + lifecycle
6. Enforces safety constraints (blocked ingredients)
7. Returns ranked recommendations

CATALOG LOADING (v1.1.0):
- Uses /api/v1/catalog/wiring/products endpoint
- Maps ingredient_tags to target_biomarkers via INGREDIENT_BIOMARKER_MAP
- Caches catalog in memory for 5 minutes (CATALOG_CACHE_TTL_SECONDS)
- Falls back to stale cache on errors

USAGE:
    orchestrator = await get_orchestrator()
    result = await orchestrator.run(
        submission_id="sub_123",
        user_id="user_456",
        markers=[{"code": "vitamin_d_25oh", "value": 18, "unit": "ng/mL"}],
        blocked_ingredients=["ashwagandha"],
        caution_ingredients=["niacin"],
        gender="male",
        lifecycle_phase=None,
        goals=["energy", "immunity"],
        confidence_score=0.95
    )

SAFETY RULES:
- "Blood does not negotiate" - blocked ingredients are absolute
- Modules containing blocked ingredients are never recommended
- Caution ingredients reduce module scores but don't block
- Lifecycle requirements (pregnancy) can add required ingredients
- User exclusions are treated as blocks

SCORING ALGORITHM:
- Base score from evidence tier (TIER_1: 30, TIER_2: 20)
- Biomarker match: +10 per match * severity * priority
- Goal alignment: +15 per matching goal
- Lifecycle bonus: +15-25 for recommended/required
- Confidence multiplier: 0.5 + (confidence * 0.5)
- Caution penalty: 0.8x final score
"""
