"""
GenoMAX² Safety Routing Service
===============================
Connects BloodworkEngineV2 output to ingredient-level routing.
"Blood does not negotiate" - deterministic constraint enforcement.

This service:
1. Takes BloodworkResult from engine_v2
2. Queries ingredient_safety_flags from database
3. Returns filtered ingredient lists for Brain Resolver

Usage:
    from bloodwork_engine.safety_router import SafetyRouter
    
    router = SafetyRouter(db_session)
    routing = router.get_routing_constraints(bloodwork_result)
    
    # Use in product filtering
    allowed_products = router.filter_products(products, routing)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from enum import Enum
import json
import logging

# Configure logging
logger = logging.getLogger(__name__)


class FlagType(str, Enum):
    """Ingredient safety flag types."""
    BLOCKED = "BLOCKED"
    CAUTION = "CAUTION"
    RECOMMENDED = "RECOMMENDED"


@dataclass
class RoutingConstraints:
    """
    Container for all routing constraints derived from bloodwork.
    Used by Brain Resolver to filter and prioritize products.
    """
    # Hard blocks - these ingredients MUST NOT be included
    blocked_ingredients: Set[str] = field(default_factory=set)
    
    # Cautions - these ingredients should be flagged, reduced dose, or require acknowledgment
    caution_ingredients: Dict[str, str] = field(default_factory=dict)  # ingredient -> rationale
    
    # Recommendations - these ingredients should be prioritized
    recommended_ingredients: Dict[str, str] = field(default_factory=dict)  # ingredient -> rationale
    
    # Active gate codes for audit trail
    active_gates: List[str] = field(default_factory=list)
    
    # Constraint codes for reference
    active_constraints: Set[str] = field(default_factory=set)
    
    # Engine metadata
    engine_version: str = ""
    ruleset_version: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for API responses."""
        return {
            "blocked_ingredients": list(self.blocked_ingredients),
            "caution_ingredients": self.caution_ingredients,
            "recommended_ingredients": self.recommended_ingredients,
            "active_gates": self.active_gates,
            "active_constraints": list(self.active_constraints),
            "engine_version": self.engine_version,
            "ruleset_version": self.ruleset_version,
            "summary": {
                "total_blocked": len(self.blocked_ingredients),
                "total_cautions": len(self.caution_ingredients),
                "total_recommended": len(self.recommended_ingredients),
                "total_gates": len(self.active_gates)
            }
        }
    
    def is_ingredient_blocked(self, ingredient_code: str) -> bool:
        """Check if an ingredient is blocked."""
        return ingredient_code.lower() in {i.lower() for i in self.blocked_ingredients}
    
    def is_ingredient_cautioned(self, ingredient_code: str) -> bool:
        """Check if an ingredient requires caution."""
        return ingredient_code.lower() in {i.lower() for i in self.caution_ingredients.keys()}
    
    def is_ingredient_recommended(self, ingredient_code: str) -> bool:
        """Check if an ingredient is recommended."""
        return ingredient_code.lower() in {i.lower() for i in self.recommended_ingredients.keys()}


@dataclass 
class FilteredProduct:
    """Product with routing metadata applied."""
    product_id: str
    product_name: str
    is_allowed: bool
    blocked_ingredients: List[str] = field(default_factory=list)
    caution_ingredients: List[str] = field(default_factory=list)
    recommended_ingredients: List[str] = field(default_factory=list)
    block_reasons: List[str] = field(default_factory=list)
    caution_reasons: List[str] = field(default_factory=list)
    recommendation_reasons: List[str] = field(default_factory=list)
    priority_score: float = 0.0


class SafetyRouter:
    """
    Routes bloodwork constraints to ingredient-level filtering.
    
    This is the bridge between BloodworkEngineV2 output and the 
    Brain Resolver's product selection logic.
    """
    
    def __init__(self, db_session=None, ingredient_flags: Optional[Dict] = None):
        """
        Initialize SafetyRouter.
        
        Args:
            db_session: SQLAlchemy session for database queries
            ingredient_flags: Optional pre-loaded flags dict (for testing/caching)
        """
        self.db = db_session
        self._cached_flags: Optional[Dict[str, List[Dict]]] = ingredient_flags
        
    def _load_ingredient_flags(self) -> Dict[str, List[Dict]]:
        """
        Load ingredient safety flags from database.
        Returns dict keyed by constraint_code -> list of ingredient records.
        """
        if self._cached_flags is not None:
            return self._cached_flags
            
        if self.db is None:
            logger.warning("No database session provided, returning empty flags")
            return {}
            
        try:
            # Query all ingredient safety flags
            query = """
                SELECT 
                    ingredient_code,
                    ingredient_name,
                    constraint_code,
                    flag_type,
                    threshold_mg,
                    per_serving,
                    rationale
                FROM ingredient_safety_flags
                ORDER BY constraint_code, flag_type
            """
            result = self.db.execute(query)
            rows = result.fetchall()
            
            # Group by constraint_code
            flags_by_constraint: Dict[str, List[Dict]] = {}
            for row in rows:
                constraint = row['constraint_code']
                if constraint not in flags_by_constraint:
                    flags_by_constraint[constraint] = []
                flags_by_constraint[constraint].append({
                    'ingredient_code': row['ingredient_code'],
                    'ingredient_name': row['ingredient_name'],
                    'flag_type': row['flag_type'],
                    'threshold_mg': row['threshold_mg'],
                    'per_serving': row['per_serving'],
                    'rationale': row['rationale']
                })
                
            self._cached_flags = flags_by_constraint
            logger.info(f"Loaded {len(rows)} ingredient safety flags for {len(flags_by_constraint)} constraints")
            return flags_by_constraint
            
        except Exception as e:
            logger.error(f"Failed to load ingredient flags: {e}")
            return {}
    
    def get_routing_constraints(self, bloodwork_result: Any) -> RoutingConstraints:
        """
        Extract routing constraints from BloodworkResult.
        
        Args:
            bloodwork_result: BloodworkResult from engine_v2.process_markers()
            
        Returns:
            RoutingConstraints with blocked/caution/recommended ingredients
        """
        constraints = RoutingConstraints()
        
        # Extract metadata
        if hasattr(bloodwork_result, 'engine_version'):
            constraints.engine_version = bloodwork_result.engine_version
        if hasattr(bloodwork_result, 'ruleset_version'):
            constraints.ruleset_version = bloodwork_result.ruleset_version
            
        # Get active gates from result
        active_gates = []
        active_constraint_codes = set()
        
        if hasattr(bloodwork_result, 'safety_gates'):
            for gate in bloodwork_result.safety_gates:
                if gate.get('status') == 'ACTIVE' or gate.get('triggered', False):
                    gate_code = gate.get('gate_code', gate.get('code', ''))
                    constraint_code = gate.get('constraint_code', gate.get('constraint', ''))
                    
                    if gate_code:
                        active_gates.append(gate_code)
                    if constraint_code:
                        active_constraint_codes.add(constraint_code)
                        
        constraints.active_gates = active_gates
        constraints.active_constraints = active_constraint_codes
        
        # Load ingredient mappings
        flags = self._load_ingredient_flags()
        
        # Map constraints to ingredients
        for constraint_code in active_constraint_codes:
            if constraint_code not in flags:
                logger.debug(f"No ingredient mappings for constraint: {constraint_code}")
                continue
                
            for flag in flags[constraint_code]:
                ingredient = flag['ingredient_code']
                rationale = flag['rationale'] or f"Triggered by {constraint_code}"
                
                if flag['flag_type'] == FlagType.BLOCKED.value:
                    constraints.blocked_ingredients.add(ingredient)
                    
                elif flag['flag_type'] == FlagType.CAUTION.value:
                    constraints.caution_ingredients[ingredient] = rationale
                    
                elif flag['flag_type'] == FlagType.RECOMMENDED.value:
                    constraints.recommended_ingredients[ingredient] = rationale
                    
        logger.info(
            f"Generated routing constraints: "
            f"{len(constraints.blocked_ingredients)} blocked, "
            f"{len(constraints.caution_ingredients)} caution, "
            f"{len(constraints.recommended_ingredients)} recommended"
        )
        
        return constraints
    
    def filter_products(
        self, 
        products: List[Dict], 
        constraints: RoutingConstraints,
        ingredient_key: str = 'ingredients'
    ) -> List[FilteredProduct]:
        """
        Filter and annotate products based on routing constraints.
        
        Args:
            products: List of product dicts with ingredient lists
            constraints: RoutingConstraints from get_routing_constraints()
            ingredient_key: Key in product dict containing ingredient list
            
        Returns:
            List of FilteredProduct with routing metadata
        """
        results = []
        
        for product in products:
            product_id = product.get('id', product.get('product_id', ''))
            product_name = product.get('name', product.get('product_name', ''))
            ingredients = product.get(ingredient_key, [])
            
            # Normalize ingredients to codes
            if isinstance(ingredients, str):
                ingredients = [i.strip() for i in ingredients.split(',')]
            ingredient_codes = [
                i.get('code', i) if isinstance(i, dict) else str(i).lower().replace(' ', '_')
                for i in ingredients
            ]
            
            # Check for blocked ingredients
            blocked = []
            block_reasons = []
            for code in ingredient_codes:
                if constraints.is_ingredient_blocked(code):
                    blocked.append(code)
                    block_reasons.append(f"{code}: Safety gate block active")
                    
            # Check for caution ingredients
            cautioned = []
            caution_reasons = []
            for code in ingredient_codes:
                if constraints.is_ingredient_cautioned(code):
                    cautioned.append(code)
                    reason = constraints.caution_ingredients.get(code, 'Caution advised')
                    caution_reasons.append(f"{code}: {reason}")
                    
            # Check for recommended ingredients
            recommended = []
            rec_reasons = []
            for code in ingredient_codes:
                if constraints.is_ingredient_recommended(code):
                    recommended.append(code)
                    reason = constraints.recommended_ingredients.get(code, 'Recommended')
                    rec_reasons.append(f"{code}: {reason}")
                    
            # Calculate priority score
            # Higher = better match for user's needs
            priority = len(recommended) * 10 - len(cautioned) * 2
            
            # Determine if product is allowed (no blocked ingredients)
            is_allowed = len(blocked) == 0
            
            results.append(FilteredProduct(
                product_id=product_id,
                product_name=product_name,
                is_allowed=is_allowed,
                blocked_ingredients=blocked,
                caution_ingredients=cautioned,
                recommended_ingredients=recommended,
                block_reasons=block_reasons,
                caution_reasons=caution_reasons,
                recommendation_reasons=rec_reasons,
                priority_score=priority
            ))
            
        # Sort by: allowed first, then by priority score
        results.sort(key=lambda p: (not p.is_allowed, -p.priority_score))
        
        logger.info(
            f"Filtered {len(products)} products: "
            f"{sum(1 for p in results if p.is_allowed)} allowed, "
            f"{sum(1 for p in results if not p.is_allowed)} blocked"
        )
        
        return results
    
    def get_blocked_for_user(self, user_id: str) -> Set[str]:
        """
        Get all blocked ingredients for a user based on their latest bloodwork.
        
        Args:
            user_id: User identifier
            
        Returns:
            Set of blocked ingredient codes
        """
        if self.db is None:
            return set()
            
        try:
            query = """
                SELECT blocked_ingredients
                FROM bloodwork_user_results
                WHERE user_id = :user_id
                ORDER BY test_date DESC
                LIMIT 1
            """
            result = self.db.execute(query, {'user_id': user_id})
            row = result.fetchone()
            
            if row and row['blocked_ingredients']:
                return set(row['blocked_ingredients'])
            return set()
            
        except Exception as e:
            logger.error(f"Failed to get blocked ingredients for user {user_id}: {e}")
            return set()


# ============================================
# Standalone Routing Functions (for API use)
# ============================================

def get_static_ingredient_flags() -> Dict[str, List[Dict]]:
    """
    Returns static ingredient safety flags for environments without database.
    Mirrors the seed data from migration 013.
    """
    return {
        "BLOCK_IRON": [
            {"ingredient_code": "iron_bisglycinate", "flag_type": "BLOCKED", "rationale": "Iron contraindicated with ferritin overload"},
            {"ingredient_code": "iron_glycinate", "flag_type": "BLOCKED", "rationale": "Iron contraindicated with ferritin overload"},
            {"ingredient_code": "ferrous_sulfate", "flag_type": "BLOCKED", "rationale": "Iron contraindicated with ferritin overload"},
            {"ingredient_code": "ferrous_fumarate", "flag_type": "BLOCKED", "rationale": "Iron contraindicated with ferritin overload"},
            {"ingredient_code": "ferrous_gluconate", "flag_type": "BLOCKED", "rationale": "Iron contraindicated with ferritin overload"},
            {"ingredient_code": "carbonyl_iron", "flag_type": "BLOCKED", "rationale": "Iron contraindicated with ferritin overload"},
            {"ingredient_code": "heme_iron", "flag_type": "BLOCKED", "rationale": "Iron contraindicated with ferritin overload"},
        ],
        "BLOCK_POTASSIUM": [
            {"ingredient_code": "potassium_citrate", "flag_type": "BLOCKED", "rationale": "Potassium contraindicated with hyperkalemia"},
            {"ingredient_code": "potassium_chloride", "flag_type": "BLOCKED", "rationale": "Potassium contraindicated with hyperkalemia"},
            {"ingredient_code": "potassium_gluconate", "flag_type": "BLOCKED", "rationale": "Potassium contraindicated with hyperkalemia"},
        ],
        "BLOCK_IODINE": [
            {"ingredient_code": "iodine", "flag_type": "BLOCKED", "rationale": "Iodine contraindicated with hyperthyroidism"},
            {"ingredient_code": "potassium_iodide", "flag_type": "BLOCKED", "rationale": "Iodine contraindicated with hyperthyroidism"},
            {"ingredient_code": "kelp", "flag_type": "BLOCKED", "rationale": "High iodine content"},
            {"ingredient_code": "bladderwrack", "flag_type": "BLOCKED", "rationale": "High iodine content"},
        ],
        "CAUTION_HEPATOTOXIC": [
            {"ingredient_code": "ashwagandha", "flag_type": "BLOCKED", "rationale": "Documented hepatotoxicity - PERMANENTLY BLOCKED"},
            {"ingredient_code": "kava", "flag_type": "CAUTION", "rationale": "Hepatotoxicity risk with elevated liver enzymes"},
            {"ingredient_code": "green_tea_extract", "flag_type": "CAUTION", "rationale": "High-dose EGCG hepatotoxicity"},
            {"ingredient_code": "black_cohosh", "flag_type": "CAUTION", "rationale": "Rare hepatotoxicity reports"},
        ],
        "CAUTION_BLOOD_THINNING": [
            {"ingredient_code": "fish_oil", "flag_type": "CAUTION", "rationale": "Mild antiplatelet effect"},
            {"ingredient_code": "omega3", "flag_type": "CAUTION", "rationale": "Mild antiplatelet effect"},
            {"ingredient_code": "vitamin_e", "flag_type": "CAUTION", "rationale": "May inhibit platelet aggregation"},
            {"ingredient_code": "ginkgo_biloba", "flag_type": "CAUTION", "rationale": "Antiplatelet properties"},
            {"ingredient_code": "nattokinase", "flag_type": "CAUTION", "rationale": "Fibrinolytic activity"},
        ],
        "FLAG_METHYLFOLATE_REQUIRED": [
            {"ingredient_code": "folic_acid", "flag_type": "BLOCKED", "rationale": "MTHFR variants require methylfolate"},
            {"ingredient_code": "methylfolate", "flag_type": "RECOMMENDED", "rationale": "Active folate for MTHFR variants"},
        ],
        "FLAG_B12_DEFICIENCY": [
            {"ingredient_code": "methylcobalamin", "flag_type": "RECOMMENDED", "rationale": "Active B12 for deficiency"},
        ],
        "FLAG_METHYLATION_SUPPORT": [
            {"ingredient_code": "methylcobalamin", "flag_type": "RECOMMENDED", "rationale": "Supports methylation"},
            {"ingredient_code": "methylfolate", "flag_type": "RECOMMENDED", "rationale": "Supports methylation"},
            {"ingredient_code": "pyridoxal_5_phosphate", "flag_type": "RECOMMENDED", "rationale": "Active B6 for homocysteine"},
        ],
        "FLAG_THYROID_SUPPORT": [
            {"ingredient_code": "selenium", "flag_type": "RECOMMENDED", "rationale": "Supports thyroid function"},
            {"ingredient_code": "zinc", "flag_type": "RECOMMENDED", "rationale": "Supports thyroid hormone synthesis"},
        ],
        "FLAG_OMEGA3_PRIORITY": [
            {"ingredient_code": "epa_dha", "flag_type": "RECOMMENDED", "rationale": "Priority for omega-3 deficiency"},
            {"ingredient_code": "fish_oil", "flag_type": "RECOMMENDED", "rationale": "Priority for omega-3 deficiency"},
        ],
        "FLAG_INSULIN_SUPPORT": [
            {"ingredient_code": "berberine", "flag_type": "RECOMMENDED", "rationale": "Supports insulin sensitivity"},
            {"ingredient_code": "chromium", "flag_type": "RECOMMENDED", "rationale": "Supports glucose metabolism"},
            {"ingredient_code": "alpha_lipoic_acid", "flag_type": "RECOMMENDED", "rationale": "Insulin-sensitizing antioxidant"},
        ],
        "FLAG_OXIDATIVE_STRESS": [
            {"ingredient_code": "nac", "flag_type": "RECOMMENDED", "rationale": "Glutathione precursor"},
            {"ingredient_code": "glutathione", "flag_type": "RECOMMENDED", "rationale": "Master antioxidant"},
        ],
    }


def create_static_router() -> SafetyRouter:
    """Create a SafetyRouter with static flags (no database required)."""
    return SafetyRouter(db_session=None, ingredient_flags=get_static_ingredient_flags())
