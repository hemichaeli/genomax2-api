"""
GenoMAX² Protocol Builder
=========================
Builds the final personalized supplement protocol with SKU selection.

Transforms recommendations into actionable protocols with:
- SKU routing based on gender (MAXimo²/MAXima²)
- Dosage calculations with constraint adjustments
- Daily schedule optimization
- Fulfillment-ready output for Supliful

Version: 1.0.0
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, time
from pydantic import BaseModel, Field
from enum import Enum
import uuid

# =============================================================================
# MODELS
# =============================================================================

class ProductLine(str, Enum):
    MAXIMO = "MAXimo²"    # Male-optimized
    MAXIMA = "MAXima²"    # Female-optimized
    UNIVERSAL = "Universal"

class DoseTime(str, Enum):
    MORNING = "morning"
    AFTERNOON = "afternoon"
    EVENING = "evening"
    BEDTIME = "bedtime"
    WITH_MEAL = "with_meal"
    EMPTY_STOMACH = "empty_stomach"

class ProtocolItem(BaseModel):
    """Single item in a protocol."""
    item_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    sku_id: str
    sku_name: str
    module_id: str
    product_line: ProductLine
    
    # Dosage
    dosage_amount: float
    dosage_unit: str
    servings_per_day: int
    dose_times: List[DoseTime]
    
    # Instructions
    instructions: str
    warnings: List[str] = []
    
    # Adjustments from constraints
    dosage_adjusted: bool = False
    adjustment_reason: Optional[str] = None
    original_dosage: Optional[float] = None

class DailySchedule(BaseModel):
    """Daily supplement schedule."""
    morning: List[ProtocolItem] = []
    afternoon: List[ProtocolItem] = []
    evening: List[ProtocolItem] = []
    bedtime: List[ProtocolItem] = []

class Protocol(BaseModel):
    """Complete personalized supplement protocol."""
    protocol_id: str = Field(default_factory=lambda: f"PROT-{uuid.uuid4().hex[:12].upper()}")
    user_id: str
    submission_id: str
    
    # Product line
    product_line: ProductLine
    gender: str
    
    # Items
    items: List[ProtocolItem]
    daily_schedule: DailySchedule
    
    # Totals
    total_items: int
    total_daily_servings: int
    
    # Fulfillment
    supliful_order: Dict[str, Any]
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    valid_until: Optional[datetime] = None
    
    # Safety summary
    warnings: List[str] = []
    contraindications: List[str] = []

# =============================================================================
# SKU CATALOG (Production pulls from database)
# =============================================================================

SKU_CATALOG = {
    # Iron Support
    "iron_support": {
        "MAXimo²": {
            "sku_id": "MXO-IRON-001",
            "sku_name": "MAXimo² Iron Support",
            "default_dosage": 18,
            "dosage_unit": "mg",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING],
            "instructions": "Take with vitamin C for better absorption. Avoid with calcium.",
            "supliful_sku": "GMXM-IRON-60",
        },
        "MAXima²": {
            "sku_id": "MXA-IRON-001",
            "sku_name": "MAXima² Iron Support",
            "default_dosage": 25,  # Higher for women
            "dosage_unit": "mg",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING],
            "instructions": "Take with vitamin C for better absorption. Avoid with calcium.",
            "supliful_sku": "GMXF-IRON-60",
        },
    },
    
    # Vitamin D3 + K2
    "vitamin_d3_k2": {
        "MAXimo²": {
            "sku_id": "MXO-D3K2-001",
            "sku_name": "MAXimo² Vitamin D3+K2",
            "default_dosage": 5000,
            "dosage_unit": "IU",
            "servings_per_bottle": 90,
            "dose_times": [DoseTime.MORNING, DoseTime.WITH_MEAL],
            "instructions": "Take with a meal containing fat for optimal absorption.",
            "supliful_sku": "GMXM-D3K2-90",
        },
        "MAXima²": {
            "sku_id": "MXA-D3K2-001",
            "sku_name": "MAXima² Vitamin D3+K2",
            "default_dosage": 4000,
            "dosage_unit": "IU",
            "servings_per_bottle": 90,
            "dose_times": [DoseTime.MORNING, DoseTime.WITH_MEAL],
            "instructions": "Take with a meal containing fat for optimal absorption.",
            "supliful_sku": "GMXF-D3K2-90",
        },
    },
    
    # Methylated B-Complex
    "methylated_b_complex": {
        "Universal": {
            "sku_id": "GMX-BCOMP-001",
            "sku_name": "GenoMAX² Methylated B-Complex",
            "default_dosage": 1,
            "dosage_unit": "capsule",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.WITH_MEAL],
            "instructions": "Take in the morning with food. May cause bright yellow urine (normal).",
            "supliful_sku": "GMX-BCOMP-60",
        },
    },
    
    # Omega-3
    "omega3_premium": {
        "MAXimo²": {
            "sku_id": "MXO-OM3-001",
            "sku_name": "MAXimo² Omega-3 Premium",
            "default_dosage": 2000,
            "dosage_unit": "mg",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.EVENING, DoseTime.WITH_MEAL],
            "instructions": "Take with meals. Store in refrigerator after opening.",
            "supliful_sku": "GMXM-OM3-60",
        },
        "MAXima²": {
            "sku_id": "MXA-OM3-001",
            "sku_name": "MAXima² Omega-3 Premium",
            "default_dosage": 2000,
            "dosage_unit": "mg",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.EVENING, DoseTime.WITH_MEAL],
            "instructions": "Take with meals. Store in refrigerator after opening.",
            "supliful_sku": "GMXF-OM3-60",
        },
    },
    
    # Magnesium Complex
    "magnesium_complex": {
        "Universal": {
            "sku_id": "GMX-MAG-001",
            "sku_name": "GenoMAX² Magnesium Complex",
            "default_dosage": 400,
            "dosage_unit": "mg",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.EVENING, DoseTime.BEDTIME],
            "instructions": "Take in the evening. May promote relaxation and sleep.",
            "supliful_sku": "GMX-MAG-60",
        },
    },
    
    # Zinc + Copper
    "zinc_copper_balance": {
        "Universal": {
            "sku_id": "GMX-ZNCU-001",
            "sku_name": "GenoMAX² Zinc + Copper",
            "default_dosage": 30,
            "dosage_unit": "mg",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.EVENING, DoseTime.WITH_MEAL],
            "instructions": "Take with dinner. Avoid taking with iron supplements.",
            "supliful_sku": "GMX-ZNCU-60",
        },
    },
    
    # Anti-Inflammatory
    "anti_inflammatory_support": {
        "Universal": {
            "sku_id": "GMX-ANTI-001",
            "sku_name": "GenoMAX² Anti-Inflammatory",
            "default_dosage": 1,
            "dosage_unit": "capsule",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.EVENING, DoseTime.WITH_MEAL],
            "instructions": "Take with meals. Contains curcumin with enhanced absorption.",
            "supliful_sku": "GMX-ANTI-60",
        },
    },
    
    # Metabolic Support
    "metabolic_support": {
        "Universal": {
            "sku_id": "GMX-META-001",
            "sku_name": "GenoMAX² Metabolic Support",
            "default_dosage": 1,
            "dosage_unit": "capsule",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.WITH_MEAL],
            "instructions": "Take with breakfast. Monitor blood sugar if diabetic.",
            "warnings": ["Consult physician if taking diabetes medication"],
            "supliful_sku": "GMX-META-60",
        },
    },
    
    # Prenatal Complete
    "prenatal_complete": {
        "MAXima²": {
            "sku_id": "MXA-PREN-001",
            "sku_name": "MAXima² Prenatal Complete",
            "default_dosage": 2,
            "dosage_unit": "capsules",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.EVENING, DoseTime.WITH_MEAL],
            "instructions": "Take 1 capsule morning and evening with food.",
            "supliful_sku": "GMXF-PREN-60",
        },
    },
    
    # Menopause Support
    "menopause_support": {
        "MAXima²": {
            "sku_id": "MXA-MENO-001",
            "sku_name": "MAXima² Menopause Support",
            "default_dosage": 1,
            "dosage_unit": "capsule",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.WITH_MEAL],
            "instructions": "Take with breakfast. Contains calcium and vitamin D.",
            "supliful_sku": "GMXF-MENO-60",
        },
    },
    
    # Men's Vitality
    "mens_vitality": {
        "MAXimo²": {
            "sku_id": "MXO-VITA-001",
            "sku_name": "MAXimo² Men's Vitality",
            "default_dosage": 1,
            "dosage_unit": "capsule",
            "servings_per_bottle": 60,
            "dose_times": [DoseTime.MORNING, DoseTime.WITH_MEAL],
            "instructions": "Take with breakfast.",
            "supliful_sku": "GMXM-VITA-60",
        },
    },
}

# =============================================================================
# PROTOCOL BUILDER
# =============================================================================

class ProtocolBuilder:
    """Builds personalized supplement protocols from recommendations."""
    
    def __init__(self, sku_catalog: Dict = None):
        self.sku_catalog = sku_catalog or SKU_CATALOG
    
    def build_protocol(
        self,
        recommendations: List[Dict[str, Any]],
        constraint_set: Dict[str, Any],
        user_id: str,
        submission_id: str,
        gender: str
    ) -> Protocol:
        """
        Build complete protocol from recommendations.
        
        Args:
            recommendations: List of ModuleRecommendation dicts
            constraint_set: ConstraintSet dict with dosage modifiers
            user_id: User ID
            submission_id: Bloodwork submission ID
            gender: male/female
        
        Returns:
            Complete Protocol ready for fulfillment
        """
        product_line = ProductLine.MAXIMO if gender == "male" else ProductLine.MAXIMA
        dosage_modifiers = constraint_set.get("dosage_modifiers", {})
        caution_ingredients = set(constraint_set.get("caution_ingredients", []))
        
        items = []
        all_warnings = []
        all_contraindications = []
        
        for rec in recommendations:
            module_id = rec.get("module_id")
            
            if module_id not in self.sku_catalog:
                continue
            
            # Get SKU for product line
            sku_config = self._get_sku_for_product_line(module_id, product_line)
            if not sku_config:
                continue
            
            # Calculate dosage with adjustments
            dosage, adjusted, adjustment_reason, original = self._calculate_dosage(
                module_id=module_id,
                sku_config=sku_config,
                rec_dosage_adj=rec.get("dosage_adjustments", {}),
                constraint_dosage_mod=dosage_modifiers
            )
            
            # Build warnings
            item_warnings = list(sku_config.get("warnings", []))
            
            # Add warnings for caution ingredients
            key_ingredients = rec.get("key_ingredients", [])
            caution_in_module = set(key_ingredients) & caution_ingredients
            if caution_in_module:
                item_warnings.append(f"Contains ingredients requiring caution: {list(caution_in_module)}")
            
            # Add contraindications
            contras = rec.get("contraindications", [])
            if contras:
                all_contraindications.extend(contras)
            
            item = ProtocolItem(
                sku_id=sku_config["sku_id"],
                sku_name=sku_config["sku_name"],
                module_id=module_id,
                product_line=product_line if sku_config.get("product_line") != "Universal" else ProductLine.UNIVERSAL,
                dosage_amount=dosage,
                dosage_unit=sku_config["dosage_unit"],
                servings_per_day=len([t for t in sku_config["dose_times"] if t not in [DoseTime.WITH_MEAL, DoseTime.EMPTY_STOMACH]]) or 1,
                dose_times=sku_config["dose_times"],
                instructions=sku_config["instructions"],
                warnings=item_warnings,
                dosage_adjusted=adjusted,
                adjustment_reason=adjustment_reason,
                original_dosage=original
            )
            
            items.append(item)
            all_warnings.extend(item_warnings)
        
        # Build daily schedule
        schedule = self._build_daily_schedule(items)
        
        # Calculate totals
        total_servings = sum(item.servings_per_day for item in items)
        
        # Build Supliful order
        supliful_order = self._build_supliful_order(items, user_id)
        
        return Protocol(
            user_id=user_id,
            submission_id=submission_id,
            product_line=product_line,
            gender=gender,
            items=items,
            daily_schedule=schedule,
            total_items=len(items),
            total_daily_servings=total_servings,
            supliful_order=supliful_order,
            warnings=list(set(all_warnings)),
            contraindications=list(set(all_contraindications))
        )
    
    def _get_sku_for_product_line(
        self, 
        module_id: str, 
        product_line: ProductLine
    ) -> Optional[Dict]:
        """Get SKU config for product line, falling back to Universal."""
        if module_id not in self.sku_catalog:
            return None
        
        module_skus = self.sku_catalog[module_id]
        
        # Try product line first
        if product_line.value in module_skus:
            config = module_skus[product_line.value].copy()
            config["product_line"] = product_line.value
            return config
        
        # Fall back to Universal
        if "Universal" in module_skus:
            config = module_skus["Universal"].copy()
            config["product_line"] = "Universal"
            return config
        
        return None
    
    def _calculate_dosage(
        self,
        module_id: str,
        sku_config: Dict,
        rec_dosage_adj: Dict[str, float],
        constraint_dosage_mod: Dict[str, float]
    ) -> Tuple[float, bool, Optional[str], Optional[float]]:
        """
        Calculate final dosage with all adjustments.
        
        Returns:
            Tuple of (final_dosage, was_adjusted, adjustment_reason, original_dosage)
        """
        base_dosage = sku_config["default_dosage"]
        final_dosage = base_dosage
        adjusted = False
        reasons = []
        
        # Apply recommendation-level adjustments (from engine)
        for ingredient, modifier in rec_dosage_adj.items():
            if modifier != 1.0:
                final_dosage *= modifier
                adjusted = True
                if modifier > 1.0:
                    reasons.append(f"Increased for {ingredient} deficiency")
                else:
                    reasons.append(f"Reduced due to elevated {ingredient}")
        
        # Apply constraint-level modifiers (from bloodwork)
        # These are already applied at recommendation level, but double-check
        
        reason = "; ".join(reasons) if reasons else None
        original = base_dosage if adjusted else None
        
        return round(final_dosage, 1), adjusted, reason, original
    
    def _build_daily_schedule(self, items: List[ProtocolItem]) -> DailySchedule:
        """Organize items into daily schedule."""
        schedule = DailySchedule()
        
        for item in items:
            for dose_time in item.dose_times:
                if dose_time == DoseTime.MORNING:
                    schedule.morning.append(item)
                elif dose_time == DoseTime.AFTERNOON:
                    schedule.afternoon.append(item)
                elif dose_time == DoseTime.EVENING:
                    schedule.evening.append(item)
                elif dose_time == DoseTime.BEDTIME:
                    schedule.bedtime.append(item)
                # WITH_MEAL and EMPTY_STOMACH are modifiers, not times
        
        return schedule
    
    def _build_supliful_order(
        self, 
        items: List[ProtocolItem], 
        user_id: str
    ) -> Dict[str, Any]:
        """Build Supliful-ready order payload."""
        line_items = []
        
        for item in items:
            sku_config = None
            for module_skus in self.sku_catalog.values():
                for pl_config in module_skus.values():
                    if pl_config.get("sku_id") == item.sku_id:
                        sku_config = pl_config
                        break
            
            if sku_config:
                line_items.append({
                    "sku": sku_config.get("supliful_sku", item.sku_id),
                    "quantity": 1,  # One bottle per protocol
                    "metadata": {
                        "module_id": item.module_id,
                        "dosage_adjusted": item.dosage_adjusted,
                        "adjustment_reason": item.adjustment_reason
                    }
                })
        
        return {
            "customer_id": user_id,
            "line_items": line_items,
            "shipping_method": "standard",
            "metadata": {
                "source": "genomax2_brain",
                "generated_at": datetime.utcnow().isoformat()
            }
        }


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_protocol_builder(
    custom_sku_catalog: Dict = None
) -> ProtocolBuilder:
    """Factory function to create protocol builder."""
    return ProtocolBuilder(custom_sku_catalog)


def build_protocol(
    recommendations: List[Dict[str, Any]],
    constraint_set: Dict[str, Any],
    user_id: str,
    submission_id: str,
    gender: str
) -> Protocol:
    """
    Convenience function for protocol building.
    
    Args:
        recommendations: ModuleRecommendation list
        constraint_set: ConstraintSet dict
        user_id: User ID
        submission_id: Submission ID
        gender: male/female
    
    Returns:
        Complete Protocol
    """
    builder = create_protocol_builder()
    return builder.build_protocol(
        recommendations=recommendations,
        constraint_set=constraint_set,
        user_id=user_id,
        submission_id=submission_id,
        gender=gender
    )
