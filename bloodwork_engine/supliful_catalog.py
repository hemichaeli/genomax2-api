"""
GenoMAX² Supliful Catalog Integration
=====================================

Integrates with Supliful's 185-product inventory for supplement fulfillment.
Implements append-only governance for catalog entries (immutable once created).

Core Principle: "Blood does not negotiate" - routing constraints override product selection.

Version: 1.1.0

CHANGELOG v1.1.0:
- Removed ProductLine.UNIVERSAL (eliminated by migration 016)
- Split former universal products into MAXimo²/MAXima² versions
- Updated get_products_for_sex() to only return gender-specific products
- All products must now be explicitly MAXimo² or MAXima²
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from enum import Enum
from datetime import datetime
import json
import hashlib


class ProductLine(Enum):
    """
    Gender-optimized product lines (canonical os_environment values).
    
    Post-migration 016: UNIVERSAL no longer exists. All products must be
    explicitly assigned to MAXimo² (male) or MAXima² (female). Former universal
    products were split into separate SKUs with -M/-F suffixes.
    """
    MAXIMO2 = "MAXimo²"  # Male biology
    MAXIMA2 = "MAXima²"  # Female biology


class ProductCategory(Enum):
    """Supplement categories"""
    VITAMIN = "vitamin"
    MINERAL = "mineral"
    OMEGA = "omega"
    AMINO_ACID = "amino_acid"
    BOTANICAL = "botanical"
    PROBIOTIC = "probiotic"
    ENZYME = "enzyme"
    HORMONE_SUPPORT = "hormone_support"
    METHYLATION = "methylation"
    CARDIOVASCULAR = "cardiovascular"
    METABOLIC = "metabolic"
    LIVER_SUPPORT = "liver_support"
    THYROID = "thyroid"
    ADRENAL = "adrenal"
    SLEEP = "sleep"
    COGNITIVE = "cognitive"
    MULTI = "multi"


class IngredientTier(Enum):
    """Evidence-based tier classification"""
    TIER_1 = "tier_1"  # Strong evidence: ≥20 RCTs, >2000 participants, validated biomarkers
    TIER_2 = "tier_2"  # Moderate evidence: 5-19 RCTs, contextual use
    TIER_3 = "tier_3"  # Insufficient evidence or safety concerns - REJECTED


@dataclass
class Ingredient:
    """Individual ingredient with evidence tier and safety profile"""
    code: str
    name: str
    tier: IngredientTier
    canonical_unit: str
    min_dose: float
    max_dose: float
    optimal_dose: float
    biomarkers_affected: List[str] = field(default_factory=list)
    contraindications: List[str] = field(default_factory=list)
    drug_interactions: List[str] = field(default_factory=list)
    safety_notes: Optional[str] = None


@dataclass
class ProductIngredient:
    """Ingredient as it appears in a specific product"""
    ingredient_code: str
    amount: float
    unit: str
    form: Optional[str] = None  # e.g., "methylcobalamin", "cyanocobalamin"


@dataclass
class SuplifulProduct:
    """Supliful catalog product with full metadata"""
    sku: str
    supliful_id: str
    name: str
    product_line: ProductLine
    category: ProductCategory
    ingredients: List[ProductIngredient]
    serving_size: str
    servings_per_container: int
    price_usd: float
    wholesale_price_usd: float
    description: str
    
    # Safety routing
    blocked_by_gates: List[str] = field(default_factory=list)
    caution_with_gates: List[str] = field(default_factory=list)
    requires_biomarkers: List[str] = field(default_factory=list)
    recommended_for_flags: List[str] = field(default_factory=list)
    
    # Catalog governance
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    version: int = 1
    active: bool = True
    checksum: Optional[str] = None
    
    def __post_init__(self):
        """Calculate checksum for integrity verification"""
        if not self.checksum:
            content = f"{self.sku}:{self.supliful_id}:{self.name}:{self.version}"
            self.checksum = hashlib.sha256(content.encode()).hexdigest()[:16]


class CatalogGovernance:
    """
    Append-only governance for catalog entries.
    Existing entries are immutable - changes create new versions.
    """
    
    def __init__(self):
        self.audit_log: List[Dict[str, Any]] = []
    
    def log_action(self, action: str, sku: str, details: Dict[str, Any]):
        """Log catalog action for audit trail"""
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "sku": sku,
            "details": details
        }
        self.audit_log.append(entry)
        return entry
    
    def can_modify(self, product: SuplifulProduct) -> bool:
        """Check if product can be modified (only if version 0 or draft)"""
        # Append-only: published products cannot be modified
        return product.version == 0
    
    def create_new_version(self, product: SuplifulProduct, changes: Dict[str, Any]) -> SuplifulProduct:
        """Create new version of product with changes (append-only)"""
        # Create copy with incremented version
        new_product = SuplifulProduct(
            sku=f"{product.sku}_v{product.version + 1}",
            supliful_id=product.supliful_id,
            name=product.name,
            product_line=product.product_line,
            category=product.category,
            ingredients=product.ingredients,
            serving_size=product.serving_size,
            servings_per_container=product.servings_per_container,
            price_usd=product.price_usd,
            wholesale_price_usd=product.wholesale_price_usd,
            description=product.description,
            blocked_by_gates=product.blocked_by_gates,
            caution_with_gates=product.caution_with_gates,
            requires_biomarkers=product.requires_biomarkers,
            recommended_for_flags=product.recommended_for_flags,
            version=product.version + 1
        )
        
        # Apply changes
        for key, value in changes.items():
            if hasattr(new_product, key):
                setattr(new_product, key, value)
        
        self.log_action("VERSION_CREATED", new_product.sku, {
            "previous_version": product.version,
            "new_version": new_product.version,
            "changes": list(changes.keys())
        })
        
        return new_product


class SuplifulCatalogManager:
    """
    Manages the Supliful product catalog with safety gate integration.
    Implements deterministic product selection based on biomarker routing.
    """
    
    def __init__(self):
        self.products: Dict[str, SuplifulProduct] = {}
        self.ingredients: Dict[str, Ingredient] = {}
        self.governance = CatalogGovernance()
        
        # Index for fast lookups
        self._by_category: Dict[ProductCategory, List[str]] = {}
        self._by_product_line: Dict[ProductLine, List[str]] = {}
        self._by_ingredient: Dict[str, List[str]] = {}
        self._by_gate_flag: Dict[str, List[str]] = {}
        
        # Load seed catalog
        self._load_seed_catalog()
    
    def _load_seed_catalog(self):
        """Load the GenoMAX² curated Supliful catalog"""
        self._load_core_ingredients()
        self._load_core_products()
    
    def _load_core_ingredients(self):
        """Load evidence-based ingredient database"""
        core_ingredients = [
            # TIER 1 - Strong Evidence
            Ingredient("vitamin_d3", "Vitamin D3 (Cholecalciferol)", IngredientTier.TIER_1, "IU", 1000, 10000, 5000,
                      ["vitamin_d_25oh"], ["hypercalcemia", "sarcoidosis"], [],
                      "Monitor calcium levels at doses >5000 IU"),
            Ingredient("omega3_epa_dha", "Omega-3 (EPA/DHA)", IngredientTier.TIER_1, "mg", 500, 4000, 2000,
                      ["omega3_index", "triglycerides"], ["bleeding_disorders"], ["blood_thinners"],
                      "Reduce dose if omega-3 index >8%"),
            Ingredient("magnesium_glycinate", "Magnesium Glycinate", IngredientTier.TIER_1, "mg", 100, 600, 400,
                      ["magnesium_serum"], ["renal_impairment"], [],
                      "Best absorbed form, less GI distress"),
            Ingredient("methylcobalamin", "Vitamin B12 (Methylcobalamin)", IngredientTier.TIER_1, "mcg", 500, 5000, 1000,
                      ["vitamin_b12"], [], [],
                      "Active form, no conversion needed"),
            Ingredient("methylfolate", "L-Methylfolate (5-MTHF)", IngredientTier.TIER_1, "mcg", 400, 15000, 800,
                      ["folate_serum", "homocysteine"], [], [],
                      "Required for MTHFR variants"),
            Ingredient("iron_bisglycinate", "Iron Bisglycinate", IngredientTier.TIER_1, "mg", 18, 65, 25,
                      ["ferritin", "hemoglobin"], ["hemochromatosis", "elevated_ferritin"], [],
                      "BLOCKED when ferritin >300 (male) or >200 (female)"),
            Ingredient("zinc_picolinate", "Zinc Picolinate", IngredientTier.TIER_1, "mg", 15, 50, 30,
                      ["zinc_serum"], ["copper_deficiency"], [],
                      "Monitor zinc:copper ratio"),
            Ingredient("selenium", "Selenium (Selenomethionine)", IngredientTier.TIER_1, "mcg", 55, 400, 200,
                      ["tsh", "free_t3", "free_t4"], [], [],
                      "Essential for thyroid conversion"),
            Ingredient("coq10_ubiquinol", "CoQ10 (Ubiquinol)", IngredientTier.TIER_1, "mg", 50, 400, 200,
                      ["ldl_cholesterol"], [], ["statins"],
                      "Ubiquinol is active form; depleted by statins"),
            
            # TIER 2 - Moderate Evidence
            Ingredient("berberine", "Berberine HCl", IngredientTier.TIER_2, "mg", 500, 1500, 1000,
                      ["fasting_glucose", "fasting_insulin", "hba1c", "ldl_cholesterol"], 
                      ["pregnancy", "breastfeeding"], ["metformin", "cyclosporine"],
                      "Flag for insulin resistance support"),
            Ingredient("alpha_lipoic_acid", "Alpha Lipoic Acid", IngredientTier.TIER_2, "mg", 100, 600, 300,
                      ["fasting_glucose", "ggt"], [], [],
                      "Antioxidant, supports glucose metabolism"),
            Ingredient("nac", "N-Acetyl Cysteine", IngredientTier.TIER_2, "mg", 600, 1800, 1200,
                      ["ggt", "homocysteine"], [], [],
                      "Glutathione precursor, liver support"),
            Ingredient("chromium_picolinate", "Chromium Picolinate", IngredientTier.TIER_2, "mcg", 200, 1000, 500,
                      ["fasting_glucose", "fasting_insulin"], [], [],
                      "Supports insulin sensitivity"),
            Ingredient("p5p", "Vitamin B6 (P5P)", IngredientTier.TIER_2, "mg", 10, 100, 50,
                      ["homocysteine"], [], [],
                      "Active form, supports methylation"),
            Ingredient("dim", "DIM (Diindolylmethane)", IngredientTier.TIER_2, "mg", 100, 300, 200,
                      ["estradiol"], [], [],
                      "Supports estrogen metabolism"),
            Ingredient("tongkat_ali", "Tongkat Ali (Eurycoma)", IngredientTier.TIER_2, "mg", 200, 600, 400,
                      ["total_testosterone", "free_testosterone"], [], [],
                      "Male hormone support"),
            Ingredient("vitex", "Vitex (Chaste Tree)", IngredientTier.TIER_2, "mg", 150, 400, 300,
                      ["progesterone", "estradiol"], ["pregnancy", "hormonal_bc"], [],
                      "Female hormone balance"),
            Ingredient("rhodiola", "Rhodiola Rosea", IngredientTier.TIER_2, "mg", 200, 600, 400,
                      ["cortisol_am"], [], [],
                      "Adaptogen, adrenal support"),
            Ingredient("phosphatidylserine", "Phosphatidylserine", IngredientTier.TIER_2, "mg", 100, 400, 200,
                      ["cortisol_am"], [], [],
                      "Cortisol modulation"),
            Ingredient("inositol", "Myo-Inositol", IngredientTier.TIER_2, "mg", 2000, 4000, 3000,
                      ["fasting_insulin", "fasting_glucose"], [], [],
                      "PCOS and insulin support"),
            
            # TIER 3 - REJECTED (Safety concerns or insufficient evidence)
            Ingredient("ashwagandha", "Ashwagandha", IngredientTier.TIER_3, "mg", 0, 0, 0,
                      [], [], [],
                      "REJECTED: Hepatotoxicity concerns - multiple case reports of liver injury"),
            Ingredient("kava", "Kava", IngredientTier.TIER_3, "mg", 0, 0, 0,
                      [], [], [],
                      "REJECTED: FDA warning for severe liver damage"),
            Ingredient("green_tea_extract_egcg", "Green Tea Extract (EGCG)", IngredientTier.TIER_3, "mg", 0, 0, 0,
                      [], [], [],
                      "REJECTED: Hepatotoxicity at high doses"),
        ]
        
        for ing in core_ingredients:
            self.ingredients[ing.code] = ing
    
    def _load_core_products(self):
        """Load GenoMAX² curated Supliful product catalog"""
        products = [
            # === MAXIMO² LINE (Male Biology) ===
            SuplifulProduct(
                sku="GMAX-M-VD5K",
                supliful_id="SUP-VD3-5000",
                name="MAXimo² Vitamin D3 5000 IU",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.VITAMIN,
                ingredients=[ProductIngredient("vitamin_d3", 5000, "IU")],
                serving_size="1 softgel",
                servings_per_container=120,
                price_usd=24.99,
                wholesale_price_usd=12.50,
                description="High-potency vitamin D3 for optimal male hormone and immune function",
                caution_with_gates=["GATE_002"],  # Vitamin D caution with hypercalcemia
                requires_biomarkers=["vitamin_d_25oh", "calcium_serum"],
                recommended_for_flags=["vitamin_d_insufficient", "vitamin_d_deficient"]
            ),
            SuplifulProduct(
                sku="GMAX-M-O3-2000",
                supliful_id="SUP-O3-2000",
                name="MAXimo² Omega-3 2000mg EPA/DHA",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.OMEGA,
                ingredients=[
                    ProductIngredient("omega3_epa_dha", 2000, "mg", "triglyceride form"),
                ],
                serving_size="2 softgels",
                servings_per_container=60,
                price_usd=34.99,
                wholesale_price_usd=17.50,
                description="Pharmaceutical-grade omega-3 for cardiovascular and cognitive support",
                caution_with_gates=["GATE_014", "GATE_020"],  # Coagulation, high TG
                requires_biomarkers=["omega3_index"],
                recommended_for_flags=["FLAG_OMEGA3_PRIORITY"]
            ),
            SuplifulProduct(
                sku="GMAX-M-MG400",
                supliful_id="SUP-MG-400",
                name="MAXimo² Magnesium Glycinate 400mg",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.MINERAL,
                ingredients=[ProductIngredient("magnesium_glycinate", 400, "mg")],
                serving_size="2 capsules",
                servings_per_container=60,
                price_usd=22.99,
                wholesale_price_usd=11.50,
                description="Highly bioavailable magnesium for muscle, sleep, and stress support",
                requires_biomarkers=["magnesium_serum"],
                recommended_for_flags=["magnesium_deficiency"]
            ),
            SuplifulProduct(
                sku="GMAX-M-METH-B",
                supliful_id="SUP-MB12-5000",
                name="MAXimo² Methylation Support",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.METHYLATION,
                ingredients=[
                    ProductIngredient("methylcobalamin", 5000, "mcg"),
                    ProductIngredient("methylfolate", 800, "mcg"),
                    ProductIngredient("p5p", 50, "mg"),
                ],
                serving_size="1 capsule",
                servings_per_container=60,
                price_usd=29.99,
                wholesale_price_usd=15.00,
                description="Complete methylation support with active B vitamins",
                requires_biomarkers=["vitamin_b12", "folate_serum", "homocysteine"],
                recommended_for_flags=["FLAG_B12_DEFICIENCY", "FLAG_METHYLATION_SUPPORT", "FLAG_METHYLFOLATE_REQUIRED"]
            ),
            SuplifulProduct(
                sku="GMAX-M-TEST",
                supliful_id="SUP-TEST-M",
                name="MAXimo² Testosterone Support",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.HORMONE_SUPPORT,
                ingredients=[
                    ProductIngredient("zinc_picolinate", 30, "mg"),
                    ProductIngredient("vitamin_d3", 3000, "IU"),
                    ProductIngredient("tongkat_ali", 400, "mg"),
                ],
                serving_size="2 capsules",
                servings_per_container=30,
                price_usd=39.99,
                wholesale_price_usd=20.00,
                description="Natural testosterone optimization for male vitality",
                requires_biomarkers=["total_testosterone", "free_testosterone", "zinc_serum"],
                recommended_for_flags=["FLAG_TESTOSTERONE_SUPPORT"]
            ),
            SuplifulProduct(
                sku="GMAX-M-METAB",
                supliful_id="SUP-METAB-M",
                name="MAXimo² Metabolic Support",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.METABOLIC,
                ingredients=[
                    ProductIngredient("berberine", 1000, "mg"),
                    ProductIngredient("chromium_picolinate", 500, "mcg"),
                    ProductIngredient("alpha_lipoic_acid", 300, "mg"),
                ],
                serving_size="2 capsules",
                servings_per_container=30,
                price_usd=44.99,
                wholesale_price_usd=22.50,
                description="Comprehensive metabolic and glucose support",
                requires_biomarkers=["fasting_glucose", "fasting_insulin", "hba1c"],
                recommended_for_flags=["FLAG_INSULIN_SUPPORT"]
            ),
            SuplifulProduct(
                sku="GMAX-M-CARDIO",
                supliful_id="SUP-CARDIO-M",
                name="MAXimo² Cardiovascular Support",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.CARDIOVASCULAR,
                ingredients=[
                    ProductIngredient("coq10_ubiquinol", 200, "mg"),
                    ProductIngredient("omega3_epa_dha", 1000, "mg"),
                    ProductIngredient("nac", 600, "mg"),
                ],
                serving_size="2 softgels",
                servings_per_container=30,
                price_usd=49.99,
                wholesale_price_usd=25.00,
                description="Heart health support with CoQ10 and omega-3",
                requires_biomarkers=["ldl_cholesterol", "apolipoprotein_b", "lp_a"],
                recommended_for_flags=["FLAG_CARDIOVASCULAR_SUPPORT", "FLAG_LPA_ELEVATED"]
            ),
            SuplifulProduct(
                sku="GMAX-M-IRON",
                supliful_id="SUP-IRON-M",
                name="MAXimo² Iron Complex",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.MINERAL,
                ingredients=[
                    ProductIngredient("iron_bisglycinate", 25, "mg"),
                    ProductIngredient("vitamin_d3", 1000, "IU"),
                ],
                serving_size="1 capsule",
                servings_per_container=60,
                price_usd=19.99,
                wholesale_price_usd=10.00,
                description="Gentle, highly absorbable iron with vitamin C",
                blocked_by_gates=["GATE_001"],  # Iron block
                requires_biomarkers=["ferritin", "hemoglobin"],
                recommended_for_flags=["FLAG_IRON_DEFICIENCY_ANEMIA"]
            ),
            SuplifulProduct(
                sku="GMAX-M-THYROID",
                supliful_id="SUP-THYROID-M",
                name="MAXimo² Thyroid Support",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.THYROID,
                ingredients=[
                    ProductIngredient("selenium", 200, "mcg"),
                    ProductIngredient("zinc_picolinate", 15, "mg"),
                ],
                serving_size="1 capsule",
                servings_per_container=60,
                price_usd=24.99,
                wholesale_price_usd=12.50,
                description="Essential nutrients for healthy thyroid function",
                blocked_by_gates=["GATE_008"],  # Iodine block if hyperthyroid
                requires_biomarkers=["tsh", "free_t3", "free_t4"],
                recommended_for_flags=["FLAG_THYROID_SUPPORT", "FLAG_T4_T3_CONVERSION_SUPPORT"]
            ),
            SuplifulProduct(
                sku="GMAX-M-LIVER",
                supliful_id="SUP-LIVER-M",
                name="MAXimo² Liver Detox",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.LIVER_SUPPORT,
                ingredients=[
                    ProductIngredient("nac", 1200, "mg"),
                    ProductIngredient("alpha_lipoic_acid", 300, "mg"),
                ],
                serving_size="2 capsules",
                servings_per_container=30,
                price_usd=34.99,
                wholesale_price_usd=17.50,
                description="Glutathione support and liver detoxification",
                requires_biomarkers=["alt", "ast", "ggt"],
                recommended_for_flags=["FLAG_OXIDATIVE_STRESS"]
            ),
            SuplifulProduct(
                sku="GMAX-M-ADRENAL",
                supliful_id="SUP-ADRENAL-M",
                name="MAXimo² Adrenal Support",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.ADRENAL,
                ingredients=[
                    ProductIngredient("rhodiola", 400, "mg"),
                    ProductIngredient("phosphatidylserine", 200, "mg"),
                ],
                serving_size="2 capsules",
                servings_per_container=30,
                price_usd=39.99,
                wholesale_price_usd=20.00,
                description="Adaptogenic support for stress and cortisol balance",
                requires_biomarkers=["cortisol_am", "dhea_s"],
                recommended_for_flags=["FLAG_CORTISOL_HIGH", "FLAG_ADRENAL_SUPPORT"]
            ),
            # MAXimo² split from former Universal products
            SuplifulProduct(
                sku="GMAX-M-PROBIOTIC",
                supliful_id="SUP-PROBIOTIC-M",
                name="MAXimo² Probiotic 50B",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.PROBIOTIC,
                ingredients=[],  # Probiotic strains not in ingredient DB
                serving_size="1 capsule",
                servings_per_container=30,
                price_usd=34.99,
                wholesale_price_usd=17.50,
                description="50 billion CFU multi-strain probiotic for male gut health",
                requires_biomarkers=[],
                recommended_for_flags=[]
            ),
            SuplifulProduct(
                sku="GMAX-M-COQ10",
                supliful_id="SUP-COQ10-200-M",
                name="MAXimo² CoQ10 Ubiquinol 200mg",
                product_line=ProductLine.MAXIMO2,
                category=ProductCategory.CARDIOVASCULAR,
                ingredients=[ProductIngredient("coq10_ubiquinol", 200, "mg")],
                serving_size="1 softgel",
                servings_per_container=60,
                price_usd=44.99,
                wholesale_price_usd=22.50,
                description="Active ubiquinol form of CoQ10 for energy and heart health",
                requires_biomarkers=["ldl_cholesterol"],
                recommended_for_flags=["FLAG_CARDIOVASCULAR_SUPPORT"]
            ),
            
            # === MAXIMA² LINE (Female Biology) ===
            SuplifulProduct(
                sku="GMAX-F-VD5K",
                supliful_id="SUP-VD3-5000-F",
                name="MAXima² Vitamin D3 5000 IU",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.VITAMIN,
                ingredients=[ProductIngredient("vitamin_d3", 5000, "IU")],
                serving_size="1 softgel",
                servings_per_container=120,
                price_usd=24.99,
                wholesale_price_usd=12.50,
                description="High-potency vitamin D3 optimized for female health",
                caution_with_gates=["GATE_002"],
                requires_biomarkers=["vitamin_d_25oh", "calcium_serum"],
                recommended_for_flags=["vitamin_d_insufficient", "vitamin_d_deficient"]
            ),
            SuplifulProduct(
                sku="GMAX-F-O3-2000",
                supliful_id="SUP-O3-2000-F",
                name="MAXima² Omega-3 2000mg EPA/DHA",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.OMEGA,
                ingredients=[
                    ProductIngredient("omega3_epa_dha", 2000, "mg", "triglyceride form"),
                ],
                serving_size="2 softgels",
                servings_per_container=60,
                price_usd=34.99,
                wholesale_price_usd=17.50,
                description="Pharmaceutical-grade omega-3 for female cardiovascular health",
                caution_with_gates=["GATE_014", "GATE_020"],
                requires_biomarkers=["omega3_index"],
                recommended_for_flags=["FLAG_OMEGA3_PRIORITY"]
            ),
            SuplifulProduct(
                sku="GMAX-F-MG400",
                supliful_id="SUP-MG-400-F",
                name="MAXima² Magnesium Glycinate 400mg",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.MINERAL,
                ingredients=[ProductIngredient("magnesium_glycinate", 400, "mg")],
                serving_size="2 capsules",
                servings_per_container=60,
                price_usd=22.99,
                wholesale_price_usd=11.50,
                description="Highly bioavailable magnesium for female wellness",
                requires_biomarkers=["magnesium_serum"],
                recommended_for_flags=["magnesium_deficiency"]
            ),
            SuplifulProduct(
                sku="GMAX-F-METH-B",
                supliful_id="SUP-MB12-5000-F",
                name="MAXima² Methylation Support",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.METHYLATION,
                ingredients=[
                    ProductIngredient("methylcobalamin", 5000, "mcg"),
                    ProductIngredient("methylfolate", 800, "mcg"),
                    ProductIngredient("p5p", 50, "mg"),
                ],
                serving_size="1 capsule",
                servings_per_container=60,
                price_usd=29.99,
                wholesale_price_usd=15.00,
                description="Complete methylation support with active B vitamins",
                requires_biomarkers=["vitamin_b12", "folate_serum", "homocysteine"],
                recommended_for_flags=["FLAG_B12_DEFICIENCY", "FLAG_METHYLATION_SUPPORT", "FLAG_METHYLFOLATE_REQUIRED"]
            ),
            SuplifulProduct(
                sku="GMAX-F-HORMONE",
                supliful_id="SUP-HORMONE-F",
                name="MAXima² Hormone Balance",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.HORMONE_SUPPORT,
                ingredients=[
                    ProductIngredient("dim", 200, "mg"),
                    ProductIngredient("vitex", 300, "mg"),
                ],
                serving_size="2 capsules",
                servings_per_container=30,
                price_usd=39.99,
                wholesale_price_usd=20.00,
                description="Natural support for female hormone balance",
                requires_biomarkers=["estradiol", "progesterone"],
                recommended_for_flags=["FLAG_ESTROGEN_DOMINANCE"]
            ),
            SuplifulProduct(
                sku="GMAX-F-IRON",
                supliful_id="SUP-IRON-F",
                name="MAXima² Iron Complex",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.MINERAL,
                ingredients=[
                    ProductIngredient("iron_bisglycinate", 25, "mg"),
                    ProductIngredient("vitamin_d3", 1000, "IU"),
                ],
                serving_size="1 capsule",
                servings_per_container=60,
                price_usd=19.99,
                wholesale_price_usd=10.00,
                description="Gentle, highly absorbable iron for women",
                blocked_by_gates=["GATE_001"],
                requires_biomarkers=["ferritin", "hemoglobin"],
                recommended_for_flags=["FLAG_IRON_DEFICIENCY_ANEMIA"]
            ),
            SuplifulProduct(
                sku="GMAX-F-METAB",
                supliful_id="SUP-METAB-F",
                name="MAXima² Metabolic Support",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.METABOLIC,
                ingredients=[
                    ProductIngredient("berberine", 1000, "mg"),
                    ProductIngredient("inositol", 2000, "mg"),
                    ProductIngredient("chromium_picolinate", 500, "mcg"),
                ],
                serving_size="2 capsules",
                servings_per_container=30,
                price_usd=44.99,
                wholesale_price_usd=22.50,
                description="Comprehensive metabolic support with inositol",
                requires_biomarkers=["fasting_glucose", "fasting_insulin", "hba1c"],
                recommended_for_flags=["FLAG_INSULIN_SUPPORT"]
            ),
            SuplifulProduct(
                sku="GMAX-F-THYROID",
                supliful_id="SUP-THYROID-F",
                name="MAXima² Thyroid Support",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.THYROID,
                ingredients=[
                    ProductIngredient("selenium", 200, "mcg"),
                    ProductIngredient("zinc_picolinate", 15, "mg"),
                ],
                serving_size="1 capsule",
                servings_per_container=60,
                price_usd=24.99,
                wholesale_price_usd=12.50,
                description="Essential nutrients for healthy thyroid function",
                blocked_by_gates=["GATE_008"],
                requires_biomarkers=["tsh", "free_t3", "free_t4"],
                recommended_for_flags=["FLAG_THYROID_SUPPORT", "FLAG_T4_T3_CONVERSION_SUPPORT"]
            ),
            SuplifulProduct(
                sku="GMAX-F-ADRENAL",
                supliful_id="SUP-ADRENAL-F",
                name="MAXima² Adrenal Support",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.ADRENAL,
                ingredients=[
                    ProductIngredient("rhodiola", 400, "mg"),
                    ProductIngredient("phosphatidylserine", 200, "mg"),
                ],
                serving_size="2 capsules",
                servings_per_container=30,
                price_usd=39.99,
                wholesale_price_usd=20.00,
                description="Adaptogenic support for stress and cortisol balance",
                requires_biomarkers=["cortisol_am", "dhea_s"],
                recommended_for_flags=["FLAG_CORTISOL_HIGH", "FLAG_ADRENAL_SUPPORT"]
            ),
            # MAXima² split from former Universal products
            SuplifulProduct(
                sku="GMAX-F-PROBIOTIC",
                supliful_id="SUP-PROBIOTIC-F",
                name="MAXima² Probiotic 50B",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.PROBIOTIC,
                ingredients=[],  # Probiotic strains not in ingredient DB
                serving_size="1 capsule",
                servings_per_container=30,
                price_usd=34.99,
                wholesale_price_usd=17.50,
                description="50 billion CFU multi-strain probiotic for female gut health",
                requires_biomarkers=[],
                recommended_for_flags=[]
            ),
            SuplifulProduct(
                sku="GMAX-F-COQ10",
                supliful_id="SUP-COQ10-200-F",
                name="MAXima² CoQ10 Ubiquinol 200mg",
                product_line=ProductLine.MAXIMA2,
                category=ProductCategory.CARDIOVASCULAR,
                ingredients=[ProductIngredient("coq10_ubiquinol", 200, "mg")],
                serving_size="1 softgel",
                servings_per_container=60,
                price_usd=44.99,
                wholesale_price_usd=22.50,
                description="Active ubiquinol form of CoQ10 for energy and heart health",
                requires_biomarkers=["ldl_cholesterol"],
                recommended_for_flags=["FLAG_CARDIOVASCULAR_SUPPORT"]
            ),
        ]
        
        # Add all products and build indices
        for product in products:
            self.add_product(product)
    
    def add_product(self, product: SuplifulProduct) -> bool:
        """Add product to catalog (append-only)"""
        if product.sku in self.products:
            return False  # Cannot overwrite existing SKU
        
        self.products[product.sku] = product
        
        # Update indices
        if product.category not in self._by_category:
            self._by_category[product.category] = []
        self._by_category[product.category].append(product.sku)
        
        if product.product_line not in self._by_product_line:
            self._by_product_line[product.product_line] = []
        self._by_product_line[product.product_line].append(product.sku)
        
        for ing in product.ingredients:
            if ing.ingredient_code not in self._by_ingredient:
                self._by_ingredient[ing.ingredient_code] = []
            self._by_ingredient[ing.ingredient_code].append(product.sku)
        
        for flag in product.recommended_for_flags:
            if flag not in self._by_gate_flag:
                self._by_gate_flag[flag] = []
            self._by_gate_flag[flag].append(product.sku)
        
        self.governance.log_action("PRODUCT_ADDED", product.sku, {
            "name": product.name,
            "product_line": product.product_line.value,
            "category": product.category.value
        })
        
        return True
    
    def get_product(self, sku: str) -> Optional[SuplifulProduct]:
        """Get product by SKU"""
        return self.products.get(sku)
    
    def get_products_for_sex(self, sex: str) -> List[SuplifulProduct]:
        """
        Get products appropriate for given sex.
        
        Post-migration 016: Returns only the appropriate gender-specific product line.
        No UNIVERSAL products exist.
        
        Args:
            sex: "male" or "female"
            
        Returns:
            List of products for the specified sex
        """
        if sex.lower() == "male":
            line = ProductLine.MAXIMO2
        else:
            line = ProductLine.MAXIMA2
        
        skus = self._by_product_line.get(line, [])
        return [self.products[sku] for sku in skus]
    
    def get_products_for_flag(self, flag: str) -> List[SuplifulProduct]:
        """Get products recommended for a specific routing flag"""
        skus = self._by_gate_flag.get(flag, [])
        return [self.products[sku] for sku in skus]
    
    def check_product_safety(self, sku: str, active_gates: List[str]) -> Dict[str, Any]:
        """Check if product is safe given active safety gates"""
        product = self.get_product(sku)
        if not product:
            return {"safe": False, "error": "Product not found"}
        
        blocked = []
        cautions = []
        
        for gate in active_gates:
            if gate in product.blocked_by_gates:
                blocked.append(gate)
            if gate in product.caution_with_gates:
                cautions.append(gate)
        
        return {
            "safe": len(blocked) == 0,
            "sku": sku,
            "blocked_by": blocked,
            "cautions": cautions,
            "can_proceed_with_caution": len(blocked) == 0 and len(cautions) > 0
        }
    
    def recommend_products(
        self, 
        sex: str,
        routing_flags: List[str],
        active_gates: List[str],
        max_products: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Recommend products based on routing flags and safety gates.
        Deterministic: Same inputs = same outputs.
        """
        recommendations = []
        eligible_products = self.get_products_for_sex(sex)
        
        # Score products by flag matching
        scored_products = []
        for product in eligible_products:
            score = 0
            matched_flags = []
            
            for flag in routing_flags:
                if flag in product.recommended_for_flags:
                    score += 10
                    matched_flags.append(flag)
            
            if score > 0:
                safety = self.check_product_safety(product.sku, active_gates)
                if safety["safe"]:
                    scored_products.append({
                        "product": product,
                        "score": score,
                        "matched_flags": matched_flags,
                        "safety": safety
                    })
        
        # Sort by score (deterministic)
        scored_products.sort(key=lambda x: (-x["score"], x["product"].sku))
        
        # Return top recommendations
        for item in scored_products[:max_products]:
            product = item["product"]
            recommendations.append({
                "sku": product.sku,
                "name": product.name,
                "product_line": product.product_line.value,
                "category": product.category.value,
                "price_usd": product.price_usd,
                "matched_flags": item["matched_flags"],
                "safety_status": "safe",
                "ingredients": [
                    {
                        "code": ing.ingredient_code,
                        "amount": ing.amount,
                        "unit": ing.unit
                    }
                    for ing in product.ingredients
                ]
            })
        
        return recommendations
    
    def get_catalog_stats(self) -> Dict[str, Any]:
        """Get catalog statistics"""
        return {
            "total_products": len(self.products),
            "total_ingredients": len(self.ingredients),
            "by_product_line": {
                line.value: len(skus) 
                for line, skus in self._by_product_line.items()
            },
            "by_category": {
                cat.value: len(skus)
                for cat, skus in self._by_category.items()
            },
            "tier_1_ingredients": len([
                i for i in self.ingredients.values() 
                if i.tier == IngredientTier.TIER_1
            ]),
            "tier_2_ingredients": len([
                i for i in self.ingredients.values()
                if i.tier == IngredientTier.TIER_2
            ]),
            "tier_3_rejected": len([
                i for i in self.ingredients.values()
                if i.tier == IngredientTier.TIER_3
            ]),
            "governance": {
                "audit_entries": len(self.governance.audit_log),
                "append_only": True
            }
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Export catalog as dictionary"""
        return {
            "version": "1.1.0",
            "generated_at": datetime.utcnow().isoformat(),
            "stats": self.get_catalog_stats(),
            "products": [
                {
                    "sku": p.sku,
                    "supliful_id": p.supliful_id,
                    "name": p.name,
                    "product_line": p.product_line.value,
                    "category": p.category.value,
                    "price_usd": p.price_usd,
                    "wholesale_price_usd": p.wholesale_price_usd,
                    "ingredients": [
                        {"code": i.ingredient_code, "amount": i.amount, "unit": i.unit}
                        for i in p.ingredients
                    ],
                    "blocked_by_gates": p.blocked_by_gates,
                    "caution_with_gates": p.caution_with_gates,
                    "recommended_for_flags": p.recommended_for_flags,
                    "active": p.active
                }
                for p in self.products.values()
            ],
            "ingredients": [
                {
                    "code": i.code,
                    "name": i.name,
                    "tier": i.tier.value,
                    "optimal_dose": i.optimal_dose,
                    "unit": i.canonical_unit,
                    "biomarkers_affected": i.biomarkers_affected,
                    "safety_notes": i.safety_notes
                }
                for i in self.ingredients.values()
            ]
        }


# Singleton instance
_catalog_manager: Optional[SuplifulCatalogManager] = None

def get_catalog_manager() -> SuplifulCatalogManager:
    """Get or create the catalog manager singleton"""
    global _catalog_manager
    if _catalog_manager is None:
        _catalog_manager = SuplifulCatalogManager()
    return _catalog_manager
