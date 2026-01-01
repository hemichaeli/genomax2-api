"""
GenoMAX² Product Intake System - Supliful Supplier Integration

Fetches and parses product data from Supliful.

Currently uses MOCK data until Supliful API integration is complete.

Version: intake_system_v1
"""

import re
from typing import Optional, Dict, Any, List
from .models import (
    ParsedPayload,
    ParsedIngredient,
    ValidationFlag,
    ValidationFlags,
)


# =============================================
# Mock Data (Replace with real API calls)
# =============================================

MOCK_PRODUCTS = {
    "omega-3-fish-oil": {
        "product_name": "Omega-3 Fish Oil 1000mg",
        "ingredients": [
            {"name": "Fish Oil", "amount": "1000", "unit": "mg"},
            {"name": "EPA (Eicosapentaenoic Acid)", "amount": "180", "unit": "mg"},
            {"name": "DHA (Docosahexaenoic Acid)", "amount": "120", "unit": "mg"},
            {"name": "Vitamin E", "amount": "10", "unit": "IU"},
        ],
        "serving_size": "1 softgel",
        "servings_per_container": 90,
        "category": "Heart Health",
        "base_price": 12.50,
        "shipping_restrictions": ["heat-sensitive"],
        "raw_text": "Fish Oil (from Anchovy, Sardine, Mackerel) 1000 mg, EPA 180 mg, DHA 120 mg, Vitamin E (as d-Alpha Tocopherol) 10 IU",
    },
    "magnesium-glycinate": {
        "product_name": "Magnesium Glycinate 400mg",
        "ingredients": [
            {"name": "Magnesium (as Magnesium Glycinate)", "amount": "400", "unit": "mg", "dv": 95},
        ],
        "serving_size": "2 capsules",
        "servings_per_container": 60,
        "category": "Minerals",
        "base_price": 18.00,
        "shipping_restrictions": [],
        "raw_text": "Magnesium (as Magnesium Glycinate) 400 mg (95% DV)",
    },
    "lions-mane": {
        "product_name": "Lion's Mane Mushroom 500mg",
        "ingredients": [
            {"name": "Lion's Mane Mushroom (Hericium erinaceus) Fruiting Body Extract", "amount": "500", "unit": "mg"},
            {"name": "Beta-Glucans", "amount": "30", "unit": "%"},
        ],
        "serving_size": "1 capsule",
        "servings_per_container": 60,
        "category": "Cognitive Support",
        "base_price": 24.00,
        "shipping_restrictions": [],
        "raw_text": "Lion's Mane Mushroom (Hericium erinaceus) Fruiting Body Extract 500 mg (standardized to 30% Beta-Glucans)",
    },
    "probiotic-50b": {
        "product_name": "Probiotic 50 Billion CFU",
        "ingredients": [
            {"name": "Lactobacillus acidophilus", "amount": "15", "unit": "billion CFU"},
            {"name": "Bifidobacterium lactis", "amount": "15", "unit": "billion CFU"},
            {"name": "Lactobacillus rhamnosus", "amount": "10", "unit": "billion CFU"},
            {"name": "Lactobacillus plantarum", "amount": "10", "unit": "billion CFU"},
        ],
        "serving_size": "1 capsule",
        "servings_per_container": 30,
        "category": "Digestive Health",
        "base_price": 28.00,
        "shipping_restrictions": ["refrigeration-required"],
        "raw_text": "Probiotic Blend 50 Billion CFU: L. acidophilus, B. lactis, L. rhamnosus, L. plantarum",
    },
}

# Canonical ingredient name mapping
INGREDIENT_CANONICAL_MAP = {
    "fish oil": "fish-oil",
    "epa": "omega-3-epa",
    "eicosapentaenoic acid": "omega-3-epa",
    "dha": "omega-3-dha",
    "docosahexaenoic acid": "omega-3-dha",
    "vitamin e": "vitamin-e",
    "d-alpha tocopherol": "vitamin-e",
    "magnesium": "magnesium",
    "magnesium glycinate": "magnesium-glycinate",
    "lion's mane": "lion-s-mane-hericium-erinaceus",
    "lion's mane mushroom": "lion-s-mane-hericium-erinaceus",
    "hericium erinaceus": "lion-s-mane-hericium-erinaceus",
    "beta-glucans": "beta-glucans",
    "lactobacillus acidophilus": "probiotics-multi-strain",
    "bifidobacterium lactis": "probiotics-multi-strain",
    "lactobacillus rhamnosus": "probiotics-multi-strain",
    "lactobacillus plantarum": "probiotics-multi-strain",
}

# Ingredient tier classification (from research database)
INGREDIENT_TIER_MAP = {
    "fish-oil": "TIER_1",
    "omega-3-epa": "TIER_1",
    "omega-3-dha": "TIER_1",
    "vitamin-e": "TIER_1",
    "magnesium": "TIER_1",
    "magnesium-glycinate": "TIER_1",
    "lion-s-mane-hericium-erinaceus": "TIER_2",
    "beta-glucans": "TIER_2",
    "probiotics-multi-strain": "TIER_1",
}


# =============================================
# URL Parsing
# =============================================

def extract_product_slug_from_url(url: str) -> Optional[str]:
    """
    Extract product slug from Supliful URL.
    
    Example: https://supliful.com/products/omega-3-fish-oil -> omega-3-fish-oil
    """
    # Handle various URL formats
    patterns = [
        r'/products/([^/?#]+)',
        r'/product/([^/?#]+)',
        r'supliful\.com/([^/?#]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


# =============================================
# Supplier API (Mock Implementation)
# =============================================

async def fetch_product_from_supliful(product_url: str) -> Dict[str, Any]:
    """
    Fetch product data from Supliful.
    
    TODO: Replace with actual Supliful API call.
    Currently returns mock data based on URL slug.
    """
    slug = extract_product_slug_from_url(product_url)
    
    if not slug:
        return {"error": "Could not extract product slug from URL"}
    
    # Check mock data
    if slug in MOCK_PRODUCTS:
        return {
            "success": True,
            "source": "mock",
            "data": MOCK_PRODUCTS[slug],
        }
    
    # Return placeholder for unknown products
    return {
        "success": False,
        "error": f"Product not found: {slug}",
        "source": "mock",
    }


def normalize_ingredient(raw_name: str) -> Optional[str]:
    """
    Normalize ingredient name to canonical form.
    """
    name_lower = raw_name.lower().strip()
    
    # Remove parenthetical content
    name_clean = re.sub(r'\([^)]*\)', '', name_lower).strip()
    
    # Direct match
    if name_clean in INGREDIENT_CANONICAL_MAP:
        return INGREDIENT_CANONICAL_MAP[name_clean]
    
    # Partial match
    for key, canonical in INGREDIENT_CANONICAL_MAP.items():
        if key in name_clean or name_clean in key:
            return canonical
    
    return None


def get_ingredient_tier(canonical_name: Optional[str]) -> Optional[str]:
    """Get tier classification for ingredient."""
    if not canonical_name:
        return None
    return INGREDIENT_TIER_MAP.get(canonical_name)


# =============================================
# Parsing
# =============================================

def parse_supplier_payload(supplier_data: Dict[str, Any]) -> ParsedPayload:
    """
    Parse raw supplier data into normalized structure.
    """
    data = supplier_data.get("data", supplier_data)
    
    # Parse ingredients
    ingredients = []
    raw_ingredients = data.get("ingredients", [])
    
    for raw_ing in raw_ingredients:
        name = raw_ing.get("name", "")
        canonical = normalize_ingredient(name)
        tier = get_ingredient_tier(canonical)
        
        parsed_ing = ParsedIngredient(
            raw_name=name,
            canonical_name=canonical,
            amount=raw_ing.get("amount"),
            unit=raw_ing.get("unit"),
            daily_value_percent=raw_ing.get("dv"),
            tier_classification=tier,
        )
        ingredients.append(parsed_ing)
    
    return ParsedPayload(
        product_name=data.get("product_name", "Unknown Product"),
        ingredients=ingredients,
        serving_size=data.get("serving_size"),
        servings_per_container=data.get("servings_per_container"),
        category=data.get("category"),
        base_price=data.get("base_price"),
        shipping_restrictions=data.get("shipping_restrictions", []),
        raw_ingredients_text=data.get("raw_text"),
    )


# =============================================
# Validation
# =============================================

def validate_supplier_data(
    supplier_payload: Dict[str, Any],
    parsed: ParsedPayload,
) -> ValidationFlags:
    """
    Validate supplier data for intake.
    
    Returns warnings and blockers.
    """
    warnings = []
    blockers = []
    
    # Check for success
    if not supplier_payload.get("success", False):
        blockers.append(ValidationFlag(
            code="SUPPLIER_FETCH_FAILED",
            severity="blocker",
            message=f"Failed to fetch product: {supplier_payload.get('error', 'Unknown error')}",
        ))
        return ValidationFlags(warnings=warnings, blockers=blockers, is_valid=False)
    
    # Check for empty ingredients
    if not parsed.ingredients:
        blockers.append(ValidationFlag(
            code="NO_INGREDIENTS",
            severity="blocker",
            message="No ingredients found in product data",
            field="ingredients",
        ))
    
    # Check for unmatched ingredients
    unmatched = [ing for ing in parsed.ingredients if not ing.canonical_name]
    if unmatched:
        for ing in unmatched:
            warnings.append(ValidationFlag(
                code="UNKNOWN_INGREDIENT",
                severity="warning",
                message=f"Unknown ingredient: {ing.raw_name}",
                field="ingredients",
            ))
    
    # Check for missing price
    if parsed.base_price is None:
        warnings.append(ValidationFlag(
            code="MISSING_PRICE",
            severity="warning",
            message="No base price found",
            field="base_price",
        ))
    
    # Check for missing category
    if not parsed.category:
        warnings.append(ValidationFlag(
            code="MISSING_CATEGORY",
            severity="warning",
            message="No category found",
            field="category",
        ))
    
    # Check for TIER_3 or hepatotoxic ingredients (blockers)
    hepatotoxic = ["ashwagandha", "kava", "germander", "chaparral"]
    for ing in parsed.ingredients:
        if ing.tier_classification == "TIER_3":
            blockers.append(ValidationFlag(
                code="TIER_3_INGREDIENT",
                severity="blocker",
                message=f"TIER_3 ingredient not allowed: {ing.raw_name}",
                field="ingredients",
            ))
        
        if ing.canonical_name and any(h in ing.canonical_name.lower() for h in hepatotoxic):
            blockers.append(ValidationFlag(
                code="HEPATOTOXIC_INGREDIENT",
                severity="blocker",
                message=f"Hepatotoxic ingredient blocked: {ing.raw_name}",
                field="ingredients",
            ))
    
    is_valid = len(blockers) == 0
    
    return ValidationFlags(
        warnings=warnings,
        blockers=blockers,
        is_valid=is_valid,
    )
