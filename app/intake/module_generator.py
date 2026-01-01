"""
GenoMAX² Product Intake System - Module Generator

Deterministic generation of:
- module_code (unique identifier)
- os_layer assignment
- biological_domain classification
- ingredient_tags normalization

RULES:
- BOTH os_environment -> generates TWO modules (M + F)
- module_code format: {DOMAIN}-{NAME}-{ENV}-{SEQ}
- Existing modules are IMMUTABLE - any change requires new module_code

Version: intake_system_v1
"""

import re
import hashlib
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime
from .models import (
    DraftModule,
    ParsedPayload,
    ParsedIngredient,
    OSEnvironment,
    OSLayer,
    BiologicalDomain,
)


# =============================================
# Domain Classification Rules
# =============================================

INGREDIENT_DOMAIN_MAP = {
    # Cardiovascular
    "omega-3": BiologicalDomain.CARDIOVASCULAR,
    "omega-3-epa": BiologicalDomain.CARDIOVASCULAR,
    "omega-3-dha": BiologicalDomain.CARDIOVASCULAR,
    "coq10": BiologicalDomain.CARDIOVASCULAR,
    "ubiquinol": BiologicalDomain.CARDIOVASCULAR,
    "fish-oil": BiologicalDomain.CARDIOVASCULAR,
    "krill-oil": BiologicalDomain.CARDIOVASCULAR,
    
    # Neurological
    "lion-s-mane": BiologicalDomain.NEUROLOGICAL,
    "lions-mane": BiologicalDomain.NEUROLOGICAL,
    "gaba": BiologicalDomain.NEUROLOGICAL,
    "l-theanine": BiologicalDomain.NEUROLOGICAL,
    "bacopa": BiologicalDomain.NEUROLOGICAL,
    "alpha-gpc": BiologicalDomain.NEUROLOGICAL,
    "phosphatidylserine": BiologicalDomain.NEUROLOGICAL,
    
    # Metabolic
    "berberine": BiologicalDomain.METABOLIC,
    "chromium": BiologicalDomain.METABOLIC,
    "alpha-lipoic-acid": BiologicalDomain.METABOLIC,
    "cinnamon": BiologicalDomain.METABOLIC,
    
    # Immune
    "vitamin-c": BiologicalDomain.IMMUNE,
    "vitamin-d": BiologicalDomain.IMMUNE,
    "vitamin-d3": BiologicalDomain.IMMUNE,
    "zinc": BiologicalDomain.IMMUNE,
    "elderberry": BiologicalDomain.IMMUNE,
    "echinacea": BiologicalDomain.IMMUNE,
    
    # Musculoskeletal
    "collagen": BiologicalDomain.MUSCULOSKELETAL,
    "glucosamine": BiologicalDomain.MUSCULOSKELETAL,
    "chondroitin": BiologicalDomain.MUSCULOSKELETAL,
    "msm": BiologicalDomain.MUSCULOSKELETAL,
    "boswellia": BiologicalDomain.MUSCULOSKELETAL,
    "creatine": BiologicalDomain.MUSCULOSKELETAL,
    
    # Endocrine
    "ashwagandha": BiologicalDomain.ENDOCRINE,  # Note: Blocked due to hepatotoxicity
    "maca": BiologicalDomain.ENDOCRINE,
    "tongkat-ali": BiologicalDomain.ENDOCRINE,
    "dim": BiologicalDomain.ENDOCRINE,
    
    # Digestive
    "probiotics": BiologicalDomain.DIGESTIVE,
    "prebiotics": BiologicalDomain.DIGESTIVE,
    "digestive-enzymes": BiologicalDomain.DIGESTIVE,
    "psyllium": BiologicalDomain.DIGESTIVE,
    "ginger": BiologicalDomain.DIGESTIVE,
    "peppermint": BiologicalDomain.DIGESTIVE,
    
    # Integumentary (Skin/Hair/Nails)
    "biotin": BiologicalDomain.INTEGUMENTARY,
    "hyaluronic-acid": BiologicalDomain.INTEGUMENTARY,
    "keratin": BiologicalDomain.INTEGUMENTARY,
    
    # General Wellness (Multivitamins, etc.)
    "multivitamin": BiologicalDomain.GENERAL_WELLNESS,
    "b-complex": BiologicalDomain.GENERAL_WELLNESS,
    "magnesium": BiologicalDomain.GENERAL_WELLNESS,
}

# Domain code prefixes for module_code generation
DOMAIN_CODE_MAP = {
    BiologicalDomain.CARDIOVASCULAR: "CARDIO",
    BiologicalDomain.NEUROLOGICAL: "NEURO",
    BiologicalDomain.METABOLIC: "META",
    BiologicalDomain.IMMUNE: "IMMUN",
    BiologicalDomain.MUSCULOSKELETAL: "MUSC",
    BiologicalDomain.ENDOCRINE: "ENDO",
    BiologicalDomain.DIGESTIVE: "DIG",
    BiologicalDomain.INTEGUMENTARY: "INTEG",
    BiologicalDomain.RESPIRATORY: "RESP",
    BiologicalDomain.GENERAL_WELLNESS: "GEN",
}


# =============================================
# Tier/Layer Classification
# =============================================

def determine_os_layer(ingredients: List[ParsedIngredient]) -> OSLayer:
    """
    Determine OS layer based on ingredient evidence tiers.
    
    Rules:
    - All TIER 1 ingredients -> Core
    - Mix of TIER 1 and TIER 2 -> Adaptive
    - Any TIER 3 or unknown -> Experimental
    """
    tiers = [ing.tier_classification for ing in ingredients if ing.tier_classification]
    
    if not tiers:
        return OSLayer.EXPERIMENTAL  # Unknown ingredients
    
    if any(t == "TIER_3" for t in tiers):
        return OSLayer.EXPERIMENTAL
    
    tier_1_count = sum(1 for t in tiers if t == "TIER_1")
    tier_2_count = sum(1 for t in tiers if t == "TIER_2")
    
    if tier_2_count == 0 and tier_1_count > 0:
        return OSLayer.CORE
    
    if tier_1_count > tier_2_count:
        return OSLayer.CORE
    
    return OSLayer.ADAPTIVE


def determine_biological_domain(ingredients: List[ParsedIngredient]) -> Optional[BiologicalDomain]:
    """
    Determine primary biological domain based on ingredients.
    
    Uses the most common domain among ingredients.
    """
    domain_counts: Dict[BiologicalDomain, int] = {}
    
    for ing in ingredients:
        # Try canonical name first, then raw name
        name = ing.canonical_name or ing.raw_name
        name_slug = slugify(name)
        
        for pattern, domain in INGREDIENT_DOMAIN_MAP.items():
            if pattern in name_slug:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
                break
    
    if not domain_counts:
        return BiologicalDomain.GENERAL_WELLNESS
    
    # Return domain with highest count
    return max(domain_counts.items(), key=lambda x: x[1])[0]


# =============================================
# Module Code Generation
# =============================================

def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')


def generate_module_code(
    product_name: str,
    os_environment: str,
    domain: Optional[BiologicalDomain],
    sequence_number: int = 1,
) -> str:
    """
    Generate deterministic module_code.
    
    Format: {DOMAIN}-{PRODUCT_SLUG}-{ENV}-{SEQ}
    
    Examples:
    - CARDIO-OMEGA3-M-001
    - NEURO-LIONS-MANE-F-001
    - DIG-PROBIOTIC-M-002
    """
    # Get domain prefix
    domain_code = DOMAIN_CODE_MAP.get(domain, "GEN") if domain else "GEN"
    
    # Generate product slug (max 15 chars)
    product_slug = slugify(product_name)[:15].upper().rstrip('-')
    
    # Environment suffix
    env_suffix = "M" if os_environment == "MAXimo²" else "F"
    
    # Sequence (3 digits, zero-padded)
    seq = f"{sequence_number:03d}"
    
    return f"{domain_code}-{product_slug}-{env_suffix}-{seq}"


def determine_os_environment(category: Optional[str], ingredients: List[ParsedIngredient]) -> OSEnvironment:
    """
    Determine OS environment (MAXimo²/MAXima²/BOTH).
    
    Rules:
    - Category contains "men" or "male" -> MAXimo²
    - Category contains "women" or "female" -> MAXima²
    - Testosterone-related ingredients -> MAXimo²
    - Fertility/menstrual-related -> MAXima²
    - Default -> BOTH (generates M + F modules)
    """
    if category:
        cat_lower = category.lower()
        if any(x in cat_lower for x in ["men", "male", "testosterone"]):
            return OSEnvironment.MAXIMO
        if any(x in cat_lower for x in ["women", "female", "prenatal", "fertility"]):
            return OSEnvironment.MAXIMA
    
    # Check ingredient patterns
    ingredient_names = [slugify(ing.canonical_name or ing.raw_name) for ing in ingredients]
    
    male_keywords = ["testosterone", "tongkat", "tribulus", "fenugreek"]
    female_keywords = ["prenatal", "fertility", "iron-women", "folate-prenatal"]
    
    has_male = any(any(kw in name for kw in male_keywords) for name in ingredient_names)
    has_female = any(any(kw in name for kw in female_keywords) for name in ingredient_names)
    
    if has_male and not has_female:
        return OSEnvironment.MAXIMO
    if has_female and not has_male:
        return OSEnvironment.MAXIMA
    
    return OSEnvironment.BOTH


# =============================================
# Draft Module Generation
# =============================================

def generate_draft_modules(
    parsed: ParsedPayload,
    os_environment: OSEnvironment,
    existing_count: int = 0,
) -> List[DraftModule]:
    """
    Generate draft module(s) from parsed payload.
    
    If os_environment is BOTH, generates TWO modules (M + F).
    """
    modules = []
    
    # Determine common properties
    os_layer = determine_os_layer(parsed.ingredients)
    domain = determine_biological_domain(parsed.ingredients)
    
    # Generate ingredient_tags (comma-separated canonical names)
    ingredient_tags = ", ".join([
        ing.canonical_name or ing.raw_name 
        for ing in parsed.ingredients 
        if ing.canonical_name or ing.raw_name
    ])
    
    # Environments to generate
    envs = []
    if os_environment == OSEnvironment.BOTH:
        envs = [("MAXimo²", "M"), ("MAXima²", "F")]
    elif os_environment == OSEnvironment.MAXIMO:
        envs = [("MAXimo²", "M")]
    else:
        envs = [("MAXima²", "F")]
    
    for i, (env_name, env_suffix) in enumerate(envs):
        seq = existing_count + i + 1
        module_code = generate_module_code(
            parsed.product_name,
            env_name,
            domain,
            seq,
        )
        
        module = DraftModule(
            module_code=module_code,
            os_environment=env_name,
            os_layer=os_layer.value,
            biological_domain=domain.value if domain else None,
            product_name=parsed.product_name,
            ingredient_tags=ingredient_tags,
            category_tags=parsed.category,
            ingredients_raw_text=parsed.raw_ingredients_text,
            wholesale_price=parsed.base_price,
            shipping_restriction=", ".join(parsed.shipping_restrictions) if parsed.shipping_restrictions else None,
        )
        modules.append(module)
    
    return modules


def generate_module_code_hash(module: DraftModule) -> str:
    """Generate deterministic hash for module content (for idempotency checks)."""
    content = f"{module.product_name}|{module.os_environment}|{module.ingredient_tags}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]
