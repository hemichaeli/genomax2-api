"""
ARCHIVED: Legacy Supliful Catalog (22 Hardcoded Products)
=========================================================

DEPRECATED: v3.40.0 (2026-01-29)
REASON: Consolidated into database catalog_products table (151 products)
REPLACEMENT: Use CatalogWiring from app/catalog/wiring.py

This file preserves the original 22 hardcoded products for Git history
and rollback purposes. DO NOT USE IN PRODUCTION.

Original location: bloodwork_engine/supliful_catalog.py
Migration: 016_consolidate_catalog
"""

# ============================================================================
# ARCHIVED LEGACY PRODUCTS (for reference only)
# ============================================================================

LEGACY_PRODUCTS_V1 = [
    # === MAXIMO² LINE (Male Biology) ===
    {"sku": "GMAX-M-VD5K", "name": "MAXimo² Vitamin D3 5000 IU", "category": "vitamin"},
    {"sku": "GMAX-M-O3-2000", "name": "MAXimo² Omega-3 2000mg EPA/DHA", "category": "omega"},
    {"sku": "GMAX-M-MG400", "name": "MAXimo² Magnesium Glycinate 400mg", "category": "mineral"},
    {"sku": "GMAX-M-METH-B", "name": "MAXimo² Methylation Support", "category": "methylation"},
    {"sku": "GMAX-M-TEST", "name": "MAXimo² Testosterone Support", "category": "hormone_support"},
    {"sku": "GMAX-M-METAB", "name": "MAXimo² Metabolic Support", "category": "metabolic"},
    {"sku": "GMAX-M-CARDIO", "name": "MAXimo² Cardiovascular Support", "category": "cardiovascular"},
    {"sku": "GMAX-M-IRON", "name": "MAXimo² Iron Complex", "category": "mineral"},
    {"sku": "GMAX-M-THYROID", "name": "MAXimo² Thyroid Support", "category": "thyroid"},
    {"sku": "GMAX-M-LIVER", "name": "MAXimo² Liver Detox", "category": "liver_support"},
    {"sku": "GMAX-M-ADRENAL", "name": "MAXimo² Adrenal Support", "category": "adrenal"},
    
    # === MAXIMA² LINE (Female Biology) ===
    {"sku": "GMAX-F-VD5K", "name": "MAXima² Vitamin D3 5000 IU", "category": "vitamin"},
    {"sku": "GMAX-F-O3-2000", "name": "MAXima² Omega-3 2000mg EPA/DHA", "category": "omega"},
    {"sku": "GMAX-F-MG400", "name": "MAXima² Magnesium Glycinate 400mg", "category": "mineral"},
    {"sku": "GMAX-F-METH-B", "name": "MAXima² Methylation Support", "category": "methylation"},
    {"sku": "GMAX-F-HORMONE", "name": "MAXima² Hormone Balance", "category": "hormone_support"},
    {"sku": "GMAX-F-IRON", "name": "MAXima² Iron Complex", "category": "mineral"},
    {"sku": "GMAX-F-METAB", "name": "MAXima² Metabolic Support", "category": "metabolic"},
    {"sku": "GMAX-F-THYROID", "name": "MAXima² Thyroid Support", "category": "thyroid"},
    {"sku": "GMAX-F-ADRENAL", "name": "MAXima² Adrenal Support", "category": "adrenal"},
    
    # === UNIVERSAL LINE ===
    {"sku": "GMAX-U-PROBIOTIC", "name": "GenoMAX² Probiotic 50B", "category": "probiotic"},
    {"sku": "GMAX-U-COQ10", "name": "GenoMAX² CoQ10 Ubiquinol 200mg", "category": "cardiovascular"},
]

LEGACY_INGREDIENTS_V1 = [
    # TIER 1 - Strong Evidence (9)
    {"code": "VIT_D3", "name": "Vitamin D3", "tier": "tier_1"},
    {"code": "OMEGA3_EPA", "name": "EPA (Omega-3)", "tier": "tier_1"},
    {"code": "OMEGA3_DHA", "name": "DHA (Omega-3)", "tier": "tier_1"},
    {"code": "MAGNESIUM_GLYCINATE", "name": "Magnesium Glycinate", "tier": "tier_1"},
    {"code": "IRON_BISGLYCINATE", "name": "Iron Bisglycinate", "tier": "tier_1"},
    {"code": "ZINC_PICOLINATE", "name": "Zinc Picolinate", "tier": "tier_1"},
    {"code": "B12_METHYL", "name": "Methylcobalamin (B12)", "tier": "tier_1"},
    {"code": "FOLATE_METHYL", "name": "Methylfolate (5-MTHF)", "tier": "tier_1"},
    {"code": "COQ10_UBIQUINOL", "name": "CoQ10 Ubiquinol", "tier": "tier_1"},
    
    # TIER 2 - Contextual Evidence (11)
    {"code": "SELENIUM", "name": "Selenium", "tier": "tier_2"},
    {"code": "IODINE", "name": "Iodine", "tier": "tier_2"},
    {"code": "TONGKAT_ALI", "name": "Tongkat Ali", "tier": "tier_2"},
    {"code": "FENUGREEK", "name": "Fenugreek", "tier": "tier_2"},
    {"code": "VITEX", "name": "Vitex (Chasteberry)", "tier": "tier_2"},
    {"code": "DIM", "name": "DIM (Diindolylmethane)", "tier": "tier_2"},
    {"code": "BERBERINE", "name": "Berberine", "tier": "tier_2"},
    {"code": "MILK_THISTLE", "name": "Milk Thistle", "tier": "tier_2"},
    {"code": "RHODIOLA", "name": "Rhodiola Rosea", "tier": "tier_2"},
    {"code": "PROBIOTIC_BLEND", "name": "Probiotic Blend", "tier": "tier_2"},
    {"code": "ADAPTOGEN_BLEND", "name": "Adaptogen Blend", "tier": "tier_2"},
]

# ============================================================================
# DEPRECATION NOTICE
# ============================================================================
"""
These 22 products have been migrated to the catalog_products database table.
The database catalog now contains 151 products (all TIER_1 + TIER_2).

To access the current catalog, use:
    from app.catalog.wiring import get_catalog, filter_to_catalog
    
    catalog = get_catalog()
    products = catalog.all_products()  # Returns 151 products

Legacy endpoint /api/v1/catalog/products now reads from CatalogWiring.
"""
