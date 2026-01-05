"""
GenoMAX² Excel vs DB Comparison Module v4
Validates os_modules_v3_1 against GenoMAX2_Catalog_Selection_v2_FINAL.xlsx

MATCHING STRATEGY (v4):
- Instead of assuming a pattern, we build an explicit mapping from
  Excel supliful_sku to DB shopify_handle
- This handles cases where DB uses shorter names than Excel
"""

import os
import re
from typing import Dict, Any, List, Optional, Set, Tuple
from fastapi import APIRouter, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/qa", tags=["QA Excel Compare"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def normalize_text(text: str) -> str:
    """Normalize text for comparison."""
    if not text:
        return ""
    text = str(text).strip().lower()
    text = text.replace("'", "'").replace("'", "'").replace(""", '"').replace(""", '"')
    text = re.sub(r'\s+', ' ', text)
    return text


# ============================================================================
# EXCEL TO DB HANDLE MAPPING
# ============================================================================
# Built by comparing Excel supliful_sku to actual DB shopify_handle base names
# Excel SKU -> DB handle base (without -maximo/-maxima suffix)
# ============================================================================

EXCEL_SKU_TO_DB_BASE = {
    # Direct matches (Excel SKU = DB base exactly)
    "appetite-balance-weight-support-strips": "appetite-balance-weight-support-strips",
    "beetroot-capsules": "beetroot-capsules",
    "diet-drops-ultra": "diet-drops-ultra",
    "energy-powder-cotton-candy": "energy-powder-cotton-candy",
    "focus-powder-sour-candy": "focus-powder-sour-candy",
    "iron-strips": "iron-strips",
    "l-glutamine-powder": "l-glutamine-powder",
    "multivitamin-bear-gummies-adult": "multivitamin-bear-gummies-adult",
    "mushroom-coffee-fusion-lions-mane-chaga-16oz": "mushroom-coffee-fusion-lions-mane-chaga-16oz",
    
    # Mappings where DB uses shorter name (without -capsules, -powder, etc.)
    "ashwagandha-capsules": "ashwagandha",
    "berberine-capsules": "berberine",
    "cognitive-support-capsules": "cognitive-support",
    "colon-gentle-cleanse-sachets": "colon-gentle-cleanse",
    "complete-multivitamin-capsules": "complete-multivitamin",
    "coq10-ubiquinone-capsules": "coq10-ubiquinone",
    "creatine-monohydrate-powder": "creatine-monohydrate",
    "green-tea-antioxidant-serum": "green-tea-antioxidant",
    "joint-support-capsules": "joint-support",
    "keto-5-capsules": "keto-5",
    "kojic-acid-turmeric-soap": "kojic-acid-turmeric",
    "liver-support-capsules": "liver-support",
    "maca-plus-capsules": "maca-plus",
    "magnesium-glycinate-capsules": "magnesium-glycinate",
    "max-detox-acai-capsules": "max-detox-acai",
    "mens-vitality-tablets": "mens-vitality",
    "moisturizing-strengthening-hair-oil-old": "moisturizing-strengthening-hair-oil",
    "nad-plus-capsules": "nad-plus",
    "nitric-oxide-capsules": "nitric-oxide",
    "omega-3-epa-dha-softgel-capsules": "omega-3-epa-dha",
    "peptide-hair-growth-serum": "peptide-hair-growth",
    "platinum-turmeric-capsules": "platinum-turmeric",
    "probiotic-40-billion-prebiotics-capsules": "probiotic-40-billion-prebiotics",
    "recovery-cream": "recovery",
    "resveratrol-50-percent-capsules": "resveratrol-50-percent",
    "sleep-formula-capsules": "sleep-formula",
    "vision-support-capsules": "vision-support",
    "vitamin-c-serum": "vitamin-c",
    "vitamin-d3-2000iu-softgel-capsules": "vitamin-d3-2000iu",
    "vitamin-glow-serum": "vitamin-glow",
}


def get_expected_db_handle(excel_sku: str, os_environment: str) -> str:
    """
    Get expected DB shopify_handle from Excel SKU and environment.
    Uses explicit mapping, falling back to pattern-based derivation.
    """
    # Look up base name from mapping
    if excel_sku in EXCEL_SKU_TO_DB_BASE:
        base = EXCEL_SKU_TO_DB_BASE[excel_sku]
    else:
        # Fallback: use Excel SKU as-is
        base = excel_sku
    
    # Add environment suffix
    if os_environment == "MAXimo²":
        return f"{base}-maximo"
    elif os_environment == "MAXima²":
        return f"{base}-maxima"
    else:
        return base


# Expected data from GenoMAX2_Catalog_Selection_v2_FINAL.xlsx
# Format: (supliful_sku, os_environment, os_layer, biological_subsystem, research_ingredient)
EXCEL_YES_ROWS = [
    ("omega-3-epa-dha-softgel-capsules", "MAXimo²", "Core", "Cardiovascular Function", "Omega-3 EPA/DHA"),
    ("omega-3-epa-dha-softgel-capsules", "MAXima²", "Core", "Cardiovascular Function", "Omega-3 EPA/DHA"),
    ("magnesium-glycinate-capsules", "MAXimo²", "Core", "Neurotransmission & Cognitive Circuits", "Magnesium"),
    ("magnesium-glycinate-capsules", "MAXima²", "Core", "Neurotransmission & Cognitive Circuits", "Magnesium"),
    ("vitamin-d3-2000iu-softgel-capsules", "MAXimo²", "Core", "Digestive & Absorptive Systems", "Vitamin D3"),
    ("vitamin-d3-2000iu-softgel-capsules", "MAXima²", "Core", "Digestive & Absorptive Systems", "Vitamin D3"),
    ("creatine-monohydrate-powder", "MAXimo²", "Support", "Musculoskeletal Integrity", "Creatine Monohydrate"),
    ("creatine-monohydrate-powder", "MAXima²", "Support", "Musculoskeletal Integrity", "Creatine Monohydrate"),
    ("coq10-ubiquinone-capsules", "MAXimo²", "Support", "Cellular Protection & Longevity Pathways", "CoQ10 Ubiquinone/Ubiquinol"),
    ("coq10-ubiquinone-capsules", "MAXima²", "Support", "Cellular Protection & Longevity Pathways", "CoQ10 Ubiquinone/Ubiquinol"),
    ("platinum-turmeric-capsules", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance", "Curcumin"),
    ("platinum-turmeric-capsules", "MAXima²", "Support", "Immune Defense & Inflammatory Balance", "Curcumin"),
    ("berberine-capsules", "MAXimo²", "Support", "Glucose & Insulin Signaling", "Berberine"),
    ("berberine-capsules", "MAXima²", "Support", "Glucose & Insulin Signaling", "Berberine"),
    ("liver-support-capsules", "MAXimo²", "Support", "Detoxification & Biotransformation", "NAC N-Acetyl Cysteine"),
    ("liver-support-capsules", "MAXima²", "Support", "Detoxification & Biotransformation", "NAC N-Acetyl Cysteine"),
    ("vision-support-capsules", "MAXimo²", "Core", "Immune Defense & Inflammatory Balance", "Zinc"),
    ("vision-support-capsules", "MAXima²", "Core", "Immune Defense & Inflammatory Balance", "Zinc"),
    ("complete-multivitamin-capsules", "MAXimo²", "Core", "Neurotransmission & Cognitive Circuits", "Vitamin B12 Methylcobalamin"),
    ("complete-multivitamin-capsules", "MAXima²", "Core", "Neurotransmission & Cognitive Circuits", "Vitamin B12 Methylcobalamin"),
    ("cognitive-support-capsules", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits", "Bacopa monnieri"),
    ("cognitive-support-capsules", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits", "Bacopa monnieri"),
    ("sleep-formula-capsules", "MAXimo²", "Support", "Circadian & Sleep Architecture", "Melatonin"),
    ("sleep-formula-capsules", "MAXima²", "Support", "Circadian & Sleep Architecture", "Melatonin"),
    ("colon-gentle-cleanse-sachets", "MAXimo²", "Support", "Digestive & Absorptive Systems", "Psyllium Husk"),
    ("colon-gentle-cleanse-sachets", "MAXima²", "Support", "Digestive & Absorptive Systems", "Psyllium Husk"),
    ("kojic-acid-turmeric-soap", "MAXimo²", "Support", "Glucose & Insulin Signaling", "Alpha-Lipoic Acid"),
    ("kojic-acid-turmeric-soap", "MAXima²", "Support", "Glucose & Insulin Signaling", "Alpha-Lipoic Acid"),
    ("ashwagandha-capsules", "MAXimo²", "Optimize", "Cardiovascular Function", "Algal Oil DHA"),
    ("ashwagandha-capsules", "MAXima²", "Optimize", "Cardiovascular Function", "Algal Oil DHA"),
    ("energy-powder-cotton-candy", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance", "Spirulina"),
    ("energy-powder-cotton-candy", "MAXima²", "Support", "Immune Defense & Inflammatory Balance", "Spirulina"),
    ("moisturizing-strengthening-hair-oil-old", "MAXimo²", "Optimize", "Immune Defense & Inflammatory Balance", "Black Seed Oil Nigella sativa"),
    ("moisturizing-strengthening-hair-oil-old", "MAXima²", "Optimize", "Immune Defense & Inflammatory Balance", "Black Seed Oil Nigella sativa"),
    ("probiotic-40-billion-prebiotics-capsules", "MAXimo²", "Core", "Digestive & Absorptive Systems", "Probiotics Multi-strain"),
    ("probiotic-40-billion-prebiotics-capsules", "MAXima²", "Core", "Digestive & Absorptive Systems", "Probiotics Multi-strain"),
    ("energy-powder-cotton-candy", "MAXimo²", "Support", "Cardiovascular Function", "Taurine"),
    ("energy-powder-cotton-candy", "MAXima²", "Support", "Cardiovascular Function", "Taurine"),
    ("cognitive-support-capsules", "MAXimo²", "Support", "Neurotransmission & Cognitive Circuits", "L-Theanine"),
    ("cognitive-support-capsules", "MAXima²", "Support", "Neurotransmission & Cognitive Circuits", "L-Theanine"),
    ("joint-support-capsules", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance", "Boswellia serrata"),
    ("joint-support-capsules", "MAXima²", "Support", "Immune Defense & Inflammatory Balance", "Boswellia serrata"),
    ("liver-support-capsules", "MAXimo²", "Support", "Digestive & Absorptive Systems", "Ginger Extract"),
    ("liver-support-capsules", "MAXima²", "Support", "Digestive & Absorptive Systems", "Ginger Extract"),
    ("vision-support-capsules", "MAXimo²", "Support", "Cardiovascular Function", "Aged Garlic Extract"),
    ("vision-support-capsules", "MAXima²", "Support", "Cardiovascular Function", "Aged Garlic Extract"),
    ("vitamin-glow-serum", "MAXimo²", "Core", "Neurotransmission & Cognitive Circuits", "Niacin Vitamin B3"),
    ("vitamin-glow-serum", "MAXima²", "Core", "Neurotransmission & Cognitive Circuits", "Niacin Vitamin B3"),
    ("vitamin-c-serum", "MAXimo²", "Core", "Immune Defense & Inflammatory Balance", "Vitamin C"),
    ("vitamin-c-serum", "MAXima²", "Core", "Immune Defense & Inflammatory Balance", "Vitamin C"),
    ("iron-strips", "MAXimo²", "Core", "Reproductive & Hormonal Axis (Female)", "Folate Methylfolate"),
    ("iron-strips", "MAXima²", "Core", "Reproductive & Hormonal Axis (Female)", "Folate Methylfolate"),
    ("complete-multivitamin-capsules", "MAXimo²", "Core", "Neurotransmission & Cognitive Circuits", "Riboflavin Vitamin B2"),
    ("complete-multivitamin-capsules", "MAXima²", "Core", "Neurotransmission & Cognitive Circuits", "Riboflavin Vitamin B2"),
    ("multivitamin-bear-gummies-adult", "MAXimo²", "Core", "Thyroid & Metabolic Rate Control", "Iodine"),
    ("multivitamin-bear-gummies-adult", "MAXima²", "Core", "Thyroid & Metabolic Rate Control", "Iodine"),
    ("nitric-oxide-capsules", "MAXimo²", "Support", "Musculoskeletal Integrity", "Citrulline"),
    ("nitric-oxide-capsules", "MAXima²", "Support", "Musculoskeletal Integrity", "Citrulline"),
    ("green-tea-antioxidant-serum", "MAXimo²", "Optimize", "Cellular Protection & Longevity Pathways", "Green Tea Extract EGCG"),
    ("green-tea-antioxidant-serum", "MAXima²", "Optimize", "Cellular Protection & Longevity Pathways", "Green Tea Extract EGCG"),
    ("vitamin-d3-2000iu-softgel-capsules", "MAXimo²", "Support", "Cardiovascular Function", "Vitamin K1"),
    ("vitamin-d3-2000iu-softgel-capsules", "MAXima²", "Support", "Cardiovascular Function", "Vitamin K1"),
    ("max-detox-acai-capsules", "MAXimo²", "Optimize", "Detoxification & Biotransformation", "Chlorella"),
    ("max-detox-acai-capsules", "MAXima²", "Optimize", "Detoxification & Biotransformation", "Chlorella"),
    ("complete-multivitamin-capsules", "MAXimo²", "Core", "Thyroid & Metabolic Rate Control", "Selenium"),
    ("complete-multivitamin-capsules", "MAXima²", "Core", "Thyroid & Metabolic Rate Control", "Selenium"),
    ("resveratrol-50-percent-capsules", "MAXimo²", "Optimize", "Cellular Protection & Longevity Pathways", "Resveratrol"),
    ("resveratrol-50-percent-capsules", "MAXima²", "Optimize", "Cellular Protection & Longevity Pathways", "Resveratrol"),
    ("nad-plus-capsules", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance", "Quercetin"),
    ("nad-plus-capsules", "MAXima²", "Support", "Immune Defense & Inflammatory Balance", "Quercetin"),
    ("coq10-ubiquinone-capsules", "MAXimo²", "Optimize", "Cellular Protection & Longevity Pathways", "PQQ Pyrroloquinoline quinone"),
    ("coq10-ubiquinone-capsules", "MAXima²", "Optimize", "Cellular Protection & Longevity Pathways", "PQQ Pyrroloquinoline quinone"),
    ("diet-drops-ultra", "MAXimo²", "Optimize", "HPA Axis & Stress Response", "Rhodiola rosea"),
    ("diet-drops-ultra", "MAXima²", "Optimize", "HPA Axis & Stress Response", "Rhodiola rosea"),
    ("mushroom-coffee-fusion-lions-mane-chaga-16oz", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits", "Lion's Mane Hericium erinaceus"),
    ("mushroom-coffee-fusion-lions-mane-chaga-16oz", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits", "Lion's Mane Hericium erinaceus"),
    ("focus-powder-sour-candy", "MAXimo²", "Support", "Neurotransmission & Cognitive Circuits", "Inositol Myo-inositol"),
    ("focus-powder-sour-candy", "MAXima²", "Support", "Neurotransmission & Cognitive Circuits", "Inositol Myo-inositol"),
    ("recovery-cream", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits", "SAMe S-Adenosyl-L-methionine"),
    ("recovery-cream", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits", "SAMe S-Adenosyl-L-methionine"),
    ("cognitive-support-capsules", "MAXimo²", "Support", "Neurotransmission & Cognitive Circuits", "L-Tyrosine"),
    ("cognitive-support-capsules", "MAXima²", "Support", "Neurotransmission & Cognitive Circuits", "L-Tyrosine"),
    ("nitric-oxide-capsules", "MAXimo²", "Support", "Cardiovascular Function", "L-Arginine"),
    ("nitric-oxide-capsules", "MAXima²", "Support", "Cardiovascular Function", "L-Arginine"),
    ("l-glutamine-powder", "MAXimo²", "Support", "Digestive & Absorptive Systems", "L-Glutamine"),
    ("l-glutamine-powder", "MAXima²", "Support", "Digestive & Absorptive Systems", "L-Glutamine"),
    ("mens-vitality-tablets", "MAXimo²", "Optimize", "Reproductive & Hormonal Axis (Male)", "Tongkat Ali Eurycoma longifolia"),
    ("mens-vitality-tablets", "MAXima²", "Optimize", "Reproductive & Hormonal Axis (Male)", "Tongkat Ali Eurycoma longifolia"),
    ("maca-plus-capsules", "MAXimo²", "Optimize", "Reproductive & Hormonal Axis (Female)", "Maca Root"),
    ("maca-plus-capsules", "MAXima²", "Optimize", "Reproductive & Hormonal Axis (Female)", "Maca Root"),
    ("beetroot-capsules", "MAXimo²", "Support", "Cardiovascular Function", "Beetroot/Nitrate"),
    ("beetroot-capsules", "MAXima²", "Support", "Cardiovascular Function", "Beetroot/Nitrate"),
    ("keto-5-capsules", "MAXimo²", "Support", "Musculoskeletal Integrity", "Caffeine"),
    ("keto-5-capsules", "MAXima²", "Support", "Musculoskeletal Integrity", "Caffeine"),
    ("peptide-hair-growth-serum", "MAXimo²", "Support", "Circadian & Sleep Architecture", "Glycine"),
    ("peptide-hair-growth-serum", "MAXima²", "Support", "Circadian & Sleep Architecture", "Glycine"),
    ("complete-multivitamin-capsules", "MAXimo²", "Core", "Neurotransmission & Cognitive Circuits", "Vitamin B6 Pyridoxine/P5P"),
    ("complete-multivitamin-capsules", "MAXima²", "Core", "Neurotransmission & Cognitive Circuits", "Vitamin B6 Pyridoxine/P5P"),
    ("cognitive-support-capsules", "MAXimo²", "Support", "Neurotransmission & Cognitive Circuits", "Choline Alpha-GPC/CDP"),
    ("cognitive-support-capsules", "MAXima²", "Support", "Neurotransmission & Cognitive Circuits", "Choline Alpha-GPC/CDP"),
    ("complete-multivitamin-capsules", "MAXimo²", "Core", "Immune Defense & Inflammatory Balance", "Vitamin A Retinol/Beta-carotene"),
    ("complete-multivitamin-capsules", "MAXima²", "Core", "Immune Defense & Inflammatory Balance", "Vitamin A Retinol/Beta-carotene"),
    ("appetite-balance-weight-support-strips", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits", "Saffron Crocus sativus"),
    ("appetite-balance-weight-support-strips", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits", "Saffron Crocus sativus"),
]


def get_unique_sku_env_pairs() -> Set[Tuple[str, str]]:
    """Get unique (supliful_sku, os_environment) pairs from Excel."""
    return {(row[0], row[1]) for row in EXCEL_YES_ROWS}


@router.get("/compare/db-bases")
def get_db_base_handles() -> Dict[str, Any]:
    """Get all unique DB handle base names (without -maximo/-maxima suffix)."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT shopify_handle
            FROM os_modules_v3_1
            WHERE supplier_status IS NULL 
               OR supplier_status NOT IN ('DUPLICATE_INACTIVE')
            ORDER BY shopify_handle
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        handles = [r["shopify_handle"] for r in rows]
        bases = set()
        for h in handles:
            if h.endswith("-maximo"):
                bases.add(h[:-7])
            elif h.endswith("-maxima"):
                bases.add(h[:-7])
            else:
                bases.add(h)
        
        return {
            "total_handles": len(handles),
            "unique_bases": len(bases),
            "bases": sorted(bases)
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compare/debug-handles")
def debug_handles() -> Dict[str, Any]:
    """
    Debug endpoint to compare Excel-derived handles vs actual DB handles.
    Shows the mapping pattern and what's matching/missing.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get unique DB handles with environment
        cur.execute("""
            SELECT DISTINCT shopify_handle, os_environment
            FROM os_modules_v3_1
            WHERE supplier_status IS NULL 
               OR supplier_status NOT IN ('DUPLICATE_INACTIVE')
            ORDER BY shopify_handle
        """)
        db_rows = cur.fetchall()
        db_handle_env_pairs = {(r["shopify_handle"], r["os_environment"]) for r in db_rows}
        
        cur.close()
        conn.close()
        
        # Get Excel data
        excel_sku_env_pairs = get_unique_sku_env_pairs()
        
        # Derive expected handles from Excel using explicit mapping
        expected_handle_env_pairs = set()
        for sku, os_env in excel_sku_env_pairs:
            expected_handle = get_expected_db_handle(sku, os_env)
            expected_handle_env_pairs.add((expected_handle, os_env))
        
        # Find overlaps
        matches = expected_handle_env_pairs & db_handle_env_pairs
        in_excel_only = expected_handle_env_pairs - db_handle_env_pairs
        in_db_only = db_handle_env_pairs - expected_handle_env_pairs
        
        return {
            "matching_strategy": "Explicit SKU-to-handle mapping with environment suffix",
            "excel_unique_sku_env_pairs": len(excel_sku_env_pairs),
            "expected_handle_env_pairs": len(expected_handle_env_pairs),
            "db_handle_env_pairs": len(db_handle_env_pairs),
            "exact_matches": len(matches),
            "in_excel_only": len(in_excel_only),
            "in_db_only": len(in_db_only),
            "match_rate_percent": round(100 * len(matches) / len(expected_handle_env_pairs), 1) if expected_handle_env_pairs else 0,
            "sample_expected_handles": sorted([f"{h} ({e})" for h, e in list(expected_handle_env_pairs)[:15]]),
            "sample_db_handles": sorted([f"{h} ({e})" for h, e in list(db_handle_env_pairs)[:15]]),
            "matched_sample": sorted([f"{h} ({e})" for h, e in list(matches)[:15]]) if matches else [],
            "excel_only_sample": sorted([f"{h} ({e})" for h, e in list(in_excel_only)[:15]]),
            "db_only_sample": sorted([f"{h} ({e})" for h, e in list(in_db_only)[:15]])
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Debug error: {str(e)}")


@router.get("/compare/excel-db-full")
def compare_excel_db_full() -> Dict[str, Any]:
    """
    Full comparison of DB against expected Excel YES rows.
    
    Matching strategy (v4):
    - Excel: (supliful_sku, os_environment)
    - Derived: shopify_handle via explicit EXCEL_SKU_TO_DB_BASE mapping
    - DB key: (shopify_handle, os_environment)
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get all active DB modules
        cur.execute("""
            SELECT 
                module_code, shopify_handle, os_environment, os_layer, 
                biological_domain, product_name, supplier_status
            FROM os_modules_v3_1
            WHERE supplier_status IS NULL 
               OR supplier_status NOT IN ('DUPLICATE_INACTIVE')
            ORDER BY shopify_handle, os_environment
        """)
        db_rows = cur.fetchall()
        
        # Build DB lookup by (shopify_handle, os_environment)
        db_by_key: Dict[Tuple[str, str], dict] = {}
        for row in db_rows:
            key = (row["shopify_handle"], row["os_environment"])
            db_by_key[key] = dict(row)
        
        # Get unique Excel SKU+env pairs
        excel_sku_env_pairs = get_unique_sku_env_pairs()
        
        results = {
            "audit_type": "Excel vs DB Comparison (v4 - explicit mapping)",
            "excel_total_rows": len(EXCEL_YES_ROWS),
            "excel_unique_sku_env_pairs": len(excel_sku_env_pairs),
            "db_active_modules": len(db_rows),
            "db_unique_keys": len(db_by_key),
            "coverage": {
                "matched": [],
                "missing_in_db": [],
                "extra_in_db": []
            },
            "field_diffs": [],
            "summary": {},
            "overall_status": "PENDING"
        }
        
        # Track matched DB keys
        matched_db_keys: Set[Tuple[str, str]] = set()
        
        # Match Excel SKU+env pairs to DB using explicit mapping
        for sku, os_env in excel_sku_env_pairs:
            expected_handle = get_expected_db_handle(sku, os_env)
            db_key = (expected_handle, os_env)
            
            if db_key in db_by_key:
                db_row = db_by_key[db_key]
                matched_db_keys.add(db_key)
                
                # Get Excel rows for this SKU+env
                excel_rows_for_key = [r for r in EXCEL_YES_ROWS if r[0] == sku and r[1] == os_env]
                excel_os_layer = excel_rows_for_key[0][2]
                excel_bio_subsystem = excel_rows_for_key[0][3]
                excel_ingredients = [r[4] for r in excel_rows_for_key]
                
                match_record = {
                    "excel_sku": sku,
                    "derived_handle": expected_handle,
                    "os_environment": os_env,
                    "db_module_code": db_row["module_code"],
                    "db_product_name": db_row["product_name"],
                    "excel_ingredients": excel_ingredients
                }
                results["coverage"]["matched"].append(match_record)
                
                # Check field diffs
                diffs = []
                
                db_os_layer = normalize_text(db_row.get("os_layer", ""))
                if normalize_text(excel_os_layer) != db_os_layer:
                    diffs.append({
                        "field": "os_layer",
                        "excel_value": excel_os_layer,
                        "db_value": db_row.get("os_layer")
                    })
                
                db_bio_domain = normalize_text(db_row.get("biological_domain", ""))
                if normalize_text(excel_bio_subsystem) != db_bio_domain:
                    diffs.append({
                        "field": "biological_subsystem/domain",
                        "excel_value": excel_bio_subsystem,
                        "db_value": db_row.get("biological_domain")
                    })
                
                if diffs:
                    results["field_diffs"].append({
                        "excel_sku": sku,
                        "derived_handle": expected_handle,
                        "db_module_code": db_row["module_code"],
                        "diffs": diffs
                    })
            else:
                excel_rows_for_key = [r for r in EXCEL_YES_ROWS if r[0] == sku and r[1] == os_env]
                results["coverage"]["missing_in_db"].append({
                    "excel_sku": sku,
                    "expected_handle": expected_handle,
                    "os_environment": os_env,
                    "excel_os_layer": excel_rows_for_key[0][2] if excel_rows_for_key else None,
                    "excel_bio_subsystem": excel_rows_for_key[0][3] if excel_rows_for_key else None,
                    "excel_ingredients": [r[4] for r in excel_rows_for_key]
                })
        
        # Find extra DB rows not in Excel
        for db_key, db_row in db_by_key.items():
            if db_key not in matched_db_keys:
                results["coverage"]["extra_in_db"].append({
                    "shopify_handle": db_key[0],
                    "os_environment": db_key[1],
                    "module_code": db_row["module_code"],
                    "product_name": db_row["product_name"],
                    "biological_domain": db_row.get("biological_domain")
                })
        
        cur.close()
        conn.close()
        
        # Calculate summary
        results["summary"] = {
            "excel_unique_sku_env_pairs": len(excel_sku_env_pairs),
            "matched_count": len(results["coverage"]["matched"]),
            "missing_in_db_count": len(results["coverage"]["missing_in_db"]),
            "extra_in_db_count": len(results["coverage"]["extra_in_db"]),
            "diff_count": len(results["field_diffs"]),
            "match_rate_percent": round(100 * len(results["coverage"]["matched"]) / len(excel_sku_env_pairs), 1) if excel_sku_env_pairs else 0
        }
        
        # Determine overall status
        if (results["summary"]["missing_in_db_count"] == 0 and 
            results["summary"]["diff_count"] == 0):
            results["overall_status"] = "PASS"
            results["decision"] = "DB is 100% aligned with Excel YES rows"
        else:
            results["overall_status"] = "FAIL"
            issues = []
            if results["summary"]["missing_in_db_count"] > 0:
                issues.append(f"{results['summary']['missing_in_db_count']} expected handles missing in DB")
            if results["summary"]["diff_count"] > 0:
                issues.append(f"{results['summary']['diff_count']} field differences")
            results["decision"] = f"Do not proceed. Issues: {'; '.join(issues)}"
        
        return results
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Comparison error: {str(e)}")


@router.get("/compare/excel-db-summary")
def compare_excel_db_summary() -> Dict[str, Any]:
    """Quick summary of Excel vs DB comparison."""
    full = compare_excel_db_full()
    return {
        "overall_status": full["overall_status"],
        "summary": full["summary"],
        "decision": full.get("decision", ""),
        "recommendation": full.get("recommendation", "")
    }


@router.get("/compare/missing-skus")
def get_missing_skus() -> Dict[str, Any]:
    """List expected handles from Excel that are missing in DB."""
    full = compare_excel_db_full()
    return {
        "missing_count": len(full["coverage"]["missing_in_db"]),
        "missing_products": full["coverage"]["missing_in_db"]
    }


@router.get("/compare/extra-db-modules")
def get_extra_db_modules() -> Dict[str, Any]:
    """List DB modules not expected from Excel YES rows."""
    full = compare_excel_db_full()
    return {
        "extra_count": len(full["coverage"]["extra_in_db"]),
        "extra_modules": full["coverage"]["extra_in_db"][:50]
    }


@router.get("/compare/mapping-check")
def check_mapping() -> Dict[str, Any]:
    """Check if the Excel-to-DB mapping covers all Excel SKUs."""
    excel_skus = {row[0] for row in EXCEL_YES_ROWS}
    mapped_skus = set(EXCEL_SKU_TO_DB_BASE.keys())
    
    unmapped = excel_skus - mapped_skus
    extra_mappings = mapped_skus - excel_skus
    
    return {
        "excel_unique_skus": len(excel_skus),
        "mapped_skus": len(mapped_skus),
        "unmapped_skus": sorted(unmapped) if unmapped else [],
        "extra_mappings": sorted(extra_mappings) if extra_mappings else [],
        "all_mapped": len(unmapped) == 0
    }
