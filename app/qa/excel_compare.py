"""
GenoMAX² Excel vs DB Comparison Module v2
Validates os_modules_v3_1 against GenoMAX2_Catalog_Selection_v2_FINAL.xlsx

Matching Key: (supliful_sku, os_environment) -> (shopify_handle, os_environment)

Key insight: Excel contains multiple research_ingredients per (SKU, environment) pair
because one Supliful product can contain multiple ingredients. The DB should have
one module per (shopify_handle, os_environment) pair.
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
    """Normalize text for comparison: trim, lower, fix quotes/spaces."""
    if not text:
        return ""
    text = str(text).strip().lower()
    text = text.replace("'", "'").replace("'", "'").replace(""", '"').replace(""", '"')
    text = re.sub(r'\s+', ' ', text)
    return text


# Expected data from GenoMAX2_Catalog_Selection_v2_FINAL.xlsx
# Format: (supliful_sku, os_environment, os_layer, biological_subsystem, research_ingredient)
# Multiple rows per SKU+env pair are valid (multi-ingredient products)
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


@router.get("/compare/excel-db-full")
def compare_excel_db_full() -> Dict[str, Any]:
    """
    Full comparison of DB against expected Excel YES rows.
    
    Matching strategy:
    - Key: (supliful_sku, os_environment) in Excel -> (shopify_handle, os_environment) in DB
    - One DB module per unique (shopify_handle, os_environment) pair
    - Excel may have multiple research_ingredients per SKU (multi-ingredient products)
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
            if key in db_by_key:
                # Duplicate - should not happen after fix
                pass
            db_by_key[key] = dict(row)
        
        # Get unique Excel SKU+env pairs
        excel_sku_env_pairs = get_unique_sku_env_pairs()
        
        results = {
            "audit_type": "Excel vs DB Comparison (SKU-based)",
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
        
        # Match Excel SKU+env pairs to DB
        for sku, os_env in excel_sku_env_pairs:
            db_key = (sku, os_env)
            
            if db_key in db_by_key:
                db_row = db_by_key[db_key]
                matched_db_keys.add(db_key)
                
                # Get Excel rows for this SKU+env (may have multiple ingredients)
                excel_rows_for_key = [r for r in EXCEL_YES_ROWS if r[0] == sku and r[1] == os_env]
                
                # Use first row for os_layer/bio_subsystem (should be consistent)
                excel_os_layer = excel_rows_for_key[0][2]
                excel_bio_subsystem = excel_rows_for_key[0][3]
                excel_ingredients = [r[4] for r in excel_rows_for_key]
                
                match_record = {
                    "excel_key": f"({sku}, {os_env})",
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
                        "excel_key": f"({sku}, {os_env})",
                        "db_module_code": db_row["module_code"],
                        "diffs": diffs
                    })
            else:
                # Get Excel info for this missing pair
                excel_rows_for_key = [r for r in EXCEL_YES_ROWS if r[0] == sku and r[1] == os_env]
                results["coverage"]["missing_in_db"].append({
                    "supliful_sku": sku,
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
            results["decision"] = "DB is 100% aligned with Excel YES rows (by SKU+environment)"
        else:
            results["overall_status"] = "FAIL"
            issues = []
            if results["summary"]["missing_in_db_count"] > 0:
                issues.append(f"{results['summary']['missing_in_db_count']} SKU+env pairs missing in DB")
            if results["summary"]["diff_count"] > 0:
                issues.append(f"{results['summary']['diff_count']} field differences")
            results["decision"] = f"Do not proceed. Issues: {'; '.join(issues)}"
            
            # Add recommendation if missing
            if results["summary"]["missing_in_db_count"] > 0:
                results["recommendation"] = "Add missing SKU+environment pairs to os_modules_v3_1 or update shopify_handle values"
        
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
    """List SKUs from Excel that are missing in DB."""
    full = compare_excel_db_full()
    return {
        "missing_count": len(full["coverage"]["missing_in_db"]),
        "missing_skus": full["coverage"]["missing_in_db"]
    }


@router.get("/compare/extra-db-modules")
def get_extra_db_modules() -> Dict[str, Any]:
    """List DB modules not in Excel YES rows."""
    full = compare_excel_db_full()
    return {
        "extra_count": len(full["coverage"]["extra_in_db"]),
        "extra_modules": full["coverage"]["extra_in_db"][:50]  # Limit output
    }
