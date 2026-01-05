"""
GenoMAX² Excel vs DB Comparison Module
Validates os_modules_v3_1 against GenoMAX2_Catalog_Selection_v2_FINAL.xlsx

Matching Key: (os_environment, research_ingredient) -> genomax_ingredients/ingredient_tags
"""

import os
import re
from typing import Dict, Any, List, Optional
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
    # Replace smart quotes with regular quotes
    text = text.replace("'", "'").replace("'", "'").replace(""", '"').replace(""", '"')
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    return text


def ingredient_match(search_ingredient: str, db_ingredients: str, db_tags: str) -> bool:
    """Check if search_ingredient is found in DB genomax_ingredients or ingredient_tags."""
    search_norm = normalize_text(search_ingredient)
    if not search_norm:
        return False
    
    # Check genomax_ingredients
    if db_ingredients:
        ingredients_norm = normalize_text(db_ingredients)
        if search_norm in ingredients_norm:
            return True
    
    # Check ingredient_tags
    if db_tags:
        tags_norm = normalize_text(db_tags)
        if search_norm in tags_norm:
            return True
    
    return False


@router.get("/compare/excel-db-full")
def compare_excel_db_full() -> Dict[str, Any]:
    """
    Full comparison of DB against expected Excel YES rows.
    Uses hardcoded expected data from GenoMAX2_Catalog_Selection_v2_FINAL.xlsx.
    
    Returns comprehensive match report with field-by-field diffs.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    # Expected YES rows from Excel (104 total: 52 MAXimo² + 52 MAXima²)
    # Format: (research_ingredient, os_environment, os_layer, biological_subsystem)
    excel_yes_rows = [
        ("Omega-3 EPA/DHA", "MAXimo²", "Core", "Cardiovascular Function"),
        ("Omega-3 EPA/DHA", "MAXima²", "Core", "Cardiovascular Function"),
        ("Magnesium", "MAXimo²", "Core", "Neurotransmission & Cognitive Circuits"),
        ("Magnesium", "MAXima²", "Core", "Neurotransmission & Cognitive Circuits"),
        ("Vitamin D3", "MAXimo²", "Core", "Digestive & Absorptive Systems"),
        ("Vitamin D3", "MAXima²", "Core", "Digestive & Absorptive Systems"),
        ("Creatine Monohydrate", "MAXimo²", "Support", "Musculoskeletal Integrity"),
        ("Creatine Monohydrate", "MAXima²", "Support", "Musculoskeletal Integrity"),
        ("CoQ10 Ubiquinone/Ubiquinol", "MAXimo²", "Support", "Cellular Protection & Longevity Pathways"),
        ("CoQ10 Ubiquinone/Ubiquinol", "MAXima²", "Support", "Cellular Protection & Longevity Pathways"),
        ("Zinc", "MAXimo²", "Core", "Immune Defense & Inflammatory Balance"),
        ("Zinc", "MAXima²", "Core", "Immune Defense & Inflammatory Balance"),
        ("B-Complex", "MAXimo²", "Core", "Neurotransmission & Cognitive Circuits"),
        ("B-Complex", "MAXima²", "Core", "Neurotransmission & Cognitive Circuits"),
        ("Vitamin C", "MAXimo²", "Core", "Immune Defense & Inflammatory Balance"),
        ("Vitamin C", "MAXima²", "Core", "Immune Defense & Inflammatory Balance"),
        ("Probiotics", "MAXimo²", "Core", "Digestive & Absorptive Systems"),
        ("Probiotics", "MAXima²", "Core", "Digestive & Absorptive Systems"),
        ("Curcumin/Turmeric", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Curcumin/Turmeric", "MAXima²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Alpha-Lipoic Acid", "MAXimo²", "Support", "Glucose & Insulin Signaling"),
        ("Alpha-Lipoic Acid", "MAXima²", "Support", "Glucose & Insulin Signaling"),
        ("NAC (N-Acetyl Cysteine)", "MAXimo²", "Support", "Cellular Protection & Longevity Pathways"),
        ("NAC (N-Acetyl Cysteine)", "MAXima²", "Support", "Cellular Protection & Longevity Pathways"),
        ("Berberine", "MAXimo²", "Support", "Glucose & Insulin Signaling"),
        ("Berberine", "MAXima²", "Support", "Glucose & Insulin Signaling"),
        ("Collagen Peptides", "MAXimo²", "Support", "Musculoskeletal Integrity"),
        ("Collagen Peptides", "MAXima²", "Support", "Musculoskeletal Integrity"),
        ("Resveratrol", "MAXimo²", "Optimize", "Cellular Protection & Longevity Pathways"),
        ("Resveratrol", "MAXima²", "Optimize", "Cellular Protection & Longevity Pathways"),
        ("Vitamin K2 MK-7", "MAXimo²", "Support", "Cardiovascular Function"),
        ("Vitamin K2 MK-7", "MAXima²", "Support", "Cardiovascular Function"),
        ("Quercetin", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Quercetin", "MAXima²", "Support", "Immune Defense & Inflammatory Balance"),
        ("L-Theanine", "MAXimo²", "Support", "Neurotransmission & Cognitive Circuits"),
        ("L-Theanine", "MAXima²", "Support", "Neurotransmission & Cognitive Circuits"),
        ("Psyllium Husk", "MAXimo²", "Support", "Digestive & Absorptive Systems"),
        ("Psyllium Husk", "MAXima²", "Support", "Digestive & Absorptive Systems"),
        ("Astaxanthin", "MAXimo²", "Optimize", "Cellular Protection & Longevity Pathways"),
        ("Astaxanthin", "MAXima²", "Optimize", "Cellular Protection & Longevity Pathways"),
        ("Selenium", "MAXimo²", "Core", "Thyroid & Metabolic Rate Control"),
        ("Selenium", "MAXima²", "Core", "Thyroid & Metabolic Rate Control"),
        ("Digestive Enzymes", "MAXimo²", "Support", "Digestive & Absorptive Systems"),
        ("Digestive Enzymes", "MAXima²", "Support", "Digestive & Absorptive Systems"),
        ("Lion's Mane", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("Lion's Mane", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("PQQ (Pyrroloquinoline Quinone)", "MAXimo²", "Optimize", "Cellular Protection & Longevity Pathways"),
        ("PQQ (Pyrroloquinoline Quinone)", "MAXima²", "Optimize", "Cellular Protection & Longevity Pathways"),
        ("Betaine HCl", "MAXimo²", "Support", "Digestive & Absorptive Systems"),
        ("Betaine HCl", "MAXima²", "Support", "Digestive & Absorptive Systems"),
        ("Milk Thistle (Silymarin)", "MAXimo²", "Support", "Detoxification & Biotransformation"),
        ("Milk Thistle (Silymarin)", "MAXima²", "Support", "Detoxification & Biotransformation"),
        ("Glucosamine + Chondroitin", "MAXimo²", "Support", "Musculoskeletal Integrity"),
        ("Glucosamine + Chondroitin", "MAXima²", "Support", "Musculoskeletal Integrity"),
        ("Boron", "MAXimo²", "Optimize", "Musculoskeletal Integrity"),
        ("Boron", "MAXima²", "Optimize", "Musculoskeletal Integrity"),
        ("Lutein + Zeaxanthin", "MAXimo²", "Support", "Cellular Protection & Longevity Pathways"),
        ("Lutein + Zeaxanthin", "MAXima²", "Support", "Cellular Protection & Longevity Pathways"),
        ("Melatonin", "MAXimo²", "Support", "Circadian & Sleep Architecture"),
        ("Melatonin", "MAXima²", "Support", "Circadian & Sleep Architecture"),
        ("GABA", "MAXimo²", "Support", "Neurotransmission & Cognitive Circuits"),
        ("GABA", "MAXima²", "Support", "Neurotransmission & Cognitive Circuits"),
        ("5-HTP", "MAXimo²", "Support", "Neurotransmission & Cognitive Circuits"),
        ("5-HTP", "MAXima²", "Support", "Neurotransmission & Cognitive Circuits"),
        ("Olive Leaf Extract", "MAXimo²", "Optimize", "Cardiovascular Function"),
        ("Olive Leaf Extract", "MAXima²", "Optimize", "Cardiovascular Function"),
        ("Rhodiola Rosea", "MAXimo²", "Optimize", "HPA Axis & Stress Response"),
        ("Rhodiola Rosea", "MAXima²", "Optimize", "HPA Axis & Stress Response"),
        ("Ginkgo Biloba", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("Ginkgo Biloba", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("Elderberry", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Elderberry", "MAXima²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Cordyceps", "MAXimo²", "Optimize", "Musculoskeletal Integrity"),
        ("Cordyceps", "MAXima²", "Optimize", "Musculoskeletal Integrity"),
        ("Reishi", "MAXimo²", "Optimize", "HPA Axis & Stress Response"),
        ("Reishi", "MAXima²", "Optimize", "HPA Axis & Stress Response"),
        ("DIM (Diindolylmethane)", "MAXimo²", "Optimize", "Reproductive & Hormonal Axis (Male)"),
        ("DIM (Diindolylmethane)", "MAXima²", "Optimize", "Reproductive & Hormonal Axis (Female)"),
        ("Saw Palmetto", "MAXimo²", "Optimize", "Reproductive & Hormonal Axis (Male)"),
        ("Maca Root", "MAXima²", "Optimize", "Reproductive & Hormonal Axis (Female)"),
        ("Fenugreek", "MAXimo²", "Optimize", "Reproductive & Hormonal Axis (Male)"),
        ("Vitex (Chasteberry)", "MAXima²", "Optimize", "Reproductive & Hormonal Axis (Female)"),
        ("Tongkat Ali", "MAXimo²", "Optimize", "Reproductive & Hormonal Axis (Male)"),
        ("Evening Primrose Oil", "MAXima²", "Optimize", "Reproductive & Hormonal Axis (Female)"),
        ("Iodine", "MAXimo²", "Core", "Thyroid & Metabolic Rate Control"),
        ("Iodine", "MAXima²", "Core", "Thyroid & Metabolic Rate Control"),
        ("Iron (Gentle/Bisglycinate)", "MAXima²", "Core", "Cardiovascular Function"),
        ("Calcium + Magnesium", "MAXima²", "Core", "Musculoskeletal Integrity"),
        ("Folate (Methylfolate)", "MAXima²", "Core", "Reproductive & Hormonal Axis (Female)"),
        ("Chromium Picolinate", "MAXimo²", "Support", "Glucose & Insulin Signaling"),
        ("Chromium Picolinate", "MAXima²", "Support", "Glucose & Insulin Signaling"),
        ("Phosphatidylserine", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("Phosphatidylserine", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("Chlorophyll / Chlorella", "MAXimo²", "Optimize", "Detoxification & Biotransformation"),
        ("Chlorophyll / Chlorella", "MAXima²", "Optimize", "Detoxification & Biotransformation"),
        ("Spirulina", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Spirulina", "MAXima²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Beta-Glucan", "MAXimo²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Beta-Glucan", "MAXima²", "Support", "Immune Defense & Inflammatory Balance"),
        ("Bacopa Monnieri", "MAXimo²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("Bacopa Monnieri", "MAXima²", "Optimize", "Neurotransmission & Cognitive Circuits"),
        ("Garlic Extract", "MAXimo²", "Support", "Cardiovascular Function"),
        ("Garlic Extract", "MAXima²", "Support", "Cardiovascular Function"),
        ("Citrus Bergamot", "MAXimo²", "Optimize", "Cardiovascular Function"),
        ("Citrus Bergamot", "MAXima²", "Optimize", "Cardiovascular Function"),
    ]
    
    try:
        cur = conn.cursor()
        
        # Get all active DB modules with relevant fields (only columns that exist)
        cur.execute("""
            SELECT 
                module_code, os_environment, os_layer, biological_domain,
                product_name, genomax_ingredients, ingredient_tags,
                suggested_use_full, safety_notes, contraindications,
                dosing_protocol, supplier_status
            FROM os_modules_v3_1
            WHERE supplier_status IS NULL 
               OR supplier_status NOT IN ('DUPLICATE_INACTIVE')
            ORDER BY os_environment, product_name
        """)
        db_rows = cur.fetchall()
        
        results = {
            "audit_type": "Excel vs DB Comparison",
            "excel_yes_count": len(excel_yes_rows),
            "db_active_count": len(db_rows),
            "coverage": {
                "matched": [],
                "missing_in_db": [],
                "ambiguous_matches": [],
                "extra_in_db": []
            },
            "field_diffs": [],
            "placeholders_in_db": [],
            "summary": {},
            "overall_status": "PENDING"
        }
        
        # Build DB lookup by os_environment
        db_by_env = {"MAXimo²": [], "MAXima²": []}
        for row in db_rows:
            env = row["os_environment"]
            if env in db_by_env:
                db_by_env[env].append(dict(row))
        
        # Track which DB rows have been matched
        matched_db_codes = set()
        
        # Match Excel rows to DB
        for excel_row in excel_yes_rows:
            ingredient, os_env, os_layer, bio_subsystem = excel_row
            
            # Find matching DB rows
            matches = []
            for db_row in db_by_env.get(os_env, []):
                if ingredient_match(ingredient, db_row.get("genomax_ingredients"), db_row.get("ingredient_tags")):
                    matches.append(db_row)
            
            if len(matches) == 0:
                results["coverage"]["missing_in_db"].append({
                    "research_ingredient": ingredient,
                    "os_environment": os_env,
                    "os_layer": os_layer,
                    "biological_subsystem": bio_subsystem
                })
            elif len(matches) == 1:
                db_match = matches[0]
                matched_db_codes.add(db_match["module_code"])
                
                match_record = {
                    "excel_key": f"({os_env}, {ingredient})",
                    "db_module_code": db_match["module_code"],
                    "db_product_name": db_match["product_name"]
                }
                results["coverage"]["matched"].append(match_record)
                
                # Check field-by-field diffs
                diffs = []
                
                # os_layer comparison
                if normalize_text(os_layer) != normalize_text(db_match.get("os_layer", "")):
                    diffs.append({
                        "field": "os_layer",
                        "excel_value": os_layer,
                        "db_value": db_match.get("os_layer")
                    })
                
                # biological_subsystem vs biological_domain
                if normalize_text(bio_subsystem) != normalize_text(db_match.get("biological_domain", "")):
                    diffs.append({
                        "field": "biological_subsystem/domain",
                        "excel_value": bio_subsystem,
                        "db_value": db_match.get("biological_domain")
                    })
                
                if diffs:
                    results["field_diffs"].append({
                        "excel_key": f"({os_env}, {ingredient})",
                        "db_module_code": db_match["module_code"],
                        "diffs": diffs
                    })
            else:
                results["coverage"]["ambiguous_matches"].append({
                    "research_ingredient": ingredient,
                    "os_environment": os_env,
                    "match_count": len(matches),
                    "matching_modules": [m["module_code"] for m in matches]
                })
        
        # Find extra DB rows not matched to any Excel YES row
        for db_row in db_rows:
            if db_row["module_code"] not in matched_db_codes:
                results["coverage"]["extra_in_db"].append({
                    "module_code": db_row["module_code"],
                    "os_environment": db_row["os_environment"],
                    "product_name": db_row["product_name"],
                    "biological_domain": db_row["biological_domain"]
                })
        
        # Check for placeholders in DB (only existing columns)
        cur.execute("""
            SELECT module_code, os_environment, 
                   CASE 
                       WHEN COALESCE(suggested_use_full,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M' THEN 'suggested_use_full'
                       WHEN COALESCE(safety_notes,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M' THEN 'safety_notes'
                       WHEN COALESCE(contraindications,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M' THEN 'contraindications'
                       ELSE 'unknown'
                   END AS placeholder_field
            FROM os_modules_v3_1
            WHERE (supplier_status IS NULL OR supplier_status NOT IN ('DUPLICATE_INACTIVE'))
              AND (
                  COALESCE(suggested_use_full,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M'
                  OR COALESCE(safety_notes,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M'
                  OR COALESCE(contraindications,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M'
              )
            LIMIT 20
        """)
        placeholders = cur.fetchall()
        results["placeholders_in_db"] = [dict(p) for p in placeholders]
        
        cur.close()
        conn.close()
        
        # Calculate summary
        results["summary"] = {
            "excel_yes_count": len(excel_yes_rows),
            "matched_count": len(results["coverage"]["matched"]),
            "missing_in_db_count": len(results["coverage"]["missing_in_db"]),
            "ambiguous_match_count": len(results["coverage"]["ambiguous_matches"]),
            "extra_in_db_count": len(results["coverage"]["extra_in_db"]),
            "diff_count": len(results["field_diffs"]),
            "placeholders_count": len(results["placeholders_in_db"])
        }
        
        # Determine overall status
        if (results["summary"]["missing_in_db_count"] == 0 and 
            results["summary"]["ambiguous_match_count"] == 0 and
            results["summary"]["diff_count"] == 0 and
            results["summary"]["placeholders_count"] == 0):
            results["overall_status"] = "PASS"
            results["decision"] = "DB is 100% aligned with GenoMAX2_Catalog_Selection_v2_FINAL.xlsx for YES rows"
        else:
            results["overall_status"] = "FAIL"
            issues = []
            if results["summary"]["missing_in_db_count"] > 0:
                issues.append(f"{results['summary']['missing_in_db_count']} Excel YES rows missing in DB")
            if results["summary"]["ambiguous_match_count"] > 0:
                issues.append(f"{results['summary']['ambiguous_match_count']} ambiguous matches")
            if results["summary"]["diff_count"] > 0:
                issues.append(f"{results['summary']['diff_count']} field differences")
            if results["summary"]["placeholders_count"] > 0:
                issues.append(f"{results['summary']['placeholders_count']} placeholder texts in DB")
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
        "decision": full.get("decision", "")
    }
