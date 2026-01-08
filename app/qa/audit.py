"""
GenoMAX² QA Audit Module v2.4
Post-Migration Validation for os_modules_v3_1

SPLIT AUDIT MODES:
1. DB_INTEGRITY - Schema, uniqueness, environment values (should always PASS)
2. READY_FOR_DESIGN - Requires link/net_quantity/fda_disclaimer/no placeholders 
   (will FAIL until Supliful API integration)

Individual checks:
- A1-A4: Schema and uniqueness checks (DB_INTEGRITY)
- B1: os_environment validation (DB_INTEGRITY)
- B2: Placeholder check (READY_FOR_DESIGN)
- B3: Required fields check (READY_FOR_DESIGN) - NOW RESPECTS disclaimer_applicability
- D1-D2: OS-environment pairing (DB_INTEGRITY)
- E1-E2: New modules verification (DB_INTEGRITY)
- F1: Index verification (DB_INTEGRITY)

Returns comprehensive JSON report with PASS/FAIL status per mode.

v2.4 CHANGES:
- B3 now respects disclaimer_applicability:
  - SUPPLEMENT modules require fda_disclaimer
  - TOPICAL modules exempt from fda_disclaimer requirement
"""

import os
import re
from typing import Dict, Any, List, Optional, Union
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/qa", tags=["QA Audit"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


class FixDuplicatesRequest(BaseModel):
    shopify_handle: str
    keep_module_code: str


# Check classification by audit mode
DB_INTEGRITY_CHECKS = [
    "A1_new_columns",
    "A2_total_modules", 
    "A3_shopify_handle_unique",
    "A4_module_code_unique",
    "B1_os_environment_valid",
    "D1_single_environment_products",
    "D2_pairing_statistics",
    "E1_new_modules_list",
    "E2_new_modules_count",
    "F1_supliful_handle_index",
]

READY_FOR_DESIGN_CHECKS = [
    "B2_no_placeholders",
    "B3_required_fields",
]


# ============================================================================
# CLEAN COPY ENDPOINTS
# ============================================================================

@router.get("/copy/clean/count")
def clean_copy_count_only() -> Dict[str, Union[int, float]]:
    """
    Fast dashboard endpoint for clean copy metrics.
    
    Single optimized query - no examples, no heavy calculations.
    Returns all counts needed for dashboard display.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        # Single query that computes everything
        cur.execute("""
            SELECT 
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE front_label_text IS NOT NULL 
                      AND BTRIM(front_label_text) <> ''
                      AND back_label_text IS NOT NULL 
                      AND BTRIM(back_label_text) <> ''
                      AND front_label_text !~* '(TBD|MISSING|REVIEW|PLACEHOLDER)'
                      AND back_label_text !~* '(TBD|MISSING|REVIEW|PLACEHOLDER)'
                ) AS clean
            FROM os_modules_v3_1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        total = row["total"]
        clean = row["clean"]
        dirty = total - clean
        
        return {
            "total": total,
            "clean": clean,
            "dirty": dirty,
            "clean_pct": round(100 * clean / total, 1) if total > 0 else 0,
            "dirty_pct": round(100 * dirty / total, 1) if total > 0 else 0
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Clean copy count error: {str(e)}")


@router.get("/copy/clean/summary")
def clean_copy_summary() -> Dict[str, Any]:
    """
    Clean Copy Summary for os_modules_v3_1.
    
    Definition - A module has "clean copy" if:
    - front_label_text is NOT NULL and NOT empty
    - back_label_text is NOT NULL and NOT empty
    - Neither field contains placeholders (TBD|MISSING|REVIEW|PLACEHOLDER, case-insensitive)
    
    Returns counts with mathematical invariant validation.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Total modules
        cur.execute("SELECT COUNT(*) AS total FROM os_modules_v3_1")
        total_modules = cur.fetchone()["total"]
        
        # Missing front_label_text
        cur.execute("""
            SELECT COUNT(*) AS count FROM os_modules_v3_1
            WHERE front_label_text IS NULL OR BTRIM(front_label_text) = ''
        """)
        missing_front = cur.fetchone()["count"]
        
        # Missing back_label_text
        cur.execute("""
            SELECT COUNT(*) AS count FROM os_modules_v3_1
            WHERE back_label_text IS NULL OR BTRIM(back_label_text) = ''
        """)
        missing_back = cur.fetchone()["count"]
        
        # Placeholders in front OR back (only in non-empty fields)
        cur.execute("""
            SELECT COUNT(*) AS count FROM os_modules_v3_1
            WHERE (
                (front_label_text IS NOT NULL AND BTRIM(front_label_text) <> '' 
                 AND front_label_text ~* '(TBD|MISSING|REVIEW|PLACEHOLDER)')
                OR
                (back_label_text IS NOT NULL AND BTRIM(back_label_text) <> '' 
                 AND back_label_text ~* '(TBD|MISSING|REVIEW|PLACEHOLDER)')
            )
        """)
        placeholders_front_or_back = cur.fetchone()["count"]
        
        # Missing front OR back (union)
        cur.execute("""
            SELECT COUNT(*) AS count FROM os_modules_v3_1
            WHERE front_label_text IS NULL OR BTRIM(front_label_text) = ''
               OR back_label_text IS NULL OR BTRIM(back_label_text) = ''
        """)
        missing_front_or_back = cur.fetchone()["count"]
        
        # Overlap: missing AND has placeholders (modules counted in both categories)
        cur.execute("""
            SELECT COUNT(*) AS count FROM os_modules_v3_1
            WHERE (front_label_text IS NULL OR BTRIM(front_label_text) = ''
                   OR back_label_text IS NULL OR BTRIM(back_label_text) = '')
              AND (
                (front_label_text IS NOT NULL AND BTRIM(front_label_text) <> '' 
                 AND front_label_text ~* '(TBD|MISSING|REVIEW|PLACEHOLDER)')
                OR
                (back_label_text IS NOT NULL AND BTRIM(back_label_text) <> '' 
                 AND back_label_text ~* '(TBD|MISSING|REVIEW|PLACEHOLDER)')
              )
        """)
        overlap_missing_and_placeholders = cur.fetchone()["count"]
        
        # Clean copy count (direct query for accuracy)
        cur.execute("""
            SELECT COUNT(*) AS count FROM os_modules_v3_1
            WHERE front_label_text IS NOT NULL 
              AND BTRIM(front_label_text) <> ''
              AND back_label_text IS NOT NULL 
              AND BTRIM(back_label_text) <> ''
              AND front_label_text !~* '(TBD|MISSING|REVIEW|PLACEHOLDER)'
              AND back_label_text !~* '(TBD|MISSING|REVIEW|PLACEHOLDER)'
        """)
        clean_copy_count = cur.fetchone()["count"]
        
        # Get examples of modules with placeholders
        cur.execute("""
            SELECT module_code, shopify_handle, os_environment
            FROM os_modules_v3_1
            WHERE (
                (front_label_text IS NOT NULL AND BTRIM(front_label_text) <> '' 
                 AND front_label_text ~* '(TBD|MISSING|REVIEW|PLACEHOLDER)')
                OR
                (back_label_text IS NOT NULL AND BTRIM(back_label_text) <> '' 
                 AND back_label_text ~* '(TBD|MISSING|REVIEW|PLACEHOLDER)')
            )
            ORDER BY shopify_handle, os_environment
            LIMIT 10
        """)
        placeholder_examples = [dict(r) for r in cur.fetchall()]
        
        # Get examples of modules missing front or back
        cur.execute("""
            SELECT module_code, shopify_handle, os_environment,
                   CASE WHEN front_label_text IS NULL OR BTRIM(front_label_text) = '' THEN 'front' ELSE '' END AS missing_front,
                   CASE WHEN back_label_text IS NULL OR BTRIM(back_label_text) = '' THEN 'back' ELSE '' END AS missing_back
            FROM os_modules_v3_1
            WHERE front_label_text IS NULL OR BTRIM(front_label_text) = ''
               OR back_label_text IS NULL OR BTRIM(back_label_text) = ''
            ORDER BY shopify_handle, os_environment
            LIMIT 10
        """)
        missing_examples = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        # Calculate dirty count using inclusion-exclusion principle
        # dirty = missing_front_or_back + placeholders_front_or_back - overlap
        dirty_count = missing_front_or_back + placeholders_front_or_back - overlap_missing_and_placeholders
        
        # Validate invariant: clean + dirty = total
        invariant_pass = (clean_copy_count + dirty_count) == total_modules
        
        return {
            "table": "os_modules_v3_1",
            "total_modules": total_modules,
            "missing_front": missing_front,
            "missing_back": missing_back,
            "missing_front_or_back": missing_front_or_back,
            "placeholders_front_or_back": placeholders_front_or_back,
            "overlap_missing_and_placeholders": overlap_missing_and_placeholders,
            "dirty_count": dirty_count,
            "clean_copy_count": clean_copy_count,
            "clean_copy_percentage": round(100 * clean_copy_count / total_modules, 1) if total_modules > 0 else 0,
            "invariant_check": {
                "rule": "clean_copy_count + dirty_count = total_modules",
                "values": f"{clean_copy_count} + {dirty_count} = {total_modules}",
                "status": "PASS" if invariant_pass else "FAIL"
            },
            "examples": {
                "placeholder_examples": placeholder_examples,
                "missing_examples": missing_examples
            },
            "definition": {
                "clean_copy": "front_label_text and back_label_text both non-empty and without placeholders",
                "placeholders_pattern": "(TBD|MISSING|REVIEW|PLACEHOLDER) case-insensitive"
            }
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Clean copy audit error: {str(e)}")


# ============================================================================
# MAIN AUDIT ENDPOINTS
# ============================================================================

@router.get("/audit/os-modules")
def audit_os_modules(mode: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """
    Complete QA audit for os_modules_v3_1 table.
    
    Parameters:
    - mode: Optional filter for audit scope
      - "integrity" - Only DB integrity checks (should PASS)
      - "design" - Only ready-for-design checks (may FAIL until Supliful API)
      - None/omitted - All checks
    
    Returns PASS/FAIL status for each check category.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = {
        "audit_version": "2.4.0",
        "table": "os_modules_v3_1",
        "mode": mode or "all",
        "checks": {},
        "summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "warnings": 0
        },
        "integrity_summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "status": "PENDING"
        },
        "design_summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "status": "PENDING"
        },
        "overall_status": "PENDING"
    }
    
    try:
        cur = conn.cursor()
        
        # ========== PART A: Schema & Base DB Checks (DB_INTEGRITY) ==========
        
        if mode in (None, "integrity"):
            # A1: Verify new columns exist
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'os_modules_v3_1'
                  AND column_name IN ('net_quantity','supliful_handle','disclaimer_symbol','disclaimer_applicability')
                ORDER BY column_name
            """)
            a1_rows = cur.fetchall()
            a1_columns = [r["column_name"] for r in a1_rows]
            results["checks"]["A1_new_columns"] = {
                "description": "Verify required columns exist",
                "expected": ["disclaimer_applicability", "disclaimer_symbol", "net_quantity", "supliful_handle"],
                "found": a1_columns,
                "status": "PASS" if len(a1_columns) == 4 else "FAIL",
                "mode": "DB_INTEGRITY"
            }
            
            # A2: Total module count
            cur.execute("SELECT COUNT(*) AS total FROM os_modules_v3_1")
            a2_count = cur.fetchone()["total"]
            results["checks"]["A2_total_modules"] = {
                "description": "Total module count (expected 210 after migration)",
                "expected": 210,
                "found": a2_count,
                "status": "PASS" if a2_count == 210 else "FAIL",
                "mode": "DB_INTEGRITY"
            }
            
            # A3: shopify_handle uniqueness (excluding DUPLICATE_INACTIVE)
            cur.execute("""
                SELECT shopify_handle, COUNT(*) AS c
                FROM os_modules_v3_1
                WHERE supplier_status IS NULL OR supplier_status != 'DUPLICATE_INACTIVE'
                GROUP BY shopify_handle
                HAVING COUNT(*) > 1
                ORDER BY c DESC, shopify_handle
                LIMIT 10
            """)
            a3_dups = cur.fetchall()
            results["checks"]["A3_shopify_handle_unique"] = {
                "description": "No duplicate shopify_handle values (excluding DUPLICATE_INACTIVE)",
                "duplicates_found": len(a3_dups),
                "duplicates": [dict(r) for r in a3_dups] if a3_dups else [],
                "status": "PASS" if len(a3_dups) == 0 else "FAIL",
                "mode": "DB_INTEGRITY"
            }
            
            # A4: module_code uniqueness
            cur.execute("""
                SELECT module_code, COUNT(*) AS c
                FROM os_modules_v3_1
                GROUP BY module_code
                HAVING COUNT(*) > 1
                ORDER BY c DESC, module_code
                LIMIT 10
            """)
            a4_dups = cur.fetchall()
            results["checks"]["A4_module_code_unique"] = {
                "description": "No duplicate module_code values",
                "duplicates_found": len(a4_dups),
                "duplicates": [dict(r) for r in a4_dups] if a4_dups else [],
                "status": "PASS" if len(a4_dups) == 0 else "FAIL",
                "mode": "DB_INTEGRITY"
            }
            
            # B1: os_environment valid values
            cur.execute("""
                SELECT os_environment, COUNT(*) AS count
                FROM os_modules_v3_1
                GROUP BY os_environment
                ORDER BY COUNT(*) DESC
            """)
            b1_envs = cur.fetchall()
            b1_env_names = [r["os_environment"] for r in b1_envs]
            valid_envs = {"MAXimo²", "MAXima²"}
            invalid_envs = [e for e in b1_env_names if e not in valid_envs]
            results["checks"]["B1_os_environment_valid"] = {
                "description": "os_environment contains only MAXimo² and MAXima²",
                "expected": list(valid_envs),
                "found": [dict(r) for r in b1_envs],
                "invalid_values": invalid_envs,
                "status": "PASS" if len(invalid_envs) == 0 else "FAIL",
                "mode": "DB_INTEGRITY"
            }
        
        # ========== PART B2-B3: Design Readiness Checks (READY_FOR_DESIGN) ==========
        
        if mode in (None, "design"):
            # B2: Placeholder check
            cur.execute("""
                SELECT shopify_handle, os_environment, module_code,
                       front_label_text, back_label_text, fda_disclaimer, net_quantity
                FROM os_modules_v3_1
                WHERE
                  COALESCE(front_label_text,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M'
                  OR COALESCE(back_label_text,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M'
                  OR COALESCE(fda_disclaimer,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M'
                  OR COALESCE(net_quantity,'') ~* '\\m(TBD|MISSING|REVIEW|PLACEHOLDER)\\M'
                ORDER BY shopify_handle, os_environment
                LIMIT 50
            """)
            b2_placeholders = cur.fetchall()
            results["checks"]["B2_no_placeholders"] = {
                "description": "No TBD/MISSING/REVIEW/PLACEHOLDER in text fields",
                "placeholders_found": len(b2_placeholders),
                "examples": [
                    {
                        "shopify_handle": r["shopify_handle"],
                        "os_environment": r["os_environment"],
                        "module_code": r["module_code"]
                    } for r in b2_placeholders[:10]
                ],
                "status": "PASS" if len(b2_placeholders) == 0 else "FAIL",
                "mode": "READY_FOR_DESIGN",
                "note": "Expected to FAIL until Supliful API populates fields"
            }
            
            # B3: Required fields check (READY_FOR_DESIGN gate)
            # NOW RESPECTS disclaimer_applicability:
            # - SUPPLEMENT modules require fda_disclaimer
            # - TOPICAL modules exempt from fda_disclaimer
            cur.execute("""
                SELECT
                  SUM(CASE WHEN NOT (product_name IS NOT NULL AND BTRIM(product_name) <> '') THEN 1 ELSE 0 END) AS missing_product_name,
                  SUM(CASE WHEN NOT ((url IS NOT NULL AND BTRIM(url) <> '') OR (supplier_page_url IS NOT NULL AND BTRIM(supplier_page_url) <> '')) THEN 1 ELSE 0 END) AS missing_link,
                  SUM(CASE WHEN NOT (net_quantity IS NOT NULL AND BTRIM(net_quantity) <> '') THEN 1 ELSE 0 END) AS missing_net_quantity,
                  SUM(CASE WHEN NOT (front_label_text IS NOT NULL AND BTRIM(front_label_text) <> '') THEN 1 ELSE 0 END) AS missing_front_label,
                  SUM(CASE WHEN NOT (back_label_text IS NOT NULL AND BTRIM(back_label_text) <> '') THEN 1 ELSE 0 END) AS missing_back_label,
                  SUM(CASE 
                    WHEN disclaimer_applicability = 'SUPPLEMENT' 
                         AND NOT (fda_disclaimer IS NOT NULL AND BTRIM(fda_disclaimer) <> '') 
                    THEN 1 
                    ELSE 0 
                  END) AS missing_fda_disclaimer_supplement,
                  SUM(CASE WHEN disclaimer_applicability = 'TOPICAL' THEN 1 ELSE 0 END) AS topical_count
                FROM os_modules_v3_1
            """)
            b3_summary = cur.fetchone()
            # Check all required fields (fda_disclaimer only for SUPPLEMENT)
            b3_all_zero = (
                b3_summary["missing_product_name"] == 0 and
                b3_summary["missing_link"] == 0 and
                b3_summary["missing_net_quantity"] == 0 and
                b3_summary["missing_front_label"] == 0 and
                b3_summary["missing_back_label"] == 0 and
                b3_summary["missing_fda_disclaimer_supplement"] == 0
            )
            results["checks"]["B3_required_fields"] = {
                "description": "READY_FOR_DESIGN required fields populated (respects disclaimer_applicability)",
                "missing_counts": {
                    "missing_product_name": b3_summary["missing_product_name"],
                    "missing_link": b3_summary["missing_link"],
                    "missing_net_quantity": b3_summary["missing_net_quantity"],
                    "missing_front_label": b3_summary["missing_front_label"],
                    "missing_back_label": b3_summary["missing_back_label"],
                    "missing_fda_disclaimer_supplement": b3_summary["missing_fda_disclaimer_supplement"],
                },
                "topical_modules_exempt": b3_summary["topical_count"],
                "status": "PASS" if b3_all_zero else "FAIL",
                "mode": "READY_FOR_DESIGN",
                "note": "TOPICAL modules exempt from fda_disclaimer requirement"
            }
            
            # B3b: Detailed list of modules missing required fields (limited)
            cur.execute("""
                SELECT shopify_handle, os_environment, module_code, disclaimer_applicability
                FROM os_modules_v3_1
                WHERE
                  NOT (product_name IS NOT NULL AND BTRIM(product_name) <> '')
                  OR NOT ((url IS NOT NULL AND BTRIM(url) <> '') OR (supplier_page_url IS NOT NULL AND BTRIM(supplier_page_url) <> ''))
                  OR NOT (net_quantity IS NOT NULL AND BTRIM(net_quantity) <> '')
                  OR NOT (front_label_text IS NOT NULL AND BTRIM(front_label_text) <> '')
                  OR NOT (back_label_text IS NOT NULL AND BTRIM(back_label_text) <> '')
                  OR (disclaimer_applicability = 'SUPPLEMENT' AND NOT (fda_disclaimer IS NOT NULL AND BTRIM(fda_disclaimer) <> ''))
                ORDER BY shopify_handle, os_environment
                LIMIT 20
            """)
            b3b_missing = cur.fetchall()
            results["checks"]["B3_required_fields"]["modules_with_missing_fields"] = [
                {
                    "shopify_handle": r["shopify_handle"],
                    "os_environment": r["os_environment"],
                    "module_code": r["module_code"],
                    "disclaimer_applicability": r["disclaimer_applicability"]
                } for r in b3b_missing
            ]
        
        # ========== PART D: OS-Environment Pairing (DB_INTEGRITY) ==========
        
        if mode in (None, "integrity"):
            # D1: Single-environment products
            cur.execute("""
                SELECT supliful_handle, COUNT(DISTINCT os_environment) AS env_count,
                       STRING_AGG(DISTINCT os_environment, ', ') AS environments
                FROM os_modules_v3_1
                WHERE supliful_handle IS NOT NULL
                GROUP BY supliful_handle
                HAVING COUNT(DISTINCT os_environment) = 1
                ORDER BY supliful_handle
            """)
            d1_single = cur.fetchall()
            results["checks"]["D1_single_environment_products"] = {
                "description": "Products with only one os_environment (may be intentional)",
                "count": len(d1_single),
                "products": [dict(r) for r in d1_single],
                "status": "INFO",
                "mode": "DB_INTEGRITY"
            }
            
            # D2: Pairing statistics
            cur.execute("""
                SELECT 
                  COUNT(*) FILTER (WHERE env_count = 2) AS properly_paired,
                  COUNT(*) FILTER (WHERE env_count = 1) AS single_env,
                  COUNT(*) FILTER (WHERE env_count > 2) AS over_paired
                FROM (
                  SELECT supliful_handle, COUNT(DISTINCT os_environment) AS env_count
                  FROM os_modules_v3_1
                  WHERE supliful_handle IS NOT NULL
                  GROUP BY supliful_handle
                ) sub
            """)
            d2_stats = cur.fetchone()
            results["checks"]["D2_pairing_statistics"] = {
                "description": "Product environment pairing summary",
                "properly_paired": d2_stats["properly_paired"],
                "single_environment": d2_stats["single_env"],
                "over_paired": d2_stats["over_paired"],
                "status": "PASS" if d2_stats["over_paired"] == 0 else "WARNING",
                "mode": "DB_INTEGRITY"
            }
            
            # ========== PART E: New Modules Verification (DB_INTEGRITY) ==========
            
            # E1: New modules list
            cur.execute("""
                SELECT module_code, shopify_handle, os_environment, os_layer, 
                       biological_domain, supliful_handle
                FROM os_modules_v3_1
                WHERE module_code LIKE 'NEW-%'
                ORDER BY supliful_handle, os_environment
            """)
            e1_new = cur.fetchall()
            results["checks"]["E1_new_modules_list"] = {
                "description": "Modules inserted during migration (module_code LIKE 'NEW-%')",
                "count": len(e1_new),
                "expected_count": 8,
                "modules": [dict(r) for r in e1_new],
                "status": "PASS" if len(e1_new) == 8 else "FAIL",
                "mode": "DB_INTEGRITY"
            }
            
            # E2: New modules count verification
            results["checks"]["E2_new_modules_count"] = {
                "description": "Exactly 8 new modules inserted",
                "expected": 8,
                "found": len(e1_new),
                "status": "PASS" if len(e1_new) == 8 else "FAIL",
                "mode": "DB_INTEGRITY"
            }
            
            # ========== PART F: Index Verification (DB_INTEGRITY) ==========
            
            # F1: supliful_handle index
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'os_modules_v3_1'
                  AND indexname = 'idx_os_modules_v3_1_supliful_handle'
            """)
            f1_index = cur.fetchone()
            results["checks"]["F1_supliful_handle_index"] = {
                "description": "Index idx_os_modules_v3_1_supliful_handle exists",
                "found": dict(f1_index) if f1_index else None,
                "status": "PASS" if f1_index else "FAIL",
                "mode": "DB_INTEGRITY"
            }
        
        cur.close()
        conn.close()
        
        # ========== Calculate Summary by Mode ==========
        for check_name, check_result in results["checks"].items():
            results["summary"]["total_checks"] += 1
            status = check_result.get("status", "UNKNOWN")
            check_mode = check_result.get("mode", "UNKNOWN")
            
            if status == "PASS":
                results["summary"]["passed"] += 1
            elif status == "FAIL":
                results["summary"]["failed"] += 1
            elif status in ("WARNING", "INFO"):
                results["summary"]["warnings"] += 1
            
            # Track by mode
            if check_mode == "DB_INTEGRITY":
                results["integrity_summary"]["total_checks"] += 1
                if status == "PASS":
                    results["integrity_summary"]["passed"] += 1
                elif status == "FAIL":
                    results["integrity_summary"]["failed"] += 1
            elif check_mode == "READY_FOR_DESIGN":
                results["design_summary"]["total_checks"] += 1
                if status == "PASS":
                    results["design_summary"]["passed"] += 1
                elif status == "FAIL":
                    results["design_summary"]["failed"] += 1
        
        # Mode-specific status
        if results["integrity_summary"]["total_checks"] > 0:
            results["integrity_summary"]["status"] = (
                "PASS" if results["integrity_summary"]["failed"] == 0 else "FAIL"
            )
        
        if results["design_summary"]["total_checks"] > 0:
            results["design_summary"]["status"] = (
                "PASS" if results["design_summary"]["failed"] == 0 else "FAIL"
            )
        
        # Overall status
        if results["summary"]["failed"] == 0:
            results["overall_status"] = "PASS"
        else:
            results["overall_status"] = "FAIL"
        
        return results
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Audit error: {str(e)}")


@router.get("/audit/os-modules/summary")
def audit_os_modules_summary(mode: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """
    Quick summary of os_modules_v3_1 audit status.
    
    Parameters:
    - mode: "integrity" or "design" to filter, None for all
    """
    full_audit = audit_os_modules(mode=mode)
    
    failed_integrity = [
        name for name, check in full_audit["checks"].items()
        if check.get("status") == "FAIL" and check.get("mode") == "DB_INTEGRITY"
    ]
    
    failed_design = [
        name for name, check in full_audit["checks"].items()
        if check.get("status") == "FAIL" and check.get("mode") == "READY_FOR_DESIGN"
    ]
    
    return {
        "table": full_audit["table"],
        "audit_version": full_audit["audit_version"],
        "mode": full_audit["mode"],
        "overall_status": full_audit["overall_status"],
        "integrity_status": full_audit["integrity_summary"]["status"],
        "design_status": full_audit["design_summary"]["status"],
        "summary": full_audit["summary"],
        "integrity_summary": full_audit["integrity_summary"],
        "design_summary": full_audit["design_summary"],
        "failed_integrity_checks": failed_integrity,
        "failed_design_checks": failed_design,
        "note": "DB_INTEGRITY should PASS. READY_FOR_DESIGN expected to FAIL until Supliful API integration."
    }


@router.get("/audit/os-modules/integrity")
def audit_integrity_only() -> Dict[str, Any]:
    """
    Run only DB integrity checks (schema, uniqueness, pairing).
    These should always PASS after successful migration.
    """
    return audit_os_modules(mode="integrity")


@router.get("/audit/os-modules/design")
def audit_design_only() -> Dict[str, Any]:
    """
    Run only ready-for-design checks (placeholders, required fields).
    Expected to FAIL until Supliful API integration provides missing data.
    """
    return audit_os_modules(mode="design")


@router.get("/audit/os-modules/duplicates/{shopify_handle}")
def get_duplicate_details(shopify_handle: str) -> Dict[str, Any]:
    """
    Get full details of duplicate shopify_handle records.
    Use this to investigate and decide which record to keep.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT module_code, product_name, shopify_handle, os_environment, 
                   os_layer, biological_domain, supliful_handle, supplier_status,
                   supplier_page_url, url, created_at, updated_at
            FROM os_modules_v3_1
            WHERE shopify_handle = %s
            ORDER BY
              (supplier_page_url IS NOT NULL AND BTRIM(supplier_page_url) <> '') DESC,
              (url IS NOT NULL AND BTRIM(url) <> '') DESC,
              (product_name IS NOT NULL AND BTRIM(product_name) <> '') DESC,
              updated_at DESC
        """, (shopify_handle,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "shopify_handle": shopify_handle,
            "count": len(rows),
            "recommendation": rows[0]["module_code"] if rows else None,
            "records": [dict(r) for r in rows]
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@router.post("/audit/os-modules/fix-duplicates")
def fix_duplicates(request: FixDuplicatesRequest) -> Dict[str, Any]:
    """
    Mark duplicate shopify_handle records as DUPLICATE_INACTIVE.
    Keeps the specified module_code active, marks others as inactive.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # First verify the keep_module_code exists for this shopify_handle
        cur.execute("""
            SELECT module_code FROM os_modules_v3_1
            WHERE shopify_handle = %s AND module_code = %s
        """, (request.shopify_handle, request.keep_module_code))
        
        if not cur.fetchone():
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=404, 
                detail=f"Module {request.keep_module_code} not found for shopify_handle {request.shopify_handle}"
            )
        
        # Mark all other records as DUPLICATE_INACTIVE
        cur.execute("""
            UPDATE os_modules_v3_1
            SET supplier_status = 'DUPLICATE_INACTIVE',
                updated_at = NOW()
            WHERE shopify_handle = %s
              AND module_code != %s
            RETURNING module_code, supplier_status
        """, (request.shopify_handle, request.keep_module_code))
        
        updated_rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "shopify_handle": request.shopify_handle,
            "kept_module_code": request.keep_module_code,
            "marked_inactive": [dict(r) for r in updated_rows],
            "inactive_count": len(updated_rows)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Fix error: {str(e)}")


@router.get("/audit/os-modules/export")
def audit_os_modules_export() -> Dict[str, Any]:
    """
    Export os_modules_v3_1 data for Designer View comparison.
    Returns all modules with fields needed for Excel comparison.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
              module_code,
              shopify_handle,
              product_name,
              COALESCE(url, supplier_page_url) AS product_link,
              os_environment,
              os_layer,
              net_quantity AS net_quantity_label,
              front_label_text,
              back_label_text,
              fda_disclaimer,
              supliful_handle,
              biological_domain,
              disclaimer_applicability,
              disclaimer_symbol
            FROM os_modules_v3_1
            ORDER BY shopify_handle, os_environment
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "table": "os_modules_v3_1",
            "row_count": len(rows),
            "columns": [
                "module_code", "shopify_handle", "product_name", "product_link",
                "os_environment", "os_layer", "net_quantity_label",
                "front_label_text", "back_label_text", "fda_disclaimer",
                "supliful_handle", "biological_domain",
                "disclaimer_applicability", "disclaimer_symbol"
            ],
            "data": [dict(r) for r in rows]
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")
