"""
GenoMAX² QA Audit Module
Post-Migration Validation for os_modules_v3_1

Validates:
- A1-A4: Schema and uniqueness checks
- B1-B3: Core field validation
- D1-D2: OS-environment pairing
- E1-E2: New modules verification  
- F1: Index verification

Returns comprehensive JSON report with PASS/FAIL status.
"""

import os
import re
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException
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


@router.get("/audit/os-modules")
def audit_os_modules() -> Dict[str, Any]:
    """
    Complete QA audit for os_modules_v3_1 table.
    Returns PASS/FAIL status for each check category.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = {
        "audit_version": "1.0.0",
        "table": "os_modules_v3_1",
        "checks": {},
        "summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "warnings": 0
        },
        "overall_status": "PENDING"
    }
    
    try:
        cur = conn.cursor()
        
        # ========== PART A: Schema & Base DB Checks ==========
        
        # A1: Verify new columns exist
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'os_modules_v3_1'
              AND column_name IN ('net_quantity','supliful_handle')
            ORDER BY column_name
        """)
        a1_rows = cur.fetchall()
        a1_columns = [r["column_name"] for r in a1_rows]
        results["checks"]["A1_new_columns"] = {
            "description": "Verify net_quantity and supliful_handle columns exist",
            "expected": ["net_quantity", "supliful_handle"],
            "found": a1_columns,
            "status": "PASS" if len(a1_columns) == 2 else "FAIL"
        }
        
        # A2: Total module count
        cur.execute("SELECT COUNT(*) AS total FROM os_modules_v3_1")
        a2_count = cur.fetchone()["total"]
        results["checks"]["A2_total_modules"] = {
            "description": "Total module count (expected 210 after migration)",
            "expected": 210,
            "found": a2_count,
            "status": "PASS" if a2_count == 210 else "FAIL"
        }
        
        # A3: shopify_handle uniqueness
        cur.execute("""
            SELECT shopify_handle, COUNT(*) AS c
            FROM os_modules_v3_1
            GROUP BY shopify_handle
            HAVING COUNT(*) > 1
            ORDER BY c DESC, shopify_handle
            LIMIT 10
        """)
        a3_dups = cur.fetchall()
        results["checks"]["A3_shopify_handle_unique"] = {
            "description": "No duplicate shopify_handle values",
            "duplicates_found": len(a3_dups),
            "duplicates": [dict(r) for r in a3_dups] if a3_dups else [],
            "status": "PASS" if len(a3_dups) == 0 else "FAIL"
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
            "status": "PASS" if len(a4_dups) == 0 else "FAIL"
        }
        
        # ========== PART B: Core Field Validation ==========
        
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
            "status": "PASS" if len(invalid_envs) == 0 else "FAIL"
        }
        
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
            "status": "PASS" if len(b2_placeholders) == 0 else "FAIL"
        }
        
        # B3: Required fields check (READY_FOR_DESIGN gate)
        cur.execute("""
            SELECT
              SUM(CASE WHEN NOT (product_name IS NOT NULL AND TRIM(product_name) <> '') THEN 1 ELSE 0 END) AS missing_product_name,
              SUM(CASE WHEN NOT ((url IS NOT NULL AND TRIM(url) <> '') OR (supplier_page_url IS NOT NULL AND TRIM(supplier_page_url) <> '')) THEN 1 ELSE 0 END) AS missing_link,
              SUM(CASE WHEN NOT (net_quantity IS NOT NULL AND TRIM(net_quantity) <> '') THEN 1 ELSE 0 END) AS missing_net_quantity,
              SUM(CASE WHEN NOT (front_label_text IS NOT NULL AND TRIM(front_label_text) <> '') THEN 1 ELSE 0 END) AS missing_front_label,
              SUM(CASE WHEN NOT (back_label_text IS NOT NULL AND TRIM(back_label_text) <> '') THEN 1 ELSE 0 END) AS missing_back_label,
              SUM(CASE WHEN NOT (fda_disclaimer IS NOT NULL AND TRIM(fda_disclaimer) <> '') THEN 1 ELSE 0 END) AS missing_fda_disclaimer
            FROM os_modules_v3_1
        """)
        b3_summary = cur.fetchone()
        b3_all_zero = all(v == 0 for v in b3_summary.values())
        results["checks"]["B3_required_fields"] = {
            "description": "READY_FOR_DESIGN required fields populated",
            "missing_counts": dict(b3_summary),
            "status": "PASS" if b3_all_zero else "FAIL"
        }
        
        # B3b: Detailed list of modules missing required fields (limited)
        cur.execute("""
            SELECT shopify_handle, os_environment, module_code
            FROM os_modules_v3_1
            WHERE
              NOT (product_name IS NOT NULL AND TRIM(product_name) <> '')
              OR NOT ((url IS NOT NULL AND TRIM(url) <> '') OR (supplier_page_url IS NOT NULL AND TRIM(supplier_page_url) <> ''))
              OR NOT (net_quantity IS NOT NULL AND TRIM(net_quantity) <> '')
              OR NOT (front_label_text IS NOT NULL AND TRIM(front_label_text) <> '')
              OR NOT (back_label_text IS NOT NULL AND TRIM(back_label_text) <> '')
              OR NOT (fda_disclaimer IS NOT NULL AND TRIM(fda_disclaimer) <> '')
            ORDER BY shopify_handle, os_environment
            LIMIT 20
        """)
        b3b_missing = cur.fetchall()
        results["checks"]["B3_required_fields"]["modules_with_missing_fields"] = [
            {
                "shopify_handle": r["shopify_handle"],
                "os_environment": r["os_environment"],
                "module_code": r["module_code"]
            } for r in b3b_missing
        ]
        
        # ========== PART D: OS-Environment Pairing ==========
        
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
            "status": "INFO"  # Not necessarily a failure
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
            "status": "PASS" if d2_stats["over_paired"] == 0 else "WARNING"
        }
        
        # ========== PART E: New Modules Verification ==========
        
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
            "status": "PASS" if len(e1_new) == 8 else "FAIL"
        }
        
        # E2: New modules count verification
        results["checks"]["E2_new_modules_count"] = {
            "description": "Exactly 8 new modules inserted",
            "expected": 8,
            "found": len(e1_new),
            "status": "PASS" if len(e1_new) == 8 else "FAIL"
        }
        
        # ========== PART F: Index Verification ==========
        
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
            "status": "PASS" if f1_index else "FAIL"
        }
        
        cur.close()
        conn.close()
        
        # ========== Calculate Summary ==========
        for check_name, check_result in results["checks"].items():
            results["summary"]["total_checks"] += 1
            status = check_result.get("status", "UNKNOWN")
            if status == "PASS":
                results["summary"]["passed"] += 1
            elif status == "FAIL":
                results["summary"]["failed"] += 1
            elif status in ("WARNING", "INFO"):
                results["summary"]["warnings"] += 1
        
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
def audit_os_modules_summary() -> Dict[str, Any]:
    """
    Quick summary of os_modules_v3_1 audit status.
    """
    full_audit = audit_os_modules()
    return {
        "table": full_audit["table"],
        "overall_status": full_audit["overall_status"],
        "summary": full_audit["summary"],
        "failed_checks": [
            name for name, check in full_audit["checks"].items()
            if check.get("status") == "FAIL"
        ]
    }


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
              biological_domain
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
                "supliful_handle", "biological_domain"
            ],
            "data": [dict(r) for r in rows]
        }
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")
