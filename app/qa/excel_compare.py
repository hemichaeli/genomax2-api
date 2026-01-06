"""
GenoMAX² QA Compare Module v7.1
Batch-aware comparison using payload snapshots from override batches

CHANGES FROM v7:
- Added excluded/attempted counts to summary
- Added invariant_check for mathematical consistency validation
- Rule 1: attempted + excluded = payload_records
- Rule 2: matched_in_db + not_found_in_db = attempted

SOURCE OF TRUTH:
- For batch comparison: catalog_override_payload_snapshot_v1 table
- DB values compared against persisted payload from specific batch
- Same batch_id -> same payload -> deterministic comparison
"""

import os
import re
from typing import Dict, Any, List, Optional, Set, Tuple
from fastapi import APIRouter, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/qa", tags=["QA Compare"])

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


# Authoritative fields for comparison (must match OVERRIDE_FIELDS in override module)
AUTHORITATIVE_FIELDS = [
    'os_layer',
    'biological_domain',
    'suggested_use_full',
    'safety_notes',
    'contraindications',
    'dosing_protocol',
]


# ============================================================================
# BATCH-AWARE COMPARISON (Primary QA endpoint)
# ============================================================================

@router.get("/compare/batch/{batch_id}")
def compare_batch(batch_id: str) -> Dict[str, Any]:
    """
    Compare DB against payload snapshot from a specific override batch.
    This is the authoritative QA comparison - uses persisted payload, not hardcoded data.
    
    Invariants:
    - attempted + excluded = payload_records
    - matched_in_db + not_found_in_db = attempted
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Verify batch exists
        cur.execute("""
            SELECT status, payload_count, modules_updated, completed_at
            FROM catalog_override_batch_v1
            WHERE batch_id = %s
        """, (batch_id,))
        batch_meta = cur.fetchone()
        
        if not batch_meta:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")
        
        # Get payload snapshot
        cur.execute("""
            SELECT supliful_sku, os_environment, shopify_handle,
                   os_layer, biological_domain, suggested_use_full,
                   safety_notes, contraindications, dosing_protocol
            FROM catalog_override_payload_snapshot_v1
            WHERE batch_id = %s
        """, (batch_id,))
        payload_rows = cur.fetchall()
        
        if not payload_rows:
            return {
                "batch_id": batch_id,
                "overall_status": "NO_PAYLOAD",
                "message": "No payload snapshot found for this batch"
            }
        
        # Get current DB values
        cur.execute("""
            SELECT module_code, shopify_handle, os_environment,
                   os_layer, biological_domain, suggested_use_full,
                   safety_notes, contraindications, dosing_protocol
            FROM os_modules_v3_1
            WHERE supplier_status IS NULL 
               OR supplier_status NOT IN ('DUPLICATE_INACTIVE')
        """)
        db_rows = cur.fetchall()
        
        db_by_key = {}
        for row in db_rows:
            key = (row['shopify_handle'], row['os_environment'])
            db_by_key[key] = dict(row)
        
        # Compare payload against DB
        excluded = 0      # Records without shopify_handle (no DB mapping)
        attempted = 0     # Records with handle that we tried to compare
        matched = 0       # Found in DB
        not_found = []    # Not found in DB
        diffs = []
        field_diff_counts = {f: 0 for f in AUTHORITATIVE_FIELDS}
        
        for payload_row in payload_rows:
            sku = payload_row['supliful_sku']
            env = payload_row['os_environment']
            handle = payload_row['shopify_handle']
            
            if not handle:
                # Skip records without handle mapping
                excluded += 1
                continue
            
            attempted += 1
            key = (handle, env)
            
            if key not in db_by_key:
                not_found.append({
                    'supliful_sku': sku,
                    'os_environment': env,
                    'expected_handle': handle
                })
                continue
            
            db_row = db_by_key[key]
            matched += 1
            
            # Compare authoritative fields
            record_diffs = []
            for field in AUTHORITATIVE_FIELDS:
                payload_value = normalize_text(payload_row.get(field) or '')
                db_value = normalize_text(db_row.get(field) or '')
                
                if payload_value != db_value:
                    record_diffs.append({
                        'field': field,
                        'payload_value': (payload_row.get(field) or '')[:100],
                        'db_value': (db_row.get(field) or '')[:100]
                    })
                    field_diff_counts[field] += 1
            
            if record_diffs:
                diffs.append({
                    'supliful_sku': sku,
                    'os_environment': env,
                    'shopify_handle': handle,
                    'module_code': db_row['module_code'],
                    'diffs': record_diffs
                })
        
        cur.close()
        conn.close()
        
        total_diffs = sum(field_diff_counts.values())
        payload_records = len(payload_rows)
        not_found_count = len(not_found)
        
        # Validate invariants
        invariant1_pass = (attempted + excluded) == payload_records
        invariant2_pass = (matched + not_found_count) == attempted
        
        invariant_check = {
            "rules": [
                {
                    "name": "attempted + excluded = payload_records",
                    "values": f"{attempted} + {excluded} = {payload_records}",
                    "status": "PASS" if invariant1_pass else "FAIL"
                },
                {
                    "name": "matched_in_db + not_found_in_db = attempted",
                    "values": f"{matched} + {not_found_count} = {attempted}",
                    "status": "PASS" if invariant2_pass else "FAIL"
                }
            ],
            "overall_status": "PASS" if (invariant1_pass and invariant2_pass) else "FAIL"
        }
        
        # Determine overall status
        if total_diffs == 0 and not_found_count == 0:
            overall_status = "PASS"
            decision = "DB is 100% aligned with override payload for all authoritative fields"
        else:
            overall_status = "FAIL"
            issues = []
            if total_diffs > 0:
                issues.append(f"{total_diffs} field differences across {len(diffs)} records")
            if not_found_count > 0:
                issues.append(f"{not_found_count} handles not found in DB")
            decision = f"Discrepancies found: {'; '.join(issues)}"
        
        return {
            "batch_id": batch_id,
            "batch_status": batch_meta['status'],
            "batch_completed_at": str(batch_meta['completed_at']) if batch_meta['completed_at'] else None,
            "overall_status": overall_status,
            "decision": decision,
            "summary": {
                "payload_records": payload_records,
                "excluded": excluded,
                "attempted": attempted,
                "matched_in_db": matched,
                "not_found_in_db": not_found_count,
                "records_with_diffs": len(diffs),
                "total_field_diffs": total_diffs
            },
            "invariant_check": invariant_check,
            "field_diff_counts": field_diff_counts,
            "authoritative_fields": AUTHORITATIVE_FIELDS,
            "not_found": not_found[:10],
            "diff_samples": diffs[:20]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Comparison error: {str(e)}")


@router.get("/compare/batch/{batch_id}/summary")
def compare_batch_summary(batch_id: str) -> Dict[str, Any]:
    """Quick summary of batch comparison."""
    full = compare_batch(batch_id)
    return {
        "batch_id": full["batch_id"],
        "overall_status": full["overall_status"],
        "decision": full["decision"],
        "summary": full["summary"],
        "invariant_check": full["invariant_check"],
        "field_diff_counts": full["field_diff_counts"]
    }


@router.get("/compare/latest-batch")
def compare_latest_batch() -> Dict[str, Any]:
    """Compare DB against the most recent completed override batch."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT batch_id
            FROM catalog_override_batch_v1
            WHERE status = 'COMPLETED'
            ORDER BY completed_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if not row:
            return {
                "overall_status": "NO_BATCH",
                "message": "No completed override batches found"
            }
        
        return compare_batch(row['batch_id'])
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Latest batch lookup error: {str(e)}")


# ============================================================================
# LEGACY ENDPOINTS (Deprecated - use batch-aware endpoints instead)
# ============================================================================

# Gender-specific exclusions
GENDER_SPECIFIC_EXCLUSIONS = {
    ("mens-vitality-tablets", "MAXima²"),
}

# Products not in DB
EXCEL_PRODUCTS_NOT_IN_DB = {
    "kojic-acid-turmeric-soap",
    "moisturizing-strengthening-hair-oil-old",
    "green-tea-antioxidant-serum",
    "vitamin-glow-serum",
    "vitamin-c-serum",
    "recovery-cream",
    "peptide-hair-growth-serum",
}

# SKU to handle mapping
EXCEL_SKU_TO_DB_BASE = {
    "appetite-balance-weight-support-strips": "appetite-balance-weight-support-strips",
    "beetroot-capsules": "beetroot-capsules",
    "diet-drops-ultra": "diet-drops-ultra",
    "energy-powder-cotton-candy": "energy-powder-cotton-candy",
    "focus-powder-sour-candy": "focus-powder-sour-candy",
    "iron-strips": "iron-strips",
    "l-glutamine-powder": "l-glutamine-powder",
    "multivitamin-bear-gummies-adult": "multivitamin-bear-gummies-adult",
    "mushroom-coffee-fusion-lions-mane-chaga-16oz": "mushroom-coffee-fusion-lions-mane-chaga-16oz",
    "ashwagandha-capsules": "ashwagandha",
    "berberine-capsules": "berberine",
    "cognitive-support-capsules": "cognitive-support",
    "colon-gentle-cleanse-sachets": "colon-gentle-cleanse",
    "complete-multivitamin-capsules": "complete-multivitamin",
    "coq10-ubiquinone-capsules": "coq10-ubiquinone",
    "creatine-monohydrate-powder": "creatine-monohydrate",
    "joint-support-capsules": "joint-support",
    "keto-5-capsules": "keto-5",
    "liver-support-capsules": "liver-support",
    "maca-plus-capsules": "maca-plus",
    "magnesium-glycinate-capsules": "magnesium-glycinate",
    "nitric-oxide-capsules": "nitric-oxide",
    "platinum-turmeric-capsules": "platinum-turmeric",
    "sleep-formula-capsules": "sleep-formula",
    "vision-support-capsules": "vision-support",
    "omega-3-epa-dha-softgel-capsules": "omega-3-epa-180mg-dha-120mg",
    "vitamin-d3-2000iu-softgel-capsules": "vitamin-d3-2-000-iu",
    "nad-plus-capsules": "nad",
    "probiotic-40-billion-prebiotics-capsules": "probiotic-40-billion-with-prebiotics",
    "resveratrol-50-percent-capsules": "resveratrol-50-600mg",
    "max-detox-acai-capsules": "max-detox-acai-detox",
    "mens-vitality-tablets": "men-s-vitality",
}


@router.get("/compare/excel-db-summary")
def compare_excel_db_summary(batch_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """
    Quick summary of Excel vs DB comparison.
    
    If batch_id is provided, uses batch-aware comparison (recommended).
    Otherwise falls back to legacy hardcoded comparison (deprecated).
    """
    if batch_id:
        # Use batch-aware comparison
        return compare_batch_summary(batch_id)
    
    # Legacy: redirect to latest batch if available
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT batch_id FROM catalog_override_batch_v1
                WHERE status = 'COMPLETED'
                ORDER BY completed_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                result = compare_batch_summary(row['batch_id'])
                result["note"] = "Using latest completed batch. For specific batch, add ?batch_id=<uuid>"
                return result
        except:
            pass
    
    # Fallback to legacy (will show stale data warning)
    return {
        "overall_status": "DEPRECATED",
        "message": "Legacy comparison uses stale hardcoded data. Run an override batch first, then use ?batch_id=<uuid>",
        "hint": "GET /api/v1/qa/compare/latest-batch for most recent batch comparison"
    }


@router.get("/compare/excel-db-full")
def compare_excel_db_full(batch_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """
    Full comparison of DB against Excel/payload.
    
    If batch_id is provided, uses batch-aware comparison.
    """
    if batch_id:
        return compare_batch(batch_id)
    
    # Try latest batch
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT batch_id FROM catalog_override_batch_v1
                WHERE status = 'COMPLETED'
                ORDER BY completed_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                result = compare_batch(row['batch_id'])
                result["note"] = "Using latest completed batch"
                return result
        except:
            pass
    
    return {
        "overall_status": "DEPRECATED",
        "message": "No batch available. Run override first.",
        "hint": "POST /api/v1/catalog/override/execute?confirm=true with Excel file"
    }


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
    """Debug endpoint for handle mappings."""
    return {
        "message": "Use /api/v1/qa/compare/batch/{batch_id} for authoritative comparison",
        "mappings_count": len(EXCEL_SKU_TO_DB_BASE),
        "exclusions_count": len(EXCEL_PRODUCTS_NOT_IN_DB),
        "gender_exclusions_count": len(GENDER_SPECIFIC_EXCLUSIONS)
    }


@router.get("/compare/mapping-check")
def check_mapping() -> Dict[str, Any]:
    """Check SKU-to-handle mapping coverage."""
    return {
        "mapped_skus": len(EXCEL_SKU_TO_DB_BASE),
        "not_in_db_skus": len(EXCEL_PRODUCTS_NOT_IN_DB),
        "gender_excluded_pairs": len(GENDER_SPECIFIC_EXCLUSIONS),
        "note": "For authoritative comparison, use /compare/batch/{batch_id}"
    }


@router.get("/compare/missing-skus")
def get_missing_skus(batch_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """List handles from payload not found in DB."""
    if batch_id:
        full = compare_batch(batch_id)
        return {
            "batch_id": batch_id,
            "missing_count": len(full.get("not_found", [])),
            "missing": full.get("not_found", [])
        }
    return {"message": "Provide batch_id parameter"}


@router.get("/compare/expected-not-in-db")
def get_expected_not_in_db() -> Dict[str, Any]:
    """List products known to not exist in DB."""
    return {
        "count": len(EXCEL_PRODUCTS_NOT_IN_DB),
        "products": sorted(EXCEL_PRODUCTS_NOT_IN_DB)
    }


@router.get("/compare/extra-db-modules")
def get_extra_db_modules() -> Dict[str, Any]:
    """List DB modules not covered by any override batch."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get all DB handles
        cur.execute("""
            SELECT DISTINCT shopify_handle, os_environment, module_code, product_name
            FROM os_modules_v3_1
            WHERE supplier_status IS NULL 
               OR supplier_status NOT IN ('DUPLICATE_INACTIVE')
        """)
        db_rows = cur.fetchall()
        
        # Get all payload handles from latest batch
        cur.execute("""
            SELECT DISTINCT shopify_handle, os_environment
            FROM catalog_override_payload_snapshot_v1
            WHERE batch_id = (
                SELECT batch_id FROM catalog_override_batch_v1
                WHERE status = 'COMPLETED'
                ORDER BY completed_at DESC LIMIT 1
            )
        """)
        payload_rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        payload_keys = {(r['shopify_handle'], r['os_environment']) for r in payload_rows if r['shopify_handle']}
        
        extra = []
        for row in db_rows:
            key = (row['shopify_handle'], row['os_environment'])
            if key not in payload_keys:
                extra.append({
                    'shopify_handle': row['shopify_handle'],
                    'os_environment': row['os_environment'],
                    'module_code': row['module_code'],
                    'product_name': row['product_name']
                })
        
        return {
            "extra_count": len(extra),
            "extra_modules": extra[:50],
            "note": "Modules in DB not covered by latest override batch"
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))
