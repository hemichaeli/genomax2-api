"""
Launch v1 Enforcement Router for GenoMAX²
==========================================
Pipeline enforcement ensuring only Launch v1 products (TIER 1 + TIER 2)
reach external systems (Design Export, Shopify).

HARD GUARDRAILS:
- All external outputs MUST filter: is_launch_v1 = TRUE AND supplier_status = 'ACTIVE'
- No fuzzy matching. No heuristic inclusion.
- Brain logic and research views remain unfiltered.

Endpoints:
- GET  /api/v1/qa/launch-v1/pairing - Validate environment pairing (no half products)
- GET  /api/v1/launch-v1/export/design - Excel export with LAUNCH_V1_SUMMARY tab
- GET  /api/v1/launch-v1/products - List Launch v1 products with base_handle

v3.27.0
"""

import os
import re
import json
import io
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

router = APIRouter(tags=["launch-v1"])

DATABASE_URL = os.getenv("DATABASE_URL")


# ===== Constants =====

# Hard guardrail: Launch v1 scope query
LAUNCH_V1_SCOPE_FILTER = """
    is_launch_v1 = TRUE 
    AND supplier_status = 'ACTIVE'
"""

# Regex to strip environment suffix from shopify_handle
ENV_SUFFIX_PATTERN = re.compile(r'-maximo$|-maxima$', re.IGNORECASE)


# ===== Database Helpers =====

def get_db():
    """Get database connection."""
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def derive_base_handle(shopify_handle: Optional[str]) -> Optional[str]:
    """
    Derive base_handle by stripping -maximo or -maxima suffix.
    
    Example:
        vitamin-d3-5000iu-maximo -> vitamin-d3-5000iu
        omega-3-epa-maxima -> omega-3-epa
    """
    if not shopify_handle:
        return None
    return ENV_SUFFIX_PATTERN.sub('', shopify_handle)


def now_iso() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


# ===== Models =====

class PairingOffender(BaseModel):
    """A base product with incorrect environment count."""
    base_handle: str
    env_count: int
    handles: List[str]
    environments: List[str]


class PairingQAResult(BaseModel):
    """Result of Launch v1 pairing QA check."""
    overall_status: str = Field(description="PASS or FAIL")
    timestamp: str
    scope: Dict[str, Any]
    distribution: List[Dict[str, int]]
    expected_env_count: int = 2
    total_base_products: int
    total_modules: int
    maximo_count: int
    maxima_count: int
    offenders: List[PairingOffender]
    invariants: Dict[str, bool]


class LaunchV1Summary(BaseModel):
    """Summary stats for Launch v1 export."""
    modules_count: int
    base_products_count: int
    maximo_rows: int
    maxima_rows: int
    tier_1_count: int
    tier_2_count: int
    pairing_status: str
    invariant_modules_equals_sum: bool
    invariant_pairs_complete: bool


# ===== Core Functions =====

def fetch_launch_v1_products() -> List[Dict[str, Any]]:
    """
    Fetch all Launch v1 products with HARD GUARDRAILS.
    
    Returns only:
    - is_launch_v1 = TRUE
    - supplier_status = 'ACTIVE'
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute(f"""
            SELECT 
                module_code,
                product_name,
                shopify_handle,
                os_environment,
                os_layer,
                tier,
                biological_domain,
                net_quantity,
                fda_disclaimer,
                shopify_body,
                front_label_text,
                back_label_text,
                suggested_use_full,
                safety_notes,
                contraindications,
                dosing_protocol,
                supplier_page_url,
                supliful_handle,
                supplier_status,
                is_launch_v1,
                created_at,
                updated_at
            FROM os_modules_v3_1
            WHERE {LAUNCH_V1_SCOPE_FILTER}
            ORDER BY tier, os_environment, module_code
        """)
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        # Add derived base_handle
        products = []
        for row in rows:
            product = dict(row)
            product['base_handle'] = derive_base_handle(product.get('shopify_handle'))
            products.append(product)
        
        return products
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def compute_pairing_analysis(products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze environment pairing for Launch v1 products.
    
    Each base product should exist exactly twice:
    - Once for MAXimo² (male)
    - Once for MAXima² (female)
    """
    # Group by base_handle
    base_groups: Dict[str, List[Dict[str, Any]]] = {}
    
    for product in products:
        base_handle = product.get('base_handle')
        if not base_handle:
            continue
        
        if base_handle not in base_groups:
            base_groups[base_handle] = []
        base_groups[base_handle].append(product)
    
    # Analyze distribution
    distribution: Dict[int, int] = {}  # env_count -> number of base products
    offenders: List[PairingOffender] = []
    
    for base_handle, group in base_groups.items():
        env_count = len(group)
        distribution[env_count] = distribution.get(env_count, 0) + 1
        
        if env_count != 2:
            offenders.append(PairingOffender(
                base_handle=base_handle,
                env_count=env_count,
                handles=[p.get('shopify_handle', '') for p in group],
                environments=[p.get('os_environment', '') for p in group]
            ))
    
    # Count by environment
    maximo_count = sum(1 for p in products if p.get('os_environment') == 'MAXimo²')
    maxima_count = sum(1 for p in products if p.get('os_environment') == 'MAXima²')
    
    return {
        'base_groups': base_groups,
        'distribution': distribution,
        'offenders': offenders,
        'maximo_count': maximo_count,
        'maxima_count': maxima_count,
        'total_base_products': len(base_groups),
        'total_modules': len(products),
    }


# ===== Endpoints =====

@router.get("/api/v1/qa/launch-v1/pairing", response_model=PairingQAResult)
def qa_launch_v1_pairing():
    """
    QA Check: Validate Launch v1 has complete environment pairing.
    
    PASS conditions:
    - Every base_handle has exactly 2 modules (MAXimo² + MAXima²)
    - No orphan products
    
    FAIL conditions:
    - Any base_handle with env_count != 2
    
    Returns distribution and list of offenders if any.
    """
    # Fetch products with hard guardrails
    products = fetch_launch_v1_products()
    
    # Compute pairing analysis
    analysis = compute_pairing_analysis(products)
    
    # Build distribution list
    dist_list = [
        {"env_count": env_count, "base_products": count}
        for env_count, count in sorted(analysis['distribution'].items(), reverse=True)
    ]
    
    # Determine pass/fail
    offenders = analysis['offenders']
    overall_status = "PASS" if len(offenders) == 0 else "FAIL"
    
    # Invariant checks
    invariants = {
        "modules_equals_maximo_plus_maxima": (
            analysis['total_modules'] == analysis['maximo_count'] + analysis['maxima_count']
        ),
        "pairs_complete_if_pass": (
            overall_status == "FAIL" or 
            analysis['total_modules'] == analysis['total_base_products'] * 2
        ),
    }
    
    return PairingQAResult(
        overall_status=overall_status,
        timestamp=now_iso(),
        scope={
            "is_launch_v1": True,
            "supplier_status": "ACTIVE"
        },
        distribution=dist_list,
        expected_env_count=2,
        total_base_products=analysis['total_base_products'],
        total_modules=analysis['total_modules'],
        maximo_count=analysis['maximo_count'],
        maxima_count=analysis['maxima_count'],
        offenders=[o.model_dump() for o in offenders],
        invariants=invariants
    )


@router.get("/api/v1/launch-v1/products")
def list_launch_v1_products(
    environment: Optional[str] = Query(None, description="Filter by os_environment"),
    tier: Optional[str] = Query(None, description="Filter by tier (TIER 1, TIER 2)"),
    limit: int = Query(500, ge=1, le=1000),
):
    """
    List all Launch v1 products with base_handle derived.
    
    HARD GUARDRAILS enforced:
    - is_launch_v1 = TRUE
    - supplier_status = 'ACTIVE'
    """
    products = fetch_launch_v1_products()
    
    # Apply optional filters
    if environment:
        products = [p for p in products if p.get('os_environment') == environment]
    
    if tier:
        products = [p for p in products if p.get('tier') == tier]
    
    # Apply limit
    products = products[:limit]
    
    # Compute summary
    tier_counts = {}
    env_counts = {}
    for p in products:
        t = p.get('tier', 'UNKNOWN')
        e = p.get('os_environment', 'UNKNOWN')
        tier_counts[t] = tier_counts.get(t, 0) + 1
        env_counts[e] = env_counts.get(e, 0) + 1
    
    return {
        "count": len(products),
        "timestamp": now_iso(),
        "scope": {
            "is_launch_v1": True,
            "supplier_status": "ACTIVE",
            "environment_filter": environment,
            "tier_filter": tier,
        },
        "tier_distribution": tier_counts,
        "environment_distribution": env_counts,
        "products": products,
    }


@router.get("/api/v1/launch-v1/export/design")
def export_design_excel(
    format: str = Query("xlsx", description="Export format (xlsx only for now)"),
):
    """
    Export Launch v1 products to Excel for Design team.
    
    HARD GUARDRAILS enforced:
    - is_launch_v1 = TRUE
    - supplier_status = 'ACTIVE'
    
    Includes:
    - LAUNCH_V1_SUMMARY tab with counts and invariants
    - READY_FOR_DESIGN tab with all Launch v1 products
    - base_handle column derived from shopify_handle
    
    Prevents "332 products" confusion by clearly showing:
    - modules_count (total rows)
    - base_products_count (unique base_handle)
    - breakdown by environment
    """
    if not OPENPYXL_AVAILABLE:
        raise HTTPException(
            status_code=500, 
            detail="openpyxl not installed. Install with: pip install openpyxl"
        )
    
    # Fetch products with hard guardrails
    products = fetch_launch_v1_products()
    
    # Compute pairing analysis
    analysis = compute_pairing_analysis(products)
    
    # Compute tier counts
    tier_1_count = sum(1 for p in products if p.get('tier') == 'TIER 1')
    tier_2_count = sum(1 for p in products if p.get('tier') == 'TIER 2')
    
    # Pairing status
    offenders = analysis['offenders']
    pairing_status = "PASS" if len(offenders) == 0 else "FAIL"
    
    # Invariant checks
    modules_equals_sum = analysis['total_modules'] == analysis['maximo_count'] + analysis['maxima_count']
    pairs_complete = pairing_status == "PASS" and analysis['total_modules'] == analysis['total_base_products'] * 2
    
    # Create workbook
    wb = openpyxl.Workbook()
    
    # ===== LAUNCH_V1_SUMMARY Tab =====
    ws_summary = wb.active
    ws_summary.title = "LAUNCH_V1_SUMMARY"
    
    # Styling
    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    pass_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    fail_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    
    # Summary data
    summary_data = [
        ("Launch v1 Export Summary", ""),
        ("Generated At", now_iso()),
        ("", ""),
        ("COUNTS", ""),
        ("Total Modules (rows)", analysis['total_modules']),
        ("Unique Base Products", analysis['total_base_products']),
        ("", ""),
        ("ENVIRONMENT BREAKDOWN", ""),
        ("MAXimo² Rows", analysis['maximo_count']),
        ("MAXima² Rows", analysis['maxima_count']),
        ("", ""),
        ("TIER BREAKDOWN", ""),
        ("TIER 1 Count", tier_1_count),
        ("TIER 2 Count", tier_2_count),
        ("", ""),
        ("INVARIANT CHECKS", ""),
        ("modules_count = maximo + maxima", "PASS" if modules_equals_sum else "FAIL"),
        ("Pairing Complete (all pairs = 2)", pairing_status),
        ("", ""),
        ("SCOPE FILTERS", ""),
        ("is_launch_v1", "TRUE"),
        ("supplier_status", "ACTIVE"),
    ]
    
    for row_idx, (label, value) in enumerate(summary_data, start=1):
        ws_summary.cell(row=row_idx, column=1, value=label)
        ws_summary.cell(row=row_idx, column=2, value=value)
        
        # Style headers
        if label in ["Launch v1 Export Summary", "COUNTS", "ENVIRONMENT BREAKDOWN", "TIER BREAKDOWN", "INVARIANT CHECKS", "SCOPE FILTERS"]:
            ws_summary.cell(row=row_idx, column=1).font = Font(bold=True)
        
        # Style pass/fail
        if value == "PASS":
            ws_summary.cell(row=row_idx, column=2).fill = pass_fill
        elif value == "FAIL":
            ws_summary.cell(row=row_idx, column=2).fill = fail_fill
    
    # Adjust column widths
    ws_summary.column_dimensions['A'].width = 35
    ws_summary.column_dimensions['B'].width = 25
    
    # ===== READY_FOR_DESIGN Tab =====
    ws_design = wb.create_sheet("READY_FOR_DESIGN")
    
    # Headers
    headers = [
        "module_code",
        "product_name",
        "base_handle",
        "shopify_handle",
        "os_environment",
        "tier",
        "os_layer",
        "biological_domain",
        "net_quantity",
        "fda_disclaimer",
        "front_label_text",
        "back_label_text",
        "suggested_use_full",
        "safety_notes",
        "supplier_status",
    ]
    
    for col_idx, header in enumerate(headers, start=1):
        cell = ws_design.cell(row=1, column=col_idx, value=header)
        cell.fill = header_fill
        cell.font = header_font
    
    # Data rows
    for row_idx, product in enumerate(products, start=2):
        for col_idx, header in enumerate(headers, start=1):
            value = product.get(header, "")
            # Truncate long text
            if isinstance(value, str) and len(value) > 32000:
                value = value[:32000] + "..."
            ws_design.cell(row=row_idx, column=col_idx, value=value)
    
    # Adjust column widths
    for col_idx, header in enumerate(headers, start=1):
        ws_design.column_dimensions[get_column_letter(col_idx)].width = min(40, max(12, len(header) + 2))
    
    # ===== PAIRING_OFFENDERS Tab (if any) =====
    if offenders:
        ws_offenders = wb.create_sheet("PAIRING_OFFENDERS")
        
        offender_headers = ["base_handle", "env_count", "handles", "environments"]
        for col_idx, header in enumerate(offender_headers, start=1):
            cell = ws_offenders.cell(row=1, column=col_idx, value=header)
            cell.fill = fail_fill
            cell.font = Font(bold=True)
        
        for row_idx, offender in enumerate(offenders, start=2):
            ws_offenders.cell(row=row_idx, column=1, value=offender.base_handle)
            ws_offenders.cell(row=row_idx, column=2, value=offender.env_count)
            ws_offenders.cell(row=row_idx, column=3, value=", ".join(offender.handles))
            ws_offenders.cell(row=row_idx, column=4, value=", ".join(offender.environments))
    
    # Save to BytesIO
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    # Generate filename
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"genomax2_launch_v1_design_export_{timestamp}.xlsx"
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@router.get("/api/v1/launch-v1/summary")
def get_launch_v1_summary():
    """
    Quick summary of Launch v1 scope without full product list.
    
    Useful for dashboard and status checks.
    """
    products = fetch_launch_v1_products()
    analysis = compute_pairing_analysis(products)
    
    tier_1_count = sum(1 for p in products if p.get('tier') == 'TIER 1')
    tier_2_count = sum(1 for p in products if p.get('tier') == 'TIER 2')
    
    offenders = analysis['offenders']
    pairing_status = "PASS" if len(offenders) == 0 else "FAIL"
    
    modules_equals_sum = analysis['total_modules'] == analysis['maximo_count'] + analysis['maxima_count']
    pairs_complete = pairing_status == "PASS" and analysis['total_modules'] == analysis['total_base_products'] * 2
    
    return LaunchV1Summary(
        modules_count=analysis['total_modules'],
        base_products_count=analysis['total_base_products'],
        maximo_rows=analysis['maximo_count'],
        maxima_rows=analysis['maxima_count'],
        tier_1_count=tier_1_count,
        tier_2_count=tier_2_count,
        pairing_status=pairing_status,
        invariant_modules_equals_sum=modules_equals_sum,
        invariant_pairs_complete=pairs_complete
    )
