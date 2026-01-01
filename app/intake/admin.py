"""
GenoMAX² Product Intake System - Admin Endpoints

Three endpoints for the intake workflow:
1. POST /api/v1/catalog/intake - Create draft intake
2. POST /api/v1/catalog/approve - Approve and insert to os_modules
3. POST /api/v1/catalog/reject - Reject with reason

Security: Requires X-Admin-API-Key header.

Version: intake_system_v1
"""

import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, Header, HTTPException, Depends
import psycopg2
from psycopg2.extras import RealDictCursor

from .models import (
    IntakeStatus,
    IntakeCreateRequest,
    IntakeApproveRequest,
    IntakeRejectRequest,
    IntakeCreateResponse,
    IntakeApproveResponse,
    IntakeRejectResponse,
    IntakeListResponse,
    IntakeListItem,
    IntakeDetailResponse,
    DraftModule,
    DraftCopy,
    ValidationFlags,
    SnapshotResponse,
)
from .supliful import (
    fetch_product_from_supliful,
    parse_supplier_payload,
    validate_supplier_data,
)
from .module_generator import (
    generate_draft_modules,
    determine_os_environment,
    generate_module_code_hash,
)


# =============================================
# Router Setup
# =============================================

router = APIRouter(
    prefix="/api/v1/catalog",
    tags=["admin", "intake"],
)


# =============================================
# Database Connection
# =============================================

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    """Get database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def now_iso() -> str:
    """Get current timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


# =============================================
# Security
# =============================================

def verify_admin_key(x_admin_api_key: str = Header(None, alias="X-Admin-API-Key")) -> str:
    """
    Verify admin API key from header.
    
    Raises 401 if missing or invalid.
    """
    expected_key = os.environ.get("ADMIN_API_KEY")
    
    if not expected_key:
        # Fail open in dev if ADMIN_API_KEY not set
        return "dev_mode"
    
    if not x_admin_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing X-Admin-API-Key header"
        )
    
    if x_admin_api_key != expected_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid admin API key"
        )
    
    return x_admin_api_key


# =============================================
# Copy Generation
# =============================================

def generate_draft_copy(modules: List[DraftModule]) -> DraftCopy:
    """
    Generate OS-compliant copy for labels.
    
    Rules:
    - No benefit claims
    - Biological domain framing only
    - FDA disclaimer always present
    """
    if not modules:
        return DraftCopy()
    
    module = modules[0]  # Use first module for copy
    
    front_label = f"{module.product_name}"
    back_label = f"Dietary Supplement\n\n{module.ingredients_raw_text or ''}\n\n{module.fda_disclaimer}"
    
    shopify_title = f"{module.product_name} | GenoMAX² {module.os_environment}"
    shopify_body = f"""<p>{module.product_name} - Optimized for {module.os_environment} biological environment.</p>
<p><strong>Biological Domain:</strong> {module.biological_domain or 'General Wellness'}</p>
<p><strong>OS Layer:</strong> {module.os_layer}</p>
<p><em>{module.fda_disclaimer}</em></p>"""
    
    return DraftCopy(
        front_label_text=front_label,
        back_label_text=back_label,
        shopify_title=shopify_title,
        shopify_body=shopify_body,
    )


# =============================================
# Duplicate Check
# =============================================

def check_duplicate(conn, product_url: str) -> dict:
    """
    Check if product_url already exists in os_modules or catalog_intakes.
    
    Returns duplicate details if found.
    """
    cur = conn.cursor()
    
    # Check os_modules by URL
    cur.execute(
        "SELECT module_code, product_name FROM os_modules_v3_1 WHERE url = %s OR shopify_product_url_template ILIKE %s LIMIT 1",
        (product_url, f"%{product_url}%")
    )
    existing_module = cur.fetchone()
    if existing_module:
        cur.close()
        return {
            "is_duplicate": True,
            "location": "os_modules",
            "module_code": existing_module["module_code"],
            "product_name": existing_module["product_name"],
        }
    
    # Check catalog_intakes
    cur.execute(
        "SELECT id, status, product_url FROM catalog_intakes WHERE product_url = %s AND status != 'rejected' LIMIT 1",
        (product_url,)
    )
    existing_intake = cur.fetchone()
    if existing_intake:
        cur.close()
        return {
            "is_duplicate": True,
            "location": "catalog_intakes",
            "intake_id": str(existing_intake["id"]),
            "status": existing_intake["status"],
        }
    
    cur.close()
    return {"is_duplicate": False}


def get_module_count(conn) -> int:
    """Get current module count for sequence generation."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1")
    result = cur.fetchone()
    cur.close()
    return result["count"] if result else 0


# =============================================
# Endpoint: Create Intake (Draft)
# =============================================

@router.post("/intake", response_model=IntakeCreateResponse)
async def create_intake(
    request: IntakeCreateRequest,
    admin_key: str = Depends(verify_admin_key),
):
    """
    Step 1: Create a draft intake from a Supliful product URL.
    
    Actions:
    - Fetch supplier data
    - Parse and normalize ingredients
    - Check for duplicates
    - Generate draft OS modules (M/F if BOTH)
    - Generate draft copy
    - Store in catalog_intakes with status='draft'
    
    NO insertion to os_modules at this stage.
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        # Check for duplicate
        dup_check = check_duplicate(conn, request.product_url)
        if dup_check["is_duplicate"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "DUPLICATE_PRODUCT",
                    "message": f"Product already exists in {dup_check['location']}",
                    "details": dup_check,
                }
            )
        
        # Fetch supplier data
        supplier_payload = await fetch_product_from_supliful(request.product_url)
        
        # Parse payload
        parsed = parse_supplier_payload(supplier_payload)
        
        # Validate
        validation = validate_supplier_data(supplier_payload, parsed)
        validation.duplicate_check = dup_check
        
        # If blockers exist, still create draft but mark as blocked
        if not validation.is_valid:
            # Still create intake for review
            pass
        
        # Determine OS environment
        os_env = determine_os_environment(parsed.category, parsed.ingredients)
        
        # Get existing count for sequence
        existing_count = get_module_count(conn)
        
        # Generate draft modules
        draft_modules = generate_draft_modules(parsed, os_env, existing_count)
        
        # Generate draft copy
        draft_copy = generate_draft_copy(draft_modules)
        
        # Insert to catalog_intakes
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO catalog_intakes (
                supplier,
                product_url,
                supplier_payload,
                parsed_payload,
                draft_modules,
                draft_copy,
                status,
                validation_flags,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            RETURNING id, created_at
        """, (
            request.supplier,
            request.product_url,
            json.dumps(supplier_payload, default=str),
            json.dumps(parsed.model_dump(), default=str),
            json.dumps([m.model_dump() for m in draft_modules], default=str),
            json.dumps(draft_copy.model_dump(), default=str),
            IntakeStatus.DRAFT.value,
            json.dumps(validation.model_dump(), default=str),
        ))
        
        result = cur.fetchone()
        intake_id = str(result["id"])
        created_at = result["created_at"]
        
        # Audit log
        cur.execute("""
            INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
            VALUES ('catalog_intake', %s, 'create_draft', %s, NOW())
        """, (
            intake_id,
            json.dumps({
                "product_url": request.product_url,
                "modules_generated": len(draft_modules),
                "validation_warnings": len(validation.warnings),
                "validation_blockers": len(validation.blockers),
            }),
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return IntakeCreateResponse(
            status="success",
            intake_id=intake_id,
            supplier=request.supplier,
            product_url=request.product_url,
            parsed_payload=parsed,
            draft_modules=draft_modules,
            draft_copy=draft_copy,
            validation_flags=validation,
            workflow_status=IntakeStatus.DRAFT,
            created_at=created_at,
            next_step="approve" if validation.is_valid else "review_blockers",
        )
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Intake creation failed: {str(e)}")


# =============================================
# Endpoint: Approve Intake
# =============================================

@router.post("/approve", response_model=IntakeApproveResponse)
async def approve_intake(
    request: IntakeApproveRequest,
    admin_key: str = Depends(verify_admin_key),
):
    """
    Step 2: Approve intake and insert to os_modules.
    
    Actions:
    - Re-validate no duplicates
    - Freeze module_codes
    - INSERT draft_modules into os_modules_v3_1 (append-only)
    - Create snapshot export
    - Update intake status to 'approved'
    
    Rules:
    - ON CONFLICT(module_code) DO NOTHING
    - No updates to existing rows
    - Atomic transaction
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Fetch intake
        cur.execute("""
            SELECT id, product_url, draft_modules, validation_flags, status
            FROM catalog_intakes
            WHERE id = %s
        """, (request.intake_id,))
        
        intake = cur.fetchone()
        if not intake:
            raise HTTPException(status_code=404, detail=f"Intake not found: {request.intake_id}")
        
        if intake["status"] != IntakeStatus.DRAFT.value:
            raise HTTPException(
                status_code=400,
                detail=f"Intake is not in draft status. Current: {intake['status']}"
            )
        
        # Re-validate no duplicates
        dup_check = check_duplicate(conn, intake["product_url"])
        if dup_check["is_duplicate"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "DUPLICATE_DETECTED_ON_APPROVAL",
                    "message": "Duplicate product detected during approval",
                    "details": dup_check,
                }
            )
        
        # Parse draft modules
        draft_modules_raw = intake["draft_modules"]
        if isinstance(draft_modules_raw, str):
            draft_modules_raw = json.loads(draft_modules_raw)
        
        draft_modules = [DraftModule(**m) for m in draft_modules_raw]
        
        # Check validation blockers
        validation_raw = intake["validation_flags"]
        if isinstance(validation_raw, str):
            validation_raw = json.loads(validation_raw)
        
        if validation_raw.get("blockers"):
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "VALIDATION_BLOCKERS_EXIST",
                    "message": "Cannot approve intake with validation blockers",
                    "blockers": validation_raw["blockers"],
                }
            )
        
        # INSERT into os_modules_v3_1 (append-only)
        inserted_modules = []
        for module in draft_modules:
            cur.execute("""
                INSERT INTO os_modules_v3_1 (
                    module_code,
                    os_environment,
                    os_layer,
                    biological_domain,
                    shopify_store,
                    shopify_handle,
                    product_name,
                    ingredient_tags,
                    category_tags,
                    ingredients_raw_text,
                    suggested_use_full,
                    safety_notes,
                    contraindications,
                    drug_interactions,
                    wholesale_price,
                    shipping_restriction,
                    fda_disclaimer,
                    supplier_status,
                    created_at,
                    updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE', NOW(), NOW()
                )
                ON CONFLICT (module_code) DO NOTHING
                RETURNING module_code
            """, (
                module.module_code,
                module.os_environment,
                module.os_layer,
                module.biological_domain,
                module.shopify_store,
                module.shopify_handle,
                module.product_name,
                module.ingredient_tags,
                module.category_tags,
                module.ingredients_raw_text,
                module.suggested_use_full,
                module.safety_notes,
                module.contraindications,
                module.drug_interactions,
                module.wholesale_price,
                module.shipping_restriction,
                module.fda_disclaimer,
            ))
            
            result = cur.fetchone()
            if result:
                inserted_modules.append(result["module_code"])
        
        if not inserted_modules:
            raise HTTPException(
                status_code=409,
                detail="No modules inserted. All module_codes may already exist."
            )
        
        # Update intake status
        approved_at = datetime.now(timezone.utc)
        cur.execute("""
            UPDATE catalog_intakes
            SET status = %s, approved_by = %s, approved_at = %s, updated_at = NOW()
            WHERE id = %s
        """, (IntakeStatus.APPROVED.value, request.approved_by, approved_at, request.intake_id))
        
        # Create snapshot
        cur.execute("SELECT COUNT(*) as count FROM os_modules_v3_1")
        module_count = cur.fetchone()["count"]
        
        # Generate version tag
        version_tag = f"v{module_count}_LOCK"
        
        cur.execute("""
            INSERT INTO catalog_snapshots (version_tag, module_count, generated_by, created_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (version_tag) DO NOTHING
            RETURNING id, version_tag
        """, (version_tag, module_count, request.approved_by))
        
        snapshot_result = cur.fetchone()
        snapshot_id = str(snapshot_result["id"]) if snapshot_result else None
        
        # Audit log
        cur.execute("""
            INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
            VALUES ('catalog_intake', %s, 'approve', %s, NOW())
        """, (
            request.intake_id,
            json.dumps({
                "approved_by": request.approved_by,
                "inserted_modules": inserted_modules,
                "snapshot_version": version_tag,
            }),
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return IntakeApproveResponse(
            status="success",
            intake_id=request.intake_id,
            approved_by=request.approved_by,
            approved_at=approved_at,
            inserted_modules=inserted_modules,
            snapshot_id=snapshot_id,
            snapshot_version=version_tag,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Approval failed: {str(e)}")


# =============================================
# Endpoint: Reject Intake
# =============================================

@router.post("/reject", response_model=IntakeRejectResponse)
async def reject_intake(
    request: IntakeRejectRequest,
    admin_key: str = Depends(verify_admin_key),
):
    """
    Step 3 (Alternative): Reject intake with reason.
    
    Actions:
    - Mark intake as rejected
    - Preserve all data for audit
    - No catalog impact
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Fetch intake
        cur.execute("""
            SELECT id, status FROM catalog_intakes WHERE id = %s
        """, (request.intake_id,))
        
        intake = cur.fetchone()
        if not intake:
            raise HTTPException(status_code=404, detail=f"Intake not found: {request.intake_id}")
        
        if intake["status"] != IntakeStatus.DRAFT.value:
            raise HTTPException(
                status_code=400,
                detail=f"Can only reject drafts. Current status: {intake['status']}"
            )
        
        # Update status
        cur.execute("""
            UPDATE catalog_intakes
            SET status = %s, rejection_reason = %s, updated_at = NOW()
            WHERE id = %s
        """, (IntakeStatus.REJECTED.value, request.reason, request.intake_id))
        
        # Audit log
        cur.execute("""
            INSERT INTO audit_log (entity_type, entity_id, action, metadata, created_at)
            VALUES ('catalog_intake', %s, 'reject', %s, NOW())
        """, (
            request.intake_id,
            json.dumps({"reason": request.reason}),
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return IntakeRejectResponse(
            status="success",
            intake_id=request.intake_id,
            rejection_reason=request.reason,
            workflow_status=IntakeStatus.REJECTED,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Rejection failed: {str(e)}")


# =============================================
# Additional Endpoints: List and Detail
# =============================================

@router.get("/intakes", response_model=IntakeListResponse)
async def list_intakes(
    status: Optional[str] = None,
    admin_key: str = Depends(verify_admin_key),
):
    """List all intakes with optional status filter."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        query = """
            SELECT id, supplier, product_url, parsed_payload, status, 
                   validation_flags, created_at, updated_at
            FROM catalog_intakes
        """
        params = []
        
        if status:
            query += " WHERE status = %s"
            params.append(status)
        
        query += " ORDER BY created_at DESC LIMIT 100"
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        intakes = []
        for row in rows:
            parsed = row["parsed_payload"]
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            
            validation = row["validation_flags"]
            if isinstance(validation, str):
                validation = json.loads(validation)
            
            intakes.append(IntakeListItem(
                intake_id=str(row["id"]),
                supplier=row["supplier"],
                product_url=row["product_url"],
                product_name=parsed.get("product_name") if parsed else None,
                status=IntakeStatus(row["status"]),
                validation_warnings=len(validation.get("warnings", [])) if validation else 0,
                validation_blockers=len(validation.get("blockers", [])) if validation else 0,
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            ))
        
        cur.close()
        conn.close()
        
        return IntakeListResponse(
            status="success",
            total=len(intakes),
            intakes=intakes,
        )
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/intakes/{intake_id}", response_model=IntakeDetailResponse)
async def get_intake_detail(
    intake_id: str,
    admin_key: str = Depends(verify_admin_key),
):
    """Get full intake details by ID."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT * FROM catalog_intakes WHERE id = %s
        """, (intake_id,))
        
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Intake not found: {intake_id}")
        
        # Parse JSON fields
        def parse_json(val):
            if val is None:
                return None
            if isinstance(val, str):
                return json.loads(val)
            return val
        
        supplier_payload = parse_json(row["supplier_payload"])
        parsed_payload = parse_json(row["parsed_payload"])
        draft_modules = parse_json(row["draft_modules"])
        draft_copy = parse_json(row["draft_copy"])
        validation_flags = parse_json(row["validation_flags"])
        
        cur.close()
        conn.close()
        
        return IntakeDetailResponse(
            status="success",
            intake_id=str(row["id"]),
            supplier=row["supplier"],
            product_url=row["product_url"],
            supplier_payload=supplier_payload,
            parsed_payload=parsed_payload,
            draft_modules=[DraftModule(**m) for m in draft_modules] if draft_modules else [],
            draft_copy=DraftCopy(**draft_copy) if draft_copy else None,
            validation_flags=ValidationFlags(**validation_flags) if validation_flags else ValidationFlags(),
            workflow_status=IntakeStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            approved_by=row["approved_by"],
            approved_at=row["approved_at"],
            rejection_reason=row["rejection_reason"],
        )
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def intake_health():
    """Health check for intake module."""
    return {
        "status": "ok",
        "module": "product_intake",
        "version": "intake_system_v1",
        "timestamp": now_iso(),
    }
