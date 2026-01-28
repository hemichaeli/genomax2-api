"""
GenoMAX² OCR Upload Endpoint - Minimal Testing Version
Add this to api_server.py or import as a separate router
"""

import os
import uuid
import hashlib
import base64
import httpx
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from pydantic import BaseModel, Field
import asyncpg

router = APIRouter(prefix="/api/v1/lab", tags=["Lab Integration"])

# ============================================================================
# CONFIGURATION
# ============================================================================

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "genomax2-lab-uploads")
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
ALLOWED_TYPES = {"application/pdf", "image/jpeg", "image/png", "image/webp"}

# 13 Priority Biomarkers
PRIORITY_BIOMARKERS = {
    "ferritin", "serum_iron", "tibc", "transferrin_sat",
    "vitamin_d_25oh", "vitamin_b12", "folate",
    "homocysteine", "hscrp", "omega3_index",
    "hba1c", "magnesium_rbc", "zinc"
}

# Name normalization map (common variations -> GenoMAX² code)
BIOMARKER_ALIASES = {
    # Ferritin
    "ferritin": "ferritin", "ferritin, serum": "ferritin", "serum ferritin": "ferritin",
    # Iron
    "iron": "serum_iron", "iron, serum": "serum_iron", "serum iron": "serum_iron", "fe": "serum_iron",
    # TIBC
    "tibc": "tibc", "total iron binding capacity": "tibc", "iron binding capacity": "tibc",
    # Transferrin Saturation
    "transferrin saturation": "transferrin_sat", "transferrin sat": "transferrin_sat",
    "iron saturation": "transferrin_sat", "% saturation": "transferrin_sat", "tsat": "transferrin_sat",
    # Vitamin D
    "vitamin d": "vitamin_d_25oh", "vitamin d, 25-hydroxy": "vitamin_d_25oh",
    "25-hydroxy vitamin d": "vitamin_d_25oh", "25-oh vitamin d": "vitamin_d_25oh",
    "vitamin d 25-oh": "vitamin_d_25oh", "25(oh)d": "vitamin_d_25oh", "calcidiol": "vitamin_d_25oh",
    # B12
    "vitamin b12": "vitamin_b12", "b12": "vitamin_b12", "cobalamin": "vitamin_b12",
    "cyanocobalamin": "vitamin_b12",
    # Folate
    "folate": "folate", "folic acid": "folate", "vitamin b9": "folate", "folate, serum": "folate",
    # Homocysteine
    "homocysteine": "homocysteine", "hcy": "homocysteine", "homocysteine, plasma": "homocysteine",
    # hsCRP
    "hscrp": "hscrp", "hs-crp": "hscrp", "c-reactive protein": "hscrp",
    "high sensitivity crp": "hscrp", "crp, high sensitivity": "hscrp", "cardiac crp": "hscrp",
    # Omega-3
    "omega-3 index": "omega3_index", "omega 3 index": "omega3_index", "o3 index": "omega3_index",
    # HbA1c
    "hba1c": "hba1c", "hemoglobin a1c": "hba1c", "a1c": "hba1c", "glycated hemoglobin": "hba1c",
    "glycohemoglobin": "hba1c",
    # Magnesium RBC
    "magnesium, rbc": "magnesium_rbc", "rbc magnesium": "magnesium_rbc",
    "magnesium red blood cell": "magnesium_rbc", "mg rbc": "magnesium_rbc",
    # Zinc
    "zinc": "zinc", "zinc, serum": "zinc", "serum zinc": "zinc", "zn": "zinc",
}

# ============================================================================
# MODELS
# ============================================================================

class ExtractedMarker(BaseModel):
    code: str
    original_name: str
    value: float
    unit: str
    reference_low: Optional[float] = None
    reference_high: Optional[float] = None
    flag: Optional[str] = None  # H, L, N, C
    confidence: float = 1.0

class OCRUploadResponse(BaseModel):
    submission_id: str
    status: str
    confidence_score: float
    markers_count: int
    priority_markers_found: List[str]
    needs_review: bool
    review_reasons: List[str]
    markers: List[ExtractedMarker]

class SubmissionStatus(BaseModel):
    submission_id: str
    status: str
    created_at: datetime
    markers_count: int
    needs_review: bool
    brain_run_id: Optional[str] = None

# ============================================================================
# LLM EXTRACTION (Claude)
# ============================================================================

EXTRACTION_PROMPT = """You are a medical lab report parser. Extract ALL biomarkers from this lab report.

For EACH biomarker found, output a JSON object with:
- name: the exact test name as shown
- value: numeric value only (no units)
- unit: the unit of measurement
- reference_low: lower reference range if shown
- reference_high: upper reference range if shown
- flag: "H" (high), "L" (low), "N" (normal), or null

Return a JSON array of all biomarkers. Example:
[
  {"name": "Ferritin", "value": 150.5, "unit": "ng/mL", "reference_low": 30, "reference_high": 400, "flag": "N"},
  {"name": "Vitamin D, 25-Hydroxy", "value": 28, "unit": "ng/mL", "reference_low": 30, "reference_high": 100, "flag": "L"}
]

IMPORTANT:
- Extract ALL numeric lab values, not just common ones
- Use exact names as printed on the report
- Include reference ranges when available
- Return ONLY the JSON array, no other text

Lab Report Text:
"""

async def extract_markers_with_llm(text: str) -> List[Dict[str, Any]]:
    """Use Claude to extract structured biomarker data from OCR text."""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(500, "ANTHROPIC_API_KEY not configured")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "messages": [{
                    "role": "user",
                    "content": EXTRACTION_PROMPT + text[:15000]  # Limit input size
                }]
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(500, f"LLM extraction failed: {response.text}")
        
        result = response.json()
        content = result["content"][0]["text"]
        
        # Parse JSON from response
        import json
        try:
            # Handle potential markdown code blocks
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            return json.loads(content.strip())
        except json.JSONDecodeError:
            return []

# ============================================================================
# OCR SIMULATION (replace with real GCS + Cloud Vision in production)
# ============================================================================

async def perform_ocr(file_content: bytes, content_type: str) -> str:
    """
    Placeholder OCR function. In production, replace with:
    1. Upload to GCS
    2. Call Cloud Vision API document_text_detection
    3. Return extracted text
    """
    # For testing: if it's a text-based PDF, try basic extraction
    if content_type == "application/pdf":
        # In production: use Cloud Vision or pdf2image + OCR
        return "[PDF content - implement Cloud Vision OCR]"
    
    # For images: would call Cloud Vision
    return "[Image content - implement Cloud Vision OCR]"

# ============================================================================
# MARKER NORMALIZATION
# ============================================================================

def normalize_marker_name(name: str) -> str:
    """Convert various marker names to GenoMAX² standard codes."""
    normalized = name.lower().strip()
    return BIOMARKER_ALIASES.get(normalized, normalized.replace(" ", "_").replace(",", "").replace("-", "_"))

def normalize_markers(raw_markers: List[Dict]) -> List[ExtractedMarker]:
    """Normalize extracted markers to GenoMAX² format."""
    normalized = []
    for m in raw_markers:
        try:
            code = normalize_marker_name(m.get("name", ""))
            normalized.append(ExtractedMarker(
                code=code,
                original_name=m.get("name", ""),
                value=float(m.get("value", 0)),
                unit=m.get("unit", ""),
                reference_low=m.get("reference_low"),
                reference_high=m.get("reference_high"),
                flag=m.get("flag"),
                confidence=0.95 if code in PRIORITY_BIOMARKERS else 0.85
            ))
        except (ValueError, TypeError):
            continue
    return normalized

def calculate_confidence(markers: List[ExtractedMarker]) -> float:
    """Calculate overall confidence score weighted by priority markers."""
    if not markers:
        return 0.0
    
    total_weight = 0
    weighted_sum = 0
    
    for m in markers:
        weight = 2.0 if m.code in PRIORITY_BIOMARKERS else 1.0
        weighted_sum += m.confidence * weight
        total_weight += weight
    
    return round(weighted_sum / total_weight, 3) if total_weight > 0 else 0.0

def check_needs_review(markers: List[ExtractedMarker], confidence: float) -> tuple[bool, List[str]]:
    """Determine if submission needs manual review."""
    reasons = []
    
    if confidence < 0.85:
        reasons.append(f"Low confidence score: {confidence}")
    
    priority_found = [m.code for m in markers if m.code in PRIORITY_BIOMARKERS]
    if len(priority_found) < 3:
        reasons.append(f"Few priority markers found: {len(priority_found)}")
    
    # Check for missing units
    missing_units = [m.original_name for m in markers if not m.unit]
    if missing_units:
        reasons.append(f"Missing units for: {', '.join(missing_units[:3])}")
    
    # Check for extreme values that might be OCR errors
    for m in markers:
        if m.code == "ferritin" and (m.value < 1 or m.value > 5000):
            reasons.append(f"Suspicious ferritin value: {m.value}")
        if m.code == "vitamin_d_25oh" and (m.value < 1 or m.value > 200):
            reasons.append(f"Suspicious vitamin D value: {m.value}")
    
    return len(reasons) > 0, reasons

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

async def get_db_pool():
    """Get database connection pool."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(500, "DATABASE_URL not configured")
    return await asyncpg.create_pool(database_url, min_size=1, max_size=5)

async def save_submission(
    pool: asyncpg.Pool,
    user_id: Optional[str],
    filename: str,
    file_hash: str,
    raw_text: str,
    markers: List[ExtractedMarker],
    confidence: float,
    needs_review: bool,
    review_reasons: List[str]
) -> str:
    """Save bloodwork submission to database."""
    import json
    
    submission_id = str(uuid.uuid4())
    priority_found = [m.code for m in markers if m.code in PRIORITY_BIOMARKERS]
    
    status = "pending_review" if needs_review else "ready"
    
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO bloodwork_submissions (
                id, user_id, source, filename, file_hash, raw_text,
                normalized_markers, confidence_score, priority_markers_found,
                needs_review, review_reasons, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
        """,
            submission_id,
            uuid.UUID(user_id) if user_id else None,
            "ocr_upload",
            filename,
            file_hash,
            raw_text,
            json.dumps([m.model_dump() for m in markers]),
            confidence,
            json.dumps(priority_found),
            needs_review,
            json.dumps(review_reasons),
            status
        )
    
    return submission_id

# ============================================================================
# ENDPOINTS
# ============================================================================

@router.post("/upload", response_model=OCRUploadResponse)
async def upload_lab_results(
    file: UploadFile = File(...),
    user_id: Optional[str] = None
):
    """
    Upload a lab report PDF or image for OCR processing.
    
    Accepts: PDF, JPEG, PNG, WebP (max 10MB)
    Returns: Extracted and normalized biomarkers
    """
    # Validate file type
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(400, f"Invalid file type. Allowed: {', '.join(ALLOWED_TYPES)}")
    
    # Read and validate size
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(400, f"File too large. Max size: {MAX_FILE_SIZE // 1024 // 1024}MB")
    
    # Calculate hash for deduplication
    file_hash = hashlib.sha256(content).hexdigest()
    
    # Perform OCR
    raw_text = await perform_ocr(content, file.content_type)
    
    # For testing without real OCR, use sample data
    if raw_text.startswith("["):
        # Simulate with sample extraction
        raw_markers = [
            {"name": "Ferritin", "value": 85, "unit": "ng/mL", "reference_low": 30, "reference_high": 400, "flag": "N"},
            {"name": "Vitamin D, 25-Hydroxy", "value": 32, "unit": "ng/mL", "reference_low": 30, "reference_high": 100, "flag": "N"},
            {"name": "Vitamin B12", "value": 450, "unit": "pg/mL", "reference_low": 200, "reference_high": 900, "flag": "N"},
            {"name": "Hemoglobin A1c", "value": 5.4, "unit": "%", "reference_low": 4.0, "reference_high": 5.6, "flag": "N"},
        ]
    else:
        # Real LLM extraction
        raw_markers = await extract_markers_with_llm(raw_text)
    
    # Normalize markers
    markers = normalize_markers(raw_markers)
    
    # Calculate confidence
    confidence = calculate_confidence(markers)
    
    # Check if needs review
    needs_review, review_reasons = check_needs_review(markers, confidence)
    
    # Get priority markers found
    priority_found = [m.code for m in markers if m.code in PRIORITY_BIOMARKERS]
    
    # Save to database
    try:
        pool = await get_db_pool()
        submission_id = await save_submission(
            pool, user_id, file.filename, file_hash, raw_text,
            markers, confidence, needs_review, review_reasons
        )
        await pool.close()
    except Exception as e:
        # For testing without DB, generate ID
        submission_id = str(uuid.uuid4())
        print(f"DB save skipped (testing mode): {e}")
    
    return OCRUploadResponse(
        submission_id=submission_id,
        status="pending_review" if needs_review else "ready",
        confidence_score=confidence,
        markers_count=len(markers),
        priority_markers_found=priority_found,
        needs_review=needs_review,
        review_reasons=review_reasons,
        markers=markers
    )

@router.get("/submission/{submission_id}", response_model=SubmissionStatus)
async def get_submission_status(submission_id: str):
    """Get the status of a bloodwork submission."""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, status, created_at, markers_count, needs_review, brain_run_id
                FROM bloodwork_submissions WHERE id = $1
            """, uuid.UUID(submission_id))
        await pool.close()
        
        if not row:
            raise HTTPException(404, "Submission not found")
        
        return SubmissionStatus(
            submission_id=str(row["id"]),
            status=row["status"],
            created_at=row["created_at"],
            markers_count=row["markers_count"] or 0,
            needs_review=row["needs_review"],
            brain_run_id=str(row["brain_run_id"]) if row["brain_run_id"] else None
        )
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(500, "Database tables not initialized. Run migration first.")

@router.get("/submissions", response_model=List[SubmissionStatus])
async def list_submissions(
    user_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20
):
    """List bloodwork submissions with optional filters."""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            query = """
                SELECT id, status, created_at, markers_count, needs_review, brain_run_id
                FROM bloodwork_submissions
                WHERE ($1::uuid IS NULL OR user_id = $1)
                  AND ($2::text IS NULL OR status = $2)
                ORDER BY created_at DESC
                LIMIT $3
            """
            rows = await conn.fetch(
                query,
                uuid.UUID(user_id) if user_id else None,
                status,
                limit
            )
        await pool.close()
        
        return [SubmissionStatus(
            submission_id=str(row["id"]),
            status=row["status"],
            created_at=row["created_at"],
            markers_count=row["markers_count"] or 0,
            needs_review=row["needs_review"],
            brain_run_id=str(row["brain_run_id"]) if row["brain_run_id"] else None
        ) for row in rows]
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(500, "Database tables not initialized. Run migration first.")

# ============================================================================
# HEALTH CHECK
# ============================================================================

@router.get("/health")
async def lab_health():
    """Health check for lab integration endpoints."""
    checks = {
        "anthropic_api": bool(ANTHROPIC_API_KEY),
        "database": bool(os.getenv("DATABASE_URL")),
        "gcs_bucket": GCS_BUCKET
    }
    return {
        "status": "healthy" if all([checks["anthropic_api"], checks["database"]]) else "degraded",
        "checks": checks,
        "priority_biomarkers": len(PRIORITY_BIOMARKERS)
    }

# ============================================================================
# INTEGRATION INSTRUCTIONS
# ============================================================================
"""
To integrate into api_server.py:

1. Add import at top:
   from lab_upload import router as lab_router

2. Include router:
   app.include_router(lab_router)

3. Set environment variables:
   ANTHROPIC_API_KEY=sk-ant-...
   DATABASE_URL=postgresql://...
   GCS_BUCKET_NAME=genomax2-lab-uploads

4. Run database migration first:
   psql $DATABASE_URL -f V3.33.0__lab_integration_tables.sql

5. Test endpoint:
   curl -X POST http://localhost:8000/api/v1/lab/upload \
     -F "file=@sample_lab_report.pdf"
"""
