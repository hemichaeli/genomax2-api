"""
GenoMAX² Webhook Data Models
Pydantic models for lab integration webhook payloads

Junction (Vital) API: https://docs.tryvital.io/webhooks
Lab Testing API: https://labtestingapi.com/docs
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field
from enum import Enum


class WebhookEventType(str, Enum):
    """Supported webhook event types"""
    # Junction/Vital events
    LABTEST_ORDER_CREATED = "labtest.order.created"
    LABTEST_ORDER_UPDATED = "labtest.order.updated"
    LABTEST_RESULTS_READY = "labtest.results.ready"
    LABTEST_RESULTS_CRITICAL = "labtest.results.critical"
    
    # Lab Testing API events
    LTA_ORDER_COMPLETED = "order.completed"
    LTA_RESULTS_AVAILABLE = "results.available"
    
    # Internal events
    UNKNOWN = "unknown"


class Biomarker(BaseModel):
    """Individual biomarker result"""
    code: str = Field(..., description="Biomarker code (e.g., 'vitamin_d_25oh')")
    name: str = Field(..., description="Human-readable name")
    value: float = Field(..., description="Measured value")
    unit: str = Field(..., description="Unit of measurement")
    reference_range: Optional[Dict[str, Any]] = Field(default=None)
    flag: Optional[Literal["L", "H", "N", "C"]] = Field(
        default=None, 
        description="L=Low, H=High, N=Normal, C=Critical"
    )
    loinc_code: Optional[str] = Field(default=None, description="LOINC standardized code")


class LabResult(BaseModel):
    """Complete lab result from external provider"""
    order_id: str = Field(..., description="External order ID")
    user_id: str = Field(..., description="GenoMAX² user ID")
    provider: Literal["junction", "lab_testing_api", "manual"] = Field(...)
    status: Literal["pending", "processing", "completed", "failed", "cancelled"] = Field(...)
    collected_at: Optional[datetime] = Field(default=None)
    processed_at: Optional[datetime] = Field(default=None)
    biomarkers: List[Biomarker] = Field(default_factory=list)
    raw_pdf_url: Optional[str] = Field(default=None)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WebhookEvent(BaseModel):
    """Generic webhook event wrapper"""
    event_id: str = Field(..., description="Unique event ID")
    event_type: WebhookEventType = Field(...)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str = Field(..., description="Source provider")
    payload: Dict[str, Any] = Field(default_factory=dict)
    signature: Optional[str] = Field(default=None, description="HMAC signature for verification")
    verified: bool = Field(default=False)


# === Junction (Vital) Specific Models ===

class JunctionUser(BaseModel):
    """Junction user reference"""
    user_id: str
    client_user_id: Optional[str] = None  # Our GenoMAX² user ID


class JunctionOrderDetails(BaseModel):
    """Junction lab test order details"""
    order_id: str
    team_id: str
    user: JunctionUser
    status: str
    sample_type: str = "blood"
    lab_id: Optional[str] = None
    tracking_number: Optional[str] = None


class JunctionResultData(BaseModel):
    """Junction lab results data"""
    order_id: str
    results: List[Dict[str, Any]]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class JunctionWebhookPayload(BaseModel):
    """
    Junction (Vital) webhook payload structure
    Ref: https://docs.tryvital.io/webhooks
    """
    event_type: str = Field(..., description="e.g., 'labtest.results.ready'")
    team_id: str = Field(...)
    data: Dict[str, Any] = Field(...)
    timestamp: str = Field(...)
    
    class Config:
        extra = "allow"


# === Lab Testing API Specific Models ===

class LabTestingAPIResult(BaseModel):
    """Lab Testing API result structure"""
    test_id: str
    test_name: str
    result_value: str
    result_unit: str
    reference_range: Optional[str] = None
    abnormal_flag: Optional[str] = None


class LabTestingAPIWebhookPayload(BaseModel):
    """
    Lab Testing API webhook payload structure
    Ref: https://labtestingapi.com/docs
    """
    event: str = Field(..., description="Event type")
    order_id: str = Field(...)
    status: str = Field(...)
    patient_id: Optional[str] = Field(default=None)
    results: Optional[List[Dict[str, Any]]] = Field(default=None)
    pdf_url: Optional[str] = Field(default=None)
    timestamp: str = Field(...)
    
    class Config:
        extra = "allow"


# === Biomarker Mapping ===

BIOMARKER_CODE_MAP = {
    # Junction codes -> GenoMAX² codes
    "25-hydroxyvitamin-d": "vitamin_d_25oh",
    "vitamin-d-25-hydroxy": "vitamin_d_25oh",
    "ferritin": "ferritin",
    "serum-ferritin": "ferritin",
    "hemoglobin": "hemoglobin",
    "hba1c": "hba1c",
    "glycated-hemoglobin": "hba1c",
    "fasting-glucose": "glucose_fasting",
    "glucose-fasting": "glucose_fasting",
    "creatinine": "creatinine",
    "serum-creatinine": "creatinine",
    "egfr": "egfr",
    "alt": "alt",
    "alanine-aminotransferase": "alt",
    "ast": "ast",
    "aspartate-aminotransferase": "ast",
    "total-cholesterol": "total_cholesterol",
    "ldl-cholesterol": "ldl_cholesterol",
    "hdl-cholesterol": "hdl_cholesterol",
    "triglycerides": "triglycerides",
    "tsh": "tsh",
    "crp": "crp",
    "c-reactive-protein": "crp",
    "hs-crp": "hs_crp",
    "high-sensitivity-crp": "hs_crp",
    "homocysteine": "homocysteine",
    "vitamin-b12": "vitamin_b12",
    "cobalamin": "vitamin_b12",
    "folate": "folate",
    "serum-folate": "folate",
    "iron": "iron",
    "serum-iron": "iron",
    "tibc": "tibc",
    "transferrin-saturation": "transferrin_saturation",
    "zinc": "zinc",
    "serum-zinc": "zinc",
    "magnesium": "magnesium",
    "serum-magnesium": "magnesium"
}


def normalize_biomarker_code(external_code: str) -> str:
    """
    Normalize external biomarker codes to GenoMAX² canonical codes.
    Returns original code if no mapping exists.
    """
    normalized = external_code.lower().strip().replace(" ", "-").replace("_", "-")
    return BIOMARKER_CODE_MAP.get(normalized, external_code)


UNIT_CONVERSIONS = {
    # (from_unit, to_unit): conversion_factor
    ("nmol/l", "ng/ml"): 0.4,  # Vitamin D
    ("pmol/l", "pg/ml"): 1.0,  # B12 (approx)
    ("umol/l", "mg/dl"): 0.0113,  # Creatinine
    ("mmol/l", "mg/dl"): 18.0,  # Glucose
}


def convert_unit(value: float, from_unit: str, to_unit: str) -> float:
    """Convert biomarker values between units"""
    from_unit = from_unit.lower().strip()
    to_unit = to_unit.lower().strip()
    
    if from_unit == to_unit:
        return value
    
    key = (from_unit, to_unit)
    if key in UNIT_CONVERSIONS:
        return value * UNIT_CONVERSIONS[key]
    
    # Try reverse
    reverse_key = (to_unit, from_unit)
    if reverse_key in UNIT_CONVERSIONS:
        return value / UNIT_CONVERSIONS[reverse_key]
    
    # No conversion available - return original
    return value
