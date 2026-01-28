"""
GenoMAX² Junction/Vital API Client
==================================
Handles lab order creation, status tracking, and results retrieval.

Integration: https://docs.tryvital.io/lab/overview
Version: 1.0.0
"""

import os
import httpx
import hashlib
import hmac
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Request, Header, BackgroundTasks

router = APIRouter(prefix="/api/v1/labs", tags=["labs"])

# =============================================================================
# CONFIGURATION
# =============================================================================

JUNCTION_API_KEY = os.getenv("JUNCTION_API_KEY", "")
JUNCTION_WEBHOOK_SECRET = os.getenv("JUNCTION_WEBHOOK_SECRET", "")
JUNCTION_ENVIRONMENT = os.getenv("JUNCTION_ENVIRONMENT", "sandbox")  # sandbox or production
JUNCTION_BASE_URL = "https://api.tryvital.io/v3" if JUNCTION_ENVIRONMENT == "production" else "https://api.sandbox.tryvital.io/v3"

# =============================================================================
# ENUMS & MODELS
# =============================================================================

class PanelType(str, Enum):
    ESSENTIAL = "essential"
    COMPLETE = "complete"
    CUSTOM = "custom"

class CollectionMethod(str, Enum):
    WALK_IN = "walk-in"
    AT_HOME_KIT = "testkit"
    AT_HOME_PHLEBOTOMY = "at-home-phlebotomy"

class OrderStatus(str, Enum):
    CREATED = "created"
    PENDING_PAYMENT = "pending_payment"
    REQUISITION_READY = "requisition.created"
    KIT_SHIPPED = "testkit.shipped"
    KIT_DELIVERED = "testkit.delivered"
    APPOINTMENT_SCHEDULED = "appointment.scheduled"
    SAMPLE_COLLECTED = "sample.collected"
    SAMPLE_RECEIVED = "sample.received"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"

# GenoMAX² Panel Configurations
PANEL_CONFIGS = {
    PanelType.ESSENTIAL: {
        "name": "GenoMAX² Essential Panel",
        "marker_codes": [
            "ferritin", "serum_iron", "tibc", "transferrin_sat",
            "vitamin_d_25oh", "vitamin_b12", "folate", "hba1c",
            # CMP-14 markers
            "glucose", "bun", "creatinine", "sodium", "potassium",
            "chloride", "co2", "calcium", "protein_total", "albumin",
            "bilirubin_total", "alkaline_phosphatase", "ast", "alt"
        ],
        "loinc_codes": [
            "2498-4", "2502-6", "2500-0", "2502-3",  # Iron panel
            "1989-3", "2132-9", "2284-8", "4548-4",  # Vitamins + HbA1c
            # CMP-14
            "2345-7", "3094-0", "2160-0", "2951-2", "2823-3",
            "2075-0", "2028-9", "17861-6", "2885-2", "1751-7",
            "1975-2", "6768-6", "1920-8", "1742-6"
        ],
        "estimated_price_cents": 15000  # $150
    },
    PanelType.COMPLETE: {
        "name": "GenoMAX² Complete Panel",
        "marker_codes": [
            # All Essential markers plus:
            "hscrp", "homocysteine", "omega3_index", "magnesium_rbc", "zinc",
            # Thyroid
            "tsh", "t3_free", "t4_free",
            # CBC
            "wbc", "rbc", "hemoglobin", "hematocrit", "platelets",
            # Lipid panel
            "cholesterol_total", "ldl", "hdl", "triglycerides",
            # Additional
            "uric_acid", "ggt"
        ],
        "loinc_codes": [
            # Essential LOINCs plus:
            "30522-7", "13965-9", "82810-3", "19123-9", "2601-3",
            # Thyroid
            "3016-3", "3051-0", "3024-7",
            # CBC
            "6690-2", "789-8", "718-7", "4544-3", "777-3",
            # Lipids
            "2093-3", "13457-7", "2085-9", "2571-8",
            # Additional
            "3084-1", "2324-2"
        ],
        "estimated_price_cents": 35000  # $350
    }
}

class PatientInfo(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: Optional[str] = None
    date_of_birth: str  # YYYY-MM-DD
    gender: str  # male, female
    address_line_1: str
    address_line_2: Optional[str] = None
    city: str
    state: str  # 2-letter code
    zip_code: str
    country: str = "US"

class CreateOrderRequest(BaseModel):
    user_id: str
    panel_type: PanelType = PanelType.ESSENTIAL
    collection_method: CollectionMethod = CollectionMethod.WALK_IN
    patient_info: PatientInfo
    custom_markers: Optional[List[str]] = None  # For custom panels

class OrderResponse(BaseModel):
    order_id: str
    junction_order_id: str
    status: str
    panel_type: str
    collection_method: str
    requisition_url: Optional[str] = None
    tracking_url: Optional[str] = None
    appointment_url: Optional[str] = None
    estimated_price_cents: int
    created_at: datetime

class LabResult(BaseModel):
    code: str
    display_name: str
    value: float
    unit: str
    reference_low: Optional[float] = None
    reference_high: Optional[float] = None
    flag: Optional[str] = None  # H, L, N, C
    loinc: Optional[str] = None

class OrderResultsResponse(BaseModel):
    order_id: str
    status: str
    collection_date: Optional[datetime] = None
    results_date: Optional[datetime] = None
    markers: List[LabResult] = []

# =============================================================================
# JUNCTION API CLIENT
# =============================================================================

class JunctionClient:
    """Client for Junction/Vital Lab API."""
    
    def __init__(self):
        self.base_url = JUNCTION_BASE_URL
        self.api_key = JUNCTION_API_KEY
        self.headers = {
            "x-vital-api-key": self.api_key,
            "Content-Type": "application/json"
        }
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make authenticated request to Junction API."""
        async with httpx.AsyncClient() as client:
            url = f"{self.base_url}{endpoint}"
            response = await client.request(
                method=method,
                url=url,
                headers=self.headers,
                **kwargs
            )
            
            if response.status_code >= 400:
                error_detail = response.json() if response.content else {"message": "Unknown error"}
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Junction API error: {error_detail}"
                )
            
            return response.json() if response.content else {}
    
    async def create_user(self, user_id: str, patient: PatientInfo) -> Dict[str, Any]:
        """Create or get Junction user for GenoMAX² user."""
        # Check if user exists
        try:
            existing = await self._request("GET", f"/user/{user_id}")
            return existing
        except HTTPException as e:
            if e.status_code != 404:
                raise
        
        # Create new user
        payload = {
            "client_user_id": user_id,
            "first_name": patient.first_name,
            "last_name": patient.last_name,
            "email": patient.email,
            "phone_number": patient.phone,
            "date_of_birth": patient.date_of_birth,
            "gender": patient.gender
        }
        
        return await self._request("POST", "/user", json=payload)
    
    async def create_order(self, request: CreateOrderRequest) -> Dict[str, Any]:
        """Create lab order through Junction."""
        # Ensure user exists in Junction
        junction_user = await self.create_user(request.user_id, request.patient_info)
        junction_user_id = junction_user.get("user_id")
        
        # Get panel configuration
        if request.panel_type == PanelType.CUSTOM and request.custom_markers:
            marker_codes = request.custom_markers
        else:
            panel_config = PANEL_CONFIGS.get(request.panel_type, PANEL_CONFIGS[PanelType.ESSENTIAL])
            marker_codes = panel_config["marker_codes"]
        
        # Build order payload
        payload = {
            "user_id": junction_user_id,
            "lab_test": {
                "marker_ids": marker_codes,
                "collection_method": request.collection_method.value
            },
            "patient_details": {
                "first_name": request.patient_info.first_name,
                "last_name": request.patient_info.last_name,
                "email": request.patient_info.email,
                "phone_number": request.patient_info.phone,
                "dob": request.patient_info.date_of_birth,
                "gender": request.patient_info.gender
            },
            "patient_address": {
                "first_line": request.patient_info.address_line_1,
                "second_line": request.patient_info.address_line_2,
                "city": request.patient_info.city,
                "state": request.patient_info.state,
                "zip_code": request.patient_info.zip_code,
                "country": request.patient_info.country
            }
        }
        
        return await self._request("POST", "/order", json=payload)
    
    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get order status and details."""
        return await self._request("GET", f"/order/{order_id}")
    
    async def get_order_results(self, order_id: str) -> Dict[str, Any]:
        """Get lab results for completed order."""
        return await self._request("GET", f"/order/{order_id}/result")
    
    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order (if not yet collected)."""
        return await self._request("POST", f"/order/{order_id}/cancel")
    
    async def get_phlebotomy_appointment_availability(
        self, 
        zip_code: str, 
        date_from: str, 
        date_to: str
    ) -> List[Dict[str, Any]]:
        """Get available appointment slots for at-home phlebotomy."""
        params = {
            "zip_code": zip_code,
            "start_date": date_from,
            "end_date": date_to
        }
        return await self._request("GET", "/appointment/availability", params=params)
    
    async def schedule_appointment(
        self, 
        order_id: str, 
        slot_id: str
    ) -> Dict[str, Any]:
        """Schedule phlebotomy appointment for an order."""
        payload = {"booking_key": slot_id}
        return await self._request("POST", f"/order/{order_id}/appointment", json=payload)
    
    async def get_psc_locations(
        self, 
        zip_code: str, 
        radius_miles: int = 25
    ) -> List[Dict[str, Any]]:
        """Get nearby Patient Service Centers for walk-in collection."""
        params = {
            "zip_code": zip_code,
            "radius": radius_miles
        }
        return await self._request("GET", "/psc/search", params=params)

# Singleton client instance
junction_client = JunctionClient()

# =============================================================================
# API ENDPOINTS
# =============================================================================

@router.post("/orders", response_model=OrderResponse)
async def create_lab_order(request: CreateOrderRequest):
    """
    Create a new lab order through Junction.
    
    Panel Types:
    - essential: 20+ markers including iron panel, vitamins, CMP-14 (~$150)
    - complete: 40+ markers including thyroid, CBC, lipids (~$350)
    - custom: Specify your own markers
    
    Collection Methods:
    - walk-in: Patient visits a PSC location
    - testkit: At-home collection kit shipped to patient
    - at-home-phlebotomy: Phlebotomist visits patient
    """
    # Create order in Junction
    junction_response = await junction_client.create_order(request)
    
    # Get panel config for pricing
    panel_config = PANEL_CONFIGS.get(request.panel_type, PANEL_CONFIGS[PanelType.ESSENTIAL])
    
    # TODO: Store in database
    # await db.lab_orders.insert({...})
    
    return OrderResponse(
        order_id=f"gmax_{junction_response.get('id', 'unknown')}",
        junction_order_id=junction_response.get("id", ""),
        status=junction_response.get("status", "created"),
        panel_type=request.panel_type.value,
        collection_method=request.collection_method.value,
        requisition_url=junction_response.get("requisition_form_url"),
        tracking_url=junction_response.get("testkit", {}).get("tracking_url"),
        appointment_url=junction_response.get("appointment", {}).get("booking_url"),
        estimated_price_cents=panel_config["estimated_price_cents"],
        created_at=datetime.utcnow()
    )

@router.get("/orders/{order_id}", response_model=OrderResponse)
async def get_lab_order(order_id: str):
    """Get status and details of a lab order."""
    # Extract Junction ID
    junction_id = order_id.replace("gmax_", "") if order_id.startswith("gmax_") else order_id
    
    junction_response = await junction_client.get_order(junction_id)
    
    return OrderResponse(
        order_id=order_id,
        junction_order_id=junction_id,
        status=junction_response.get("status", "unknown"),
        panel_type=junction_response.get("lab_test", {}).get("name", "unknown"),
        collection_method=junction_response.get("lab_test", {}).get("collection_method", "unknown"),
        requisition_url=junction_response.get("requisition_form_url"),
        tracking_url=junction_response.get("testkit", {}).get("tracking_url"),
        appointment_url=junction_response.get("appointment", {}).get("booking_url"),
        estimated_price_cents=0,  # TODO: Look up from DB
        created_at=datetime.fromisoformat(junction_response.get("created_at", datetime.utcnow().isoformat()))
    )

@router.get("/orders/{order_id}/results", response_model=OrderResultsResponse)
async def get_lab_results(order_id: str):
    """Get lab results for a completed order."""
    junction_id = order_id.replace("gmax_", "") if order_id.startswith("gmax_") else order_id
    
    junction_response = await junction_client.get_order_results(junction_id)
    
    # Transform Junction results to GenoMAX² format
    markers = []
    for result in junction_response.get("results", []):
        markers.append(LabResult(
            code=result.get("slug", ""),
            display_name=result.get("name", ""),
            value=result.get("value", 0),
            unit=result.get("unit", ""),
            reference_low=result.get("min_range_value"),
            reference_high=result.get("max_range_value"),
            flag=_determine_flag(result),
            loinc=result.get("loinc", "")
        ))
    
    return OrderResultsResponse(
        order_id=order_id,
        status=junction_response.get("status", "unknown"),
        collection_date=junction_response.get("date_collected"),
        results_date=junction_response.get("date_reported"),
        markers=markers
    )

@router.post("/orders/{order_id}/cancel")
async def cancel_lab_order(order_id: str):
    """Cancel a lab order (only if sample not yet collected)."""
    junction_id = order_id.replace("gmax_", "") if order_id.startswith("gmax_") else order_id
    
    await junction_client.cancel_order(junction_id)
    
    return {"status": "cancelled", "order_id": order_id}

@router.get("/locations")
async def get_psc_locations(zip_code: str, radius: int = 25):
    """Get nearby Patient Service Centers for walk-in blood draws."""
    locations = await junction_client.get_psc_locations(zip_code, radius)
    return {"locations": locations, "count": len(locations)}

@router.get("/appointments/availability")
async def get_appointment_availability(
    zip_code: str,
    date_from: str,
    date_to: str
):
    """Get available slots for at-home phlebotomy appointments."""
    slots = await junction_client.get_phlebotomy_appointment_availability(
        zip_code, date_from, date_to
    )
    return {"slots": slots, "count": len(slots)}

@router.post("/orders/{order_id}/appointment")
async def schedule_appointment(order_id: str, slot_id: str):
    """Schedule an at-home phlebotomy appointment."""
    junction_id = order_id.replace("gmax_", "") if order_id.startswith("gmax_") else order_id
    
    result = await junction_client.schedule_appointment(junction_id, slot_id)
    
    return {
        "status": "scheduled",
        "order_id": order_id,
        "appointment": result
    }

# =============================================================================
# WEBHOOK HANDLER
# =============================================================================

def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Verify Junction webhook signature using HMAC-SHA256."""
    if not JUNCTION_WEBHOOK_SECRET:
        return True  # Skip verification in development
    
    expected = hmac.new(
        JUNCTION_WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(f"sha256={expected}", signature)

@router.post("/webhooks/junction")
async def handle_junction_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_vital_signature: str = Header(None)
):
    """
    Handle Junction/Vital webhook events.
    
    Events:
    - order.created: Order successfully created
    - requisition.created: Requisition form ready
    - testkit.shipped: Kit shipped to patient
    - testkit.delivered: Kit delivered
    - sample.received: Sample received at lab
    - labtest.completed: Results ready
    - order.cancelled: Order was cancelled
    """
    payload = await request.body()
    
    # Verify signature
    if x_vital_signature and not verify_webhook_signature(payload, x_vital_signature):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
    
    event = await request.json()
    event_type = event.get("event_type", "")
    data = event.get("data", {})
    
    # Process event in background
    background_tasks.add_task(process_webhook_event, event_type, data)
    
    return {"received": True, "event_type": event_type}

async def process_webhook_event(event_type: str, data: Dict[str, Any]):
    """Process webhook event asynchronously."""
    order_id = data.get("order_id", "")
    user_id = data.get("user_id", "")
    
    # Map Junction events to GenoMAX² status
    status_map = {
        "order.created": "created",
        "requisition.created": "requisition_ready",
        "testkit.shipped": "kit_shipped",
        "testkit.delivered": "kit_delivered",
        "appointment.scheduled": "appointment_scheduled",
        "appointment.completed": "sample_collected",
        "sample.received": "sample_received",
        "labtest.processing": "processing",
        "labtest.completed": "completed",
        "order.cancelled": "cancelled",
        "order.failed": "failed"
    }
    
    new_status = status_map.get(event_type)
    
    if new_status:
        # TODO: Update database
        # await db.lab_orders.update(
        #     {"provider_order_id": order_id},
        #     {"$set": {"status": new_status}, "$push": {"status_history": {...}}}
        # )
        pass
    
    # Handle results completion
    if event_type == "labtest.completed":
        await handle_results_ready(order_id, data)

async def handle_results_ready(order_id: str, data: Dict[str, Any]):
    """
    Process completed lab results.
    
    1. Fetch full results from Junction
    2. Normalize to GenoMAX² format
    3. Create bloodwork_submission record
    4. Trigger Brain pipeline if auto-process enabled
    """
    # Fetch results
    results = await junction_client.get_order_results(order_id)
    
    # Normalize markers
    normalized_markers = []
    for result in results.get("results", []):
        normalized_markers.append({
            "code": _normalize_marker_code(result.get("slug", "")),
            "original_name": result.get("name", ""),
            "value": result.get("value"),
            "unit": result.get("unit", ""),
            "reference_range": {
                "low": result.get("min_range_value"),
                "high": result.get("max_range_value")
            },
            "flag": _determine_flag(result),
            "confidence": 1.0,  # Direct from lab = perfect confidence
            "loinc": result.get("loinc", "")
        })
    
    # TODO: Create bloodwork_submission record
    # submission_id = await db.bloodwork_submissions.insert({
    #     "user_id": user_id,
    #     "lab_order_id": order_id,
    #     "source": "junction",
    #     "normalized_markers": normalized_markers,
    #     "confidence_score": 1.0,
    #     "status": "ready"
    # })
    
    # TODO: Optionally trigger Brain pipeline
    # if user_settings.auto_process_results:
    #     await trigger_brain_run(user_id, submission_id)
    
    print(f"Results ready for order {order_id}: {len(normalized_markers)} markers")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _determine_flag(result: Dict[str, Any]) -> str:
    """Determine H/L/N/C flag from result values."""
    value = result.get("value")
    min_val = result.get("min_range_value")
    max_val = result.get("max_range_value")
    
    if value is None:
        return "N"
    
    # Check for critical values (typically 2x outside range)
    if min_val and value < min_val * 0.5:
        return "C"  # Critical low
    if max_val and value > max_val * 2:
        return "C"  # Critical high
    
    if min_val and value < min_val:
        return "L"  # Low
    if max_val and value > max_val:
        return "H"  # High
    
    return "N"  # Normal

def _normalize_marker_code(junction_slug: str) -> str:
    """Map Junction marker slug to GenoMAX² code."""
    # Junction uses different naming conventions
    slug_map = {
        "ferritin_serum": "ferritin",
        "iron_serum": "serum_iron",
        "total_iron_binding_capacity": "tibc",
        "transferrin_saturation": "transferrin_sat",
        "vitamin_d_25_hydroxy": "vitamin_d_25oh",
        "vitamin_b12_cobalamin": "vitamin_b12",
        "folate_serum": "folate",
        "hemoglobin_a1c": "hba1c",
        "hs_crp": "hscrp",
        "c_reactive_protein_hs": "hscrp",
        "homocysteine_plasma": "homocysteine",
        "omega_3_index": "omega3_index",
        "magnesium_rbc": "magnesium_rbc",
        "zinc_serum": "zinc"
    }
    
    return slug_map.get(junction_slug, junction_slug)

# =============================================================================
# INTEGRATION NOTES
# =============================================================================
"""
INTEGRATION STEPS:
1. Import router: from junction_client import router as junction_router
2. Include in FastAPI: app.include_router(junction_router)
3. Set environment variables:
   - JUNCTION_API_KEY
   - JUNCTION_WEBHOOK_SECRET
   - JUNCTION_ENVIRONMENT (sandbox/production)

WEBHOOK SETUP:
Configure webhook URL in Junction dashboard:
https://your-domain.com/api/v1/labs/webhooks/junction

TESTING:
1. Create test order with sandbox credentials
2. Use Junction's webhook simulator to test events
3. Verify database updates and Brain pipeline triggers
"""
