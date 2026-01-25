"""
GenoMAX² Lab API Adapter Interface
===================================
Unified interface for lab API integrations (Junction/Vital, Quest, LabCorp, etc.)

Primary Integration: Junction (https://junction.com, formerly Vital/tryvital.io)
- Aggregates Quest, LabCorp, BioReference through single API
- Transparent pricing, no test upcharges
- Operates in restricted states (NY, NJ, RI)

Secondary Integration: Lab Testing API (https://labtestingapi.com)
- Direct Quest Diagnostics access (2,300+ locations)
- Up to 80% off standard lab pricing
- 47 states (excludes NY, NJ, RI)
- PWNHealth physician oversight included

Usage:
    from bloodwork_engine.lab_adapters import VitalAdapter, LabTestingAPIAdapter, get_adapter
    
    # Get adapter by name
    adapter = get_adapter("vital")  # or "lab_testing_api"
    
    # Fetch results
    results = adapter.fetch_results(user_id="12345")
    
    # Convert to engine input format
    markers = adapter.to_engine_input(results)
"""

import os
import logging
import httpx
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict, field
from datetime import datetime, date
from enum import Enum

logger = logging.getLogger(__name__)


class LabProvider(Enum):
    """Supported lab providers."""
    VITAL = "vital"           # Junction (formerly Vital) - aggregator
    QUEST = "quest"           # Quest Diagnostics (via Junction)
    LABCORP = "labcorp"       # LabCorp (via Junction)
    BIOREFERENCE = "bioreference"  # BioReference (via Junction)
    HEALTH_GORILLA = "health_gorilla"  # Health Gorilla aggregator
    LAB_TESTING_API = "lab_testing_api"  # Lab Testing API (Quest access)
    MANUAL = "manual"         # Manual entry / OCR upload


@dataclass
class LabPatient:
    """Patient information for lab orders."""
    external_id: str          # GenoMAX user ID
    lab_patient_id: Optional[str] = None  # Lab's patient ID (Junction user_id)
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    sex: Optional[str] = None  # 'male' or 'female'
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[Dict[str, str]] = None  # {street, city, state, zip, country}


@dataclass
class LabOrder:
    """A lab test order."""
    order_id: str
    patient: LabPatient
    status: str              # 'pending', 'received', 'collecting', 'collected', 'completed', 'cancelled'
    tests_ordered: List[str]  # Test codes/marker IDs
    ordered_at: datetime
    collected_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    lab_provider: Optional[str] = None
    lab_location: Optional[Dict[str, Any]] = None
    collection_method: str = "walk_in_test"  # or 'at_home_phlebotomy', 'testkit'
    requisition_url: Optional[str] = None  # PDF link for walk-in


@dataclass
class LabMarkerResult:
    """A single marker result from a lab."""
    marker_code: str         # Lab's code for the marker
    marker_name: str         # Lab's display name
    value: Any               # Numeric value or string
    unit: str                # Unit as reported by lab
    reference_low: Optional[float] = None
    reference_high: Optional[float] = None
    flag: Optional[str] = None  # 'L', 'H', 'C' (critical), etc.
    notes: Optional[str] = None
    performed_at: Optional[datetime] = None
    loinc_code: Optional[str] = None  # LOINC standardized code


@dataclass
class LabResults:
    """Complete lab results response."""
    result_id: str
    order_id: str
    patient: LabPatient
    lab_provider: str
    report_date: date
    markers: List[LabMarkerResult]
    raw_response: Optional[Dict] = None  # Original API response
    pdf_url: Optional[str] = None
    status: str = "final"    # 'preliminary', 'partial', 'final'


@dataclass
class LabTest:
    """A lab test that can be ordered."""
    test_id: str
    name: str
    description: Optional[str] = None
    markers: List[str] = field(default_factory=list)  # Biomarkers included
    sample_type: str = "blood"  # blood, urine, saliva
    fasting_required: bool = False
    price: Optional[float] = None
    turnaround_days: Optional[int] = None
    lab_provider: Optional[str] = None


@dataclass 
class LabLocation:
    """A lab collection site (PSC - Patient Service Center)."""
    location_id: str
    name: str
    address: str
    city: str
    state: str
    zip_code: str
    phone: Optional[str] = None
    hours: Optional[Dict[str, str]] = None  # {monday: "7am-5pm", ...}
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_miles: Optional[float] = None
    lab_provider: str = "Quest Diagnostics"


class LabAdapter(ABC):
    """
    Abstract base class for lab API integrations.
    
    Each lab provider implements this interface to provide a unified
    way to order tests and fetch results.
    """
    
    provider: LabProvider
    
    @abstractmethod
    def __init__(self, api_key: Optional[str] = None, **config):
        pass
    
    @abstractmethod
    def validate_credentials(self) -> Dict[str, Any]:
        """Validate credentials and return status."""
        pass
    
    @abstractmethod
    def fetch_results(
        self,
        user_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        pass
    
    @abstractmethod
    def get_order_status(self, order_id: str) -> LabOrder:
        pass
    
    def to_engine_input(self, results: LabResults) -> List[Dict[str, Any]]:
        """Convert lab results to BloodworkEngineV2 input format."""
        mapped_markers = []
        
        for marker in results.markers:
            canonical_code = self._map_marker_code(marker.marker_code, marker.marker_name)
            
            if canonical_code:
                mapped_markers.append({
                    "code": canonical_code,
                    "value": marker.value,
                    "unit": marker.unit
                })
            else:
                logger.warning(f"Unknown marker code: {marker.marker_code} ({marker.marker_name})")
        
        return mapped_markers
    
    def _map_marker_code(self, lab_code: str, lab_name: str) -> Optional[str]:
        """Map lab-specific marker code to GenoMAX canonical code."""
        code_lower = lab_code.lower()
        name_lower = lab_name.lower()
        
        # Common mappings based on LOINC codes and common names
        mappings = {
            # Iron
            "ferritin": "ferritin", "2276-4": "ferritin",
            "iron": "iron", "serum iron": "iron",
            "hemoglobin": "hemoglobin", "hgb": "hemoglobin", "718-7": "hemoglobin",
            
            # Vitamins
            "vitamin d": "vitamin_d_25oh", "25-hydroxy": "vitamin_d_25oh",
            "25-oh vitamin d": "vitamin_d_25oh", "1989-3": "vitamin_d_25oh",
            "vitamin b12": "vitamin_b12", "b12": "vitamin_b12", "cobalamin": "vitamin_b12",
            "folate": "folate_serum", "folic acid": "folate_serum",
            
            # Liver
            "alt": "alt", "alanine aminotransferase": "alt", "sgpt": "alt", "1742-6": "alt",
            "ast": "ast", "aspartate aminotransferase": "ast", "sgot": "ast", "1920-8": "ast",
            "ggt": "ggt", "gamma-glutamyl": "ggt", "gamma glutamyl": "ggt",
            
            # Kidney
            "creatinine": "creatinine", "2160-0": "creatinine",
            "egfr": "egfr", "estimated gfr": "egfr", "glomerular filtration": "egfr",
            
            # Glucose/Metabolic
            "glucose": "fasting_glucose", "fasting glucose": "fasting_glucose",
            "hba1c": "hba1c", "hemoglobin a1c": "hba1c", "a1c": "hba1c", "glycated": "hba1c",
            "insulin": "fasting_insulin", "fasting insulin": "fasting_insulin",
            "homocysteine": "homocysteine",
            
            # Inflammation
            "crp": "hs_crp", "c-reactive": "hs_crp", "hs-crp": "hs_crp",
            "high sensitivity crp": "hs_crp",
            
            # Minerals
            "calcium": "calcium_serum", "17861-6": "calcium_serum",
            "magnesium": "magnesium_serum", "19123-9": "magnesium_serum",
            "potassium": "potassium", "2823-3": "potassium",
            "zinc": "zinc_serum", "zinc serum": "zinc_serum",
            "copper": "copper_serum", "copper serum": "copper_serum",
            
            # Thyroid
            "tsh": "tsh", "thyroid stimulating": "tsh", "3016-3": "tsh",
            "free t3": "free_t3", "ft3": "free_t3", "triiodothyronine free": "free_t3",
            "free t4": "free_t4", "ft4": "free_t4", "thyroxine free": "free_t4",
            
            # Lipids
            "triglycerides": "triglycerides", "2571-8": "triglycerides",
            "ldl": "ldl_cholesterol", "ldl cholesterol": "ldl_cholesterol",
            "hdl": "hdl_cholesterol", "hdl cholesterol": "hdl_cholesterol",
            "apolipoprotein b": "apolipoprotein_b", "apob": "apolipoprotein_b", "apo b": "apolipoprotein_b",
            "lp(a)": "lp_a", "lipoprotein(a)": "lp_a", "lipoprotein a": "lp_a",
            
            # Hormones
            "testosterone": "total_testosterone", "total testosterone": "total_testosterone",
            "free testosterone": "free_testosterone",
            "estradiol": "estradiol", "e2": "estradiol",
            "progesterone": "progesterone",
            "cortisol": "cortisol_am",
            "dhea": "dhea_s", "dhea-s": "dhea_s", "dhea sulfate": "dhea_s",
            "shbg": "shbg", "sex hormone binding": "shbg",
            
            # Other
            "uric acid": "uric_acid",
            "platelets": "platelet_count", "platelet count": "platelet_count",
            "omega-3": "omega3_index", "omega-3 index": "omega3_index",
        }
        
        for key, canonical in mappings.items():
            if key in code_lower or key in name_lower:
                return canonical
        
        return None


class VitalAdapter(LabAdapter):
    """
    Adapter for Junction (formerly Vital) lab API.
    
    Junction aggregates multiple labs including Quest, LabCorp, and BioReference.
    Documentation: https://docs.junction.com/
    
    Features:
    - Single API for multiple lab networks
    - Walk-in tests at 2,300+ Quest/LabCorp locations
    - At-home phlebotomy service
    - At-home test kits
    - Operates in restricted states (NY, NJ, RI)
    
    API Version: v3 (updated January 2025)
    """
    
    provider = LabProvider.VITAL
    
    # GenoMAX Panel - our standard bloodwork panel
    GENOMAX_PANEL_MARKERS = [
        "ferritin", "vitamin_d_25oh", "vitamin_b12", "folate_serum",
        "alt", "ast", "ggt", "creatinine", "egfr",
        "fasting_glucose", "hba1c", "fasting_insulin", "homocysteine",
        "hs_crp", "calcium_serum", "magnesium_serum", "potassium",
        "zinc_serum", "copper_serum",
        "tsh", "free_t3", "free_t4",
        "triglycerides", "ldl_cholesterol", "hdl_cholesterol",
        "apolipoprotein_b", "lp_a",
        "total_testosterone", "estradiol", "cortisol_am", "dhea_s", "shbg",
        "omega3_index"
    ]
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        environment: str = "sandbox",  # 'sandbox' or 'production'
        region: str = "us",  # 'us' or 'eu'
        timeout: int = 30
    ):
        self.api_key = api_key or os.environ.get("VITAL_API_KEY")
        self.environment = environment
        self.region = region
        self.timeout = timeout
        
        # Base URL - Junction v3 API (updated January 2025)
        # Supports both junction.com and tryvital.io domains
        if environment == "production":
            self.base_url = "https://api.tryvital.io/v3"
        else:
            self.base_url = "https://api.sandbox.tryvital.io/v3"
        
        # Authentication header per Junction docs
        self.headers = {
            "x-vital-api-key": self.api_key or "",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make HTTP request to Junction API."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    json=data,
                    params=params
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Junction API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Junction API request failed: {e}")
            raise
    
    def validate_credentials(self) -> Dict[str, Any]:
        """Validate Junction API credentials."""
        if not self.api_key:
            return {
                "valid": False,
                "error": "VITAL_API_KEY not configured",
                "environment": self.environment
            }
        
        try:
            # Test with lab tests endpoint (valid v3 endpoint)
            result = self._request("GET", "/lab_tests/labs")
            return {
                "valid": True,
                "labs_available": len(result) if isinstance(result, list) else 0,
                "environment": self.environment,
                "region": self.region,
                "api_version": "v3"
            }
        except Exception as e:
            return {
                "valid": False,
                "error": str(e),
                "environment": self.environment
            }
    
    # =========================================================
    # USER MANAGEMENT
    # =========================================================
    
    def create_user(self, patient: LabPatient) -> Dict[str, Any]:
        """
        Create a user in Junction.
        
        Args:
            patient: Patient information
        
        Returns:
            Dict with user_id and client_user_id
        """
        data = {
            "client_user_id": patient.external_id
        }
        
        result = self._request("POST", "/user", data=data)
        
        # Update patient with Junction user_id
        patient.lab_patient_id = result.get("user_id")
        
        return result
    
    def get_user(self, user_id: str) -> Dict[str, Any]:
        """Get user details from Junction."""
        return self._request("GET", f"/user/{user_id}")
    
    def delete_user(self, user_id: str) -> Dict[str, Any]:
        """Delete a user from Junction."""
        return self._request("DELETE", f"/user/{user_id}")
    
    # =========================================================
    # LAB TESTS CATALOG
    # =========================================================
    
    def list_lab_tests(self) -> List[LabTest]:
        """
        List available lab tests from Junction.
        
        Returns:
            List of LabTest objects
        """
        result = self._request("GET", "/lab_tests")
        
        tests = []
        test_list = result.get("lab_tests", []) if isinstance(result, dict) else result
        
        for test in test_list:
            tests.append(LabTest(
                test_id=str(test.get("id")),
                name=test.get("name"),
                description=test.get("description"),
                markers=[m.get("name") for m in test.get("markers", [])],
                sample_type=test.get("sample_type", "blood"),
                fasting_required=test.get("fasting", False),
                turnaround_days=test.get("turnaround_time_days"),
                lab_provider=test.get("lab", {}).get("name") if isinstance(test.get("lab"), dict) else test.get("lab")
            ))
        
        return tests
    
    def get_lab_test(self, test_id: str) -> LabTest:
        """Get details for a specific lab test."""
        result = self._request("GET", f"/lab_tests/{test_id}")
        
        return LabTest(
            test_id=str(result.get("id")),
            name=result.get("name"),
            description=result.get("description"),
            markers=[m.get("name") for m in result.get("markers", [])],
            sample_type=result.get("sample_type", "blood"),
            fasting_required=result.get("fasting", False),
            turnaround_days=result.get("turnaround_time_days"),
            lab_provider=result.get("lab", {}).get("name") if isinstance(result.get("lab"), dict) else result.get("lab")
        )
    
    def list_markers(self) -> List[Dict[str, Any]]:
        """List all available biomarkers from Junction."""
        result = self._request("GET", "/lab_tests/markers")
        return result.get("markers", []) if isinstance(result, dict) else result
    
    def list_labs(self) -> List[Dict[str, Any]]:
        """List all available labs from Junction."""
        result = self._request("GET", "/lab_tests/labs")
        return result if isinstance(result, list) else result.get("labs", [])
    
    # =========================================================
    # AREA/COVERAGE INFO
    # =========================================================
    
    def get_area_info(self, zip_code: str) -> Dict[str, Any]:
        """
        Check service coverage for a ZIP code.
        
        Args:
            zip_code: US ZIP code
        
        Returns:
            Coverage information for the area
        """
        return self._request("GET", f"/lab_tests/area_info", params={"zip_code": zip_code})
    
    # =========================================================
    # LAB LOCATIONS (PSC - Patient Service Centers)
    # =========================================================
    
    def find_lab_locations(
        self,
        zip_code: str,
        radius_miles: int = 25,
        lab_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Find nearby lab locations for walk-in tests.
        
        Args:
            zip_code: US ZIP code
            radius_miles: Search radius
            lab_id: Filter by specific lab (quest, labcorp)
        
        Returns:
            List of lab locations with address and hours
        """
        params = {
            "zip_code": zip_code,
            "radius": radius_miles
        }
        if lab_id:
            params["lab_id"] = lab_id
        
        result = self._request("GET", "/lab_tests/psc", params=params)
        return result.get("pscs", []) if isinstance(result, dict) else result
    
    # =========================================================
    # ORDER MANAGEMENT
    # =========================================================
    
    def create_order(
        self,
        user_id: str,
        lab_test_id: str,
        collection_method: str = "walk_in_test",
        patient_details: Optional[Dict] = None,
        patient_address: Optional[Dict] = None,
        physician: Optional[Dict] = None
    ) -> LabOrder:
        """
        Create a lab test order.
        
        Args:
            user_id: Junction user ID
            lab_test_id: Lab test ID to order
            collection_method: 'walk_in_test', 'at_home_phlebotomy', or 'testkit'
            patient_details: {first_name, last_name, dob, gender, email, phone_number}
            patient_address: {street, city, state, zip_code, country}
            physician: Optional ordering physician info
        
        Returns:
            LabOrder with order details and requisition URL
        """
        data = {
            "user_id": user_id,
            "lab_test_id": lab_test_id,
            "collection_method": collection_method
        }
        
        if patient_details:
            data["patient_details"] = patient_details
        
        if patient_address:
            data["patient_address"] = patient_address
        
        if physician:
            data["physician"] = physician
        
        result = self._request("POST", "/order", data=data)
        
        patient = LabPatient(
            external_id=result.get("user_id"),
            lab_patient_id=result.get("user_id"),
            first_name=patient_details.get("first_name") if patient_details else None,
            last_name=patient_details.get("last_name") if patient_details else None
        )
        
        return LabOrder(
            order_id=result.get("id"),
            patient=patient,
            status=result.get("status", "pending"),
            tests_ordered=[lab_test_id],
            ordered_at=datetime.fromisoformat(result.get("created_at").replace("Z", "+00:00")) if result.get("created_at") else datetime.now(),
            lab_provider=result.get("lab", {}).get("name") if isinstance(result.get("lab"), dict) else result.get("lab"),
            collection_method=collection_method,
            requisition_url=result.get("requisition_form", {}).get("url") if isinstance(result.get("requisition_form"), dict) else None
        )
    
    def get_order_status(self, order_id: str) -> LabOrder:
        """Get the current status of a lab order."""
        result = self._request("GET", f"/order/{order_id}")
        
        patient = LabPatient(
            external_id=result.get("user_id"),
            lab_patient_id=result.get("user_id")
        )
        
        status_map = {
            "pending": "pending",
            "requisition_created": "pending",
            "received.testkit.registered": "received",
            "collecting_sample": "collecting",
            "sample_with_lab": "processing",
            "partial_results": "partial",
            "completed": "completed",
            "cancelled": "cancelled",
            "failed": "failed"
        }
        
        return LabOrder(
            order_id=result.get("id"),
            patient=patient,
            status=status_map.get(result.get("status"), result.get("status")),
            tests_ordered=[result.get("lab_test", {}).get("id")] if isinstance(result.get("lab_test"), dict) else [],
            ordered_at=datetime.fromisoformat(result.get("created_at").replace("Z", "+00:00")) if result.get("created_at") else datetime.now(),
            completed_at=datetime.fromisoformat(result.get("completed_at").replace("Z", "+00:00")) if result.get("completed_at") else None,
            lab_provider=result.get("lab", {}).get("name") if isinstance(result.get("lab"), dict) else result.get("lab"),
            collection_method=result.get("details", {}).get("type", "walk_in_test") if isinstance(result.get("details"), dict) else "walk_in_test",
            requisition_url=result.get("requisition_form", {}).get("url") if isinstance(result.get("requisition_form"), dict) else None
        )
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel a pending lab order."""
        return self._request("POST", f"/order/{order_id}/cancel")
    
    def list_orders(
        self,
        user_id: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[LabOrder]:
        """List lab orders with optional filters."""
        params = {}
        if user_id:
            params["user_id"] = user_id
        if status:
            params["status"] = status
        if start_date:
            params["start_date"] = start_date.isoformat()
        if end_date:
            params["end_date"] = end_date.isoformat()
        
        result = self._request("GET", "/orders", params=params)
        
        orders = []
        order_list = result.get("orders", []) if isinstance(result, dict) else result
        
        for order in order_list:
            patient = LabPatient(
                external_id=order.get("user_id"),
                lab_patient_id=order.get("user_id")
            )
            orders.append(LabOrder(
                order_id=order.get("id"),
                patient=patient,
                status=order.get("status"),
                tests_ordered=[order.get("lab_test", {}).get("id")] if isinstance(order.get("lab_test"), dict) else [],
                ordered_at=datetime.fromisoformat(order.get("created_at").replace("Z", "+00:00")) if order.get("created_at") else datetime.now(),
                lab_provider=order.get("lab", {}).get("name") if isinstance(order.get("lab"), dict) else order.get("lab")
            ))
        
        return orders
    
    # =========================================================
    # RESULTS
    # =========================================================
    
    def fetch_results(
        self,
        user_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        """
        Fetch lab results from Junction.
        
        Args:
            user_id: Filter by Junction user ID
            order_id: Filter by specific order ID
            from_date: Filter results from this date
            to_date: Filter results until this date
        
        Returns:
            List of LabResults
        """
        if order_id:
            # Fetch specific order results
            result = self._request("GET", f"/order/{order_id}/result")
            return [self._parse_result(result, order_id)]
        
        # Fetch by user
        params = {}
        if from_date:
            params["start_date"] = from_date.isoformat()
        if to_date:
            params["end_date"] = to_date.isoformat()
        
        result = self._request("GET", f"/user/{user_id}/results", params=params)
        
        results = []
        result_list = result.get("results", []) if isinstance(result, dict) else result
        
        for r in result_list:
            results.append(self._parse_result(r, r.get("order_id")))
        
        return results
    
    def _parse_result(self, data: Dict, order_id: str) -> LabResults:
        """Parse Junction API result into LabResults object."""
        markers = []
        
        result_list = data.get("results", []) if isinstance(data, dict) else []
        
        for biomarker in result_list:
            markers.append(LabMarkerResult(
                marker_code=biomarker.get("slug", biomarker.get("name", "")),
                marker_name=biomarker.get("name", ""),
                value=biomarker.get("value"),
                unit=biomarker.get("unit", ""),
                reference_low=biomarker.get("min_range_value"),
                reference_high=biomarker.get("max_range_value"),
                flag=biomarker.get("flag"),
                loinc_code=biomarker.get("loinc", {}).get("code") if isinstance(biomarker.get("loinc"), dict) else None
            ))
        
        patient = LabPatient(
            external_id=data.get("user_id", ""),
            lab_patient_id=data.get("user_id", "")
        )
        
        report_date_str = data.get("date_collected") or data.get("created_at")
        if report_date_str:
            report_date = datetime.fromisoformat(report_date_str.replace("Z", "+00:00")).date()
        else:
            report_date = date.today()
        
        return LabResults(
            result_id=data.get("id", order_id),
            order_id=order_id,
            patient=patient,
            lab_provider=data.get("provider", {}).get("name", "Junction") if isinstance(data.get("provider"), dict) else "Junction",
            report_date=report_date,
            markers=markers,
            raw_response=data,
            pdf_url=data.get("pdf_url"),
            status="final"
        )
    
    def get_result_pdf(self, order_id: str) -> Optional[str]:
        """Get PDF URL for lab results."""
        result = self._request("GET", f"/order/{order_id}/result/pdf")
        return result.get("url")


class LabTestingAPIAdapter(LabAdapter):
    """
    Adapter for Lab Testing API (labtestingapi.com).
    
    Provides direct access to Quest Diagnostics through a developer-friendly API.
    Documentation: https://labtestingapi.com/
    
    Features:
    - Direct Quest Diagnostics access (2,300+ locations)
    - Up to 80% off standard lab pricing
    - Transparent pricing (e.g., CMP-14 at $30.50)
    - 47 states coverage (excludes NY, NJ, RI)
    - PWNHealth physician network oversight
    - Real-time order status
    - Results in PDF and raw format
    - Same-day testing with 3-4 hour results turnaround
    - HIPAA-compliant messaging and results delivery
    
    Pricing Examples:
    - Comprehensive Metabolic Panel (CMP-14): $30.50
    - General Wellness Male Panel: $389.80
    
    API Version: v1 (2025)
    """
    
    provider = LabProvider.LAB_TESTING_API
    
    # Standard test codes for common panels
    TEST_CODES = {
        "cmp": "10165",        # Comprehensive Metabolic Panel
        "cbc": "10001",        # Complete Blood Count
        "lipid": "10216",      # Lipid Panel
        "tsh": "10195",        # TSH
        "vitamin_d": "10201",  # Vitamin D, 25-Hydroxy
        "iron": "10055",       # Iron & TIBC
        "ferritin": "10190",   # Ferritin
        "hba1c": "10192",      # Hemoglobin A1c
        "testosterone_total": "10237",  # Testosterone, Total
    }
    
    # Pricing (as of 2025)
    PRICING = {
        "10165": 30.50,   # CMP-14
        "10001": 18.50,   # CBC
        "10216": 23.00,   # Lipid Panel
        "10195": 32.00,   # TSH
        "10201": 45.00,   # Vitamin D
        "10055": 26.00,   # Iron & TIBC
        "10190": 28.00,   # Ferritin
        "10192": 35.00,   # HbA1c
        "10237": 42.00,   # Testosterone
    }
    
    # Restricted states (no service)
    RESTRICTED_STATES = ["NY", "NJ", "RI"]
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        environment: str = "production",  # 'sandbox' or 'production'
        timeout: int = 30
    ):
        """
        Initialize Lab Testing API adapter.
        
        Args:
            api_key: API key from labtestingapi.com
            environment: 'sandbox' for testing, 'production' for live
            timeout: Request timeout in seconds
        """
        self.api_key = api_key or os.environ.get("LAB_TESTING_API_KEY")
        self.environment = environment
        self.timeout = timeout
        
        # Base URL
        if environment == "sandbox":
            self.base_url = "https://sandbox.labtestingapi.com/api/v1"
        else:
            self.base_url = "https://api.labtestingapi.com/api/v1"
        
        # Authentication header
        self.headers = {
            "Authorization": f"Bearer {self.api_key}" if self.api_key else "",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make HTTP request to Lab Testing API."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    json=data,
                    params=params
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Lab Testing API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Lab Testing API request failed: {e}")
            raise
    
    def validate_credentials(self) -> Dict[str, Any]:
        """Validate Lab Testing API credentials."""
        if not self.api_key:
            return {
                "valid": False,
                "error": "LAB_TESTING_API_KEY not configured",
                "environment": self.environment,
                "provider": "Lab Testing API"
            }
        
        try:
            # Test with tests listing endpoint
            result = self._request("GET", "/tests")
            return {
                "valid": True,
                "tests_available": len(result.get("tests", [])) if isinstance(result, dict) else 0,
                "environment": self.environment,
                "provider": "Lab Testing API",
                "lab_network": "Quest Diagnostics",
                "coverage": "47 states (excludes NY, NJ, RI)"
            }
        except Exception as e:
            return {
                "valid": False,
                "error": str(e),
                "environment": self.environment,
                "provider": "Lab Testing API"
            }
    
    # =========================================================
    # TEST CATALOG
    # =========================================================
    
    def list_lab_tests(self) -> List[LabTest]:
        """
        List available lab tests.
        
        Returns:
            List of LabTest objects
        """
        result = self._request("GET", "/tests")
        
        tests = []
        test_list = result.get("tests", []) if isinstance(result, dict) else result
        
        for test in test_list:
            tests.append(LabTest(
                test_id=str(test.get("id", test.get("code"))),
                name=test.get("name"),
                description=test.get("description"),
                markers=test.get("biomarkers", []),
                sample_type=test.get("sample_type", "blood"),
                fasting_required=test.get("fasting_required", False),
                price=test.get("price"),
                turnaround_days=test.get("turnaround_days", 1),
                lab_provider="Quest Diagnostics"
            ))
        
        return tests
    
    def get_lab_test(self, test_id: str) -> LabTest:
        """Get details for a specific lab test."""
        result = self._request("GET", f"/tests/{test_id}")
        
        return LabTest(
            test_id=str(result.get("id", result.get("code"))),
            name=result.get("name"),
            description=result.get("description"),
            markers=result.get("biomarkers", []),
            sample_type=result.get("sample_type", "blood"),
            fasting_required=result.get("fasting_required", False),
            price=result.get("price"),
            turnaround_days=result.get("turnaround_days", 1),
            lab_provider="Quest Diagnostics"
        )
    
    def get_test_price(self, test_id: str) -> Optional[float]:
        """Get pricing for a specific test."""
        # Check local cache first
        if test_id in self.PRICING:
            return self.PRICING[test_id]
        
        try:
            result = self._request("GET", f"/tests/{test_id}/price")
            return result.get("price")
        except Exception:
            return None
    
    # =========================================================
    # LAB LOCATIONS
    # =========================================================
    
    def find_lab_locations(
        self,
        zip_code: str,
        radius_miles: int = 25
    ) -> List[LabLocation]:
        """
        Find nearby Quest Diagnostics locations.
        
        Args:
            zip_code: US ZIP code
            radius_miles: Search radius (default 25)
        
        Returns:
            List of LabLocation objects
        """
        params = {
            "zip_code": zip_code,
            "radius": radius_miles
        }
        
        result = self._request("GET", "/locations", params=params)
        
        locations = []
        location_list = result.get("locations", []) if isinstance(result, dict) else result
        
        for loc in location_list:
            locations.append(LabLocation(
                location_id=str(loc.get("id")),
                name=loc.get("name", "Quest Diagnostics"),
                address=loc.get("address", {}).get("street", loc.get("address_line1", "")),
                city=loc.get("address", {}).get("city", loc.get("city", "")),
                state=loc.get("address", {}).get("state", loc.get("state", "")),
                zip_code=loc.get("address", {}).get("zip", loc.get("zip_code", "")),
                phone=loc.get("phone"),
                hours=loc.get("hours"),
                latitude=loc.get("latitude"),
                longitude=loc.get("longitude"),
                distance_miles=loc.get("distance"),
                lab_provider="Quest Diagnostics"
            ))
        
        return locations
    
    def check_state_coverage(self, state: str) -> bool:
        """Check if a state is covered by Lab Testing API."""
        return state.upper() not in self.RESTRICTED_STATES
    
    # =========================================================
    # ORDER MANAGEMENT
    # =========================================================
    
    def create_order(
        self,
        patient: LabPatient,
        test_ids: List[str],
        location_id: Optional[str] = None
    ) -> LabOrder:
        """
        Create a lab test order.
        
        Args:
            patient: Patient information (must include DOB, sex, address)
            test_ids: List of test IDs to order
            location_id: Optional preferred Quest location
        
        Returns:
            LabOrder with requisition details
        """
        # Validate state coverage
        if patient.address:
            state = patient.address.get("state", "")
            if not self.check_state_coverage(state):
                raise ValueError(f"Lab Testing API does not operate in {state}. "
                               f"Restricted states: {', '.join(self.RESTRICTED_STATES)}")
        
        data = {
            "patient": {
                "external_id": patient.external_id,
                "first_name": patient.first_name,
                "last_name": patient.last_name,
                "date_of_birth": patient.date_of_birth.isoformat() if patient.date_of_birth else None,
                "sex": patient.sex,
                "email": patient.email,
                "phone": patient.phone,
                "address": patient.address
            },
            "tests": test_ids
        }
        
        if location_id:
            data["location_id"] = location_id
        
        result = self._request("POST", "/orders", data=data)
        
        # Update patient with lab ID
        patient.lab_patient_id = result.get("patient_id")
        
        return LabOrder(
            order_id=result.get("id", result.get("order_id")),
            patient=patient,
            status=result.get("status", "pending"),
            tests_ordered=test_ids,
            ordered_at=datetime.fromisoformat(result.get("created_at").replace("Z", "+00:00")) if result.get("created_at") else datetime.now(),
            lab_provider="Quest Diagnostics",
            collection_method="walk_in_test",
            requisition_url=result.get("requisition_url", result.get("pdf_url"))
        )
    
    def get_order_status(self, order_id: str) -> LabOrder:
        """Get the current status of a lab order."""
        result = self._request("GET", f"/orders/{order_id}")
        
        patient_data = result.get("patient", {})
        patient = LabPatient(
            external_id=patient_data.get("external_id", ""),
            lab_patient_id=patient_data.get("id")
        )
        
        status_map = {
            "pending": "pending",
            "requisition_ready": "pending",
            "in_progress": "collecting",
            "sample_received": "processing",
            "results_pending": "processing",
            "completed": "completed",
            "cancelled": "cancelled",
            "failed": "failed"
        }
        
        return LabOrder(
            order_id=result.get("id"),
            patient=patient,
            status=status_map.get(result.get("status"), result.get("status")),
            tests_ordered=result.get("tests", []),
            ordered_at=datetime.fromisoformat(result.get("created_at").replace("Z", "+00:00")) if result.get("created_at") else datetime.now(),
            completed_at=datetime.fromisoformat(result.get("completed_at").replace("Z", "+00:00")) if result.get("completed_at") else None,
            lab_provider="Quest Diagnostics",
            collection_method="walk_in_test",
            requisition_url=result.get("requisition_url")
        )
    
    def get_requisition_pdf(self, order_id: str) -> Optional[str]:
        """Get requisition PDF URL for walk-in testing."""
        result = self._request("GET", f"/orders/{order_id}/requisition")
        return result.get("url", result.get("pdf_url"))
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel a pending lab order."""
        return self._request("POST", f"/orders/{order_id}/cancel")
    
    def list_orders(
        self,
        patient_id: Optional[str] = None,
        status: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabOrder]:
        """List lab orders with optional filters."""
        params = {}
        if patient_id:
            params["patient_id"] = patient_id
        if status:
            params["status"] = status
        if from_date:
            params["from_date"] = from_date.isoformat()
        if to_date:
            params["to_date"] = to_date.isoformat()
        
        result = self._request("GET", "/orders", params=params)
        
        orders = []
        order_list = result.get("orders", []) if isinstance(result, dict) else result
        
        for order in order_list:
            patient_data = order.get("patient", {})
            patient = LabPatient(
                external_id=patient_data.get("external_id", ""),
                lab_patient_id=patient_data.get("id")
            )
            orders.append(LabOrder(
                order_id=order.get("id"),
                patient=patient,
                status=order.get("status"),
                tests_ordered=order.get("tests", []),
                ordered_at=datetime.fromisoformat(order.get("created_at").replace("Z", "+00:00")) if order.get("created_at") else datetime.now(),
                lab_provider="Quest Diagnostics"
            ))
        
        return orders
    
    # =========================================================
    # RESULTS
    # =========================================================
    
    def fetch_results(
        self,
        user_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        """
        Fetch lab results.
        
        Args:
            user_id: Patient external ID
            order_id: Specific order ID
            from_date: Filter results from this date
            to_date: Filter results until this date
        
        Returns:
            List of LabResults
        """
        if order_id:
            result = self._request("GET", f"/orders/{order_id}/results")
            return [self._parse_result(result, order_id)]
        
        # List results by patient
        params = {}
        if user_id:
            params["patient_external_id"] = user_id
        if from_date:
            params["from_date"] = from_date.isoformat()
        if to_date:
            params["to_date"] = to_date.isoformat()
        
        result = self._request("GET", "/results", params=params)
        
        results = []
        result_list = result.get("results", []) if isinstance(result, dict) else result
        
        for r in result_list:
            results.append(self._parse_result(r, r.get("order_id")))
        
        return results
    
    def _parse_result(self, data: Dict, order_id: str) -> LabResults:
        """Parse Lab Testing API result into LabResults object."""
        markers = []
        
        biomarker_list = data.get("biomarkers", data.get("results", []))
        if isinstance(data, dict) and not biomarker_list:
            biomarker_list = []
        
        for biomarker in biomarker_list:
            markers.append(LabMarkerResult(
                marker_code=biomarker.get("code", biomarker.get("name", "")),
                marker_name=biomarker.get("name", ""),
                value=biomarker.get("value"),
                unit=biomarker.get("unit", ""),
                reference_low=biomarker.get("reference_low", biomarker.get("ref_low")),
                reference_high=biomarker.get("reference_high", biomarker.get("ref_high")),
                flag=biomarker.get("flag"),
                loinc_code=biomarker.get("loinc_code")
            ))
        
        patient_data = data.get("patient", {})
        patient = LabPatient(
            external_id=patient_data.get("external_id", ""),
            lab_patient_id=patient_data.get("id")
        )
        
        report_date_str = data.get("report_date") or data.get("completed_at") or data.get("created_at")
        if report_date_str:
            try:
                report_date = datetime.fromisoformat(report_date_str.replace("Z", "+00:00")).date()
            except ValueError:
                report_date = date.today()
        else:
            report_date = date.today()
        
        return LabResults(
            result_id=data.get("id", order_id),
            order_id=order_id,
            patient=patient,
            lab_provider="Quest Diagnostics (via Lab Testing API)",
            report_date=report_date,
            markers=markers,
            raw_response=data,
            pdf_url=data.get("pdf_url"),
            status=data.get("status", "final")
        )
    
    def get_result_pdf(self, order_id: str) -> Optional[str]:
        """Get PDF URL for lab results."""
        result = self._request("GET", f"/orders/{order_id}/results/pdf")
        return result.get("url", result.get("pdf_url"))


class ManualAdapter(LabAdapter):
    """
    Adapter for manual entry or OCR-parsed results.
    
    Does not connect to any external API - used for user-uploaded results.
    """
    
    provider = LabProvider.MANUAL
    
    def __init__(self, api_key: Optional[str] = None, **config):
        pass
    
    def validate_credentials(self) -> Dict[str, Any]:
        return {"valid": True, "method": "manual"}
    
    def fetch_results(
        self,
        user_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        return []
    
    def get_order_status(self, order_id: str) -> LabOrder:
        raise NotImplementedError("Manual adapter does not support orders")
    
    def create_results_from_markers(
        self,
        patient: LabPatient,
        markers: List[Dict[str, Any]],
        lab_name: Optional[str] = None,
        report_date: Optional[date] = None
    ) -> LabResults:
        """
        Create LabResults from parsed markers (e.g., from OCR).
        
        Args:
            patient: Patient information
            markers: List of {code, value, unit} dicts
            lab_name: Optional lab name from OCR
            report_date: Optional report date from OCR
        
        Returns:
            LabResults object ready for processing
        """
        marker_results = [
            LabMarkerResult(
                marker_code=m["code"],
                marker_name=m["code"],
                value=m["value"],
                unit=m["unit"]
            )
            for m in markers
        ]
        
        return LabResults(
            result_id=f"manual_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            order_id="manual",
            patient=patient,
            lab_provider=lab_name or "Manual Entry",
            report_date=report_date or date.today(),
            markers=marker_results
        )


# Adapter registry
_ADAPTERS = {
    LabProvider.VITAL: VitalAdapter,
    LabProvider.LAB_TESTING_API: LabTestingAPIAdapter,
    LabProvider.MANUAL: ManualAdapter,
}


def get_adapter(provider: str, **config) -> LabAdapter:
    """
    Get a lab adapter by provider name.
    
    Args:
        provider: Provider name ('vital', 'lab_testing_api', 'manual')
        **config: Provider-specific configuration
    
    Returns:
        Configured LabAdapter instance
    """
    try:
        lab_provider = LabProvider(provider.lower())
    except ValueError:
        raise ValueError(f"Unknown lab provider: {provider}. "
                        f"Available: {[p.value for p in LabProvider if p in _ADAPTERS]}")
    
    adapter_class = _ADAPTERS.get(lab_provider)
    if not adapter_class:
        raise ValueError(f"No adapter implemented for: {provider}")
    
    return adapter_class(**config)


def list_providers() -> List[Dict[str, Any]]:
    """List available lab providers with status."""
    providers = []
    
    for provider, adapter_class in _ADAPTERS.items():
        adapter = adapter_class()
        status = adapter.validate_credentials()
        
        providers.append({
            "provider": provider.value,
            "name": provider.name,
            "configured": status.get("valid", False),
            "environment": status.get("environment"),
            "details": status
        })
    
    return providers


def get_vital_status() -> Dict[str, Any]:
    """Get Junction (Vital) API configuration status."""
    api_key = os.environ.get("VITAL_API_KEY")
    environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
    
    if not api_key:
        return {
            "configured": False,
            "error": "VITAL_API_KEY not set",
            "environment": environment
        }
    
    adapter = VitalAdapter(api_key=api_key, environment=environment)
    return adapter.validate_credentials()


def get_lab_testing_api_status() -> Dict[str, Any]:
    """Get Lab Testing API configuration status."""
    api_key = os.environ.get("LAB_TESTING_API_KEY")
    environment = os.environ.get("LAB_TESTING_API_ENVIRONMENT", "production")
    
    if not api_key:
        return {
            "configured": False,
            "error": "LAB_TESTING_API_KEY not set",
            "environment": environment,
            "provider": "Lab Testing API"
        }
    
    adapter = LabTestingAPIAdapter(api_key=api_key, environment=environment)
    return adapter.validate_credentials()


def get_best_available_adapter() -> Optional[LabAdapter]:
    """
    Get the best available lab adapter based on configuration.
    
    Priority:
    1. Junction (Vital) - if configured and Lab Testing enabled
    2. Lab Testing API - if configured
    3. None - if no adapters configured
    
    Returns:
        Configured LabAdapter or None
    """
    # Try Junction first
    vital_status = get_vital_status()
    if vital_status.get("valid"):
        return VitalAdapter()
    
    # Fall back to Lab Testing API
    lta_status = get_lab_testing_api_status()
    if lta_status.get("valid"):
        return LabTestingAPIAdapter()
    
    return None
