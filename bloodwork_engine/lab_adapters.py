"""
GenoMAX² Lab API Adapter Interface
===================================
Unified interface for lab API integrations (Vital/Junction, Quest, LabCorp, etc.)

Primary Integration: Vital (https://tryvital.io)
- Aggregates Quest, LabCorp, BioReference through single API
- Transparent pricing, no test upcharges
- Operates in restricted states (NY, NJ, RI)

Usage:
    from bloodwork_engine.lab_adapters import VitalAdapter, get_adapter
    
    # Get adapter by name
    adapter = get_adapter("vital")
    
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
    VITAL = "vital"           # Vital (formerly Junction) - aggregator
    QUEST = "quest"           # Quest Diagnostics (via Vital)
    LABCORP = "labcorp"       # LabCorp (via Vital)
    BIOREFERENCE = "bioreference"  # BioReference (via Vital)
    HEALTH_GORILLA = "health_gorilla"  # Health Gorilla aggregator
    LAB_TESTING_API = "lab_testing_api"  # Lab Testing API (Quest access)
    MANUAL = "manual"         # Manual entry / OCR upload


@dataclass
class LabPatient:
    """Patient information for lab orders."""
    external_id: str          # GenoMAX user ID
    lab_patient_id: Optional[str] = None  # Lab's patient ID (Vital user_id)
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
    Adapter for Vital (formerly Junction) lab API.
    
    Vital aggregates multiple labs including Quest, LabCorp, and BioReference.
    Documentation: https://docs.tryvital.io/
    
    Features:
    - Single API for multiple lab networks
    - Walk-in tests at 2,300+ Quest/LabCorp locations
    - At-home phlebotomy service
    - At-home test kits
    - Operates in restricted states (NY, NJ, RI)
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
        
        # Base URL depends on environment
        if environment == "production":
            self.base_url = "https://api.tryvital.io/v2"
        else:
            self.base_url = "https://api.sandbox.tryvital.io/v2"
        
        self.headers = {
            "x-vital-api-key": self.api_key or "",
            "Content-Type": "application/json"
        }
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make HTTP request to Vital API."""
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
            logger.error(f"Vital API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Vital API request failed: {e}")
            raise
    
    def validate_credentials(self) -> Dict[str, Any]:
        """Validate Vital API credentials."""
        if not self.api_key:
            return {
                "valid": False,
                "error": "VITAL_API_KEY not configured",
                "environment": self.environment
            }
        
        try:
            # Test with team info endpoint
            result = self._request("GET", "/team")
            return {
                "valid": True,
                "team_id": result.get("team_id"),
                "environment": self.environment,
                "region": self.region
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
        Create a user in Vital.
        
        Args:
            patient: Patient information
        
        Returns:
            Dict with user_id and client_user_id
        """
        data = {
            "client_user_id": patient.external_id
        }
        
        result = self._request("POST", "/user", data=data)
        
        # Update patient with Vital user_id
        patient.lab_patient_id = result.get("user_id")
        
        return result
    
    def get_user(self, user_id: str) -> Dict[str, Any]:
        """Get user details from Vital."""
        return self._request("GET", f"/user/{user_id}")
    
    def delete_user(self, user_id: str) -> Dict[str, Any]:
        """Delete a user from Vital."""
        return self._request("DELETE", f"/user/{user_id}")
    
    # =========================================================
    # LAB TESTS CATALOG
    # =========================================================
    
    def list_lab_tests(self) -> List[LabTest]:
        """
        List available lab tests from Vital.
        
        Returns:
            List of LabTest objects
        """
        result = self._request("GET", "/lab_tests")
        
        tests = []
        for test in result.get("lab_tests", []):
            tests.append(LabTest(
                test_id=test.get("id"),
                name=test.get("name"),
                description=test.get("description"),
                markers=[m.get("name") for m in test.get("markers", [])],
                sample_type=test.get("sample_type", "blood"),
                fasting_required=test.get("fasting", False),
                turnaround_days=test.get("turnaround_time_days"),
                lab_provider=test.get("lab", {}).get("name")
            ))
        
        return tests
    
    def get_lab_test(self, test_id: str) -> LabTest:
        """Get details for a specific lab test."""
        result = self._request("GET", f"/lab_tests/{test_id}")
        
        return LabTest(
            test_id=result.get("id"),
            name=result.get("name"),
            description=result.get("description"),
            markers=[m.get("name") for m in result.get("markers", [])],
            sample_type=result.get("sample_type", "blood"),
            fasting_required=result.get("fasting", False),
            turnaround_days=result.get("turnaround_time_days"),
            lab_provider=result.get("lab", {}).get("name")
        )
    
    def list_markers(self) -> List[Dict[str, Any]]:
        """List all available biomarkers from Vital."""
        result = self._request("GET", "/lab_tests/markers")
        return result.get("markers", [])
    
    # =========================================================
    # LAB LOCATIONS
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
        
        result = self._request("GET", "/lab_tests/labs", params=params)
        return result.get("labs", [])
    
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
            user_id: Vital user ID
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
            lab_provider=result.get("lab", {}).get("name"),
            collection_method=collection_method,
            requisition_url=result.get("requisition_form", {}).get("url")
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
            tests_ordered=[result.get("lab_test", {}).get("id")],
            ordered_at=datetime.fromisoformat(result.get("created_at").replace("Z", "+00:00")) if result.get("created_at") else datetime.now(),
            completed_at=datetime.fromisoformat(result.get("completed_at").replace("Z", "+00:00")) if result.get("completed_at") else None,
            lab_provider=result.get("lab", {}).get("name"),
            collection_method=result.get("details", {}).get("type", "walk_in_test"),
            requisition_url=result.get("requisition_form", {}).get("url")
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
        for order in result.get("orders", []):
            patient = LabPatient(
                external_id=order.get("user_id"),
                lab_patient_id=order.get("user_id")
            )
            orders.append(LabOrder(
                order_id=order.get("id"),
                patient=patient,
                status=order.get("status"),
                tests_ordered=[order.get("lab_test", {}).get("id")],
                ordered_at=datetime.fromisoformat(order.get("created_at").replace("Z", "+00:00")) if order.get("created_at") else datetime.now(),
                lab_provider=order.get("lab", {}).get("name")
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
        Fetch lab results from Vital.
        
        Args:
            user_id: Filter by Vital user ID
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
        for r in result.get("results", []):
            results.append(self._parse_result(r, r.get("order_id")))
        
        return results
    
    def _parse_result(self, data: Dict, order_id: str) -> LabResults:
        """Parse Vital API result into LabResults object."""
        markers = []
        
        for biomarker in data.get("results", []):
            markers.append(LabMarkerResult(
                marker_code=biomarker.get("slug", biomarker.get("name", "")),
                marker_name=biomarker.get("name", ""),
                value=biomarker.get("value"),
                unit=biomarker.get("unit", ""),
                reference_low=biomarker.get("min_range_value"),
                reference_high=biomarker.get("max_range_value"),
                flag=biomarker.get("flag"),
                loinc_code=biomarker.get("loinc", {}).get("code")
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
            lab_provider=data.get("provider", {}).get("name", "Vital"),
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
    LabProvider.MANUAL: ManualAdapter,
}


def get_adapter(provider: str, **config) -> LabAdapter:
    """
    Get a lab adapter by provider name.
    
    Args:
        provider: Provider name ('vital', 'manual')
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
    """Get Vital API configuration status."""
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
