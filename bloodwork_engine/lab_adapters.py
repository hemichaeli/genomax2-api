"""
GenoMAX² Lab API Adapter Interface
===================================
Abstract interface for lab API integrations (Vital/Junction, Quest, LabCorp, etc.)

This module provides a unified interface for fetching bloodwork results from
various lab partners. Each lab implementation extends the base LabAdapter class.

Usage:
    from bloodwork_engine.lab_adapters import VitalAdapter, get_adapter
    
    # Get adapter by name
    adapter = get_adapter("vital")
    
    # Fetch results
    results = adapter.fetch_results(patient_id="12345")
    
    # Convert to engine input format
    markers = adapter.to_engine_input(results)
"""

import os
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, date
from enum import Enum

logger = logging.getLogger(__name__)


class LabProvider(Enum):
    """Supported lab providers."""
    VITAL = "vital"           # Vital (formerly Junction) - aggregator
    QUEST = "quest"           # Quest Diagnostics
    LABCORP = "labcorp"       # LabCorp
    BIOREFERENCE = "bioreference"  # BioReference Laboratories
    HEALTH_GORILLA = "health_gorilla"  # Health Gorilla aggregator
    LAB_TESTING_API = "lab_testing_api"  # Lab Testing API (Quest access)
    MANUAL = "manual"         # Manual entry / OCR upload


@dataclass
class LabPatient:
    """Patient information for lab orders."""
    external_id: str          # GenoMAX user ID
    lab_patient_id: Optional[str] = None  # Lab's patient ID
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    sex: Optional[str] = None  # 'male' or 'female'
    email: Optional[str] = None
    phone: Optional[str] = None


@dataclass
class LabOrder:
    """A lab test order."""
    order_id: str
    patient: LabPatient
    status: str              # 'pending', 'collected', 'processing', 'completed', 'cancelled'
    tests_ordered: List[str]  # Test codes
    ordered_at: datetime
    collected_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    lab_provider: Optional[str] = None
    lab_location: Optional[str] = None


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
    status: str = "final"    # 'preliminary', 'final', 'amended'


class LabAdapter(ABC):
    """
    Abstract base class for lab API integrations.
    
    Each lab provider implements this interface to provide a unified
    way to order tests and fetch results.
    """
    
    provider: LabProvider
    
    @abstractmethod
    def __init__(self, api_key: Optional[str] = None, **config):
        """
        Initialize the adapter with credentials.
        
        Args:
            api_key: API key for the lab service
            **config: Additional provider-specific configuration
        """
        pass
    
    @abstractmethod
    def validate_credentials(self) -> bool:
        """
        Validate that credentials are configured correctly.
        
        Returns:
            True if credentials are valid
        """
        pass
    
    @abstractmethod
    def fetch_results(
        self,
        patient_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        """
        Fetch lab results.
        
        Args:
            patient_id: Filter by patient ID
            order_id: Filter by specific order ID
            from_date: Filter results from this date
            to_date: Filter results until this date
        
        Returns:
            List of LabResults
        """
        pass
    
    @abstractmethod
    def get_order_status(self, order_id: str) -> LabOrder:
        """
        Get the status of a lab order.
        
        Args:
            order_id: The order ID to check
        
        Returns:
            LabOrder with current status
        """
        pass
    
    def to_engine_input(self, results: LabResults) -> List[Dict[str, Any]]:
        """
        Convert lab results to BloodworkEngineV2 input format.
        
        Args:
            results: LabResults object
        
        Returns:
            List of {code, value, unit} dicts for engine processing
        """
        # Map lab codes to GenoMAX canonical codes
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
        """
        Map lab-specific marker code to GenoMAX canonical code.
        
        Override in subclass for provider-specific mappings.
        
        Args:
            lab_code: Lab's internal code
            lab_name: Lab's display name for the marker
        
        Returns:
            GenoMAX canonical code or None if unknown
        """
        # Default mapping (can be overridden by subclasses)
        # This uses common LOINC-based mappings
        
        code_lower = lab_code.lower()
        name_lower = lab_name.lower()
        
        # Common mappings
        mappings = {
            # Iron
            "ferritin": "ferritin",
            "iron": "iron",
            "hemoglobin": "hemoglobin",
            "hgb": "hemoglobin",
            
            # Vitamins
            "vitamin d": "vitamin_d_25oh",
            "25-hydroxy": "vitamin_d_25oh",
            "vitamin b12": "vitamin_b12",
            "b12": "vitamin_b12",
            "folate": "folate_serum",
            "folic acid": "folate_serum",
            
            # Liver
            "alt": "alt",
            "alanine": "alt",
            "sgpt": "alt",
            "ast": "ast",
            "aspartate": "ast",
            "sgot": "ast",
            "ggt": "ggt",
            "gamma": "ggt",
            
            # Kidney
            "creatinine": "creatinine",
            "egfr": "egfr",
            "gfr": "egfr",
            
            # Glucose
            "glucose": "fasting_glucose",
            "hba1c": "hba1c",
            "a1c": "hba1c",
            "insulin": "fasting_insulin",
            "homocysteine": "homocysteine",
            
            # Inflammation
            "crp": "hs_crp",
            "c-reactive": "hs_crp",
            
            # Minerals
            "calcium": "calcium_serum",
            "magnesium": "magnesium_serum",
            "potassium": "potassium",
            "zinc": "zinc_serum",
            "copper": "copper_serum",
            
            # Thyroid
            "tsh": "tsh",
            "free t3": "free_t3",
            "ft3": "free_t3",
            "free t4": "free_t4",
            "ft4": "free_t4",
            
            # Lipids
            "triglycerides": "triglycerides",
            "ldl": "ldl_cholesterol",
            "hdl": "hdl_cholesterol",
            "apolipoprotein b": "apolipoprotein_b",
            "apob": "apolipoprotein_b",
            "lp(a)": "lp_a",
            "lipoprotein(a)": "lp_a",
            
            # Hormones
            "testosterone": "total_testosterone",
            "free testosterone": "free_testosterone",
            "estradiol": "estradiol",
            "progesterone": "progesterone",
            "cortisol": "cortisol_am",
            "dhea": "dhea_s",
            "shbg": "shbg",
            
            # Other
            "uric acid": "uric_acid",
            "platelets": "platelet_count",
            "platelet count": "platelet_count",
            "omega-3": "omega3_index",
        }
        
        # Check both code and name
        for key, canonical in mappings.items():
            if key in code_lower or key in name_lower:
                return canonical
        
        return None


class VitalAdapter(LabAdapter):
    """
    Adapter for Vital (formerly Junction) lab API.
    
    Vital aggregates multiple labs including Quest, LabCorp, and BioReference.
    Documentation: https://docs.tryvital.io/
    """
    
    provider = LabProvider.VITAL
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        environment: str = "sandbox",  # 'sandbox' or 'production'
        region: str = "us"  # 'us' or 'eu'
    ):
        self.api_key = api_key or os.environ.get("VITAL_API_KEY")
        self.environment = environment
        self.region = region
        
        # Base URL depends on environment
        if environment == "production":
            self.base_url = f"https://api.tryvital.io/v2"
        else:
            self.base_url = f"https://api.sandbox.tryvital.io/v2"
        
        self._client = None
    
    def validate_credentials(self) -> bool:
        """Validate Vital API credentials."""
        if not self.api_key:
            logger.error("VITAL_API_KEY not configured")
            return False
        
        # TODO: Make test API call
        return True
    
    def fetch_results(
        self,
        patient_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        """
        Fetch lab results from Vital.
        
        Uses Vital's Results API endpoint.
        """
        # TODO: Implement actual API call
        # Placeholder implementation
        logger.info(f"Fetching Vital results for patient={patient_id}, order={order_id}")
        return []
    
    def get_order_status(self, order_id: str) -> LabOrder:
        """Get order status from Vital."""
        # TODO: Implement actual API call
        raise NotImplementedError("Vital order status not yet implemented")
    
    def create_order(
        self,
        patient: LabPatient,
        tests: List[str],
        collection_method: str = "walk_in_test"  # or 'at_home_phlebotomy'
    ) -> LabOrder:
        """
        Create a new lab order in Vital.
        
        Args:
            patient: Patient information
            tests: List of test codes to order
            collection_method: How blood will be collected
        
        Returns:
            LabOrder with order details
        """
        # TODO: Implement actual API call
        raise NotImplementedError("Vital order creation not yet implemented")


class QuestAdapter(LabAdapter):
    """
    Adapter for Quest Diagnostics API.
    
    Requires enterprise partnership agreement with Quest.
    """
    
    provider = LabProvider.QUEST
    
    def __init__(self, api_key: Optional[str] = None, **config):
        self.api_key = api_key or os.environ.get("QUEST_API_KEY")
        self.config = config
    
    def validate_credentials(self) -> bool:
        if not self.api_key:
            logger.error("QUEST_API_KEY not configured")
            return False
        return True
    
    def fetch_results(
        self,
        patient_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        # TODO: Implement Quest API
        logger.info("Quest integration requires enterprise agreement")
        return []
    
    def get_order_status(self, order_id: str) -> LabOrder:
        raise NotImplementedError("Quest order status not yet implemented")


class LabCorpAdapter(LabAdapter):
    """
    Adapter for LabCorp API.
    
    Requires enterprise partnership agreement with LabCorp.
    """
    
    provider = LabProvider.LABCORP
    
    def __init__(self, api_key: Optional[str] = None, **config):
        self.api_key = api_key or os.environ.get("LABCORP_API_KEY")
        self.config = config
    
    def validate_credentials(self) -> bool:
        if not self.api_key:
            logger.error("LABCORP_API_KEY not configured")
            return False
        return True
    
    def fetch_results(
        self,
        patient_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        # TODO: Implement LabCorp API
        logger.info("LabCorp integration requires enterprise agreement")
        return []
    
    def get_order_status(self, order_id: str) -> LabOrder:
        raise NotImplementedError("LabCorp order status not yet implemented")


class ManualAdapter(LabAdapter):
    """
    Adapter for manual entry or OCR-parsed results.
    
    Does not connect to any external API - used for user-uploaded results.
    """
    
    provider = LabProvider.MANUAL
    
    def __init__(self, api_key: Optional[str] = None, **config):
        pass  # No credentials needed
    
    def validate_credentials(self) -> bool:
        return True  # Always valid
    
    def fetch_results(
        self,
        patient_id: Optional[str] = None,
        order_id: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[LabResults]:
        # Manual adapter doesn't fetch - results are pushed to it
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
                marker_name=m["code"],  # Use code as name for manual
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
    LabProvider.QUEST: QuestAdapter,
    LabProvider.LABCORP: LabCorpAdapter,
    LabProvider.MANUAL: ManualAdapter,
}


def get_adapter(provider: str, **config) -> LabAdapter:
    """
    Get a lab adapter by provider name.
    
    Args:
        provider: Provider name ('vital', 'quest', 'labcorp', 'manual')
        **config: Provider-specific configuration
    
    Returns:
        Configured LabAdapter instance
    """
    try:
        lab_provider = LabProvider(provider.lower())
    except ValueError:
        raise ValueError(f"Unknown lab provider: {provider}. "
                        f"Available: {[p.value for p in LabProvider]}")
    
    adapter_class = _ADAPTERS.get(lab_provider)
    if not adapter_class:
        raise ValueError(f"No adapter implemented for: {provider}")
    
    return adapter_class(**config)


def list_providers() -> List[str]:
    """List available lab providers."""
    return [p.value for p in LabProvider]
