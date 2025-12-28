"""
Catalog Governance Models (Issue #5)

Pydantic models for SKU metadata validation and coverage reporting.

Version: catalog_governance_v1
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
import hashlib
import json


class SkuValidationStatus(str, Enum):
    """SKU validation status."""
    VALID = "VALID"
    AUTO_BLOCKED = "AUTO_BLOCKED"


class GenderLine(str, Enum):
    """Product line gender targeting."""
    MAXIMO2 = "MAXimo2"
    MAXIMA2 = "MAXima2"
    UNISEX = "UNISEX"


class CatalogSkuMetaV1(BaseModel):
    """
    Required metadata schema for each SKU.
    
    A SKU must have ALL required fields populated to be considered VALID.
    Missing or empty required fields result in AUTO_BLOCKED status.
    """
    
    # Required identifiers
    sku_id: str = Field(..., description="Unique SKU identifier (product name as slug)")
    product_name: str = Field(..., description="Human-readable product name")
    product_url: Optional[str] = Field(None, description="Supliful product URL")
    
    # Required for routing (Issue #6)
    ingredient_tags: List[str] = Field(
        default_factory=list,
        description="Canonical ingredient identifiers used by routing (e.g., ['iron', 'vitamin_b12'])"
    )
    category_tags: List[str] = Field(
        default_factory=list,
        description="Product category tags (e.g., ['vitamin', 'mineral', 'adaptogen'])"
    )
    risk_tags: List[str] = Field(
        default_factory=list,
        description="Risk classification tags (e.g., ['hepatotoxic', 'renal_load', 'stimulant'])"
    )
    
    # Gender targeting for Issue #7
    gender_line: GenderLine = Field(
        default=GenderLine.UNISEX,
        description="Target product line: MAXimo2 (male), MAXima2 (female), or UNISEX"
    )
    
    # Metadata versioning
    metadata_version: str = Field(
        default="catalog_meta_v1",
        description="Schema version for metadata"
    )
    
    # Audit
    updated_at: Optional[datetime] = Field(
        default=None,
        description="Last metadata update timestamp"
    )
    
    # Evidence tier from ingredient database
    evidence_tier: Optional[str] = Field(
        None,
        description="Evidence tier: TIER_1_OS_CORE, TIER_2_CONTEXTUAL, TIER_3_EXPLORATORY, BLOCKED"
    )
    
    # Additional metadata
    sell_recommendation: Optional[str] = Field(
        None,
        description="Sell recommendation status"
    )
    
    contraindications: List[str] = Field(
        default_factory=list,
        description="Known contraindications"
    )
    
    drug_interactions: List[str] = Field(
        default_factory=list,
        description="Known drug interactions"
    )
    
    @field_validator('ingredient_tags', 'category_tags', mode='before')
    @classmethod
    def normalize_tags(cls, v):
        """Normalize tags to lowercase list."""
        if v is None:
            return []
        if isinstance(v, str):
            # Handle comma-separated strings
            return [t.strip().lower() for t in v.split(',') if t.strip()]
        return [str(t).lower().strip() for t in v if t]
    
    def to_validation_dict(self) -> Dict[str, Any]:
        """Return dict suitable for deterministic hashing."""
        return {
            "sku_id": self.sku_id,
            "ingredient_tags": sorted(self.ingredient_tags),
            "category_tags": sorted(self.category_tags),
            "risk_tags": sorted(self.risk_tags),
            "gender_line": self.gender_line.value,
        }


class SkuValidationResult(BaseModel):
    """
    Validation result for a single SKU.
    """
    
    sku_id: str = Field(..., description="SKU identifier")
    product_name: str = Field(..., description="Product name")
    status: SkuValidationStatus = Field(..., description="Validation status")
    reason_codes: List[str] = Field(
        default_factory=list,
        description="Reason codes for blocking (e.g., ['INSUFFICIENT_METADATA'])"
    )
    missing_fields: List[str] = Field(
        default_factory=list,
        description="List of missing required fields"
    )
    unknown_ingredients: List[str] = Field(
        default_factory=list,
        description="Ingredients not found in canonical dictionary"
    )
    metadata: Optional[CatalogSkuMetaV1] = Field(
        None,
        description="Full metadata if VALID"
    )
    
    def to_hash_dict(self) -> Dict[str, Any]:
        """Return dict suitable for deterministic hashing."""
        return {
            "sku_id": self.sku_id,
            "status": self.status.value,
            "reason_codes": sorted(self.reason_codes),
            "missing_fields": sorted(self.missing_fields),
        }


class CatalogCoverageReportV1(BaseModel):
    """
    Coverage report for catalog validation run.
    """
    
    # Counts
    total_skus: int = Field(..., description="Total SKUs in catalog")
    valid_skus: int = Field(..., description="SKUs with complete metadata")
    auto_blocked_skus: int = Field(..., description="SKUs blocked due to missing metadata")
    
    # Percentage
    percent_valid: float = Field(..., description="Percentage of valid SKUs (1 decimal)")
    
    # Breakdown
    top_missing_fields: Dict[str, int] = Field(
        default_factory=dict,
        description="Field name to count of SKUs missing that field"
    )
    
    blocked_by_evidence_tier: Dict[str, int] = Field(
        default_factory=dict,
        description="Evidence tier to count of blocked SKUs"
    )
    
    unknown_ingredients_count: int = Field(
        default=0,
        description="Total count of unknown ingredient strings"
    )
    
    # Metadata
    generated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Report generation timestamp"
    )
    
    catalog_version: str = Field(
        default="supliful_v1",
        description="Catalog source version"
    )
    
    ruleset_version: str = Field(
        default="catalog_governance_v1",
        description="Governance ruleset version"
    )
    
    @classmethod
    def from_results(
        cls,
        results: List[SkuValidationResult],
        catalog_version: str = "supliful_v1"
    ) -> "CatalogCoverageReportV1":
        """Build coverage report from validation results."""
        total = len(results)
        valid = sum(1 for r in results if r.status == SkuValidationStatus.VALID)
        blocked = total - valid
        
        # Count missing fields
        field_counts: Dict[str, int] = {}
        unknown_count = 0
        
        for r in results:
            for field in r.missing_fields:
                field_counts[field] = field_counts.get(field, 0) + 1
            unknown_count += len(r.unknown_ingredients)
        
        # Sort by count descending
        top_missing = dict(sorted(
            field_counts.items(),
            key=lambda x: (-x[1], x[0])
        ))
        
        percent = round((valid / total * 100) if total > 0 else 0.0, 1)
        
        return cls(
            total_skus=total,
            valid_skus=valid,
            auto_blocked_skus=blocked,
            percent_valid=percent,
            top_missing_fields=top_missing,
            unknown_ingredients_count=unknown_count,
            catalog_version=catalog_version,
        )


class CatalogValidationRunV1(BaseModel):
    """
    Complete validation run with results and audit trail.
    """
    
    # Identifiers
    run_id: str = Field(..., description="Unique run identifier (UUID)")
    
    # Versioning
    catalog_version: str = Field(
        default="supliful_v1",
        description="Catalog source version"
    )
    ruleset_version: str = Field(
        default="catalog_governance_v1",
        description="Governance ruleset version"
    )
    
    # Results
    results: List[SkuValidationResult] = Field(
        default_factory=list,
        description="Per-SKU validation results"
    )
    
    # Coverage
    coverage_report: CatalogCoverageReportV1 = Field(
        ...,
        description="Coverage statistics"
    )
    
    # Determinism
    results_hash: str = Field(
        ...,
        description="SHA256 hash of sorted results for determinism verification"
    )
    
    # Audit
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Run creation timestamp"
    )
    
    @staticmethod
    def compute_results_hash(results: List[SkuValidationResult]) -> str:
        """
        Compute deterministic SHA256 hash of validation results.
        
        Ensures: same input -> same hash
        """
        # Sort by sku_id for determinism
        sorted_results = sorted(results, key=lambda r: r.sku_id)
        
        # Extract hashable data
        hash_data = [r.to_hash_dict() for r in sorted_results]
        
        # Serialize deterministically
        json_str = json.dumps(hash_data, sort_keys=True, default=str)
        
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


# Reason codes
class ReasonCode:
    """Standard reason codes for catalog governance."""
    
    INSUFFICIENT_METADATA = "INSUFFICIENT_METADATA"
    MISSING_INGREDIENT_TAGS = "MISSING_INGREDIENT_TAGS"
    MISSING_CATEGORY_TAGS = "MISSING_CATEGORY_TAGS"
    EMPTY_INGREDIENT_TAGS = "EMPTY_INGREDIENT_TAGS"
    EMPTY_CATEGORY_TAGS = "EMPTY_CATEGORY_TAGS"
    BLOCKED_BY_EVIDENCE = "BLOCKED_BY_EVIDENCE"
    HEPATOTOXICITY_RISK = "HEPATOTOXICITY_RISK"
