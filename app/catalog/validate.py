"""
Catalog Validation (Issue #5)

Core validation logic for SKU metadata.

Principle: A SKU without required metadata is UNSAFE and must be BLOCKED.

Version: catalog_governance_v1
"""

import uuid
from datetime import datetime
from typing import List, Tuple, Optional

from .models import (
    CatalogSkuMetaV1,
    SkuValidationStatus,
    SkuValidationResult,
    CatalogValidationRunV1,
    CatalogCoverageReportV1,
    ReasonCode,
)
from .mapper import CatalogMapper


def validate_sku(
    meta: CatalogSkuMetaV1,
    unknown_ingredients: List[str]
) -> SkuValidationResult:
    """
    Validate a single SKU's metadata.
    
    Rules:
    - Missing ingredient_tags -> AUTO_BLOCKED (INSUFFICIENT_METADATA)
    - Empty ingredient_tags -> AUTO_BLOCKED (EMPTY_INGREDIENT_TAGS)
    - Missing category_tags -> AUTO_BLOCKED (INSUFFICIENT_METADATA)
    - Empty category_tags -> AUTO_BLOCKED (EMPTY_CATEGORY_TAGS)
    - Blocked evidence tier -> AUTO_BLOCKED (BLOCKED_BY_EVIDENCE)
    - blocked_ingredient in risk_tags -> AUTO_BLOCKED (HEPATOTOXICITY_RISK)
    
    Args:
        meta: SKU metadata
        unknown_ingredients: Ingredients not found in dictionary
        
    Returns:
        SkuValidationResult with status and reason codes
    """
    reason_codes: List[str] = []
    missing_fields: List[str] = []
    
    # Check ingredient_tags
    if not hasattr(meta, 'ingredient_tags') or meta.ingredient_tags is None:
        missing_fields.append('ingredient_tags')
        reason_codes.append(ReasonCode.MISSING_INGREDIENT_TAGS)
    elif len(meta.ingredient_tags) == 0:
        missing_fields.append('ingredient_tags')
        reason_codes.append(ReasonCode.EMPTY_INGREDIENT_TAGS)
    
    # Check category_tags
    if not hasattr(meta, 'category_tags') or meta.category_tags is None:
        missing_fields.append('category_tags')
        reason_codes.append(ReasonCode.MISSING_CATEGORY_TAGS)
    elif len(meta.category_tags) == 0:
        missing_fields.append('category_tags')
        reason_codes.append(ReasonCode.EMPTY_CATEGORY_TAGS)
    
    # Check for blocked evidence tier (e.g., Ashwagandha)
    if meta.evidence_tier and 'BLOCKED' in meta.evidence_tier.upper():
        reason_codes.append(ReasonCode.BLOCKED_BY_EVIDENCE)
    
    # Check for blocked ingredient in risk tags
    if 'blocked_ingredient' in meta.risk_tags:
        reason_codes.append(ReasonCode.HEPATOTOXICITY_RISK)
    
    # Determine status
    if missing_fields or ReasonCode.BLOCKED_BY_EVIDENCE in reason_codes:
        status = SkuValidationStatus.AUTO_BLOCKED
        if missing_fields:
            reason_codes.insert(0, ReasonCode.INSUFFICIENT_METADATA)
    else:
        status = SkuValidationStatus.VALID
    
    return SkuValidationResult(
        sku_id=meta.sku_id,
        product_name=meta.product_name,
        status=status,
        reason_codes=list(set(reason_codes)),  # Dedupe
        missing_fields=missing_fields,
        unknown_ingredients=unknown_ingredients,
        metadata=meta if status == SkuValidationStatus.VALID else None,
    )


def validate_catalog_snapshot(
    mapper: Optional[CatalogMapper] = None,
    catalog_version: str = "supliful_v1"
) -> Tuple[List[SkuValidationResult], CatalogCoverageReportV1]:
    """
    Validate entire catalog snapshot.
    
    This is the main entry point for catalog validation.
    
    Args:
        mapper: CatalogMapper instance (creates default if None)
        catalog_version: Version string for the catalog source
        
    Returns:
        Tuple of (validation_results, coverage_report)
    """
    if mapper is None:
        mapper = CatalogMapper()
    
    # Load catalog
    catalog_data = mapper.load_catalog_auto()
    
    # Validate each SKU
    results: List[SkuValidationResult] = []
    
    for meta, unknown in catalog_data:
        result = validate_sku(meta, unknown)
        results.append(result)
    
    # Sort by sku_id for determinism
    results.sort(key=lambda r: r.sku_id)
    
    # Build coverage report
    coverage = CatalogCoverageReportV1.from_results(results, catalog_version)
    
    return results, coverage


def create_validation_run(
    results: List[SkuValidationResult],
    coverage: CatalogCoverageReportV1,
    catalog_version: str = "supliful_v1"
) -> CatalogValidationRunV1:
    """
    Create a complete validation run record.
    
    Args:
        results: Validation results
        coverage: Coverage report
        catalog_version: Catalog source version
        
    Returns:
        CatalogValidationRunV1 with hash for determinism verification
    """
    run_id = str(uuid.uuid4())
    results_hash = CatalogValidationRunV1.compute_results_hash(results)
    
    return CatalogValidationRunV1(
        run_id=run_id,
        catalog_version=catalog_version,
        results=results,
        coverage_report=coverage,
        results_hash=results_hash,
        created_at=datetime.utcnow(),
    )


def get_valid_skus(results: List[SkuValidationResult]) -> List[SkuValidationResult]:
    """
    Filter to only VALID SKUs.
    
    These are the SKUs that can proceed to routing (Issue #6).
    """
    return [r for r in results if r.status == SkuValidationStatus.VALID]


def get_blocked_skus(results: List[SkuValidationResult]) -> List[SkuValidationResult]:
    """
    Filter to only AUTO_BLOCKED SKUs.
    
    These SKUs cannot participate in routing or matching.
    """
    return [r for r in results if r.status == SkuValidationStatus.AUTO_BLOCKED]


def get_skus_missing_field(
    results: List[SkuValidationResult],
    field: str
) -> List[SkuValidationResult]:
    """
    Get SKUs missing a specific field.
    
    Args:
        results: Validation results
        field: Field name (e.g., 'ingredient_tags')
        
    Returns:
        SKUs where the field is missing
    """
    return [r for r in results if field in r.missing_fields]


def get_unknown_ingredients_summary(
    results: List[SkuValidationResult]
) -> dict:
    """
    Get summary of unknown ingredients across all SKUs.
    
    Returns:
        Dict mapping ingredient string to list of SKU IDs that use it
    """
    summary = {}
    
    for result in results:
        for ingredient in result.unknown_ingredients:
            if ingredient not in summary:
                summary[ingredient] = []
            summary[ingredient].append(result.sku_id)
    
    # Sort by frequency descending
    return dict(sorted(
        summary.items(),
        key=lambda x: (-len(x[1]), x[0])
    ))
