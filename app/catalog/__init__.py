"""
GenoMAX² Catalog Governance Module (Issue #5)

Purpose: Ensure product catalog is SAFE TO USE before routing/matching.

This module does NOT:
- Decide what to recommend
- Apply routing constraints
- Match intents

This module ONLY:
- Validates SKU metadata completeness
- Classifies SKUs as VALID or AUTO_BLOCKED
- Produces coverage reports

Principle: A SKU without required metadata is UNSAFE and must be BLOCKED.

Version: catalog_governance_v1
"""

from .models import (
    CatalogSkuMetaV1,
    SkuValidationStatus,
    SkuValidationResult,
    CatalogValidationRunV1,
    CatalogCoverageReportV1,
)
from .validate import validate_catalog_snapshot
from .mapper import CatalogMapper

__version__ = "catalog_governance_v1"

__all__ = [
    "CatalogSkuMetaV1",
    "SkuValidationStatus",
    "SkuValidationResult",
    "CatalogValidationRunV1",
    "CatalogCoverageReportV1",
    "validate_catalog_snapshot",
    "CatalogMapper",
    "__version__",
]
