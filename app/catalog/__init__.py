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
- Provides catalog wiring for Brain integration (Issue #15)

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

# Catalog Wiring (Issue #15)
from .wiring import (
    get_catalog,
    ensure_catalog_available,
    filter_to_catalog,
    get_catalog_health,
    CatalogWiring,
    CatalogWiringError,
    CatalogProduct,
    CatalogStatus,
    CATALOG_WIRING_VERSION,
)

__version__ = "catalog_governance_v1"

__all__ = [
    # Models
    "CatalogSkuMetaV1",
    "SkuValidationStatus",
    "SkuValidationResult",
    "CatalogValidationRunV1",
    "CatalogCoverageReportV1",
    # Validation
    "validate_catalog_snapshot",
    "CatalogMapper",
    # Wiring (Issue #15)
    "get_catalog",
    "ensure_catalog_available",
    "filter_to_catalog",
    "get_catalog_health",
    "CatalogWiring",
    "CatalogWiringError",
    "CatalogProduct",
    "CatalogStatus",
    "CATALOG_WIRING_VERSION",
    # Version
    "__version__",
]
