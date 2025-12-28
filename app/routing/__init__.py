"""
Routing Layer Module (Issue #6)

Pure Safety Elimination - applies biological constraints to valid SKUs.

Design Principles:
- PURE: no side effects, no state mutation
- ELIMINATIVE: only removes, never adds
- DUMB: no intelligence, just rule application
- DETERMINISTIC: same input → same output

This layer answers ONE question only:
"Is this SKU biologically ALLOWED to exist in the protocol?"

Version: routing_layer_v1
"""

from .models import (
    RoutingInput,
    RoutingConstraints,
    RoutingResult,
    AllowedSKU,
    BlockedSKU,
    RoutingAudit,
)
from .apply import apply_routing_constraints
from .admin import router as admin_router

__all__ = [
    # Models
    "RoutingInput",
    "RoutingConstraints",
    "RoutingResult",
    "AllowedSKU",
    "BlockedSKU",
    "RoutingAudit",
    # Functions
    "apply_routing_constraints",
    # Router
    "admin_router",
]

__version__ = "routing_layer_v1"
