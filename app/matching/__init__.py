"""
GenoMAX² Matching Layer (Issue #7)

Intent → SKU Matching (Protocol Assembly)

This module answers: "Given what is ALLOWED, what actually matches the user's goals?"

This is where GenoMAX² creates real user value by:
- Taking allowed_skus from routing layer (already safety-filtered)
- Taking prioritized_intents from Brain Compose
- Matching intents to available SKUs via ingredient tags
- Respecting gender-specific product lines (MAXimo² / MAXima²)
- Fulfilling requirements from bloodwork deficiencies
- Propagating caution warnings

PRINCIPLE: Blood does not negotiate. But once safe, we optimize for outcomes.

Version: matching_layer_v1
"""

from .models import (
    MatchingInput,
    MatchingResult,
    ProtocolItem,
    MatchingAudit,
    UserContext,
    IntentInput,
)
from .match import resolve_matching

__all__ = [
    "MatchingInput",
    "MatchingResult",
    "ProtocolItem",
    "MatchingAudit",
    "UserContext",
    "IntentInput",
    "resolve_matching",
]

__version__ = "matching_layer_v1"
