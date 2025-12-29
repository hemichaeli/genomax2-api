"""
GenoMAX² Protocol Explainability Module (Issue #8)

Purpose: Transform technically-correct protocols into:
- Understandable
- Explainable
- Trust-building
- Regulatory-compliant

Design Principle:
    The Brain decides. The UX explains. Never the reverse.

This module does NOT:
- Change scoring
- Change matching
- Add new recommendations
- Marketing/Upsell
- Dosage information

This module ONLY:
- Generates "why included" explanations
- Generates "why not blocked" explanations
- Calculates confidence level (rule-based, not intelligent)
- Provides locked disclaimer copy
- Explains blocked items (negative explainability)

Version: explainability_v1
"""

from .models import (
    ExplainabilityBlock,
    ConfidenceLevel,
    ProtocolConfidence,
    BlockedItemExplanation,
    ProtocolExplainability,
    DisclaimerSet,
)
from .explain import (
    generate_explainability,
    calculate_confidence,
    get_disclaimers,
)
from .admin import router as explainability_router

__all__ = [
    # Models
    "ExplainabilityBlock",
    "ConfidenceLevel",
    "ProtocolConfidence",
    "BlockedItemExplanation",
    "ProtocolExplainability",
    "DisclaimerSet",
    # Functions
    "generate_explainability",
    "calculate_confidence",
    "get_disclaimers",
    # Router
    "explainability_router",
]

__version__ = "explainability_v1"
