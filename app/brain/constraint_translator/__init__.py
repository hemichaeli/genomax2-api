"""
GenoMAX² Constraint Translator
Translates Bloodwork Engine constraints into routing/matching enforcement

Version: 1.0.0

This module is PURE and DETERMINISTIC:
- No diagnosis, no dosing, no recommendation logic
- Only translates constraint codes into canonical enforcement fields
- Sorting is always alphabetical for stable hashing

Principle: "Blood does not negotiate."
Bloodwork constraints cannot be removed or overridden by downstream layers.
"""

from .translator import ConstraintTranslator, TranslatedConstraints
from .mappings import CONSTRAINT_MAPPINGS, get_mapping_version

__version__ = "1.0.0"
__all__ = [
    "ConstraintTranslator",
    "TranslatedConstraints",
    "CONSTRAINT_MAPPINGS",
    "get_mapping_version",
]
