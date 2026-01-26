# This file has been deprecated in favor of the module: app/brain/constraint_translator/
# 
# The new module provides:
# - app/brain/constraint_translator/__init__.py
# - app/brain/constraint_translator/translator.py
# - app/brain/constraint_translator/mappings.py
# - app/brain/constraint_translator/router.py
#
# For backwards compatibility, import from the module:
#   from app.brain.constraint_translator import ConstraintTranslator, translate

from app.brain.constraint_translator.translator import (
    ConstraintTranslator,
    TranslatedConstraints,
    translate,
    get_translator,
    filter_products_by_constraints,
    annotate_products_with_constraints,
)
from app.brain.constraint_translator.mappings import CONSTRAINT_MAPPINGS, get_mapping_version

__all__ = [
    "ConstraintTranslator",
    "TranslatedConstraints",
    "translate",
    "get_translator",
    "filter_products_by_constraints",
    "annotate_products_with_constraints",
    "CONSTRAINT_MAPPINGS",
    "get_mapping_version",
]
