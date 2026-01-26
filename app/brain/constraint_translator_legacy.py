# DEPRECATED: This file is replaced by the constraint_translator module.
# This shim exists for backwards compatibility only.
# Use: from app.brain.constraint_translator import ConstraintTranslator

# Re-export from the new module location
from app.brain.constraint_translator.translator import (
    ConstraintTranslator,
    TranslatedConstraints,
    translate as translate_constraints,
    get_translator,
    filter_products_by_constraints,
    annotate_products_with_constraints,
    __version__,
)
from app.brain.constraint_translator.mappings import (
    CONSTRAINT_MAPPINGS,
    get_mapping_version,
)

# Legacy compatibility functions
def is_ingredient_blocked(ingredient_code, constraints):
    return constraints.is_ingredient_blocked(ingredient_code)

def get_block_reason(ingredient_code, constraints):
    if is_ingredient_blocked(ingredient_code, constraints):
        for code in constraints.reason_codes:
            if "BLOCK" in code:
                return code
    return None

def merge_constraints(bloodwork_constraints, other_constraints=None):
    # Simple merge - bloodwork always wins
    if not other_constraints:
        return bloodwork_constraints
    return bloodwork_constraints

__all__ = [
    "ConstraintTranslator",
    "TranslatedConstraints",
    "translate_constraints",
    "get_translator",
    "filter_products_by_constraints",
    "annotate_products_with_constraints",
    "CONSTRAINT_MAPPINGS",
    "get_mapping_version",
    "is_ingredient_blocked",
    "get_block_reason",
    "merge_constraints",
    "__version__",
]
