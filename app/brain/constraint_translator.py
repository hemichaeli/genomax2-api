# Compatibility redirect: This file redirects to the constraint_translator/ directory module.
# This avoids Python module collision while maintaining backward compatibility.
#
# The canonical implementation is in constraint_translator/ (Issue #16):
# - __init__.py - Public API exports
# - translator.py - Core translation logic  
# - mappings.py - 33 constraint mappings (12 BLOCK, 20 CAUTION, 1 FLAG)
# - router.py - 9 REST endpoints

# Re-export everything from the directory module
from app.brain.constraint_translator import *
from app.brain.constraint_translator import (
    ConstraintTranslator,
    TranslatedConstraints,
    CONSTRAINT_MAPPINGS,
    get_mapping_version,
    get_translator,
    translate,
    translate_constraints,
    merge_constraints,
    filter_products_by_constraints,
    annotate_products_with_constraints,
    __version__,
)
