"""
GenoMAX² Constraint Translator: Admin API Endpoints
====================================================
API endpoints for testing and inspecting constraint translation.

Endpoints:
- GET  /api/v1/constraints/health        - Module health check
- GET  /api/v1/constraints/codes         - List all constraint codes
- GET  /api/v1/constraints/codes/{code}  - Get specific mapping
- POST /api/v1/constraints/translate     - Translate constraint codes
- GET  /api/v1/constraints/qa-matrix     - Run QA validation matrix
- POST /api/v1/constraints/validate      - Validate codes exist
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime

from .translator import (
    ConstraintTranslator,
    TranslatedConstraints,
    translate,
    get_translator,
    filter_products_by_constraints,
    annotate_products_with_constraints,
    __version__ as translator_version,
)
from .mappings import (
    CONSTRAINT_MAPPINGS,
    get_mapping_version,
    validate_mappings,
)

router = APIRouter(prefix="/api/v1/constraints", tags=["constraints"])


# =========================================================================
# Request/Response Models
# =========================================================================

class TranslateRequest(BaseModel):
    """Request to translate constraint codes."""
    constraint_codes: List[str] = Field(..., description="Constraint codes from Bloodwork Engine")
    sex: Optional[str] = Field(None, description="Sex for gender-specific rules")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class ValidateRequest(BaseModel):
    """Request to validate constraint codes."""
    constraint_codes: List[str] = Field(..., description="Codes to validate")


class FilterProductsRequest(BaseModel):
    """Request to filter products by constraints."""
    constraint_codes: List[str] = Field(..., description="Constraint codes")
    products: List[Dict[str, Any]] = Field(..., description="Products to filter")
    ingredient_key: str = Field("ingredient_tags", description="Key for ingredient list")


# =========================================================================
# Endpoints
# =========================================================================

@router.get("/health")
def constraint_translator_health():
    """Health check for Constraint Translator module."""
    validation = validate_mappings()
    return {
        "status": "healthy" if validation["valid"] else "degraded",
        "translator_version": translator_version,
        "mapping_version": get_mapping_version(),
        "total_mappings": validation["total_mappings"],
        "validation_errors": validation["errors"],
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/codes")
def list_constraint_codes():
    """List all known constraint codes with summaries."""
    translator = get_translator()
    codes = []
    
    for code in translator.list_codes():
        mapping = translator.get_mapping(code)
        if mapping:
            # Determine action type
            if mapping.get("blocked_ingredients"):
                action_type = "BLOCK"
            elif mapping.get("caution_flags"):
                action_type = "CAUTION"
            else:
                action_type = "FLAG"
            
            codes.append({
                "code": code,
                "action_type": action_type,
                "blocked_ingredients_count": len(mapping.get("blocked_ingredients", [])),
                "blocked_categories_count": len(mapping.get("blocked_categories", [])),
                "caution_flags_count": len(mapping.get("caution_flags", [])),
                "recommended_count": len(mapping.get("recommended_ingredients", [])),
                "reason_codes": mapping.get("reason_codes", []),
            })
    
    # Group by action type
    blocks = [c for c in codes if c["action_type"] == "BLOCK"]
    cautions = [c for c in codes if c["action_type"] == "CAUTION"]
    flags = [c for c in codes if c["action_type"] == "FLAG"]
    
    return {
        "total": len(codes),
        "by_type": {
            "BLOCK": len(blocks),
            "CAUTION": len(cautions),
            "FLAG": len(flags),
        },
        "codes": codes,
        "mapping_version": get_mapping_version(),
    }


@router.get("/codes/{code}")
def get_constraint_code_detail(code: str):
    """Get detailed mapping for a specific constraint code."""
    translator = get_translator()
    mapping = translator.get_mapping(code)
    
    if not mapping:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown constraint code: {code}. Use GET /api/v1/constraints/codes to list valid codes."
        )
    
    return {
        "code": code.upper(),
        "blocked_ingredients": mapping.get("blocked_ingredients", []),
        "blocked_categories": mapping.get("blocked_categories", []),
        "blocked_targets": mapping.get("blocked_targets", []),
        "caution_flags": mapping.get("caution_flags", []),
        "reason_codes": mapping.get("reason_codes", []),
        "recommended_ingredients": mapping.get("recommended_ingredients", []),
        "mapping_version": get_mapping_version(),
    }


@router.post("/translate")
def translate_constraints(request: TranslateRequest):
    """
    Translate constraint codes into enforcement semantics.
    
    This is the core endpoint that converts Bloodwork Engine output
    into routing/matching layer constraints.
    
    Example:
    ```json
    {
      "constraint_codes": ["BLOCK_IRON", "CAUTION_HEPATOTOXIC"]
    }
    ```
    """
    if not request.constraint_codes:
        raise HTTPException(status_code=400, detail="constraint_codes cannot be empty")
    
    result = translate(
        constraint_codes=request.constraint_codes,
        sex=request.sex,
        context=request.context,
    )
    
    return result.to_dict()


@router.post("/validate")
def validate_constraint_codes(request: ValidateRequest):
    """
    Validate that constraint codes are known.
    
    Returns which codes are valid vs invalid.
    """
    translator = get_translator()
    return translator.validate_codes(request.constraint_codes)


@router.post("/filter-products")
def filter_products(request: FilterProductsRequest):
    """
    Filter products based on constraint codes.
    
    Returns only products that are NOT blocked.
    """
    # Translate constraints
    constraints = translate(request.constraint_codes)
    
    # Filter products
    allowed = filter_products_by_constraints(
        products=request.products,
        constraints=constraints,
        ingredient_key=request.ingredient_key,
    )
    
    return {
        "input_products": len(request.products),
        "allowed_products": len(allowed),
        "blocked_products": len(request.products) - len(allowed),
        "constraint_summary": {
            "blocked_ingredients": constraints.blocked_ingredients,
            "reason_codes": constraints.reason_codes,
        },
        "products": allowed,
    }


@router.post("/annotate-products")
def annotate_products(request: FilterProductsRequest):
    """
    Annotate ALL products with constraint status.
    
    Each product gets:
    - _constraint_status: BLOCKED | RECOMMENDED | NEUTRAL
    - _blocked_ingredients: list of blocked ingredients found
    - _recommended_ingredients: list of recommended ingredients found
    """
    # Translate constraints
    constraints = translate(request.constraint_codes)
    
    # Annotate products
    annotated = annotate_products_with_constraints(
        products=request.products,
        constraints=constraints,
        ingredient_key=request.ingredient_key,
    )
    
    # Count by status
    status_counts = {"BLOCKED": 0, "RECOMMENDED": 0, "NEUTRAL": 0}
    for p in annotated:
        status = p.get("_constraint_status", "NEUTRAL")
        status_counts[status] = status_counts.get(status, 0) + 1
    
    return {
        "input_products": len(request.products),
        "status_counts": status_counts,
        "constraint_summary": constraints.to_dict()["summary"],
        "products": annotated,
    }


@router.get("/qa-matrix")
def run_qa_matrix():
    """
    Run QA validation matrix for all constraint codes.
    
    Validates that each constraint code produces expected outputs.
    """
    results = []
    passed = 0
    failed = 0
    
    # Test cases: code, expected_blocked, expected_cautions, expected_recommended
    test_cases = [
        ("BLOCK_IRON", ["iron"], [], []),
        ("BLOCK_POTASSIUM", ["potassium"], [], []),
        ("BLOCK_IODINE", ["iodine"], [], []),
        ("CAUTION_HEPATOTOXIC", ["ashwagandha"], ["hepatic_sensitive"], []),
        ("CAUTION_RENAL", [], ["renal_sensitive"], []),
        ("FLAG_METHYLATION_SUPPORT", [], ["methylation_impaired"], ["methylcobalamin", "methylfolate"]),
        ("FLAG_B12_DEFICIENCY", [], ["b12_deficiency"], ["methylcobalamin"]),
        ("FLAG_OMEGA3_PRIORITY", [], [], ["omega3", "fish_oil"]),
        ("FLAG_INSULIN_RESISTANCE", [], ["insulin_resistance"], ["berberine"]),
        ("BLOCK_POST_MI", ["l_arginine"], ["post_mi"], []),
    ]
    
    for code, exp_blocked, exp_cautions, exp_recommended in test_cases:
        try:
            result = translate([code])
            
            # Validate
            checks = []
            all_pass = True
            
            # Check blocked
            for ing in exp_blocked:
                found = ing in result.blocked_ingredients
                checks.append({"type": "blocked", "expected": ing, "found": found})
                if not found:
                    all_pass = False
            
            # Check cautions
            for flag in exp_cautions:
                found = flag in result.caution_flags
                checks.append({"type": "caution", "expected": flag, "found": found})
                if not found:
                    all_pass = False
            
            # Check recommended
            for ing in exp_recommended:
                found = ing in result.recommended_ingredients
                checks.append({"type": "recommended", "expected": ing, "found": found})
                if not found:
                    all_pass = False
            
            status = "PASS" if all_pass else "FAIL"
            if all_pass:
                passed += 1
            else:
                failed += 1
            
            results.append({
                "code": code,
                "status": status,
                "checks": checks,
                "output_hash": result.output_hash,
            })
            
        except Exception as e:
            failed += 1
            results.append({
                "code": code,
                "status": "ERROR",
                "error": str(e),
            })
    
    return {
        "summary": {
            "total_tests": len(test_cases),
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{passed/len(test_cases)*100:.1f}%",
        },
        "results": results,
        "translator_version": translator_version,
        "mapping_version": get_mapping_version(),
    }


@router.get("/determinism-test")
def test_determinism():
    """
    Test that translation is deterministic.
    
    Runs same translation 5 times and verifies identical output hashes.
    """
    test_codes = ["BLOCK_IRON", "CAUTION_HEPATOTOXIC", "FLAG_METHYLATION_SUPPORT"]
    
    hashes = []
    for i in range(5):
        result = translate(test_codes)
        hashes.append(result.output_hash)
    
    all_same = len(set(hashes)) == 1
    
    return {
        "test": "determinism",
        "input_codes": test_codes,
        "iterations": 5,
        "all_hashes_identical": all_same,
        "status": "PASS" if all_same else "FAIL",
        "hashes": hashes,
    }
