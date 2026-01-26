"""
GenoMAX² Constraint Translator Admin Endpoints
===============================================
API endpoints for testing and inspecting constraint translation.

Endpoints:
- GET  /api/v1/constraints/health - Module health check
- GET  /api/v1/constraints/mappings - List all constraint mappings
- GET  /api/v1/constraints/mappings/{code} - Get specific mapping
- POST /api/v1/constraints/translate - Translate constraint codes
- POST /api/v1/constraints/test - Test with sample bloodwork
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime

from app.brain.constraint_translator import (
    ConstraintTranslator,
    TranslatedConstraints,
    translate_constraints,
    get_translator,
    merge_constraints,
    CONSTRAINT_MAPPINGS,
    __version__ as translator_version,
)

router = APIRouter(prefix="/api/v1/constraints", tags=["constraints"])


# ============================================
# Request/Response Models
# ============================================

class TranslateRequest(BaseModel):
    """Request to translate constraint codes."""
    constraint_codes: List[str] = Field(..., description="List of constraint codes from Bloodwork Engine")
    sex: Optional[str] = Field(None, description="Sex for gender-specific rules")
    additional_context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class TranslateResponse(BaseModel):
    """Response from constraint translation."""
    blocked_ingredients: List[str]
    blocked_categories: List[str]
    blocked_targets: List[str]
    caution_flags: List[str]
    reason_codes: List[str]
    recommended_ingredients: List[str]
    translator_version: str
    input_hash: str
    output_hash: str
    translated_at: str


class TestBloodworkRequest(BaseModel):
    """Request to test with sample bloodwork markers."""
    markers: List[Dict[str, Any]] = Field(..., description="List of bloodwork markers")
    sex: str = Field("male", description="Sex (male/female)")


class ConstraintMappingResponse(BaseModel):
    """Response for a single constraint mapping."""
    code: str
    blocked_ingredients: List[str]
    blocked_categories: List[str]
    blocked_targets: List[str]
    caution_flags: List[str]
    reason_codes: List[str]
    recommended_ingredients: List[str]


# ============================================
# Endpoints
# ============================================

@router.get("/health")
def constraint_health():
    """Health check for Constraint Translator module."""
    translator = get_translator()
    return {
        "status": "healthy",
        "version": translator_version,
        "total_mappings": len(CONSTRAINT_MAPPINGS),
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/mappings")
def list_constraint_mappings():
    """List all known constraint mappings."""
    mappings = []
    for code, mapping in sorted(CONSTRAINT_MAPPINGS.items()):
        mappings.append({
            "code": code,
            "action_type": "BLOCK" if mapping.get("blocked_ingredients") else "CAUTION" if mapping.get("caution_flags") else "FLAG",
            "blocked_count": len(mapping.get("blocked_ingredients", [])),
            "caution_count": len(mapping.get("caution_flags", [])),
            "recommended_count": len(mapping.get("recommended_ingredients", [])),
        })
    
    return {
        "total": len(mappings),
        "mappings": mappings,
        "version": translator_version,
    }


@router.get("/mappings/{code}")
def get_constraint_mapping(code: str):
    """Get detailed mapping for a specific constraint code."""
    code_upper = code.upper()
    if code_upper not in CONSTRAINT_MAPPINGS:
        raise HTTPException(status_code=404, detail=f"Unknown constraint code: {code}")
    
    mapping = CONSTRAINT_MAPPINGS[code_upper]
    return ConstraintMappingResponse(
        code=code_upper,
        blocked_ingredients=mapping.get("blocked_ingredients", []),
        blocked_categories=mapping.get("blocked_categories", []),
        blocked_targets=mapping.get("blocked_targets", []),
        caution_flags=mapping.get("caution_flags", []),
        reason_codes=mapping.get("reason_codes", []),
        recommended_ingredients=mapping.get("recommended_ingredients", []),
    )


@router.post("/translate", response_model=TranslateResponse)
def translate_constraint_codes(request: TranslateRequest):
    """
    Translate constraint codes into enforcement semantics.
    
    This is the core function that converts Bloodwork Engine output
    into routing/matching layer constraints.
    """
    result = translate_constraints(
        constraint_codes=request.constraint_codes,
        sex=request.sex,
        additional_context=request.additional_context,
    )
    
    return TranslateResponse(
        blocked_ingredients=result.blocked_ingredients,
        blocked_categories=result.blocked_categories,
        blocked_targets=result.blocked_targets,
        caution_flags=result.caution_flags,
        reason_codes=result.reason_codes,
        recommended_ingredients=result.recommended_ingredients,
        translator_version=result.translator_version,
        input_hash=result.input_hash,
        output_hash=result.output_hash,
        translated_at=result.translated_at,
    )


@router.post("/test")
async def test_with_bloodwork(request: TestBloodworkRequest):
    """
    Test constraint translation with sample bloodwork.
    
    This endpoint:
    1. Calls Bloodwork Engine to process markers
    2. Extracts routing_constraints from response
    3. Translates constraints via ConstraintTranslator
    4. Returns full audit trail
    """
    import httpx
    
    # Call Bloodwork Engine
    bloodwork_url = "https://web-production-7110.up.railway.app/api/v1/bloodwork/process"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                bloodwork_url,
                json={
                    "markers": request.markers,
                    "sex": request.sex,
                }
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Bloodwork Engine error: {response.text}"
                )
            
            bloodwork_result = response.json()
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Bloodwork Engine unavailable: {str(e)}"
        )
    
    # Extract routing constraints
    routing_constraints = bloodwork_result.get("routing_constraints", [])
    safety_gates = bloodwork_result.get("safety_gates", [])
    
    # Translate constraints
    translated = translate_constraints(
        constraint_codes=routing_constraints,
        sex=request.sex,
    )
    
    return {
        "bloodwork_result": {
            "engine_version": bloodwork_result.get("engine_version"),
            "processed_at": bloodwork_result.get("processed_at"),
            "routing_constraints": routing_constraints,
            "safety_gates_triggered": len(safety_gates),
        },
        "translated_constraints": translated.to_dict(),
        "audit": {
            "markers_submitted": len(request.markers),
            "constraints_found": len(routing_constraints),
            "blocked_ingredients": len(translated.blocked_ingredients),
            "caution_flags": len(translated.caution_flags),
            "recommended_ingredients": len(translated.recommended_ingredients),
        },
    }


@router.get("/test-scenario/{scenario}")
def test_scenario(scenario: str):
    """
    Test predefined QA scenarios.
    
    Scenarios:
    - iron_overload: ferritin=400 male → BLOCK_IRON
    - hepatic_risk: ALT=85 → CAUTION_HEPATOTOXIC
    - methylation: homocysteine=15 → FLAG_METHYLATION_SUPPORT
    - combined: multiple constraints
    """
    scenarios = {
        "iron_overload": {
            "constraint_codes": ["BLOCK_IRON"],
            "expected_blocked": ["iron", "iron_bisglycinate", "ferrous_sulfate"],
            "expected_reason": "BLOOD_BLOCK_IRON",
        },
        "hepatic_risk": {
            "constraint_codes": ["CAUTION_HEPATOTOXIC"],
            "expected_blocked": ["ashwagandha"],
            "expected_caution": ["hepatic_sensitive", "liver_function_impaired"],
            "expected_reason": "BLOOD_CAUTION_HEPATOTOXIC",
        },
        "renal_risk": {
            "constraint_codes": ["CAUTION_RENAL"],
            "expected_caution": ["renal_sensitive", "kidney_function_impaired"],
            "expected_reason": "BLOOD_CAUTION_RENAL",
        },
        "methylation": {
            "constraint_codes": ["FLAG_METHYLATION_SUPPORT"],
            "expected_recommended": ["methylcobalamin", "methylfolate", "pyridoxal_5_phosphate"],
            "expected_reason": "BLOOD_FLAG_METHYLATION",
        },
        "combined": {
            "constraint_codes": ["BLOCK_IRON", "CAUTION_HEPATOTOXIC", "FLAG_METHYLATION_SUPPORT"],
            "description": "Multiple constraints combined",
        },
    }
    
    if scenario not in scenarios:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown scenario: {scenario}. Available: {list(scenarios.keys())}"
        )
    
    config = scenarios[scenario]
    result = translate_constraints(config["constraint_codes"])
    
    # Validate expectations
    validation = {"passed": True, "checks": []}
    
    if "expected_blocked" in config:
        for ing in config["expected_blocked"]:
            check = {
                "type": "blocked_ingredient",
                "expected": ing,
                "found": ing in result.blocked_ingredients,
            }
            validation["checks"].append(check)
            if not check["found"]:
                validation["passed"] = False
    
    if "expected_caution" in config:
        for flag in config["expected_caution"]:
            check = {
                "type": "caution_flag",
                "expected": flag,
                "found": flag in result.caution_flags,
            }
            validation["checks"].append(check)
            if not check["found"]:
                validation["passed"] = False
    
    if "expected_recommended" in config:
        for ing in config["expected_recommended"]:
            check = {
                "type": "recommended_ingredient",
                "expected": ing,
                "found": ing in result.recommended_ingredients,
            }
            validation["checks"].append(check)
            if not check["found"]:
                validation["passed"] = False
    
    if "expected_reason" in config:
        check = {
            "type": "reason_code",
            "expected": config["expected_reason"],
            "found": config["expected_reason"] in result.reason_codes,
        }
        validation["checks"].append(check)
        if not check["found"]:
            validation["passed"] = False
    
    return {
        "scenario": scenario,
        "input": config,
        "result": result.to_dict(),
        "validation": validation,
    }


@router.get("/qa-matrix")
def qa_matrix():
    """
    Run full QA scenario matrix and return results.
    
    This validates all constraint mappings work as expected.
    """
    scenarios = [
        ("iron_overload", ["BLOCK_IRON"]),
        ("hepatic_risk", ["CAUTION_HEPATOTOXIC"]),
        ("renal_risk", ["CAUTION_RENAL"]),
        ("vitamin_d_caution", ["CAUTION_VITAMIN_D"]),
        ("inflammation", ["FLAG_ACUTE_INFLAMMATION"]),
        ("methylation", ["FLAG_METHYLATION_SUPPORT"]),
        ("b12_deficiency", ["FLAG_B12_DEFICIENCY"]),
        ("thyroid_support", ["FLAG_THYROID_SUPPORT"]),
        ("hyperthyroid", ["FLAG_HYPERTHYROID"]),
        ("omega3_priority", ["FLAG_OMEGA3_PRIORITY"]),
        ("insulin_resistance", ["FLAG_INSULIN_RESISTANCE"]),
        ("oxidative_stress", ["FLAG_OXIDATIVE_STRESS"]),
        ("post_mi", ["BLOCK_POST_MI"]),
        ("mthfr", ["FLAG_METHYLFOLATE_REQUIRED"]),
    ]
    
    results = []
    passed = 0
    failed = 0
    
    for name, codes in scenarios:
        try:
            result = translate_constraints(codes)
            
            # Basic validation: should produce some output
            has_output = (
                len(result.blocked_ingredients) > 0 or
                len(result.caution_flags) > 0 or
                len(result.recommended_ingredients) > 0
            )
            
            # Should have reason codes
            has_reasons = len(result.reason_codes) > 0
            
            status = "PASS" if has_output and has_reasons else "WARN"
            if status == "PASS":
                passed += 1
            else:
                failed += 1
            
            results.append({
                "scenario": name,
                "constraint_codes": codes,
                "status": status,
                "blocked_count": len(result.blocked_ingredients),
                "caution_count": len(result.caution_flags),
                "recommended_count": len(result.recommended_ingredients),
                "reason_codes": result.reason_codes,
                "output_hash": result.output_hash,
            })
        except Exception as e:
            failed += 1
            results.append({
                "scenario": name,
                "constraint_codes": codes,
                "status": "FAIL",
                "error": str(e),
            })
    
    return {
        "summary": {
            "total": len(scenarios),
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{passed/len(scenarios)*100:.1f}%",
        },
        "results": results,
        "translator_version": translator_version,
    }
