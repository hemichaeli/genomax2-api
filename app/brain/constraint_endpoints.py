"""
GenoMAX² Constraint Translator Inline Endpoints v1.0.0
======================================================
Inline constraint endpoints that work without separate router registration.
Import this module to add /api/v1/brain/constraints/* endpoints.
"""

from datetime import datetime
from app.brain.constraint_translator import (
    translate_constraints,
    get_translator,
    CONSTRAINT_MAPPINGS,
    __version__ as translator_version,
)


def register_constraint_endpoints(brain_router):
    """Register constraint endpoints on the brain router."""
    
    @brain_router.get("/constraints/health")
    def constraint_health():
        """Health check for Constraint Translator module."""
        return {
            "status": "healthy",
            "version": translator_version,
            "total_mappings": len(CONSTRAINT_MAPPINGS),
            "checked_at": datetime.utcnow().isoformat() + "Z",
        }
    
    @brain_router.get("/constraints/mappings")
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
    
    @brain_router.get("/constraints/mappings/{code}")
    def get_constraint_mapping(code: str):
        """Get detailed mapping for a specific constraint code."""
        from fastapi import HTTPException
        
        code_upper = code.upper()
        if code_upper not in CONSTRAINT_MAPPINGS:
            raise HTTPException(status_code=404, detail=f"Unknown constraint code: {code}")
        
        mapping = CONSTRAINT_MAPPINGS[code_upper]
        return {
            "code": code_upper,
            "blocked_ingredients": mapping.get("blocked_ingredients", []),
            "blocked_categories": mapping.get("blocked_categories", []),
            "blocked_targets": mapping.get("blocked_targets", []),
            "caution_flags": mapping.get("caution_flags", []),
            "reason_codes": mapping.get("reason_codes", []),
            "recommended_ingredients": mapping.get("recommended_ingredients", []),
        }
    
    @brain_router.post("/constraints/translate")
    def translate_constraint_codes(
        constraint_codes: list,
        sex: str = None,
    ):
        """
        Translate constraint codes into enforcement semantics.
        
        This is the core function that converts Bloodwork Engine output
        into routing/matching layer constraints.
        """
        result = translate_constraints(
            constraint_codes=constraint_codes,
            sex=sex,
        )
        
        return result.to_dict()
    
    @brain_router.get("/constraints/qa-matrix")
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
                
                has_output = (
                    len(result.blocked_ingredients) > 0 or
                    len(result.caution_flags) > 0 or
                    len(result.recommended_ingredients) > 0
                )
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
    
    return [
        "/constraints/health",
        "/constraints/mappings",
        "/constraints/mappings/{code}",
        "/constraints/translate",
        "/constraints/qa-matrix",
    ]
