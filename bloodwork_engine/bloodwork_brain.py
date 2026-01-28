"""
GenoMAX² Bloodwork-to-Brain Pipeline Handoff
=============================================
Connects lab results to the Brain orchestrator for supplement recommendations.

Pipeline Flow:
1. Bloodwork submission (OCR or Junction) → normalized_markers
2. Safety gate evaluation → blocks/cautions
3. Brain orchestrator → supplement recommendations
4. Route phase → SKU selection

Version: 1.0.0
"""

import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, BackgroundTasks

router = APIRouter(prefix="/api/v1/bloodwork", tags=["bloodwork"])

# =============================================================================
# MODELS
# =============================================================================

class SafetyGateResult(str, Enum):
    PASS = "pass"
    CAUTION = "caution"
    BLOCK = "block"

class SafetyGate(BaseModel):
    gate_id: str
    name: str
    result: SafetyGateResult
    triggered_by: Optional[str] = None
    marker_value: Optional[float] = None
    threshold: Optional[float] = None
    blocked_ingredients: List[str] = []
    message: str

class NormalizedMarker(BaseModel):
    code: str
    original_name: str
    value: float
    unit: str
    reference_range: Optional[Dict[str, float]] = None
    flag: str = "N"  # H, L, N, C
    confidence: float = 1.0
    loinc: Optional[str] = None

class BloodworkCanonical(BaseModel):
    """Canonical handoff object from Bloodwork Engine to Brain."""
    submission_id: str
    user_id: str
    source: str  # junction, ocr_upload, manual_entry
    collection_date: Optional[datetime] = None
    
    # Normalized markers
    markers: List[NormalizedMarker]
    markers_count: int
    priority_markers_found: int
    
    # Safety evaluation
    safety_gates: List[SafetyGate]
    blocked_ingredients: List[str]
    caution_ingredients: List[str]
    
    # Quality metrics
    confidence_score: float
    needs_review: bool = False
    review_reasons: List[str] = []
    
    # Timestamps
    created_at: datetime
    evaluated_at: datetime

class BrainRunRequest(BaseModel):
    submission_id: str
    user_id: str
    gender: str  # male, female
    age: Optional[int] = None
    lifecycle_phase: Optional[str] = None  # pregnant, postmenopausal, etc.
    goals: List[str] = []
    excluded_ingredients: List[str] = []

class BrainRunResponse(BaseModel):
    run_id: str
    submission_id: str
    status: str
    recommended_modules: List[Dict[str, Any]]
    blocked_modules: List[Dict[str, Any]]
    safety_summary: Dict[str, Any]
    created_at: datetime

# =============================================================================
# SAFETY GATES
# =============================================================================

# 13 Priority Biomarkers with safety thresholds
SAFETY_GATES = {
    "iron_overload": {
        "name": "Iron Overload Gate",
        "markers": ["ferritin", "transferrin_sat"],
        "conditions": [
            {"marker": "ferritin", "operator": ">", "threshold": 300, "result": "block"},
            {"marker": "transferrin_sat", "operator": ">", "threshold": 45, "result": "block"},
            {"marker": "ferritin", "operator": ">", "threshold": 200, "result": "caution"},
        ],
        "blocked_ingredients": ["iron", "iron_bisglycinate", "iron_glycinate", "ferrous_sulfate"],
        "message_block": "Iron supplementation blocked due to elevated iron stores.",
        "message_caution": "Iron supplementation requires caution - elevated ferritin."
    },
    "iron_deficiency": {
        "name": "Iron Deficiency Gate",
        "markers": ["ferritin", "serum_iron"],
        "conditions": [
            {"marker": "ferritin", "operator": "<", "threshold": 30, "result": "flag"},
            {"marker": "serum_iron", "operator": "<", "threshold": 60, "result": "flag"},
        ],
        "flagged_recommendations": ["iron_bisglycinate"],
        "message": "Low iron markers detected - iron supplementation may be beneficial."
    },
    "vitamin_d_toxicity": {
        "name": "Vitamin D Toxicity Gate",
        "markers": ["vitamin_d_25oh"],
        "conditions": [
            {"marker": "vitamin_d_25oh", "operator": ">", "threshold": 100, "result": "block"},
            {"marker": "vitamin_d_25oh", "operator": ">", "threshold": 80, "result": "caution"},
        ],
        "blocked_ingredients": ["vitamin_d3", "vitamin_d2", "cholecalciferol"],
        "message_block": "Vitamin D supplementation blocked - levels already high.",
        "message_caution": "Reduce vitamin D dosage - levels approaching upper limit."
    },
    "vitamin_d_deficiency": {
        "name": "Vitamin D Deficiency Gate",
        "markers": ["vitamin_d_25oh"],
        "conditions": [
            {"marker": "vitamin_d_25oh", "operator": "<", "threshold": 20, "result": "deficient"},
            {"marker": "vitamin_d_25oh", "operator": "<", "threshold": 30, "result": "insufficient"},
        ],
        "flagged_recommendations": ["vitamin_d3"],
        "message_deficient": "Vitamin D deficiency detected - supplementation recommended.",
        "message_insufficient": "Vitamin D insufficient - supplementation may be beneficial."
    },
    "b12_deficiency": {
        "name": "B12 Deficiency Gate",
        "markers": ["vitamin_b12"],
        "conditions": [
            {"marker": "vitamin_b12", "operator": "<", "threshold": 200, "result": "deficient"},
            {"marker": "vitamin_b12", "operator": "<", "threshold": 400, "result": "suboptimal"},
        ],
        "flagged_recommendations": ["methylcobalamin", "vitamin_b12"],
        "message_deficient": "B12 deficiency detected - supplementation strongly recommended.",
        "message_suboptimal": "B12 suboptimal - supplementation may improve energy levels."
    },
    "diabetic_gate": {
        "name": "Diabetic/Pre-diabetic Gate",
        "markers": ["hba1c", "glucose"],
        "conditions": [
            {"marker": "hba1c", "operator": ">", "threshold": 6.4, "result": "diabetic"},
            {"marker": "hba1c", "operator": ">", "threshold": 5.6, "result": "prediabetic"},
            {"marker": "glucose", "operator": ">", "threshold": 125, "result": "diabetic"},
        ],
        "caution_ingredients": ["sugar", "maltodextrin", "dextrose"],
        "flagged_recommendations": ["berberine", "chromium", "alpha_lipoic_acid"],
        "message_diabetic": "HbA1c indicates diabetes - blood sugar support recommended.",
        "message_prediabetic": "HbA1c elevated - metabolic support may be beneficial."
    },
    "hepatic_caution": {
        "name": "Hepatic Function Gate",
        "markers": ["alt", "ast", "bilirubin_total"],
        "conditions": [
            {"marker": "alt", "operator": ">", "threshold": 56, "result": "caution"},
            {"marker": "ast", "operator": ">", "threshold": 40, "result": "caution"},
            {"marker": "bilirubin_total", "operator": ">", "threshold": 1.2, "result": "caution"},
        ],
        "blocked_ingredients": ["ashwagandha", "kava"],  # Hepatotoxicity risk
        "caution_ingredients": ["niacin", "red_yeast_rice"],
        "message": "Elevated liver enzymes - hepatotoxic supplements blocked."
    },
    "renal_caution": {
        "name": "Renal Function Gate",
        "markers": ["creatinine", "bun"],
        "conditions": [
            {"marker": "creatinine", "operator": ">", "threshold": 1.3, "result": "caution"},
            {"marker": "bun", "operator": ">", "threshold": 20, "result": "caution"},
        ],
        "caution_ingredients": ["creatine", "high_protein"],
        "blocked_ingredients": [],
        "message": "Elevated kidney markers - protein/creatine intake should be monitored."
    },
    "inflammation_gate": {
        "name": "Inflammation Gate",
        "markers": ["hscrp", "homocysteine"],
        "conditions": [
            {"marker": "hscrp", "operator": ">", "threshold": 3.0, "result": "elevated"},
            {"marker": "homocysteine", "operator": ">", "threshold": 15, "result": "elevated"},
        ],
        "flagged_recommendations": ["omega3", "curcumin", "methylfolate"],
        "message": "Elevated inflammation markers - anti-inflammatory support recommended."
    }
}

# =============================================================================
# SAFETY EVALUATION
# =============================================================================

def evaluate_safety_gates(
    markers: List[NormalizedMarker],
    user_context: Optional[Dict[str, Any]] = None
) -> Tuple[List[SafetyGate], List[str], List[str]]:
    """
    Evaluate all safety gates against provided markers.
    
    Returns:
        Tuple of (safety_gates, blocked_ingredients, caution_ingredients)
    """
    results = []
    all_blocked = set()
    all_caution = set()
    
    # Build marker lookup
    marker_lookup = {m.code: m for m in markers}
    
    for gate_id, gate_config in SAFETY_GATES.items():
        gate_markers = gate_config["markers"]
        conditions = gate_config["conditions"]
        
        # Check if we have the required markers
        available_markers = [m for m in gate_markers if m in marker_lookup]
        if not available_markers:
            continue
        
        # Evaluate conditions
        gate_result = SafetyGateResult.PASS
        triggered_by = None
        triggered_value = None
        triggered_threshold = None
        message = ""
        
        for condition in conditions:
            marker_code = condition["marker"]
            if marker_code not in marker_lookup:
                continue
            
            marker = marker_lookup[marker_code]
            value = marker.value
            threshold = condition["threshold"]
            operator = condition["operator"]
            result_type = condition["result"]
            
            # Evaluate condition
            triggered = False
            if operator == ">" and value > threshold:
                triggered = True
            elif operator == "<" and value < threshold:
                triggered = True
            elif operator == ">=" and value >= threshold:
                triggered = True
            elif operator == "<=" and value <= threshold:
                triggered = True
            
            if triggered:
                triggered_by = marker_code
                triggered_value = value
                triggered_threshold = threshold
                
                if result_type == "block":
                    gate_result = SafetyGateResult.BLOCK
                    message = gate_config.get("message_block", gate_config.get("message", ""))
                    all_blocked.update(gate_config.get("blocked_ingredients", []))
                    break
                elif result_type in ["caution", "elevated", "diabetic", "prediabetic"]:
                    if gate_result != SafetyGateResult.BLOCK:
                        gate_result = SafetyGateResult.CAUTION
                    message = gate_config.get(f"message_{result_type}", gate_config.get("message_caution", gate_config.get("message", "")))
                    all_caution.update(gate_config.get("caution_ingredients", []))
        
        # Add blocked ingredients for BLOCK results
        if gate_result == SafetyGateResult.BLOCK:
            blocked = gate_config.get("blocked_ingredients", [])
        else:
            blocked = []
        
        results.append(SafetyGate(
            gate_id=gate_id,
            name=gate_config["name"],
            result=gate_result,
            triggered_by=triggered_by,
            marker_value=triggered_value,
            threshold=triggered_threshold,
            blocked_ingredients=blocked,
            message=message
        ))
    
    return results, list(all_blocked), list(all_caution)

# =============================================================================
# BLOODWORK ENGINE
# =============================================================================

async def create_canonical_handoff(
    submission_id: str,
    user_id: str,
    source: str,
    markers: List[NormalizedMarker],
    collection_date: Optional[datetime] = None,
    confidence_score: float = 1.0,
    needs_review: bool = False,
    review_reasons: List[str] = []
) -> BloodworkCanonical:
    """
    Create canonical handoff object for Brain orchestrator.
    
    This is the single point of truth passed from Bloodwork Engine to Brain.
    """
    # Count priority markers
    priority_codes = {
        "ferritin", "serum_iron", "tibc", "transferrin_sat",
        "vitamin_d_25oh", "vitamin_b12", "folate", "hba1c",
        "hscrp", "homocysteine", "omega3_index", "magnesium_rbc", "zinc"
    }
    priority_found = sum(1 for m in markers if m.code in priority_codes)
    
    # Evaluate safety gates
    safety_gates, blocked, caution = evaluate_safety_gates(markers)
    
    return BloodworkCanonical(
        submission_id=submission_id,
        user_id=user_id,
        source=source,
        collection_date=collection_date,
        markers=markers,
        markers_count=len(markers),
        priority_markers_found=priority_found,
        safety_gates=safety_gates,
        blocked_ingredients=blocked,
        caution_ingredients=caution,
        confidence_score=confidence_score,
        needs_review=needs_review,
        review_reasons=review_reasons,
        created_at=datetime.utcnow(),
        evaluated_at=datetime.utcnow()
    )

# =============================================================================
# API ENDPOINTS
# =============================================================================

@router.post("/evaluate", response_model=BloodworkCanonical)
async def evaluate_bloodwork(
    submission_id: str,
    markers: List[NormalizedMarker],
    user_id: str = "anonymous",
    source: str = "api"
):
    """
    Evaluate bloodwork markers and create canonical handoff.
    
    This endpoint:
    1. Accepts normalized markers
    2. Evaluates safety gates
    3. Returns canonical object ready for Brain
    """
    return await create_canonical_handoff(
        submission_id=submission_id,
        user_id=user_id,
        source=source,
        markers=markers
    )

@router.post("/trigger-brain", response_model=BrainRunResponse)
async def trigger_brain_run(
    request: BrainRunRequest,
    background_tasks: BackgroundTasks
):
    """
    Trigger Brain orchestrator with bloodwork data.
    
    Flow:
    1. Fetch submission from database
    2. Create canonical handoff
    3. Pass to Brain orchestrator
    4. Return run_id for status tracking
    """
    # TODO: Fetch submission from database
    # submission = await db.bloodwork_submissions.find_one({"id": request.submission_id})
    
    # For now, return mock response
    run_id = f"brain_run_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    
    # TODO: Actually trigger Brain pipeline
    # background_tasks.add_task(
    #     execute_brain_run,
    #     run_id=run_id,
    #     submission_id=request.submission_id,
    #     user_context=request.dict()
    # )
    
    return BrainRunResponse(
        run_id=run_id,
        submission_id=request.submission_id,
        status="queued",
        recommended_modules=[],
        blocked_modules=[],
        safety_summary={
            "gates_evaluated": len(SAFETY_GATES),
            "blocked_count": 0,
            "caution_count": 0
        },
        created_at=datetime.utcnow()
    )

@router.get("/brain-run/{run_id}")
async def get_brain_run_status(run_id: str):
    """Get status of a Brain run."""
    # TODO: Fetch from database
    return {
        "run_id": run_id,
        "status": "pending",
        "progress": 0,
        "message": "Brain run status tracking not yet implemented"
    }

@router.get("/safety-gates")
async def list_safety_gates():
    """List all configured safety gates."""
    gates = []
    for gate_id, config in SAFETY_GATES.items():
        gates.append({
            "id": gate_id,
            "name": config["name"],
            "markers": config["markers"],
            "blocked_ingredients": config.get("blocked_ingredients", []),
            "caution_ingredients": config.get("caution_ingredients", [])
        })
    return {"gates": gates, "count": len(gates)}

@router.post("/test-safety-gates")
async def test_safety_gates(markers: List[NormalizedMarker]):
    """
    Test safety gate evaluation with provided markers.
    
    Useful for:
    - QA testing
    - Understanding which gates would trigger
    - Validating marker normalization
    """
    safety_gates, blocked, caution = evaluate_safety_gates(markers)
    
    return {
        "gates": [g.dict() for g in safety_gates],
        "blocked_ingredients": blocked,
        "caution_ingredients": caution,
        "summary": {
            "total_gates": len(safety_gates),
            "blocks": sum(1 for g in safety_gates if g.result == SafetyGateResult.BLOCK),
            "cautions": sum(1 for g in safety_gates if g.result == SafetyGateResult.CAUTION),
            "passes": sum(1 for g in safety_gates if g.result == SafetyGateResult.PASS)
        }
    }

# =============================================================================
# INTEGRATION NOTES
# =============================================================================
"""
INTEGRATION STEPS:
1. Import router: from bloodwork_brain import router as bloodwork_router
2. Include in FastAPI: app.include_router(bloodwork_router)

CANONICAL HANDOFF:
The BloodworkCanonical object is the single point of truth passed to Brain.
It contains:
- All normalized markers with confidence scores
- Safety gate evaluations
- Blocked/caution ingredient lists
- Quality metrics

BRAIN ORCHESTRATOR CONNECTION:
The Brain receives BloodworkCanonical and:
1. Filters supplement catalog by blocked ingredients
2. Scores modules by marker deficiencies
3. Applies user preferences and goals
4. Routes to SKU selection

SAFETY GATE RULES:
- BLOCK: Ingredient absolutely cannot be included
- CAUTION: Ingredient can be included with reduced dosage or warning
- PASS: No restrictions from this gate

"Blood does not negotiate" - blocked ingredients are never overridden.
"""
