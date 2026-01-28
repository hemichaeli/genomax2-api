"""
GenoMAX² Lab Integration Router
--------------------------------
Central router that combines all lab integration endpoints.
Import this single router into your main FastAPI app.

Usage in api_server.py:
    from routes.lab_integration_router import lab_router
    app.include_router(lab_router, prefix="/api/v1")

Version: 3.33.0
Author: GenoMAX² Engineering
"""

from fastapi import APIRouter

# Create main lab integration router
lab_router = APIRouter(tags=["Lab Integration"])

# Import sub-routers (these will be imported from your routes directory)
# Each module defines its own router that we include here

# ============================================================================
# ROUTER CONFIGURATION
# ============================================================================
# 
# After copying lab_upload.py, junction_client.py, and bloodwork_brain.py
# to your src/routes/ directory, update the imports below:
#
# from routes.lab_upload import router as upload_router
# from routes.junction_client import router as junction_router  
# from routes.bloodwork_brain import router as brain_router
#
# lab_router.include_router(upload_router, prefix="/lab")
# lab_router.include_router(junction_router, prefix="/labs")
# lab_router.include_router(brain_router, prefix="/bloodwork")
#
# ============================================================================

# For now, we define placeholder routes that document the expected structure
# These will be replaced when the actual modules are imported

@lab_router.get("/lab/health", summary="Lab Integration Health Check")
async def lab_health_check():
    """
    Health check for lab integration subsystem.
    Returns status of all lab integration components.
    """
    return {
        "status": "healthy",
        "version": "3.33.0",
        "components": {
            "ocr_upload": {
                "status": "ready",
                "endpoint": "/api/v1/lab/upload",
                "description": "PDF/image upload with Claude Sonnet 4 OCR"
            },
            "junction_api": {
                "status": "configured",
                "endpoint": "/api/v1/labs/orders",
                "description": "Junction/Vital lab ordering API"
            },
            "brain_handoff": {
                "status": "ready",
                "endpoint": "/api/v1/bloodwork/evaluate",
                "description": "Bloodwork-to-Brain pipeline with safety gates"
            }
        },
        "safety_gates": [
            "iron_overload",
            "iron_deficiency", 
            "vitamin_d_toxicity",
            "vitamin_d_deficiency",
            "b12_deficiency",
            "diabetic_gate",
            "hepatic_caution",
            "renal_caution",
            "inflammation_gate"
        ],
        "priority_biomarkers": 13
    }


@lab_router.get("/lab/capabilities", summary="Lab Integration Capabilities")
async def lab_capabilities():
    """
    Returns full capabilities of the lab integration system.
    """
    return {
        "ocr": {
            "supported_formats": ["application/pdf", "image/jpeg", "image/png", "image/webp"],
            "max_file_size_mb": 10,
            "extraction_model": "claude-sonnet-4-20250514",
            "biomarker_aliases": 50,
            "priority_biomarkers": [
                "ferritin", "serum_iron", "tibc", "transferrin_sat",
                "vitamin_d_25oh", "vitamin_b12", "folate", "hba1c",
                "hscrp", "homocysteine", "omega3_index", "magnesium_rbc", "zinc"
            ]
        },
        "junction": {
            "panels": {
                "ESSENTIAL": {
                    "markers": 20,
                    "price_range": "$150-200",
                    "includes": ["Iron Panel", "Vitamin D", "B12", "Folate", "HbA1c", "CMP-14"]
                },
                "COMPLETE": {
                    "markers": 40,
                    "price_range": "$300-400", 
                    "includes": ["Essential + Thyroid", "CBC", "Lipid Panel", "hs-CRP", "Homocysteine"]
                },
                "CUSTOM": {
                    "description": "User-specified LOINC codes"
                }
            },
            "order_types": ["walk-in", "testkit", "at-home-phlebotomy"],
            "coverage": "49 states (excludes NY, NJ, RI for DTC)"
        },
        "brain_handoff": {
            "safety_gates": {
                "iron_overload": {
                    "triggers": "ferritin >300 ng/mL OR transferrin_sat >45%",
                    "blocks": ["iron", "iron_bisglycinate"]
                },
                "vitamin_d_toxicity": {
                    "triggers": "vitamin_d_25oh >100 ng/mL",
                    "blocks": ["vitamin_d3", "vitamin_d2"]
                },
                "hepatic_caution": {
                    "triggers": "ALT >56 U/L OR AST >40 U/L",
                    "blocks": ["ashwagandha", "kava"]
                },
                "diabetic_gate": {
                    "triggers": "HbA1c >6.4%",
                    "cautions": ["sugar", "maltodextrin"]
                },
                "renal_caution": {
                    "triggers": "creatinine >1.3 mg/dL",
                    "cautions": ["creatine", "high_protein"]
                }
            },
            "canonical_schema": "BloodworkCanonical",
            "confidence_threshold": 0.85
        }
    }


# ============================================================================
# ENDPOINT SUMMARY
# ============================================================================
#
# Once fully integrated, the following endpoints will be available:
#
# OCR Upload (/api/v1/lab/):
#   POST /upload              - Upload PDF/image for OCR extraction
#   GET  /submissions         - List user's bloodwork submissions
#   GET  /submissions/{id}    - Get specific submission details
#
# Junction API (/api/v1/labs/):
#   POST /orders              - Create new lab order
#   GET  /orders/{id}         - Get order status
#   GET  /orders/{id}/results - Get order results
#   POST /orders/{id}/cancel  - Cancel pending order
#   GET  /locations           - Find nearby PSC locations
#   GET  /appointments        - Get appointment availability
#   POST /orders/{id}/appointment - Schedule appointment
#   POST /webhooks/junction   - Webhook handler
#
# Brain Handoff (/api/v1/bloodwork/):
#   POST /evaluate            - Evaluate markers, create canonical handoff
#   POST /trigger-brain       - Trigger Brain orchestrator
#   GET  /brain-run/{id}      - Get Brain run status
#   GET  /safety-gates        - List all safety gates
#   POST /test-safety-gates   - Test safety gate evaluation
#
# ============================================================================


# Export the router
__all__ = ["lab_router"]
