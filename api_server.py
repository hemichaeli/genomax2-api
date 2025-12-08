#!/usr/bin/env python3
"""
GenoMAX2 API Server - Cloud Ready
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum

from genomax_engine import GenoMAXEngine, UserProfile, Gender

app = FastAPI(
    title="GenoMAX2 API",
    description="Gender-Optimized Biological Operating System - Recommendation Engine",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = GenoMAXEngine()


class GenderEnum(str, Enum):
    male = "male"
    female = "female"

class RecommendationRequest(BaseModel):
    gender: GenderEnum
    goals: List[str] = Field(..., min_items=1, max_items=5)
    medications: List[str] = Field(default=[])
    conditions: List[str] = Field(default=[])
    age: Optional[int] = Field(default=None, ge=18, le=100)
    exclude_ingredients: List[str] = Field(default=[])

class DrugCheckRequest(BaseModel):
    ingredient_name: str
    medications: List[str]


@app.get("/")
async def root():
    return {"service": "GenoMAX2 Recommendation Engine", "version": "1.0.0", "status": "operational"}

@app.get("/goals")
async def get_goals():
    goals = engine.get_available_goals()
    return {"count": len(goals), "goals": goals}

@app.get("/modules")
async def get_modules():
    modules = engine.get_available_modules()
    return {"count": len(modules), "modules": modules}

@app.post("/recommend")
async def get_recommendations(request: RecommendationRequest):
    try:
        profile = UserProfile(
            gender=Gender.MALE if request.gender == GenderEnum.male else Gender.FEMALE,
            goals=request.goals,
            medications=request.medications,
            conditions=request.conditions,
            age=request.age,
            exclude_ingredients=request.exclude_ingredients
        )
        recommendation = engine.generate_recommendations(profile)
        return engine.to_dict(recommendation)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/check-interactions")
async def check_drug_interactions(request: DrugCheckRequest):
    try:
        with engine.engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT id FROM ingredients WHERE LOWER(name) = LOWER(:name)"), {'name': request.ingredient_name})
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Ingredient '{request.ingredient_name}' not found")
            ingredient_id = row[0]
        
        warnings = engine._check_drug_interactions(ingredient_id, request.medications)
        return {
            "ingredient": request.ingredient_name,
            "medications_checked": request.medications,
            "interactions_found": len(warnings),
            "interactions": warnings,
            "safe": len(warnings) == 0
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ingredient/{ingredient_name}")
async def get_ingredient_details(ingredient_name: str):
    try:
        with engine.engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("""
                SELECT id, name, category, evidence_grade, evidence_strength_score,
                    practical_dose, timing_instruction, male_relevance_score, female_relevance_score,
                    male_specific_notes, female_specific_notes, primary_biomarkers, secondary_biomarkers,
                    mechanisms, contraindications, side_effects_common, side_effects_rare,
                    study_references, num_rcts, total_participants
                FROM ingredients WHERE LOWER(name) = LOWER(:name)
            """), {'name': ingredient_name})
            
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Ingredient '{ingredient_name}' not found")
            
            ingredient_id = row[0]
            
            goals_result = conn.execute(text("""
                SELECT g.name FROM goals g
                JOIN ingredient_logic il ON g.id = il.goal_id
                WHERE il.ingredient_id = :id
            """), {'id': ingredient_id})
            goals = [r[0] for r in goals_result]
            
            synergies = engine._get_synergies(ingredient_id)
            product = engine._get_supliful_product(ingredient_id)
            
            risks_result = conn.execute(text("""
                SELECT keyword, severity FROM ingredient_risks
                WHERE ingredient_id = :id AND risk_type = 'Medication'
            """), {'id': ingredient_id})
            drug_interactions = [{'medication': r[0], 'severity': r[1]} for r in risks_result]
            
            return {
                "id": row[0], "name": row[1], "category": row[2],
                "evidence": {"grade": row[3], "strength_score": float(row[4]) if row[4] else None,
                           "num_rcts": row[18], "total_participants": row[19], "references": row[17]},
                "dosing": {"practical_dose": row[5], "timing": row[6]},
                "gender_relevance": {"male_score": float(row[7]) if row[7] else None,
                                    "female_score": float(row[8]) if row[8] else None,
                                    "male_notes": row[9], "female_notes": row[10]},
                "biomarkers": {"primary": row[11], "secondary": row[12], "mechanisms": row[13]},
                "safety": {"contraindications": row[14], "side_effects_common": row[15],
                          "side_effects_rare": row[16], "drug_interactions": drug_interactions},
                "goals": goals, "synergies": synergies, "supliful_product": product
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/products")
async def get_all_products():
    try:
        with engine.engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("""
                SELECT fc.id, fc.product_name, fc.product_url, fc.base_price, fc.serving_info,
                       fc.short_description, i.name as ingredient_name, i.evidence_grade
                FROM fulfillment_catalog fc
                LEFT JOIN ingredients i ON fc.ingredient_id = i.id
                WHERE fc.supplier_name = 'Supliful' AND fc.is_active = TRUE
                ORDER BY i.name NULLS LAST, fc.product_name
            """))
            
            products = [{
                "id": row[0], "name": row[1], "url": row[2],
                "base_price": float(row[3]) if row[3] else None,
                "serving_info": row[4], "description": row[5],
                "linked_ingredient": row[6], "evidence_grade": row[7]
            } for row in result]
            
            return {"count": len(products), "products": products}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 50)
    print("GenoMAX2 API Server")
    print("=" * 50)
    print("\nStarting server at http://localhost:8000")
    print("Swagger UI at http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
