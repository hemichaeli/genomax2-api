#!/usr/bin/env python3
"""
GenoMAX2 API Server v2.2.0
==========================
Added: API Key authentication for /intakes* endpoints
"""

import os
import uuid
import re
from datetime import datetime
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum
from sqlalchemy import text

from genomax_engine import GenoMAXEngine, UserProfile, Gender

# API Key from environment
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

app = FastAPI(
    title="GenoMAX2 API",
    description="Gender-Optimized Biological Operating System - Recommendation Engine",
    version="2.2.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = GenoMAXEngine()


# ============================================================================
# API KEY AUTHENTICATION
# ============================================================================

async def verify_admin_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """
    Verify API key for admin endpoints.
    If ADMIN_API_KEY is not set in environment, allow all requests (dev mode).
    If set, require matching X-API-Key header.
    """
    # If no API key configured, allow all (dev mode)
    if not ADMIN_API_KEY:
        return True
    
    # API key is configured - require it
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing X-API-Key header. Admin endpoints require authentication."
        )
    
    if x_api_key != ADMIN_API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    
    return True


# ============================================================================
# ENUMS
# ============================================================================

class GenderEnum(str, Enum):
    male = "male"
    female = "female"

class OSEnum(str, Enum):
    MAXimo2 = "MAXimo2"
    MAXima2 = "MAXima2"

class IntakeStatusEnum(str, Enum):
    new = "new"
    in_review = "in_review"
    completed = "completed"


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class RecommendationRequest(BaseModel):
    gender: Optional[GenderEnum] = None
    os: Optional[OSEnum] = None
    goals: List[str] = Field(..., min_items=1, max_items=5)
    medications: List[str] = Field(default=[])
    conditions: List[str] = Field(default=[])
    age: Optional[int] = Field(default=None, ge=18, le=100)
    exclude_ingredients: List[str] = Field(default=[])

class DrugCheckRequest(BaseModel):
    ingredient_name: str
    medications: List[str]

class DrugCheckBatchRequest(BaseModel):
    ingredients: List[str] = Field(..., min_items=1)
    medications: List[str] = Field(default=[])

class IntakeCreateRequest(BaseModel):
    os: OSEnum
    goals: List[str] = Field(..., min_items=1, max_items=5)
    medications: List[str] = Field(default=[])
    conditions: List[str] = Field(default=[])

class IntakeUpdateStatusRequest(BaseModel):
    status: IntakeStatusEnum


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def os_to_gender(os_value: OSEnum) -> Gender:
    """Map OS to internal gender for recommendation logic"""
    mapping = {
        OSEnum.MAXimo2: Gender.MALE,
        OSEnum.MAXima2: Gender.FEMALE
    }
    return mapping[os_value]

def to_slug(name: str) -> str:
    """Convert name to kebab-case slug"""
    slug = name.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')

def ensure_intakes_table():
    """Create intakes table if not exists"""
    with engine.engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS intakes (
                id VARCHAR(50) PRIMARY KEY,
                os VARCHAR(20) NOT NULL,
                goals JSONB NOT NULL,
                medications JSONB DEFAULT '[]',
                conditions JSONB DEFAULT '[]',
                status VARCHAR(20) DEFAULT 'new',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.commit()


# Initialize intakes table on startup
@app.on_event("startup")
async def startup_event():
    ensure_intakes_table()


# ============================================================================
# BASIC ENDPOINTS (No auth required)
# ============================================================================

@app.get("/")
async def root():
    return {
        "service": "GenoMAX2 Recommendation Engine",
        "version": "2.2.0",
        "status": "operational"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "2.2.0",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

@app.get("/version")
async def get_version():
    return {
        "version": "2.2.0",
        "api": "GenoMAX2",
        "features": ["intakes", "os_support", "goals_ids", "modules_ids", "batch_interactions", "admin_auth"]
    }


# ============================================================================
# GOALS & MODULES (No auth required)
# ============================================================================

@app.get("/goals")
async def get_goals():
    """Return goals with IDs, slugs, and legacy format"""
    with engine.engine.connect() as conn:
        result = conn.execute(text("SELECT id, name FROM goals ORDER BY name"))
        rows = list(result)
        
        goals_objects = [
            {
                "id": row[0],
                "name": row[1],
                "slug": to_slug(row[1])
            }
            for row in rows
        ]
        goals_legacy = [row[1] for row in rows]
        
        return {
            "count": len(goals_objects),
            "goals": goals_objects,
            "goals_legacy": goals_legacy
        }

@app.get("/modules")
async def get_modules():
    """Return modules with IDs, slugs, and legacy format"""
    with engine.engine.connect() as conn:
        result = conn.execute(text("SELECT id, name FROM os_modules ORDER BY name"))
        rows = list(result)
        
        modules_objects = [
            {
                "id": row[0],
                "name": row[1],
                "slug": to_slug(row[1])
            }
            for row in rows
        ]
        modules_legacy = [row[1] for row in rows]
        
        return {
            "count": len(modules_objects),
            "modules": modules_objects,
            "modules_legacy": modules_legacy
        }


# ============================================================================
# INTAKES CRUD (Admin auth required)
# ============================================================================

@app.post("/intakes", dependencies=[Depends(verify_admin_api_key)])
async def create_intake(request: IntakeCreateRequest):
    """Create a new intake (requires X-API-Key header)"""
    import json
    
    intake_id = f"int_{uuid.uuid4().hex[:12]}"
    created_at = datetime.utcnow()
    
    with engine.engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO intakes (id, os, goals, medications, conditions, status, created_at)
            VALUES (:id, :os, :goals, :medications, :conditions, 'new', :created_at)
        """), {
            'id': intake_id,
            'os': request.os.value,
            'goals': json.dumps(request.goals),
            'medications': json.dumps(request.medications),
            'conditions': json.dumps(request.conditions),
            'created_at': created_at
        })
        conn.commit()
    
    return {
        "intake_id": intake_id,
        "created_at": created_at.isoformat() + "Z"
    }

@app.get("/intakes", dependencies=[Depends(verify_admin_api_key)])
async def list_intakes(status: Optional[IntakeStatusEnum] = None, limit: int = 50, offset: int = 0):
    """List intakes for queue view (requires X-API-Key header)"""
    import json
    
    with engine.engine.connect() as conn:
        # Build query
        if status:
            query = text("""
                SELECT id, os, goals, medications, conditions, status, created_at
                FROM intakes
                WHERE status = :status
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """)
            params = {'status': status.value, 'limit': limit, 'offset': offset}
            
            count_query = text("SELECT COUNT(*) FROM intakes WHERE status = :status")
            count_params = {'status': status.value}
        else:
            query = text("""
                SELECT id, os, goals, medications, conditions, status, created_at
                FROM intakes
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """)
            params = {'limit': limit, 'offset': offset}
            
            count_query = text("SELECT COUNT(*) FROM intakes")
            count_params = {}
        
        result = conn.execute(query, params)
        rows = list(result)
        
        total_result = conn.execute(count_query, count_params)
        total = total_result.fetchone()[0]
        
        items = []
        for row in rows:
            goals = json.loads(row[2]) if isinstance(row[2], str) else row[2]
            medications = json.loads(row[3]) if isinstance(row[3], str) else row[3]
            conditions = json.loads(row[4]) if isinstance(row[4], str) else row[4]
            
            items.append({
                "intake_id": row[0],
                "created_at": row[6].isoformat() + "Z" if row[6] else None,
                "os": row[1],
                "goals_count": len(goals) if goals else 0,
                "has_medications": bool(medications and len(medications) > 0),
                "has_conditions": bool(conditions and len(conditions) > 0),
                "status": row[5]
            })
        
        return {
            "items": items,
            "total": total
        }

@app.get("/intakes/{intake_id}", dependencies=[Depends(verify_admin_api_key)])
async def get_intake(intake_id: str):
    """Get full intake details (requires X-API-Key header)"""
    import json
    
    with engine.engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, os, goals, medications, conditions, status, created_at
            FROM intakes
            WHERE id = :id
        """), {'id': intake_id})
        
        row = result.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Intake '{intake_id}' not found")
        
        goals = json.loads(row[2]) if isinstance(row[2], str) else row[2]
        medications = json.loads(row[3]) if isinstance(row[3], str) else row[3]
        conditions = json.loads(row[4]) if isinstance(row[4], str) else row[4]
        
        return {
            "intake_id": row[0],
            "created_at": row[6].isoformat() + "Z" if row[6] else None,
            "os": row[1],
            "goals": goals or [],
            "medications": medications or [],
            "conditions": conditions or [],
            "status": row[5]
        }

@app.patch("/intakes/{intake_id}/status", dependencies=[Depends(verify_admin_api_key)])
async def update_intake_status(intake_id: str, request: IntakeUpdateStatusRequest):
    """Update intake status (requires X-API-Key header)"""
    with engine.engine.connect() as conn:
        # Check exists
        check = conn.execute(text("SELECT id FROM intakes WHERE id = :id"), {'id': intake_id})
        if not check.fetchone():
            raise HTTPException(status_code=404, detail=f"Intake '{intake_id}' not found")
        
        conn.execute(text("""
            UPDATE intakes SET status = :status WHERE id = :id
        """), {'id': intake_id, 'status': request.status.value})
        conn.commit()
    
    return {"intake_id": intake_id, "status": request.status.value}


# ============================================================================
# RECOMMEND (No auth required)
# ============================================================================

@app.post("/recommend")
async def get_recommendations(request: RecommendationRequest):
    """
    Get personalized recommendations.
    Accepts either 'gender' or 'os' parameter.
    If both provided, 'os' takes precedence.
    """
    try:
        # Determine gender from os or gender parameter
        if request.os:
            gender = os_to_gender(request.os)
        elif request.gender:
            gender = Gender.MALE if request.gender == GenderEnum.male else Gender.FEMALE
        else:
            raise HTTPException(
                status_code=400,
                detail="Either 'gender' or 'os' must be provided"
            )
        
        profile = UserProfile(
            gender=gender,
            goals=request.goals,
            medications=request.medications,
            conditions=request.conditions,
            age=request.age,
            exclude_ingredients=request.exclude_ingredients
        )
        recommendation = engine.generate_recommendations(profile)
        result = engine.to_dict(recommendation)
        
        # Add os to response if provided
        if request.os:
            result['user_profile']['os'] = request.os.value
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# INTERACTIONS (No auth required)
# ============================================================================

@app.post("/check-interactions")
async def check_drug_interactions(request: DrugCheckRequest):
    """Check drug interactions for a single ingredient"""
    try:
        with engine.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT id FROM ingredients WHERE LOWER(name) = LOWER(:name)"
            ), {'name': request.ingredient_name})
            row = result.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ingredient '{request.ingredient_name}' not found"
                )
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


@app.post("/check-interactions-batch")
async def check_drug_interactions_batch(request: DrugCheckBatchRequest):
    """Check drug interactions for multiple ingredients at once"""
    try:
        results = []
        
        for ingredient_name in request.ingredients:
            # Look up ingredient
            with engine.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT id FROM ingredients WHERE LOWER(name) = LOWER(:name)"
                ), {'name': ingredient_name})
                row = result.fetchone()
            
            if not row:
                # Ingredient not found - mark as safe with note
                results.append({
                    "ingredient": ingredient_name,
                    "medications_checked": request.medications,
                    "interactions_found": 0,
                    "interactions": [],
                    "safe": True,
                    "note": "Ingredient not found in database"
                })
                continue
            
            ingredient_id = row[0]
            
            # If no medications, all are safe
            if not request.medications:
                results.append({
                    "ingredient": ingredient_name,
                    "medications_checked": [],
                    "interactions_found": 0,
                    "interactions": [],
                    "safe": True
                })
                continue
            
            # Check interactions using existing logic
            warnings = engine._check_drug_interactions(ingredient_id, request.medications)
            results.append({
                "ingredient": ingredient_name,
                "medications_checked": request.medications,
                "interactions_found": len(warnings),
                "interactions": warnings,
                "safe": len(warnings) == 0
            })
        
        # Calculate totals
        total_checked = len(results)
        total_safe = sum(1 for r in results if r["safe"])
        total_warnings = total_checked - total_safe
        
        return {
            "results": results,
            "total_checked": total_checked,
            "total_safe": total_safe,
            "total_warnings": total_warnings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# INGREDIENTS (No auth required)
# ============================================================================

@app.get("/ingredient/{ingredient_name}")
async def get_ingredient_details(ingredient_name: str):
    """Get detailed information about an ingredient"""
    try:
        with engine.engine.connect() as conn:
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
                raise HTTPException(
                    status_code=404,
                    detail=f"Ingredient '{ingredient_name}' not found"
                )
            
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
                "id": row[0],
                "name": row[1],
                "category": row[2],
                "evidence": {
                    "grade": row[3],
                    "strength_score": float(row[4]) if row[4] else None,
                    "num_rcts": row[18],
                    "total_participants": row[19],
                    "references": row[17]
                },
                "dosing": {
                    "practical_dose": row[5],
                    "timing": row[6]
                },
                "gender_relevance": {
                    "male_score": float(row[7]) if row[7] else None,
                    "female_score": float(row[8]) if row[8] else None,
                    "male_notes": row[9],
                    "female_notes": row[10]
                },
                "biomarkers": {
                    "primary": row[11],
                    "secondary": row[12],
                    "mechanisms": row[13]
                },
                "safety": {
                    "contraindications": row[14],
                    "side_effects_common": row[15],
                    "side_effects_rare": row[16],
                    "drug_interactions": drug_interactions
                },
                "goals": goals,
                "synergies": synergies,
                "supliful_product": product
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# PRODUCTS (No auth required)
# ============================================================================

@app.get("/products")
async def get_all_products():
    """Get all Supliful products"""
    try:
        with engine.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT fc.id, fc.product_name, fc.product_url, fc.base_price, fc.serving_info,
                       fc.short_description, i.name as ingredient_name, i.evidence_grade
                FROM fulfillment_catalog fc
                LEFT JOIN ingredients i ON fc.ingredient_id = i.id
                WHERE fc.supplier_name = 'Supliful' AND fc.is_active = TRUE
                ORDER BY i.name NULLS LAST, fc.product_name
            """))
            
            products = [{
                "id": row[0],
                "name": row[1],
                "url": row[2],
                "base_price": float(row[3]) if row[3] else None,
                "serving_info": row[4],
                "description": row[5],
                "linked_ingredient": row[6],
                "evidence_grade": row[7]
            } for row in result]
            
            return {"count": len(products), "products": products}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    print("\n" + "=" * 50)
    print("GenoMAX2 API Server v2.2.0")
    print("=" * 50)
    print("\nNew in v2.2.0:")
    print("  - API Key auth for /intakes* endpoints")
    print(f"\nADMIN_API_KEY: {'SET' if ADMIN_API_KEY else 'NOT SET (dev mode - no auth)'}")
    print("\nStarting server at http://localhost:8000")
    print("Swagger UI at http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
