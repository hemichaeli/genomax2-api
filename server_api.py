"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import asyncpg

# ============================================
# App Configuration
# ============================================
app = FastAPI(
    title="GenoMAX² API",
    description="Gender-Optimized Biological Operating System",
    version="3.2.0"
)

# ============================================
# CORS Configuration - Allow all origins for brain UI
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ============================================
# Include Brain Router
# ============================================
try:
    from app.brain import brain_router
    app.include_router(brain_router)
    print("✅ Brain router loaded successfully")
except ImportError as e:
    print(f"⚠️ Brain router not available: {e}")

# ============================================
# Database Connection
# ============================================
DATABASE_URL = os.getenv("DATABASE_URL")

async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# ============================================
# Pydantic Models
# ============================================
class IntakeCreate(BaseModel):
    gender_os: str  # 'maximo' or 'maxima'

class AssessmentUpdate(BaseModel):
    demographics: Optional[dict] = None
    lifestyle: Optional[dict] = None
    goals: Optional[dict] = None
    medical: Optional[dict] = None

class RecommendRequest(BaseModel):
    gender: str
    goals: List[str]
    medications: Optional[List[str]] = []
    conditions: Optional[List[str]] = []

class InteractionCheckRequest(BaseModel):
    medications: List[str]
    supplements: List[str]

# ============================================
# Health & Version Endpoints
# ============================================
@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/version")
async def version():
    return {
        "version": "3.2.0",
        "engine_version": "2.0",
        "logic_version": "1.5",
        "brain_version": "1.2.0"
    }

# ============================================
# Goals Endpoint
# ============================================
@app.get("/goals")
async def get_goals():
    try:
        conn = await get_db()
        rows = await conn.fetch("""
            SELECT id, name, category, description 
            FROM health_goals 
            ORDER BY category, name
        """)
        await conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        # Return sample data if DB fails
        return [
            {"id": "1", "name": "Sleep Optimization", "category": "Recovery", "description": "Improve sleep quality"},
            {"id": "2", "name": "Energy & Vitality", "category": "Performance", "description": "Boost daily energy"},
            {"id": "3", "name": "Stress & Mood", "category": "Mental", "description": "Reduce stress and improve mood"},
            {"id": "4", "name": "Muscle Building", "category": "Fitness", "description": "Support muscle growth"},
            {"id": "5", "name": "Fat Loss", "category": "Body Composition", "description": "Support healthy weight"},
            {"id": "6", "name": "Cognitive Function", "category": "Mental", "description": "Enhance focus and memory"},
            {"id": "7", "name": "Heart Health", "category": "Longevity", "description": "Cardiovascular support"},
            {"id": "8", "name": "Immune Support", "category": "Health", "description": "Strengthen immune system"},
            {"id": "9", "name": "Joint Health", "category": "Recovery", "description": "Support joint function"},
            {"id": "10", "name": "Hormone Balance", "category": "Optimization", "description": "Support hormonal health"},
        ]

# ============================================
# Modules Endpoint
# ============================================
@app.get("/modules")
async def get_modules():
    try:
        conn = await get_db()
        rows = await conn.fetch("SELECT * FROM os_modules ORDER BY name")
        await conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        return []

# ============================================
# Ingredients Endpoint
# ============================================
@app.get("/ingredients")
async def get_ingredients():
    try:
        conn = await get_db()
        rows = await conn.fetch("SELECT * FROM ingredients ORDER BY name")
        await conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        return []

@app.get("/ingredient/{name}")
async def get_ingredient(name: str):
    try:
        conn = await get_db()
        row = await conn.fetchrow(
            "SELECT * FROM ingredients WHERE LOWER(name) = LOWER($1)", 
            name
        )
        await conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Ingredient not found")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# Products Endpoint
# ============================================
@app.get("/products")
async def get_products():
    try:
        conn = await get_db()
        rows = await conn.fetch("SELECT * FROM supliful_products ORDER BY name")
        await conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        return []

# ============================================
# Intakes Endpoints (for Frontend Wizard)
# ============================================
import uuid
from datetime import datetime

# In-memory storage for intakes (use DB in production)
intakes_store = {}

@app.post("/intakes")
async def create_intake(intake: IntakeCreate):
    intake_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"
    
    intakes_store[intake_id] = {
        "id": intake_id,
        "gender_os": intake.gender_os,
        "status": "draft",
        "assessment": {},
        "created_at": now,
        "updated_at": now,
    }
    
    return {
        "id": intake_id,
        "gender_os": intake.gender_os,
        "status": "draft"
    }

@app.get("/intakes/{intake_id}")
async def get_intake(intake_id: str):
    if intake_id not in intakes_store:
        raise HTTPException(status_code=404, detail="Intake not found")
    return intakes_store[intake_id]

@app.patch("/intakes/{intake_id}/assessment")
async def update_assessment(intake_id: str, assessment: AssessmentUpdate):
    if intake_id not in intakes_store:
        raise HTTPException(status_code=404, detail="Intake not found")
    
    intake = intakes_store[intake_id]
    
    # Merge assessment data
    if assessment.demographics:
        intake["assessment"]["demographics"] = assessment.demographics
    if assessment.lifestyle:
        intake["assessment"]["lifestyle"] = assessment.lifestyle
    if assessment.goals:
        intake["assessment"]["goals"] = assessment.goals
    if assessment.medical:
        intake["assessment"]["medical"] = assessment.medical
    
    intake["updated_at"] = datetime.utcnow().isoformat() + "Z"
    
    return intake

@app.post("/intakes/{intake_id}/process")
async def process_intake(intake_id: str):
    if intake_id not in intakes_store:
        raise HTTPException(status_code=404, detail="Intake not found")
    
    intake = intakes_store[intake_id]
    assessment = intake.get("assessment", {})
    gender_os = intake.get("gender_os", "maximo")
    
    # Extract data from assessment
    goals = assessment.get("goals", {}).get("primary_goals", [])
    medications = assessment.get("medical", {}).get("medications", [])
    conditions = assessment.get("medical", {}).get("conditions", [])
    
    # Generate recommendations (simplified version)
    primary_recommendations = []
    
    # Sample recommendations based on goals
    goal_ingredients = {
        "1": {"name": "Magnesium Glycinate", "dosage": "400mg", "timing": "Before bed", "rationale": "Supports sleep quality and relaxation"},
        "2": {"name": "B-Complex", "dosage": "1 capsule", "timing": "Morning", "rationale": "Supports energy metabolism"},
        "3": {"name": "Ashwagandha", "dosage": "600mg", "timing": "Evening", "rationale": "Adaptogen for stress management"},
        "4": {"name": "Creatine Monohydrate", "dosage": "5g", "timing": "Post-workout", "rationale": "Supports muscle growth and strength"},
        "5": {"name": "Green Tea Extract", "dosage": "500mg", "timing": "Morning", "rationale": "Supports metabolism"},
        "6": {"name": "Lion's Mane", "dosage": "1000mg", "timing": "Morning", "rationale": "Supports cognitive function"},
        "7": {"name": "Omega-3 Fish Oil", "dosage": "2000mg", "timing": "With meals", "rationale": "Supports cardiovascular health"},
        "8": {"name": "Vitamin D3", "dosage": "5000 IU", "timing": "Morning", "rationale": "Supports immune function"},
        "9": {"name": "Glucosamine", "dosage": "1500mg", "timing": "With meals", "rationale": "Supports joint health"},
        "10": {"name": "Zinc", "dosage": "30mg", "timing": "Evening", "rationale": "Supports hormone production"},
    }
    
    for i, goal_id in enumerate(goals[:5]):
        if goal_id in goal_ingredients:
            ing = goal_ingredients[goal_id]
            primary_recommendations.append({
                "ingredient_id": goal_id,
                "name": ing["name"],
                "dosage": ing["dosage"],
                "timing": ing["timing"],
                "evidence_grade": "A" if i < 2 else "B",
                "relevance_score": 95 - (i * 5),
                "rationale": ing["rationale"]
            })
    
    # Add gender-specific recommendation
    if gender_os == "maxima":
        primary_recommendations.append({
            "ingredient_id": "female_1",
            "name": "Iron Bisglycinate",
            "dosage": "18mg",
            "timing": "Morning with vitamin C",
            "evidence_grade": "A",
            "relevance_score": 90,
            "rationale": "Supports healthy iron levels in women"
        })
    else:
        primary_recommendations.append({
            "ingredient_id": "male_1",
            "name": "Tongkat Ali",
            "dosage": "400mg",
            "timing": "Morning",
            "evidence_grade": "B",
            "relevance_score": 85,
            "rationale": "Supports healthy testosterone levels in men"
        })
    
    # Check for warnings
    warnings = []
    if medications and len(medications) > 0:
        warnings.append({
            "type": "interaction",
            "severity": "caution",
            "message": "Please consult with your healthcare provider about potential interactions with your current medications.",
            "affected_items": medications
        })
    
    # Build result
    result = {
        "metadata": {
            "result_id": str(uuid.uuid4()),
            "engine_version": "2.0",
            "logic_version": "1.5",
            "generated_at": datetime.utcnow().isoformat() + "Z"
        },
        "primary": primary_recommendations,
        "secondary": [
            {
                "ingredient_id": "sec_1",
                "name": "Vitamin K2",
                "dosage": "100mcg",
                "timing": "With Vitamin D",
                "evidence_grade": "B",
                "relevance_score": 70,
                "rationale": "Enhances vitamin D absorption and calcium metabolism"
            }
        ],
        "products": [],
        "warnings": warnings,
        "goal_conflict": False,
        "goal_conflict_explanation": None
    }
    
    # Update intake status
    intake["status"] = "completed"
    intake["updated_at"] = datetime.utcnow().isoformat() + "Z"
    
    return result

# ============================================
# Legacy Recommend Endpoint
# ============================================
@app.post("/recommend")
async def recommend(request: RecommendRequest):
    # Create temporary intake and process
    intake_id = str(uuid.uuid4())
    gender_os = "maxima" if request.gender.lower() == "female" else "maximo"
    
    # Map goal names to IDs (simplified)
    goal_map = {
        "sleep optimization": "1",
        "energy & vitality": "2",
        "stress & mood": "3",
        "muscle building": "4",
        "fat loss": "5",
        "cognitive function": "6",
        "heart health": "7",
        "immune support": "8",
        "joint health": "9",
        "hormone balance": "10",
    }
    
    goal_ids = []
    for goal in request.goals:
        goal_lower = goal.lower()
        if goal_lower in goal_map:
            goal_ids.append(goal_map[goal_lower])
        else:
            goal_ids.append(goal)
    
    intakes_store[intake_id] = {
        "id": intake_id,
        "gender_os": gender_os,
        "status": "draft",
        "assessment": {
            "goals": {"primary_goals": goal_ids},
            "medical": {
                "medications": request.medications or [],
                "conditions": request.conditions or []
            }
        },
        "created_at": datetime.utcnow().isoformat() + "Z",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
    
    return await process_intake(intake_id)

# ============================================
# Interaction Check Endpoint
# ============================================
@app.post("/check-interactions")
async def check_interactions(request: InteractionCheckRequest):
    interactions = []
    
    # Sample interaction data
    known_interactions = {
        "warfarin": ["vitamin k", "omega-3", "ginkgo"],
        "blood thinners": ["vitamin e", "fish oil", "garlic"],
        "antidepressants": ["st john's wort", "5-htp", "sam-e"],
        "thyroid medication": ["calcium", "iron", "magnesium"],
    }
    
    for med in request.medications:
        med_lower = med.lower()
        for drug, interacting_supps in known_interactions.items():
            if drug in med_lower:
                for supp in request.supplements:
                    if supp.lower() in interacting_supps:
                        interactions.append({
                            "medication": med,
                            "supplement": supp,
                            "severity": "moderate",
                            "description": f"{supp} may interact with {med}. Consult your healthcare provider."
                        })
    
    return {
        "has_interactions": len(interactions) > 0,
        "interactions": interactions
    }

# ============================================
# Run Server
# ============================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
