"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
from datetime import datetime
from app.brain import brain_router

# ============================================
# App Configuration
# ============================================
app = FastAPI(
    title="GenoMAX² API",
    description="Gender-Optimized Biological Operating System",
    version="3.2.0"
)

# ============================================
# CORS Configuration
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://genomax2-frontend.vercel.app",
        "https://genomax2-frontend-git-main-hemis-projects-6782105b.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ============================================
# Brain API Router
# ============================================
app.include_router(brain_router)

# ============================================
# Database Connection
# ============================================
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# ============================================
# Pydantic Models
# ============================================
class IntakeCreate(BaseModel):
    gender_os: str

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
# In-memory storage for intakes
# ============================================
intakes_store = {}

# ============================================
# Health & Version Endpoints
# ============================================
@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/migrate-brain")
def migrate_brain():
    conn = get_db()
    if not conn:
        return {"error": "Database connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brain_runs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID,
                status VARCHAR(20) DEFAULT 'running',
                input_hash VARCHAR(64),
                output_hash VARCHAR(64),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS signal_registry (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL,
                signal_type VARCHAR(50) NOT NULL,
                signal_hash VARCHAR(64) NOT NULL,
                signal_json JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, signal_type, signal_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_signal_user ON signal_registry(user_id);
            CREATE INDEX IF NOT EXISTS idx_brain_runs_user ON brain_runs(user_id);
        """)
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "success", "message": "Brain tables created"}
    except Exception as e:
        conn.close()
        return {"error": str(e)}

@app.get("/version")
def version():
    return {
        "version": "3.2.0",
        "engine_version": "2.0",
        "logic_version": "1.5",
        "brain_version": "1.0.0"
    }

# ============================================
# Goals Endpoint
# ============================================
@app.get("/goals")
def get_goals():
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id::text, name, category, description
                FROM health_goals
                ORDER BY category, name
            """)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error fetching goals: {e}")
            if conn:
                conn.close()

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
def get_modules():
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM os_modules ORDER BY name")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error fetching modules: {e}")
            if conn:
                conn.close()
    return []

# ============================================
# Ingredients Endpoint
# ============================================
@app.get("/ingredients")
def get_ingredients():
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM ingredients ORDER BY name")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error fetching ingredients: {e}")
            if conn:
                conn.close()
    return []

@app.get("/ingredient/{name}")
def get_ingredient(name: str):
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM ingredients WHERE LOWER(name) = LOWER(%s)",
                (name,)
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            if not row:
                raise HTTPException(status_code=404, detail="Ingredient not found")
            return dict(row)
        except HTTPException:
            raise
        except Exception as e:
            if conn:
                conn.close()
            raise HTTPException(status_code=500, detail=str(e))
    raise HTTPException(status_code=500, detail="Database connection failed")

# ============================================
# Products Endpoint
# ============================================
@app.get("/products")
def get_products():
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM supliful_products ORDER BY name")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error fetching products: {e}")
            if conn:
                conn.close()
    return []

# ============================================
# Intakes Endpoints
# ============================================
@app.post("/intakes")
def create_intake(intake: IntakeCreate):
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
def get_intake(intake_id: str):
    if intake_id not in intakes_store:
        raise HTTPException(status_code=404, detail="Intake not found")
    return intakes_store[intake_id]

@app.patch("/intakes/{intake_id}/assessment")
def update_assessment(intake_id: str, assessment: AssessmentUpdate):
    if intake_id not in intakes_store:
        raise HTTPException(status_code=404, detail="Intake not found")

    intake = intakes_store[intake_id]

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
def process_intake(intake_id: str):
    if intake_id not in intakes_store:
        raise HTTPException(status_code=404, detail="Intake not found")

    intake = intakes_store[intake_id]
    assessment = intake.get("assessment", {})
    gender_os = intake.get("gender_os", "maximo")

    goals = assessment.get("goals", {}).get("primary_goals", [])
    medications = assessment.get("medical", {}).get("medications", [])

    primary_recommendations = []

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

    warnings = []
    if medications and len(medications) > 0:
        warnings.append({
            "type": "interaction",
            "severity": "caution",
            "message": "Please consult with your healthcare provider about potential interactions with your current medications.",
            "affected_items": medications
        })

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

    intake["status"] = "completed"
    intake["updated_at"] = datetime.utcnow().isoformat() + "Z"

    return result

# ============================================
# Legacy Recommend Endpoint
# ============================================
@app.post("/recommend")
def recommend(request: RecommendRequest):
    intake_id = str(uuid.uuid4())
    gender_os = "maxima" if request.gender.lower() == "female" else "maximo"

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

    return process_intake(intake_id)

# ============================================
# Interaction Check Endpoint
# ============================================
@app.post("/check-interactions")
def check_interactions(request: InteractionCheckRequest):
    interactions = []

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