#!/usr/bin/env python3
"""
GenoMAX² API Server v3.0
Complete Recommendation Engine with Intake Lifecycle

Deploy: Railway
Database: PostgreSQL
Framework: FastAPI

Features:
- Unified Intake lifecycle (Assessment + Bloodwork)
- Full scoring algorithm (5 components)
- Explainability layer
- Confidence levels
- OS Lock
- Admin auth
"""

import os
import uuid
import json
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum

from fastapi import FastAPI, HTTPException, Depends, Header, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text, Column, Integer, String, Numeric, Text, DateTime, Boolean, Date, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import func

# =============================================================================
# CONFIGURATION
# =============================================================================

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/genomax2")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
API_VERSION = "3.0.2"
ALGORITHM_VERSION = "2.0"

# Fix for Railway PostgreSQL URL
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# =============================================================================
# DATABASE SETUP
# =============================================================================

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =============================================================================
# ENUMS
# =============================================================================

class OSType(str, Enum):
    MAXIMO = "MAXIMO"
    MAXIMA = "MAXIMA"

class IntakeStatus(str, Enum):
    DRAFT = "draft"
    AWAITING_ASSESSMENT = "awaiting_assessment"
    AWAITING_BLOODWORK = "awaiting_bloodwork"
    READY_TO_PROCESS = "ready_to_process"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    ARCHIVED = "archived"

class ConfidenceLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

# =============================================================================
# PYDANTIC SCHEMAS
# =============================================================================

# --- Common ---
class ErrorResponse(BaseModel):
    error: str
    message: str
    details: Optional[dict] = None

# --- Intake ---
class IntakeCreateRequest(BaseModel):
    os_type: Optional[OSType] = None
    source: Optional[str] = "unknown"

class IntakeResponse(BaseModel):
    intake_id: str
    status: str
    os_type: Optional[str] = None
    os_locked: bool = False
    has_assessment: bool = False
    has_bloodwork: bool = False
    created_at: datetime
    updated_at: datetime

class IntakeSnapshotResponse(IntakeResponse):
    assessment: Optional[dict] = None
    bloodwork: Optional[dict] = None
    outputs: Optional[dict] = None
    confidence: Optional[dict] = None

# --- Assessment ---
class Demographics(BaseModel):
    age_years: int = Field(..., ge=13, le=120)
    weight_kg: float = Field(..., ge=25, le=300)
    height_cm: Optional[float] = Field(None, ge=120, le=230)
    biological_sex: Optional[str] = None

class Lifestyle(BaseModel):
    activity_level: str
    nutrition_pattern: str
    sleep_hours_avg: Optional[float] = Field(None, ge=0, le=16)
    smoking: Optional[str] = None
    alcohol: Optional[str] = None

class UserItem(BaseModel):
    name: str
    code: Optional[str] = None
    notes: Optional[str] = None

class GoalItem(BaseModel):
    key: str
    severity: Optional[int] = Field(None, ge=1, le=5)
    notes: Optional[str] = None

class PainPointItem(BaseModel):
    key: str
    severity: Optional[int] = Field(None, ge=1, le=5)
    frequency: Optional[str] = None
    notes: Optional[str] = None

class Constraints(BaseModel):
    allergies: Optional[List[str]] = []
    dislikes: Optional[List[str]] = []
    budget_sensitivity: Optional[str] = None

class ExecutionPreferences(BaseModel):
    intakes_per_day: Optional[int] = Field(None, ge=1, le=4)
    preferred_times: Optional[dict] = None
    enable_walk: Optional[bool] = None
    walk_time: Optional[str] = None
    enable_workout: Optional[bool] = None
    workout_time: Optional[str] = None

class AssessmentPayload(BaseModel):
    os_type: Optional[OSType] = None
    demographics: Demographics
    lifestyle: Lifestyle
    goals: List[GoalItem] = Field(..., min_length=1)
    pain_points: List[PainPointItem] = Field(default=[])
    medications: Optional[List[UserItem]] = []
    conditions: Optional[List[UserItem]] = []
    current_supplements: Optional[List[UserItem]] = []
    constraints: Optional[Constraints] = None
    execution_preferences: Optional[ExecutionPreferences] = None
    meta: Optional[dict] = None

# --- Bloodwork ---
class ReferenceRange(BaseModel):
    low: Optional[float] = None
    high: Optional[float] = None

class BiomarkerReading(BaseModel):
    code: str
    name: str
    value: float
    unit: str
    reference_range: Optional[ReferenceRange] = None
    flagged: Optional[str] = "unknown"

class BloodworkPayload(BaseModel):
    collected_date: str  # ISO date
    lab_name: Optional[str] = None
    country: Optional[str] = None
    parsed_biomarkers: List[BiomarkerReading] = Field(..., min_length=1)
    notes: Optional[str] = None

# --- Processing ---
class ProcessRequest(BaseModel):
    force: Optional[bool] = False
    async_mode: Optional[bool] = False

class ProcessResultResponse(BaseModel):
    intake_id: str
    status: str
    confidence: dict
    primary_stack: List[dict]
    secondary_stack: List[dict]
    excluded_ingredients: List[dict]
    summary: dict

# --- Legacy Recommend ---
class RecommendRequest(BaseModel):
    os: str = Field(..., description="MAXimo2 or MAXima2")
    goals: List[str] = Field(..., min_length=1)
    medications: Optional[List[str]] = []
    conditions: Optional[List[str]] = []

# --- Admin ---
class IntakeListResponse(BaseModel):
    items: List[IntakeResponse]
    total: int
    limit: int
    offset: int

# =============================================================================
# SCORING CONSTANTS
# =============================================================================

WEIGHT_EVIDENCE = 0.35
WEIGHT_GOAL_MATCH = 0.30
WEIGHT_GENDER = 0.20
WEIGHT_TIER = 0.10
WEIGHT_SAFETY = 0.05

PRIMARY_STACK_THRESHOLD = 0.65
SECONDARY_STACK_THRESHOLD = 0.45
MAX_PRIMARY_STACK = 8
MAX_SECONDARY_STACK = 10

PRIMARY_GRADES = ['A', 'A-', 'B+', 'B']

# =============================================================================
# AUTH
# =============================================================================

def verify_admin_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """Verify API key for admin endpoints."""
    if not ADMIN_API_KEY:
        return True  # Dev mode - no auth required
    
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing X-API-Key header. Admin endpoints require authentication."
        )
    
    if x_api_key != ADMIN_API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key."
        )
    
    return True

# =============================================================================
# SCORING ENGINE
# =============================================================================

class ScoringEngine:
    """GenoMAX² Recommendation Scoring Engine v2.0"""
    
    def __init__(self, db: Session):
        self.db = db
        self._goals_cache = None
        self._ingredient_goals_cache = None
    
    def get_goals(self) -> List[dict]:
        """Get all goals with keywords."""
        if self._goals_cache is None:
            result = self.db.execute(text("""
                SELECT id, name, slug, category, outcome_keywords, biomarker_keywords
                FROM goals
            """))
            self._goals_cache = [
                {
                    "id": row[0],
                    "name": row[1],
                    "slug": row[2],
                    "category": row[3],
                    "outcome_keywords": row[4] or [],
                    "biomarker_keywords": row[5] or []
                }
                for row in result.fetchall()
            ]
        return self._goals_cache
    
    def get_ingredient_goals(self, ingredient_id: int) -> List[dict]:
        """Get goal mappings for an ingredient."""
        result = self.db.execute(text("""
            SELECT ig.goal_id, g.name, g.slug, ig.relevance_score, ig.relevance_source
            FROM ingredient_goals ig
            JOIN goals g ON ig.goal_id = g.id
            WHERE ig.ingredient_id = :ing_id
        """), {"ing_id": ingredient_id})
        
        return [
            {
                "goal_id": row[0],
                "goal_name": row[1],
                "goal_slug": row[2],
                "relevance_score": float(row[3]),
                "relevance_source": row[4]
            }
            for row in result.fetchall()
        ]
    
    def calculate_evidence_score(self, ingredient: dict) -> float:
        """Direct from DB: evidence_strength_score (0.00-1.00)"""
        return float(ingredient.get('evidence_strength_score') or 0.50)
    
    def calculate_goal_match_score(self, ingredient_goals: List[dict], user_goals: List[str]) -> Tuple[float, List[str]]:
        """Calculate goal match and return matched goal names."""
        if not ingredient_goals or not user_goals:
            return 0.0, []
        
        # Normalize user goals - remove special chars for comparison
        def normalize(s):
            return s.lower().replace("_", " ").replace("-", " ").replace("&", "").replace("  ", " ").strip()
        
        user_goals_normalized = [normalize(g) for g in user_goals]
        
        matched = []
        for ig in ingredient_goals:
            goal_name_norm = normalize(ig['goal_name'])
            goal_slug_norm = normalize(ig.get('goal_slug', '') or '')
            
            for ug in user_goals_normalized:
                # Check various match conditions
                if (ug == goal_name_norm or 
                    ug == goal_slug_norm or
                    ug in goal_name_norm or 
                    goal_name_norm in ug or
                    ug in goal_slug_norm or
                    goal_slug_norm in ug):
                    matched.append(ig)
                    break
        
        if not matched:
            return 0.0, []
        
        # Average relevance
        avg_relevance = sum(m['relevance_score'] for m in matched) / len(matched)
        
        # Multi-goal bonus (max +0.2)
        multi_bonus = min(0.1 * (len(matched) - 1), 0.2)
        
        score = min(avg_relevance + multi_bonus, 1.0)
        matched_names = [m['goal_name'] for m in matched]
        
        return score, matched_names
    
    def calculate_gender_relevance(self, ingredient: dict, os_type: str) -> float:
        """Get gender-specific relevance score."""
        if os_type == "MAXIMO" or os_type == "MAXimo2":
            return float(ingredient.get('male_relevance_score') or 0.50)
        elif os_type == "MAXIMA" or os_type == "MAXima2":
            return float(ingredient.get('female_relevance_score') or 0.50)
        else:
            male = float(ingredient.get('male_relevance_score') or 0.50)
            female = float(ingredient.get('female_relevance_score') or 0.50)
            return (male + female) / 2
    
    def calculate_tier_bonus(self, tier_classification: str) -> float:
        """TIER 1 = 0.20, TIER 2 = 0.10, TIER 3 = 0.00"""
        if not tier_classification:
            return 0.05
        
        if "TIER 1" in tier_classification:
            return 0.20
        elif "TIER 2" in tier_classification:
            return 0.10
        elif "TIER 3" in tier_classification:
            return 0.00
        return 0.05
    
    def calculate_safety_penalty(
        self, 
        medication_interactions: str, 
        contraindications: str,
        user_medications: List[str],
        user_conditions: List[str]
    ) -> Tuple[float, List[str]]:
        """Calculate safety penalty and collect warnings."""
        penalty = 0.0
        warnings = []
        
        interactions = (medication_interactions or "").lower()
        contras = (contraindications or "").lower()
        
        # Check medications
        for med in user_medications:
            med_lower = med.lower()
            if med_lower in interactions:
                penalty -= 0.25
                warnings.append(f"Interacts with {med}")
        
        # Check conditions
        for cond in user_conditions:
            cond_lower = cond.lower()
            if cond_lower in contras:
                penalty -= 0.50
                warnings.append(f"Contraindicated for {cond}")
        
        return max(penalty, -0.50), warnings
    
    def calculate_final_score(
        self,
        ingredient: dict,
        user_goals: List[str],
        os_type: str,
        medications: List[str],
        conditions: List[str]
    ) -> dict:
        """Calculate complete score with all components."""
        
        # Check OS-ready
        os_ready = ingredient.get('os_ready', 'NO')
        if os_ready == "NO":
            return {
                'score': 0.0,
                'excluded': True,
                'reason': 'Not OS-ready',
                'components': None,
                'goals_addressed': [],
                'warnings': []
            }
        
        # Get ingredient goals
        ingredient_goals = self.get_ingredient_goals(ingredient['id'])
        
        # Calculate components
        evidence = self.calculate_evidence_score(ingredient)
        goal_match, goals_addressed = self.calculate_goal_match_score(ingredient_goals, user_goals)
        
        if goal_match == 0.0:
            return {
                'score': 0.0,
                'excluded': True,
                'reason': 'No goal match',
                'components': None,
                'goals_addressed': [],
                'warnings': []
            }
        
        gender = self.calculate_gender_relevance(ingredient, os_type)
        tier = self.calculate_tier_bonus(ingredient.get('tier_classification'))
        safety, warnings = self.calculate_safety_penalty(
            ingredient.get('medication_interactions'),
            ingredient.get('contraindications'),
            medications,
            conditions
        )
        
        # Check severe safety issue
        if safety <= -0.50:
            return {
                'score': 0.0,
                'excluded': True,
                'reason': '; '.join(warnings) if warnings else 'Safety contraindication',
                'components': None,
                'goals_addressed': goals_addressed,
                'warnings': warnings
            }
        
        # Calculate final score
        final = (
            evidence * WEIGHT_EVIDENCE +
            goal_match * WEIGHT_GOAL_MATCH +
            gender * WEIGHT_GENDER +
            tier * WEIGHT_TIER +
            (1.0 + safety) * WEIGHT_SAFETY
        )
        
        return {
            'score': round(final, 3),
            'excluded': False,
            'reason': None,
            'components': {
                'evidence': round(evidence, 3),
                'goal_match': round(goal_match, 3),
                'gender_relevance': round(gender, 3),
                'tier_bonus': round(tier, 3),
                'safety_penalty': round(safety, 3)
            },
            'goals_addressed': goals_addressed,
            'warnings': warnings
        }
    
    def build_why_recommended(
        self,
        ingredient: dict,
        goals_addressed: List[str],
        os_type: str
    ) -> dict:
        """Build explainability object."""
        
        # Evidence summary
        meta = ingredient.get('number_of_meta_analyses') or 0
        rcts = ingredient.get('number_of_rcts') or 0
        participants = ingredient.get('total_participants') or 0
        
        parts = []
        if meta > 0:
            parts.append(f"{int(meta)} meta-analyses")
        if rcts > 0:
            parts.append(f"{int(rcts)} RCTs")
        if participants > 0:
            parts.append(f"{int(participants):,} participants")
        
        evidence_summary = f"Grade {ingredient.get('evidence_grade', 'N/A')}"
        if parts:
            evidence_summary += f" ({', '.join(parts)})"
        
        # Biomarkers
        biomarkers = []
        if ingredient.get('primary_biomarkers'):
            biomarkers = [b.strip() for b in ingredient['primary_biomarkers'].split(',')][:5]
        
        # Gender note
        gender_note = None
        if os_type in ["MAXIMO", "MAXimo2"] and ingredient.get('male_specific_notes'):
            gender_note = ingredient['male_specific_notes']
        elif os_type in ["MAXIMA", "MAXima2"] and ingredient.get('female_specific_notes'):
            gender_note = ingredient['female_specific_notes']
        
        return {
            "goals_addressed": goals_addressed,
            "primary_outcome": ingredient.get('primary_outcomes') or "General health support",
            "biomarkers": biomarkers,
            "evidence_summary": evidence_summary,
            "mechanism": ingredient.get('mechanisms_of_action'),
            "gender_specific": gender_note
        }
    
    def build_safety_info(
        self,
        ingredient: dict,
        warnings: List[str]
    ) -> dict:
        """Build safety information object."""
        
        risk_level = "safe"
        if warnings:
            risk_level = "caution"
        
        return {
            "risk_level": risk_level,
            "warnings": warnings,
            "contraindications": ingredient.get('contraindications'),
            "side_effects_common": ingredient.get('side_effects_common'),
            "side_effects_rare": ingredient.get('side_effects_rare')
        }
    
    def build_dosing_info(self, ingredient: dict) -> dict:
        """Build dosing information."""
        return {
            "recommended": ingredient.get('practical_consumer_doses') or "See product label",
            "timing": ingredient.get('best_time_of_day') or "Any time",
            "with_food": (ingredient.get('food_relation') or "").lower() == "with food"
        }
    
    def get_all_ingredients(self) -> List[dict]:
        """Fetch all ingredients from database."""
        result = self.db.execute(text("""
            SELECT id, name, category, subcategory, tier_classification, os_ready,
                   evidence_grade, evidence_strength_score, evidence_confidence_level,
                   number_of_meta_analyses, number_of_rcts, total_participants,
                   primary_biomarkers, secondary_biomarkers, mechanisms_of_action,
                   primary_outcomes, secondary_outcomes, effect_sizes,
                   male_relevance_score, female_relevance_score,
                   male_specific_notes, female_specific_notes,
                   best_time_of_day, food_relation, timing_reason,
                   practical_consumer_doses, contraindications,
                   side_effects_common, side_effects_rare,
                   medication_interactions, caution_populations,
                   reference_pmids
            FROM ingredients
            WHERE os_ready != 'NO'
        """))
        
        columns = [
            'id', 'name', 'category', 'subcategory', 'tier_classification', 'os_ready',
            'evidence_grade', 'evidence_strength_score', 'evidence_confidence_level',
            'number_of_meta_analyses', 'number_of_rcts', 'total_participants',
            'primary_biomarkers', 'secondary_biomarkers', 'mechanisms_of_action',
            'primary_outcomes', 'secondary_outcomes', 'effect_sizes',
            'male_relevance_score', 'female_relevance_score',
            'male_specific_notes', 'female_specific_notes',
            'best_time_of_day', 'food_relation', 'timing_reason',
            'practical_consumer_doses', 'contraindications',
            'side_effects_common', 'side_effects_rare',
            'medication_interactions', 'caution_populations',
            'reference_pmids'
        ]
        
        ingredients = []
        for row in result.fetchall():
            ing = dict(zip(columns, row))
            ingredients.append(ing)
        
        return ingredients
    
    def generate_recommendations(
        self,
        os_type: str,
        goals: List[str],
        medications: List[str] = None,
        conditions: List[str] = None
    ) -> dict:
        """Generate complete recommendation set."""
        
        medications = medications or []
        conditions = conditions or []
        
        # Get all ingredients
        ingredients = self.get_all_ingredients()
        
        # Score all ingredients
        scored = []
        excluded = []
        
        for ing in ingredients:
            result = self.calculate_final_score(
                ing, goals, os_type, medications, conditions
            )
            
            if result['excluded']:
                excluded.append({
                    "name": ing['name'],
                    "reason": result['reason']
                })
            else:
                scored.append({
                    'ingredient': ing,
                    'score': result['score'],
                    'components': result['components'],
                    'goals_addressed': result['goals_addressed'],
                    'warnings': result['warnings']
                })
        
        # Sort by score
        scored.sort(key=lambda x: x['score'], reverse=True)
        
        # Build stacks
        primary_stack = []
        secondary_stack = []
        
        for item in scored:
            ing = item['ingredient']
            score = item['score']
            os_ready = ing.get('os_ready', 'YES')
            
            # Primary stack criteria
            if len(primary_stack) < MAX_PRIMARY_STACK:
                is_primary_grade = ing.get('evidence_grade') in PRIMARY_GRADES
                is_primary_tier = 'TIER 1' in (ing.get('tier_classification') or '') or \
                                  'TIER 2' in (ing.get('tier_classification') or '')
                is_os_ready = os_ready == "YES"
                
                if score >= PRIMARY_STACK_THRESHOLD and is_primary_grade and is_primary_tier and is_os_ready:
                    rec = self._build_recommendation(item, os_type)
                    primary_stack.append(rec)
                    continue
            
            # Secondary stack
            if len(secondary_stack) < MAX_SECONDARY_STACK:
                if score >= SECONDARY_STACK_THRESHOLD:
                    rec = self._build_recommendation(item, os_type)
                    # Add disclaimer for research-only
                    if os_ready == "RESEARCH-ONLY":
                        rec['disclaimer'] = "Emerging research - evidence still developing"
                    secondary_stack.append(rec)
        
        # Build summary
        summary = {
            "total_primary": len(primary_stack),
            "total_secondary": len(secondary_stack),
            "total_excluded": len(excluded),
            "algorithm_version": ALGORITHM_VERSION,
            "os_type": os_type,
            "goals_requested": goals
        }
        
        return {
            "primary_stack": primary_stack,
            "secondary_stack": secondary_stack,
            "excluded_ingredients": excluded[:20],  # Limit to 20
            "summary": summary
        }
    
    def _build_recommendation(self, item: dict, os_type: str) -> dict:
        """Build a single recommendation object."""
        ing = item['ingredient']
        
        return {
            "id": ing['id'],
            "name": ing['name'],
            "category": ing.get('category'),
            "tier": ing.get('tier_classification'),
            "score": item['score'],
            "evidence_grade": ing.get('evidence_grade'),
            "evidence_score": float(ing.get('evidence_strength_score') or 0.5),
            "dosing": self.build_dosing_info(ing),
            "why_recommended": self.build_why_recommended(ing, item['goals_addressed'], os_type),
            "safety": self.build_safety_info(ing, item['warnings']),
            "components": item['components']
        }


# =============================================================================
# CONFIDENCE CALCULATOR
# =============================================================================

def calculate_confidence(
    has_assessment: bool,
    has_bloodwork: bool,
    assessment_data: dict = None,
    bloodwork_data: dict = None
) -> Tuple[str, List[str]]:
    """Calculate confidence level based on data completeness."""
    
    reasons = []
    score = 0
    
    if not has_assessment:
        return ("low", ["No assessment data"])
    
    score += 40
    
    if assessment_data:
        demographics = assessment_data.get('demographics', {})
        
        if demographics.get('age_years'):
            score += 10
        else:
            reasons.append("Missing age")
        
        if demographics.get('weight_kg'):
            score += 10
        else:
            reasons.append("Missing weight")
        
        if demographics.get('height_cm'):
            score += 5
        
        goals = assessment_data.get('goals', [])
        if len(goals) >= 2:
            score += 10
        elif len(goals) == 1:
            score += 5
            reasons.append("Only one goal selected")
        
        if assessment_data.get('medications'):
            score += 5
        
        if assessment_data.get('conditions'):
            score += 5
    
    if has_bloodwork:
        score += 20
        if bloodwork_data:
            biomarkers = bloodwork_data.get('parsed_biomarkers', [])
            if len(biomarkers) >= 10:
                score += 10
            elif len(biomarkers) >= 5:
                score += 5
                reasons.append("Limited bloodwork data")
    else:
        reasons.append("No bloodwork data")
    
    if score >= 80:
        level = "high"
    elif score >= 50:
        level = "medium"
    else:
        level = "low"
    
    if level == "high" and not reasons:
        reasons = ["Complete assessment and bloodwork data"]
    
    return (level, reasons)


# =============================================================================
# INTAKE STATE MACHINE
# =============================================================================

def calculate_intake_status(has_assessment: bool, has_bloodwork: bool, current_status: str) -> str:
    """Calculate appropriate status based on data presence."""
    
    # Don't change terminal/processing states
    terminal_states = [
        IntakeStatus.PROCESSING.value,
        IntakeStatus.COMPLETED.value,
        IntakeStatus.FAILED.value,
        IntakeStatus.EXPIRED.value,
        IntakeStatus.ARCHIVED.value
    ]
    
    if current_status in terminal_states:
        return current_status
    
    if has_assessment:
        return IntakeStatus.READY_TO_PROCESS.value
    elif has_bloodwork:
        return IntakeStatus.AWAITING_ASSESSMENT.value
    else:
        return IntakeStatus.DRAFT.value


def validate_process_request(status: str, has_assessment: bool, force: bool = False) -> Tuple[bool, Optional[str]]:
    """Validate if intake can be processed."""
    
    if not has_assessment:
        return (False, "Assessment required before processing. Bloodwork-only is not sufficient.")
    
    if status == IntakeStatus.PROCESSING.value:
        return (False, "Intake is already processing")
    
    if status == IntakeStatus.COMPLETED.value and not force:
        return (False, "Already completed. Use force=true to reprocess")
    
    if status in [IntakeStatus.EXPIRED.value, IntakeStatus.ARCHIVED.value]:
        return (False, f"Cannot process intake in {status} state")
    
    valid_states = [
        IntakeStatus.READY_TO_PROCESS.value,
        IntakeStatus.FAILED.value,
        IntakeStatus.COMPLETED.value
    ]
    
    if status in valid_states:
        return (True, None)
    
    return (False, f"Invalid state for processing: {status}")


# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(
    title="GenoMAX² API",
    description="Unified Intake-based Recommendation Engine",
    version=API_VERSION
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# HEALTH & VERSION ENDPOINTS
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/version")
async def get_version(db: Session = Depends(get_db)):
    """Get API version and stats."""
    
    # Get counts
    try:
        ing_count = db.execute(text("SELECT COUNT(*) FROM ingredients")).fetchone()[0]
    except:
        ing_count = 0
    
    try:
        goals_count = db.execute(text("SELECT COUNT(*) FROM goals")).fetchone()[0]
    except:
        goals_count = 0
    
    try:
        modules_count = db.execute(text("SELECT COUNT(*) FROM os_modules")).fetchone()[0]
    except:
        modules_count = 0
    
    return {
        "version": API_VERSION,
        "api": "GenoMAX2",
        "algorithm_version": ALGORITHM_VERSION,
        "features": [
            "intake_lifecycle",
            "assessment_flow",
            "bloodwork_flow",
            "os_lock",
            "confidence_levels",
            "explainability",
            "batch_interactions",
            "admin_auth"
        ],
        "data": {
            "ingredients": ing_count,
            "goals": goals_count,
            "os_modules": modules_count
        }
    }


# =============================================================================
# INTAKE ENDPOINTS
# =============================================================================

@app.post("/intakes", response_model=IntakeResponse, status_code=201, dependencies=[Depends(verify_admin_api_key)])
async def create_intake(request: IntakeCreateRequest = None, db: Session = Depends(get_db)):
    """Create a new intake."""
    
    intake_id = f"int_{uuid.uuid4().hex[:12]}"
    now = datetime.utcnow()
    
    os_type = request.os_type.value if request and request.os_type else None
    source = request.source if request else "unknown"
    
    db.execute(text("""
        INSERT INTO intakes (id, os_type, os_locked, status, source, has_assessment, has_bloodwork, created_at, updated_at)
        VALUES (:id, :os_type, :os_locked, :status, :source, false, false, :now, :now)
    """), {
        "id": intake_id,
        "os_type": os_type,
        "os_locked": os_type is not None,
        "status": IntakeStatus.DRAFT.value,
        "source": source,
        "now": now
    })
    db.commit()
    
    return IntakeResponse(
        intake_id=intake_id,
        status=IntakeStatus.DRAFT.value,
        os_type=os_type,
        os_locked=os_type is not None,
        has_assessment=False,
        has_bloodwork=False,
        created_at=now,
        updated_at=now
    )


@app.get("/intakes/{intake_id}", response_model=IntakeSnapshotResponse, dependencies=[Depends(verify_admin_api_key)])
async def get_intake(intake_id: str, db: Session = Depends(get_db)):
    """Get full intake snapshot."""
    
    result = db.execute(text("""
        SELECT id, os_type, os_locked, status, source, has_assessment, has_bloodwork,
               confidence_level, confidence_reasons, created_at, updated_at
        FROM intakes WHERE id = :id
    """), {"id": intake_id}).fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail=f"Intake '{intake_id}' not found")
    
    # Get assessment if exists
    assessment = None
    if result[5]:  # has_assessment
        ass_result = db.execute(text("""
            SELECT demographics, lifestyle, goals, pain_points, medications, conditions,
                   current_supplements, constraints, execution_preferences
            FROM assessments WHERE intake_id = :id
        """), {"id": intake_id}).fetchone()
        
        if ass_result:
            assessment = {
                "demographics": ass_result[0],
                "lifestyle": ass_result[1],
                "goals": ass_result[2],
                "pain_points": ass_result[3],
                "medications": ass_result[4],
                "conditions": ass_result[5],
                "current_supplements": ass_result[6],
                "constraints": ass_result[7],
                "execution_preferences": ass_result[8]
            }
    
    # Get bloodwork if exists
    bloodwork = None
    if result[6]:  # has_bloodwork
        bw_result = db.execute(text("""
            SELECT collected_date, lab_name, country, parsed_biomarkers
            FROM bloodwork WHERE intake_id = :id ORDER BY created_at DESC LIMIT 1
        """), {"id": intake_id}).fetchone()
        
        if bw_result:
            bloodwork = {
                "collected_date": str(bw_result[0]) if bw_result[0] else None,
                "lab_name": bw_result[1],
                "country": bw_result[2],
                "parsed_biomarkers": bw_result[3]
            }
    
    # Get outputs if completed
    outputs = None
    if result[3] == IntakeStatus.COMPLETED.value:
        rec_result = db.execute(text("""
            SELECT primary_stack, secondary_stack, excluded_ingredients, summary
            FROM recommendations WHERE intake_id = :id ORDER BY created_at DESC LIMIT 1
        """), {"id": intake_id}).fetchone()
        
        if rec_result:
            outputs = {
                "primary_stack": rec_result[0],
                "secondary_stack": rec_result[1],
                "excluded_ingredients": rec_result[2],
                "summary": rec_result[3]
            }
    
    confidence = None
    if result[7]:
        confidence = {
            "level": result[7],
            "reasons": result[8] or []
        }
    
    return IntakeSnapshotResponse(
        intake_id=result[0],
        status=result[3],
        os_type=result[1],
        os_locked=result[2] or False,
        has_assessment=result[5] or False,
        has_bloodwork=result[6] or False,
        created_at=result[9],
        updated_at=result[10],
        assessment=assessment,
        bloodwork=bloodwork,
        outputs=outputs,
        confidence=confidence
    )


@app.get("/intakes", dependencies=[Depends(verify_admin_api_key)])
async def list_intakes(
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """List intakes with optional status filter."""
    
    # Build query
    where_clause = ""
    params = {"limit": limit, "offset": offset}
    
    if status:
        where_clause = "WHERE status = :status"
        params["status"] = status
    
    # Get total
    total_result = db.execute(text(f"SELECT COUNT(*) FROM intakes {where_clause}"), params).fetchone()
    total = total_result[0] if total_result else 0
    
    # Get items
    result = db.execute(text(f"""
        SELECT id, os_type, os_locked, status, has_assessment, has_bloodwork, created_at, updated_at
        FROM intakes {where_clause}
        ORDER BY updated_at DESC
        LIMIT :limit OFFSET :offset
    """), params)
    
    items = [
        IntakeResponse(
            intake_id=row[0],
            status=row[3],
            os_type=row[1],
            os_locked=row[2] or False,
            has_assessment=row[4] or False,
            has_bloodwork=row[5] or False,
            created_at=row[6],
            updated_at=row[7]
        )
        for row in result.fetchall()
    ]
    
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset
    }


# =============================================================================
# ASSESSMENT ENDPOINT
# =============================================================================

@app.patch("/intakes/{intake_id}/assessment", response_model=IntakeResponse, dependencies=[Depends(verify_admin_api_key)])
async def upsert_assessment(intake_id: str, payload: AssessmentPayload, db: Session = Depends(get_db)):
    """Create or update assessment (idempotent)."""
    
    # Get intake
    intake = db.execute(text("""
        SELECT id, os_type, os_locked, status, has_assessment, has_bloodwork
        FROM intakes WHERE id = :id
    """), {"id": intake_id}).fetchone()
    
    if not intake:
        raise HTTPException(status_code=404, detail=f"Intake '{intake_id}' not found")
    
    current_os = intake[1]
    os_locked = intake[2]
    
    # OS Lock check
    if payload.os_type:
        new_os = payload.os_type.value
        if os_locked and current_os != new_os:
            raise HTTPException(
                status_code=409,
                detail=f"OS already locked to {current_os}. Cannot change to {new_os}"
            )
        # Lock OS if provided
        current_os = new_os
        os_locked = True
    
    now = datetime.utcnow()
    
    # Convert payload to dict
    assessment_data = {
        "demographics": payload.demographics.dict(),
        "lifestyle": payload.lifestyle.dict(),
        "goals": [g.dict() for g in payload.goals],
        "pain_points": [p.dict() for p in payload.pain_points],
        "medications": [m.dict() for m in (payload.medications or [])],
        "conditions": [c.dict() for c in (payload.conditions or [])],
        "current_supplements": [s.dict() for s in (payload.current_supplements or [])],
        "constraints": payload.constraints.dict() if payload.constraints else None,
        "execution_preferences": payload.execution_preferences.dict() if payload.execution_preferences else None,
        "meta": payload.meta
    }
    
    # Upsert assessment
    existing = db.execute(text("SELECT id FROM assessments WHERE intake_id = :id"), {"id": intake_id}).fetchone()
    
    if existing:
        db.execute(text("""
            UPDATE assessments SET
                demographics = :demographics,
                lifestyle = :lifestyle,
                goals = :goals,
                pain_points = :pain_points,
                medications = :medications,
                conditions = :conditions,
                current_supplements = :current_supplements,
                constraints = :constraints,
                execution_preferences = :execution_preferences,
                meta = :meta,
                updated_at = :now
            WHERE intake_id = :intake_id
        """), {
            "intake_id": intake_id,
            "demographics": json.dumps(assessment_data['demographics']),
            "lifestyle": json.dumps(assessment_data['lifestyle']),
            "goals": json.dumps(assessment_data['goals']),
            "pain_points": json.dumps(assessment_data['pain_points']),
            "medications": json.dumps(assessment_data['medications']),
            "conditions": json.dumps(assessment_data['conditions']),
            "current_supplements": json.dumps(assessment_data['current_supplements']),
            "constraints": json.dumps(assessment_data['constraints']) if assessment_data['constraints'] else None,
            "execution_preferences": json.dumps(assessment_data['execution_preferences']) if assessment_data['execution_preferences'] else None,
            "meta": json.dumps(assessment_data['meta']) if assessment_data['meta'] else None,
            "now": now
        })
    else:
        db.execute(text("""
            INSERT INTO assessments (
                intake_id, demographics, lifestyle, goals, pain_points,
                medications, conditions, current_supplements, constraints,
                execution_preferences, meta, created_at, updated_at
            ) VALUES (
                :intake_id, :demographics, :lifestyle, :goals, :pain_points,
                :medications, :conditions, :current_supplements, :constraints,
                :execution_preferences, :meta, :now, :now
            )
        """), {
            "intake_id": intake_id,
            "demographics": json.dumps(assessment_data['demographics']),
            "lifestyle": json.dumps(assessment_data['lifestyle']),
            "goals": json.dumps(assessment_data['goals']),
            "pain_points": json.dumps(assessment_data['pain_points']),
            "medications": json.dumps(assessment_data['medications']),
            "conditions": json.dumps(assessment_data['conditions']),
            "current_supplements": json.dumps(assessment_data['current_supplements']),
            "constraints": json.dumps(assessment_data['constraints']) if assessment_data['constraints'] else None,
            "execution_preferences": json.dumps(assessment_data['execution_preferences']) if assessment_data['execution_preferences'] else None,
            "meta": json.dumps(assessment_data['meta']) if assessment_data['meta'] else None,
            "now": now
        })
    
    # Update intake
    new_status = calculate_intake_status(True, intake[5], intake[3])
    
    db.execute(text("""
        UPDATE intakes SET
            os_type = :os_type,
            os_locked = :os_locked,
            has_assessment = true,
            status = :status,
            updated_at = :now
        WHERE id = :id
    """), {
        "id": intake_id,
        "os_type": current_os,
        "os_locked": os_locked,
        "status": new_status,
        "now": now
    })
    
    db.commit()
    
    return IntakeResponse(
        intake_id=intake_id,
        status=new_status,
        os_type=current_os,
        os_locked=os_locked,
        has_assessment=True,
        has_bloodwork=intake[5] or False,
        created_at=intake[4] if len(intake) > 6 else now,  # This might need adjustment
        updated_at=now
    )


# =============================================================================
# BLOODWORK ENDPOINT
# =============================================================================

@app.post("/intakes/{intake_id}/bloodwork", response_model=IntakeResponse, dependencies=[Depends(verify_admin_api_key)])
async def add_bloodwork(intake_id: str, payload: BloodworkPayload, db: Session = Depends(get_db)):
    """Add bloodwork data to intake."""
    
    # Get intake
    intake = db.execute(text("""
        SELECT id, os_type, os_locked, status, has_assessment, has_bloodwork, created_at
        FROM intakes WHERE id = :id
    """), {"id": intake_id}).fetchone()
    
    if not intake:
        raise HTTPException(status_code=404, detail=f"Intake '{intake_id}' not found")
    
    now = datetime.utcnow()
    
    # Insert bloodwork
    db.execute(text("""
        INSERT INTO bloodwork (
            intake_id, collected_date, lab_name, country, parsed_biomarkers, notes, created_at, updated_at
        ) VALUES (
            :intake_id, :collected_date, :lab_name, :country, :biomarkers, :notes, :now, :now
        )
    """), {
        "intake_id": intake_id,
        "collected_date": payload.collected_date,
        "lab_name": payload.lab_name,
        "country": payload.country,
        "biomarkers": json.dumps([b.dict() for b in payload.parsed_biomarkers]),
        "notes": payload.notes,
        "now": now
    })
    
    # Update intake
    new_status = calculate_intake_status(intake[4], True, intake[3])
    
    db.execute(text("""
        UPDATE intakes SET
            has_bloodwork = true,
            status = :status,
            updated_at = :now
        WHERE id = :id
    """), {
        "id": intake_id,
        "status": new_status,
        "now": now
    })
    
    db.commit()
    
    return IntakeResponse(
        intake_id=intake_id,
        status=new_status,
        os_type=intake[1],
        os_locked=intake[2] or False,
        has_assessment=intake[4] or False,
        has_bloodwork=True,
        created_at=intake[6],
        updated_at=now
    )


# =============================================================================
# PROCESSING ENDPOINT
# =============================================================================

@app.post("/intakes/{intake_id}/process", dependencies=[Depends(verify_admin_api_key)])
async def process_intake(
    intake_id: str,
    request: ProcessRequest = None,
    db: Session = Depends(get_db)
):
    """Trigger processing for an intake. THE BRAIN."""
    
    # Get intake
    intake = db.execute(text("""
        SELECT id, os_type, os_locked, status, has_assessment, has_bloodwork, created_at
        FROM intakes WHERE id = :id
    """), {"id": intake_id}).fetchone()
    
    if not intake:
        raise HTTPException(status_code=404, detail=f"Intake '{intake_id}' not found")
    
    # Validate
    force = request.force if request else False
    can_process, error = validate_process_request(intake[3], intake[4], force)
    
    if not can_process:
        raise HTTPException(status_code=409 if "already" in error.lower() else 400, detail=error)
    
    # Get assessment data
    assessment = db.execute(text("""
        SELECT demographics, lifestyle, goals, medications, conditions
        FROM assessments WHERE intake_id = :id
    """), {"id": intake_id}).fetchone()
    
    if not assessment:
        raise HTTPException(status_code=400, detail="Assessment data not found")
    
    # Parse assessment data
    demographics = json.loads(assessment[0]) if isinstance(assessment[0], str) else assessment[0]
    goals_data = json.loads(assessment[2]) if isinstance(assessment[2], str) else assessment[2]
    medications_data = json.loads(assessment[3]) if isinstance(assessment[3], str) else (assessment[3] or [])
    conditions_data = json.loads(assessment[4]) if isinstance(assessment[4], str) else (assessment[4] or [])
    
    # Extract goal names
    goal_names = [g.get('key', g.get('name', '')) for g in goals_data]
    medication_names = [m.get('name', '') for m in medications_data]
    condition_names = [c.get('name', '') for c in conditions_data]
    
    # Determine OS type
    os_type = intake[1] or "MAXIMO"  # Default to MAXIMO if not set
    
    # Update status to processing
    now = datetime.utcnow()
    db.execute(text("""
        UPDATE intakes SET status = :status, updated_at = :now WHERE id = :id
    """), {"id": intake_id, "status": IntakeStatus.PROCESSING.value, "now": now})
    db.commit()
    
    try:
        # Run scoring engine
        engine = ScoringEngine(db)
        results = engine.generate_recommendations(
            os_type=os_type,
            goals=goal_names,
            medications=medication_names,
            conditions=condition_names
        )
        
        # Get bloodwork for confidence calculation
        bloodwork_data = None
        if intake[5]:  # has_bloodwork
            bw = db.execute(text("""
                SELECT parsed_biomarkers FROM bloodwork WHERE intake_id = :id LIMIT 1
            """), {"id": intake_id}).fetchone()
            if bw:
                bloodwork_data = {"parsed_biomarkers": json.loads(bw[0]) if isinstance(bw[0], str) else bw[0]}
        
        # Calculate confidence
        assessment_dict = {
            "demographics": demographics,
            "goals": goals_data,
            "medications": medications_data,
            "conditions": conditions_data
        }
        conf_level, conf_reasons = calculate_confidence(
            has_assessment=True,
            has_bloodwork=intake[5] or False,
            assessment_data=assessment_dict,
            bloodwork_data=bloodwork_data
        )
        
        # Store recommendation
        db.execute(text("""
            INSERT INTO recommendations (
                intake_id, algorithm_version, primary_stack, secondary_stack,
                excluded_ingredients, summary, created_at
            ) VALUES (
                :intake_id, :algo_version, :primary, :secondary,
                :excluded, :summary, :now
            )
        """), {
            "intake_id": intake_id,
            "algo_version": ALGORITHM_VERSION,
            "primary": json.dumps(results['primary_stack']),
            "secondary": json.dumps(results['secondary_stack']),
            "excluded": json.dumps(results['excluded_ingredients']),
            "summary": json.dumps(results['summary']),
            "now": now
        })
        
        # Update intake to completed
        db.execute(text("""
            UPDATE intakes SET
                status = :status,
                confidence_level = :conf_level,
                confidence_reasons = :conf_reasons,
                processed_at = :now,
                processing_version = :algo_version,
                updated_at = :now
            WHERE id = :id
        """), {
            "id": intake_id,
            "status": IntakeStatus.COMPLETED.value,
            "conf_level": conf_level,
            "conf_reasons": json.dumps(conf_reasons),
            "now": now,
            "algo_version": ALGORITHM_VERSION
        })
        
        db.commit()
        
        return {
            "intake_id": intake_id,
            "status": IntakeStatus.COMPLETED.value,
            "confidence": {
                "level": conf_level,
                "reasons": conf_reasons
            },
            "primary_stack": results['primary_stack'],
            "secondary_stack": results['secondary_stack'],
            "excluded_ingredients": results['excluded_ingredients'],
            "summary": results['summary']
        }
        
    except Exception as e:
        # Update status to failed
        db.execute(text("""
            UPDATE intakes SET
                status = :status,
                last_error = :error,
                retry_count = COALESCE(retry_count, 0) + 1,
                updated_at = :now
            WHERE id = :id
        """), {
            "id": intake_id,
            "status": IntakeStatus.FAILED.value,
            "error": str(e),
            "now": now
        })
        db.commit()
        
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


# =============================================================================
# LEGACY RECOMMEND ENDPOINT (backward compatible)
# =============================================================================

@app.post("/recommend")
async def legacy_recommend(request: RecommendRequest, db: Session = Depends(get_db)):
    """Legacy stateless recommendation endpoint."""
    
    # Map OS format
    os_type = "MAXIMO" if "maximo" in request.os.lower() else "MAXIMA"
    
    engine = ScoringEngine(db)
    results = engine.generate_recommendations(
        os_type=os_type,
        goals=request.goals,
        medications=request.medications or [],
        conditions=request.conditions or []
    )
    
    return {
        "user_profile": {
            "os": request.os,
            "goals": request.goals,
            "medications": request.medications,
            "conditions": request.conditions
        },
        **results
    }


# =============================================================================
# REFERENCE DATA ENDPOINTS
# =============================================================================

@app.get("/goals")
async def get_goals(db: Session = Depends(get_db)):
    """Get all available goals."""
    try:
        result = db.execute(text("""
            SELECT id, name, slug, category FROM goals ORDER BY id
        """))
        return {
            "goals": [
                {"id": row[0], "name": row[1], "slug": row[2], "category": row[3]}
                for row in result.fetchall()
            ]
        }
    except:
        # Fallback if goals table doesn't exist
        return {"goals": []}


@app.get("/modules")
async def get_modules(db: Session = Depends(get_db)):
    """Get all OS modules."""
    try:
        result = db.execute(text("""
            SELECT id, name, slug, os_type FROM os_modules ORDER BY id
        """))
        return {
            "modules": [
                {"id": row[0], "name": row[1], "slug": row[2], "os_type": row[3]}
                for row in result.fetchall()
            ]
        }
    except:
        return {"modules": []}


@app.get("/ingredients")
async def get_ingredients(
    category: Optional[str] = None,
    os_ready: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """Get ingredients with optional filters."""
    
    where_clauses = []
    params = {"limit": limit}
    
    if category:
        where_clauses.append("category = :category")
        params["category"] = category
    
    if os_ready:
        where_clauses.append("os_ready = :os_ready")
        params["os_ready"] = os_ready
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    result = db.execute(text(f"""
        SELECT id, name, category, tier_classification, os_ready, evidence_grade
        FROM ingredients
        WHERE {where_sql}
        ORDER BY name
        LIMIT :limit
    """), params)
    
    return {
        "ingredients": [
            {
                "id": row[0],
                "name": row[1],
                "category": row[2],
                "tier": row[3],
                "os_ready": row[4],
                "evidence_grade": row[5]
            }
            for row in result.fetchall()
        ]
    }


@app.get("/ingredient/{name}")
async def get_ingredient_detail(name: str, db: Session = Depends(get_db)):
    """Get detailed ingredient information."""
    
    result = db.execute(text("""
        SELECT id, name, category, subcategory, tier_classification, os_ready,
               evidence_grade, evidence_strength_score,
               number_of_meta_analyses, number_of_rcts, total_participants,
               primary_biomarkers, secondary_biomarkers, mechanisms_of_action,
               primary_outcomes, secondary_outcomes,
               male_relevance_score, female_relevance_score,
               male_specific_notes, female_specific_notes,
               best_time_of_day, food_relation, practical_consumer_doses,
               contraindications, medication_interactions,
               side_effects_common, side_effects_rare, caution_populations,
               reference_pmids
        FROM ingredients WHERE LOWER(name) = LOWER(:name)
    """), {"name": name}).fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail=f"Ingredient '{name}' not found")
    
    return {
        "id": result[0],
        "name": result[1],
        "category": result[2],
        "subcategory": result[3],
        "tier": result[4],
        "os_ready": result[5],
        "evidence": {
            "grade": result[6],
            "score": float(result[7]) if result[7] else None,
            "meta_analyses": result[8],
            "rcts": result[9],
            "participants": result[10]
        },
        "biomarkers": {
            "primary": result[11],
            "secondary": result[12]
        },
        "mechanism": result[13],
        "outcomes": {
            "primary": result[14],
            "secondary": result[15]
        },
        "gender_relevance": {
            "male_score": float(result[16]) if result[16] else 0.5,
            "female_score": float(result[17]) if result[17] else 0.5,
            "male_notes": result[18],
            "female_notes": result[19]
        },
        "dosing": {
            "time_of_day": result[20],
            "food_relation": result[21],
            "recommended_dose": result[22]
        },
        "safety": {
            "contraindications": result[23],
            "medication_interactions": result[24],
            "side_effects_common": result[25],
            "side_effects_rare": result[26],
            "caution_populations": result[27]
        },
        "references": result[28]
    }


# =============================================================================
# INTERACTION CHECKING
# =============================================================================

@app.post("/check-interactions")
async def check_interaction(
    ingredient: str,
    medications: List[str] = [],
    conditions: List[str] = [],
    db: Session = Depends(get_db)
):
    """Check interactions for a single ingredient."""
    
    result = db.execute(text("""
        SELECT name, medication_interactions, contraindications, caution_populations
        FROM ingredients WHERE LOWER(name) = LOWER(:name)
    """), {"name": ingredient}).fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail=f"Ingredient '{ingredient}' not found")
    
    warnings = []
    interactions = (result[1] or "").lower()
    contras = (result[2] or "").lower()
    
    for med in medications:
        if med.lower() in interactions:
            warnings.append({
                "type": "medication_interaction",
                "ingredient": result[0],
                "medication": med,
                "note": result[1]
            })
    
    for cond in conditions:
        if cond.lower() in contras:
            warnings.append({
                "type": "contraindication",
                "ingredient": result[0],
                "condition": cond,
                "note": result[2]
            })
    
    return {
        "ingredient": result[0],
        "safe": len(warnings) == 0,
        "warnings": warnings,
        "caution_populations": result[3]
    }


@app.post("/check-interactions-batch")
async def check_interactions_batch(
    ingredients: List[str],
    medications: List[str] = [],
    conditions: List[str] = [],
    db: Session = Depends(get_db)
):
    """Check interactions for multiple ingredients."""
    
    all_warnings = []
    results = {}
    
    for ing_name in ingredients:
        result = db.execute(text("""
            SELECT name, medication_interactions, contraindications
            FROM ingredients WHERE LOWER(name) = LOWER(:name)
        """), {"name": ing_name}).fetchone()
        
        if not result:
            results[ing_name] = {"found": False, "safe": None, "warnings": []}
            continue
        
        warnings = []
        interactions = (result[1] or "").lower()
        contras = (result[2] or "").lower()
        
        for med in medications:
            if med.lower() in interactions:
                warning = {
                    "type": "medication_interaction",
                    "ingredient": result[0],
                    "medication": med
                }
                warnings.append(warning)
                all_warnings.append(warning)
        
        for cond in conditions:
            if cond.lower() in contras:
                warning = {
                    "type": "contraindication",
                    "ingredient": result[0],
                    "condition": cond
                }
                warnings.append(warning)
                all_warnings.append(warning)
        
        results[ing_name] = {
            "found": True,
            "safe": len(warnings) == 0,
            "warnings": warnings
        }
    
    return {
        "results": results,
        "all_warnings": all_warnings,
        "total_warnings": len(all_warnings)
    }


# =============================================================================
# DATABASE SCHEMA CREATION
# =============================================================================

@app.on_event("startup")
async def startup():
    """Create tables on startup if they don't exist."""
    
    with engine.connect() as conn:
        # Intakes table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS intakes (
                id VARCHAR(50) PRIMARY KEY,
                os_type VARCHAR(10),
                os_locked BOOLEAN DEFAULT FALSE,
                status VARCHAR(30) DEFAULT 'draft',
                source VARCHAR(20),
                has_assessment BOOLEAN DEFAULT FALSE,
                has_bloodwork BOOLEAN DEFAULT FALSE,
                processed_at TIMESTAMP,
                processing_version VARCHAR(20),
                confidence_level VARCHAR(10),
                confidence_reasons JSON,
                last_error TEXT,
                retry_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        
        # Assessments table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS assessments (
                id SERIAL PRIMARY KEY,
                intake_id VARCHAR(50) UNIQUE REFERENCES intakes(id),
                demographics JSON NOT NULL,
                lifestyle JSON NOT NULL,
                goals JSON NOT NULL,
                pain_points JSON,
                medications JSON,
                conditions JSON,
                current_supplements JSON,
                constraints JSON,
                execution_preferences JSON,
                meta JSON,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        
        # Bloodwork table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bloodwork (
                id SERIAL PRIMARY KEY,
                intake_id VARCHAR(50) REFERENCES intakes(id),
                collected_date DATE NOT NULL,
                lab_name VARCHAR(100),
                country VARCHAR(50),
                file_refs JSON,
                parsed_biomarkers JSON NOT NULL,
                parsing_method VARCHAR(20),
                parsing_confidence VARCHAR(10),
                notes TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
        
        # Recommendations table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS recommendations (
                id SERIAL PRIMARY KEY,
                intake_id VARCHAR(50) REFERENCES intakes(id),
                algorithm_version VARCHAR(20) NOT NULL,
                primary_stack JSON NOT NULL,
                secondary_stack JSON,
                excluded_ingredients JSON,
                summary JSON NOT NULL,
                full_output JSON,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        
        conn.commit()
        print("✅ Database tables verified/created")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
