#!/usr/bin/env python3
"""
GenoMAX² Recommendation Engine
==============================
The brain of the Biological Operating System.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
from sqlalchemy import create_engine, text

# CLOUD DATABASE LOGIC
# If running on Railway/Render, it uses the cloud variable.
# If running on your laptop, it falls back to your local password.
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:1!Qaz2wsx@localhost:5432/genomax2")

# Fix for some cloud providers that use 'postgres://' instead of 'postgresql://'
if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

class Gender(str, Enum):
    MALE = "male"
    FEMALE = "female"

class RiskLevel(str, Enum):
    SAFE = "safe"
    CAUTION = "caution"
    AVOID = "avoid"

@dataclass
class UserProfile:
    gender: Gender
    goals: List[str]
    medications: List[str] = field(default_factory=list)
    conditions: List[str] = field(default_factory=list)
    age: Optional[int] = None
    exclude_ingredients: List[str] = field(default_factory=list)

@dataclass
class IngredientRecommendation:
    id: int
    name: str
    category: str
    evidence_grade: str
    evidence_score: float
    relevance_score: float
    practical_dose: str
    timing: str
    why_recommended: str
    gender_note: str
    goals_addressed: List[str]
    synergies: List[str]
    warnings: List[str]
    risk_level: RiskLevel
    supliful_product: Optional[Dict[str, Any]] = None

@dataclass 
class StackRecommendation:
    user_profile: UserProfile
    primary_stack: List[IngredientRecommendation]
    secondary_stack: List[IngredientRecommendation]
    excluded_ingredients: List[Dict[str, str]]
    total_products: int
    estimated_monthly_cost: float
    safety_summary: str
    os_modules_activated: List[str]


class GenoMAXEngine:
    def __init__(self, db_url: str = DB_URL):
        self.engine = create_engine(db_url)
    
    def get_available_goals(self) -> List[str]:
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM goals ORDER BY name"))
            return [row[0] for row in result]
    
    def get_available_modules(self) -> List[str]:
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM os_modules ORDER BY name"))
            return [row[0] for row in result]
    
    def _get_ingredients_for_goals(self, goals: List[str], gender: Gender) -> List[Dict]:
        placeholders = ', '.join([f':goal_{i}' for i in range(len(goals))])
        params = {f'goal_{i}': goal for i, goal in enumerate(goals)}
        
        query = f"""
            SELECT DISTINCT
                i.id, i.name, i.category, i.evidence_grade, i.evidence_strength_score,
                i.practical_dose, i.timing_instruction, i.male_relevance_score,
                i.female_relevance_score, i.male_specific_notes, i.female_specific_notes,
                i.contraindications, i.primary_biomarkers, i.mechanisms,
                ARRAY_AGG(DISTINCT g.name) AS goals_matched,
                COUNT(DISTINCT g.name) AS goal_count
            FROM ingredients i
            JOIN ingredient_logic il ON i.id = il.ingredient_id
            JOIN goals g ON il.goal_id = g.id
            WHERE g.name IN ({placeholders})
            GROUP BY i.id
            ORDER BY COUNT(DISTINCT g.name) DESC, i.evidence_grade, i.evidence_strength_score DESC NULLS LAST
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            ingredients = []
            
            for row in result:
                if gender == Gender.MALE:
                    relevance = row[7] or 0.5
                    gender_note = row[9] or ""
                else:
                    relevance = row[8] or 0.5
                    gender_note = row[10] or ""
                
                ingredients.append({
                    'id': row[0], 'name': row[1], 'category': row[2],
                    'evidence_grade': row[3] or 'C',
                    'evidence_score': float(row[4]) if row[4] else 0.5,
                    'practical_dose': row[5] or 'See label',
                    'timing': row[6] or 'Any time',
                    'relevance_score': float(relevance),
                    'gender_note': gender_note,
                    'contraindications': row[11] or '',
                    'biomarkers': row[12] or '',
                    'mechanisms': row[13] or '',
                    'goals_matched': list(row[14]) if row[14] else [],
                    'goal_count': row[15]
                })
            return ingredients
    
    def _check_drug_interactions(self, ingredient_id: int, medications: List[str]) -> List[Dict]:
        if not medications:
            return []
        warnings = []
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT keyword, severity, notes FROM ingredient_risks
                WHERE ingredient_id = :ing_id AND risk_type = 'Medication'
            """), {'ing_id': ingredient_id})
            
            for row in result:
                keyword = row[0].lower()
                for med in medications:
                    med_lower = med.lower()
                    if keyword in med_lower or med_lower in keyword or any(word in med_lower for word in keyword.split()):
                        warnings.append({'medication': med, 'interaction': row[0], 'severity': row[1], 'notes': row[2]})
        return warnings
    
    def _check_condition_contraindications(self, ingredient_id: int, conditions: List[str], contraindications: str) -> List[str]:
        if not conditions or not contraindications:
            return []
        warnings = []
        contra_lower = contraindications.lower()
        for condition in conditions:
            cond_lower = condition.lower()
            checks = [cond_lower]
            if 'pregnancy' in cond_lower or 'pregnant' in cond_lower:
                checks.extend(['pregnancy', 'pregnant'])
            if 'thyroid' in cond_lower:
                checks.extend(['thyroid', 'hyperthyroid', 'hypothyroid'])
            if 'diabetes' in cond_lower:
                checks.extend(['diabetes', 'diabetic', 'blood sugar'])
            for check in checks:
                if check in contra_lower:
                    warnings.append(f"Contraindicated for {condition}")
                    break
        return warnings
    
    def _get_synergies(self, ingredient_id: int) -> Dict[str, List[str]]:
        synergies = {'synergistic': [], 'antagonistic': [], 'contraindicated': []}
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT related_ingredient, relationship_type, condition
                FROM ingredient_synergies WHERE ingredient_id = :ing_id
            """), {'ing_id': ingredient_id})
            
            for row in result:
                rel_type = row[1].lower()
                entry = row[0] + (f" ({row[2]})" if row[2] else "")
                if 'synerg' in rel_type:
                    synergies['synergistic'].append(entry)
                elif 'antagon' in rel_type:
                    synergies['antagonistic'].append(entry)
                elif 'contraind' in rel_type:
                    synergies['contraindicated'].append(entry)
        return synergies
    
    def _get_supliful_product(self, ingredient_id: int) -> Optional[Dict]:
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT product_name, product_url, base_price, serving_info, short_description
                FROM fulfillment_catalog
                WHERE ingredient_id = :ing_id AND supplier_name = 'Supliful' AND is_active = TRUE
                LIMIT 1
            """), {'ing_id': ingredient_id})
            row = result.fetchone()
            if row:
                return {'name': row[0], 'url': row[1], 'base_price': float(row[2]) if row[2] else None, 'serving_info': row[3], 'description': row[4]}
        return None
    
    def _get_os_modules(self, ingredient_ids: List[int]) -> List[str]:
        if not ingredient_ids:
            return []
        placeholders = ', '.join([f':id_{i}' for i in range(len(ingredient_ids))])
        params = {f'id_{i}': id for i, id in enumerate(ingredient_ids)}
        with self.engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT DISTINCT om.name FROM os_modules om
                JOIN ingredient_modules im ON om.id = im.module_id
                WHERE im.ingredient_id IN ({placeholders}) ORDER BY om.name
            """), params)
            return [row[0] for row in result]
    
    def _calculate_risk_level(self, drug_warnings: List, condition_warnings: List, synergies: Dict, medications: List[str] = None) -> RiskLevel:
        if condition_warnings:
            return RiskLevel.AVOID
        if synergies.get('contraindicated') and medications:
            for contra in synergies['contraindicated']:
                contra_lower = contra.lower()
                for med in medications:
                    if med.lower() in contra_lower or contra_lower in med.lower():
                        return RiskLevel.AVOID
        if drug_warnings:
            return RiskLevel.CAUTION
        if synergies.get('antagonistic'):
            return RiskLevel.CAUTION
        return RiskLevel.SAFE
    
    def _generate_why_recommended(self, ingredient: Dict, goals: List[str]) -> str:
        matched = ingredient['goals_matched']
        grade = ingredient['evidence_grade']
        biomarkers = ingredient['biomarkers']
        parts = []
        if len(matched) > 1:
            parts.append(f"Addresses {len(matched)} of your goals: {', '.join(matched)}")
        elif matched:
            parts.append(f"Supports your {matched[0]} goal")
        if grade == 'A':
            parts.append("Strong clinical evidence (Grade A)")
        elif grade == 'B':
            parts.append("Good clinical evidence (Grade B)")
        if biomarkers:
            parts.append(f"Targets: {biomarkers}")
        return ". ".join(parts) + "." if parts else "Recommended based on your goals."
    
    def generate_recommendations(self, profile: UserProfile) -> StackRecommendation:
        ingredients = self._get_ingredients_for_goals(profile.goals, profile.gender)
        primary_stack, secondary_stack, excluded = [], [], []
        
        for ing in ingredients:
            if ing['name'].lower() in [e.lower() for e in profile.exclude_ingredients]:
                excluded.append({'name': ing['name'], 'reason': 'User excluded'})
                continue
            
            drug_warnings = self._check_drug_interactions(ing['id'], profile.medications)
            condition_warnings = self._check_condition_contraindications(ing['id'], profile.conditions, ing['contraindications'])
            synergies = self._get_synergies(ing['id'])
            risk_level = self._calculate_risk_level(drug_warnings, condition_warnings, synergies, profile.medications)
            
            if risk_level == RiskLevel.AVOID:
                reasons = condition_warnings + [f"Interacts with {w['medication']}" for w in drug_warnings]
                excluded.append({'name': ing['name'], 'reason': '; '.join(reasons) if reasons else 'Contraindicated'})
                continue
            
            warnings = [f"Caution with {w['medication']}: {w['interaction']}" for w in drug_warnings]
            warnings.extend(condition_warnings)
            if synergies['antagonistic']:
                warnings.append(f"May reduce effect of: {', '.join(synergies['antagonistic'])}")
            
            product = self._get_supliful_product(ing['id'])
            
            rec = IngredientRecommendation(
                id=ing['id'], name=ing['name'], category=ing['category'],
                evidence_grade=ing['evidence_grade'], evidence_score=ing['evidence_score'],
                relevance_score=ing['relevance_score'], practical_dose=ing['practical_dose'],
                timing=ing['timing'], why_recommended=self._generate_why_recommended(ing, profile.goals),
                gender_note=ing['gender_note'], goals_addressed=ing['goals_matched'],
                synergies=synergies['synergistic'], warnings=warnings,
                risk_level=risk_level, supliful_product=product
            )
            
            is_primary = (ing['evidence_grade'] in ['A', 'B'] and ing['relevance_score'] >= 0.7 and 
                         risk_level == RiskLevel.SAFE and len(primary_stack) < 8)
            
            if is_primary:
                primary_stack.append(rec)
            elif len(secondary_stack) < 10:
                secondary_stack.append(rec)
        
        primary_stack.sort(key=lambda x: (-len(x.goals_addressed), x.evidence_grade, -x.relevance_score))
        secondary_stack.sort(key=lambda x: (-len(x.goals_addressed), x.evidence_grade))
        
        all_ids = [r.id for r in primary_stack + secondary_stack]
        os_modules = self._get_os_modules(all_ids)
        
        total_cost = sum(r.supliful_product['base_price'] for r in primary_stack if r.supliful_product and r.supliful_product.get('base_price'))
        
        caution_count = sum(1 for r in primary_stack if r.risk_level == RiskLevel.CAUTION)
        if caution_count == 0:
            safety_summary = "All primary recommendations are safe with no interactions detected."
        else:
            safety_summary = f"{caution_count} ingredient(s) require monitoring due to potential interactions."
        
        return StackRecommendation(
            user_profile=profile, primary_stack=primary_stack, secondary_stack=secondary_stack,
            excluded_ingredients=excluded, total_products=len(primary_stack),
            estimated_monthly_cost=total_cost, safety_summary=safety_summary, os_modules_activated=os_modules
        )
    
    def to_dict(self, rec: StackRecommendation) -> Dict:
        def ing_to_dict(r):
            return {
                'id': r.id, 'name': r.name, 'category': r.category,
                'evidence_grade': r.evidence_grade, 'evidence_score': r.evidence_score,
                'relevance_score': r.relevance_score, 'practical_dose': r.practical_dose,
                'timing': r.timing, 'why_recommended': r.why_recommended,
                'gender_note': r.gender_note, 'goals_addressed': r.goals_addressed,
                'synergies': r.synergies, 'warnings': r.warnings,
                'risk_level': r.risk_level.value, 'supliful_product': r.supliful_product
            }
        return {
            'user_profile': {'gender': rec.user_profile.gender.value, 'goals': rec.user_profile.goals,
                           'medications': rec.user_profile.medications, 'conditions': rec.user_profile.conditions},
            'primary_stack': [ing_to_dict(r) for r in rec.primary_stack],
            'secondary_stack': [ing_to_dict(r) for r in rec.secondary_stack],
            'excluded_ingredients': rec.excluded_ingredients,
            'summary': {'total_products': rec.total_products, 'estimated_monthly_cost': rec.estimated_monthly_cost,
                       'safety_summary': rec.safety_summary, 'os_modules_activated': rec.os_modules_activated}
        }


if __name__ == "__main__":
    engine = GenoMAXEngine()
    print("=" * 60)
    print("GENOMAX2 RECOMMENDATION ENGINE")
    print("=" * 60)
    
    profile = UserProfile(
        gender=Gender.MALE,
        goals=["Sleep Optimization", "Stress & Mood", "Energy & Focus"],
        medications=[],
        conditions=[]
    )
    
    print(f"\nUser: {profile.gender.value}")
    print(f"Goals: {', '.join(profile.goals)}")
    
    rec = engine.generate_recommendations(profile)
    
    print(f"\nPRIMARY STACK ({len(rec.primary_stack)} ingredients):")
    print("-" * 60)
    for i, r in enumerate(rec.primary_stack, 1):
        print(f"\n{i}. {r.name} (Grade {r.evidence_grade})")
        print(f"   Dose: {r.practical_dose}")
        print(f"   Timing: {r.timing}")
        if r.supliful_product:
            print(f"   Product: {r.supliful_product['name']}")
    
    print(f"\nOS Modules: {', '.join(rec.os_modules_activated)}")
    print("=" * 60)
