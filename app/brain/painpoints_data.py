"""
GenoMAX² Painpoints Dictionary and Lifestyle Schema
Version 1.0.0 - Issue #2

This module contains:
1. PAINPOINTS_DICTIONARY: Maps user-reported symptoms to supplement intents
2. LIFESTYLE_SCHEMA: Defines lifestyle assessment questions for frontend forms

Principle: User symptoms drive intent generation, not product selection.
The Brain decides based on constraints; painpoints inform priorities.
"""

from typing import Dict, List, Any

# ============================================================================
# PAINPOINTS DICTIONARY
# Maps user-reported symptoms/concerns to supplement intents with priorities
# ============================================================================

PAINPOINTS_DICTIONARY: Dict[str, Dict[str, Any]] = {
    # ===== ENERGY & FATIGUE =====
    "fatigue": {
        "display_name": "Chronic Fatigue",
        "category": "energy",
        "description": "Persistent tiredness that doesn't improve with rest",
        "intents": [
            {"intent_id": "b12_energy_support", "priority": 0.90, "reason": "B12 deficiency is a common cause of fatigue"},
            {"intent_id": "iron_energy_support", "priority": 0.85, "reason": "Iron deficiency anemia causes fatigue"},
            {"intent_id": "coq10_cellular_energy", "priority": 0.75, "reason": "CoQ10 supports mitochondrial energy production"},
            {"intent_id": "vitamin_d_immune", "priority": 0.70, "reason": "Low vitamin D linked to fatigue"}
        ],
        "follow_up_questions": ["How long have you experienced this?", "Does it worsen at specific times?"],
        "contraindication_flags": []
    },
    "afternoon_crash": {
        "display_name": "Afternoon Energy Crash",
        "category": "energy",
        "description": "Energy drops significantly in the afternoon",
        "intents": [
            {"intent_id": "blood_sugar_stability", "priority": 0.85, "reason": "Blood sugar fluctuations cause energy crashes"},
            {"intent_id": "b12_energy_support", "priority": 0.75, "reason": "B vitamins support sustained energy"},
            {"intent_id": "coq10_cellular_energy", "priority": 0.70, "reason": "CoQ10 supports cellular energy"}
        ],
        "follow_up_questions": ["What do you typically eat for lunch?", "Do you consume caffeine?"],
        "contraindication_flags": []
    },
    "low_stamina": {
        "display_name": "Low Physical Stamina",
        "category": "energy",
        "description": "Difficulty sustaining physical activity",
        "intents": [
            {"intent_id": "iron_energy_support", "priority": 0.85, "reason": "Iron supports oxygen transport"},
            {"intent_id": "coq10_cellular_energy", "priority": 0.80, "reason": "CoQ10 supports muscle energy"},
            {"intent_id": "creatine_performance", "priority": 0.75, "reason": "Creatine supports ATP regeneration"}
        ],
        "follow_up_questions": ["How often do you exercise?", "Do you experience shortness of breath?"],
        "contraindication_flags": ["kidney_impairment"]
    },

    # ===== SLEEP ISSUES =====
    "insomnia": {
        "display_name": "Difficulty Falling Asleep",
        "category": "sleep",
        "description": "Trouble initiating sleep at bedtime",
        "intents": [
            {"intent_id": "magnesium_for_sleep", "priority": 0.90, "reason": "Magnesium supports GABA activity and relaxation"},
            {"intent_id": "glycine_for_sleep", "priority": 0.80, "reason": "Glycine lowers core body temperature"},
            {"intent_id": "melatonin_sleep", "priority": 0.75, "reason": "Melatonin regulates sleep-wake cycle"}
        ],
        "follow_up_questions": ["What time do you try to sleep?", "Screen use before bed?"],
        "contraindication_flags": []
    },
    "poor_sleep_quality": {
        "display_name": "Poor Sleep Quality",
        "category": "sleep",
        "description": "Waking frequently or feeling unrefreshed",
        "intents": [
            {"intent_id": "magnesium_for_sleep", "priority": 0.85, "reason": "Magnesium supports deep sleep"},
            {"intent_id": "glycine_for_sleep", "priority": 0.80, "reason": "Glycine improves sleep quality"},
            {"intent_id": "regulate_circadian_rhythm", "priority": 0.70, "reason": "Circadian alignment improves sleep"}
        ],
        "follow_up_questions": ["How many hours do you sleep?", "Do you wake up during the night?"],
        "contraindication_flags": []
    },
    "waking_unrefreshed": {
        "display_name": "Waking Up Tired",
        "category": "sleep",
        "description": "Not feeling rested despite adequate sleep hours",
        "intents": [
            {"intent_id": "magnesium_for_sleep", "priority": 0.85, "reason": "Magnesium supports sleep architecture"},
            {"intent_id": "b12_energy_support", "priority": 0.75, "reason": "B12 supports energy upon waking"},
            {"intent_id": "coq10_cellular_energy", "priority": 0.70, "reason": "CoQ10 supports morning energy"}
        ],
        "follow_up_questions": ["Do you snore?", "How many hours of sleep?"],
        "contraindication_flags": []
    },

    # ===== COGNITIVE & FOCUS =====
    "brain_fog": {
        "display_name": "Brain Fog",
        "category": "cognitive",
        "description": "Difficulty thinking clearly or concentrating",
        "intents": [
            {"intent_id": "omega3_brain_support", "priority": 0.90, "reason": "Omega-3s support brain function and clarity"},
            {"intent_id": "b12_energy_support", "priority": 0.80, "reason": "B12 deficiency causes cognitive issues"},
            {"intent_id": "lions_mane_cognition", "priority": 0.75, "reason": "Lion's mane supports nerve growth factor"}
        ],
        "follow_up_questions": ["When is it worst?", "Any memory issues?"],
        "contraindication_flags": []
    },
    "poor_concentration": {
        "display_name": "Poor Concentration",
        "category": "cognitive",
        "description": "Difficulty maintaining focus on tasks",
        "intents": [
            {"intent_id": "omega3_brain_support", "priority": 0.85, "reason": "DHA supports attention and focus"},
            {"intent_id": "lions_mane_cognition", "priority": 0.80, "reason": "Lion's mane supports cognitive function"},
            {"intent_id": "enhance_cognitive_function", "priority": 0.70, "reason": "Lifestyle factors affect focus"}
        ],
        "follow_up_questions": ["How long can you focus?", "Better in morning or evening?"],
        "contraindication_flags": []
    },
    "memory_issues": {
        "display_name": "Memory Concerns",
        "category": "cognitive",
        "description": "Difficulty remembering things or mental clarity",
        "intents": [
            {"intent_id": "omega3_brain_support", "priority": 0.90, "reason": "DHA is critical for memory function"},
            {"intent_id": "lions_mane_cognition", "priority": 0.85, "reason": "Lion's mane supports neuroplasticity"},
            {"intent_id": "b12_energy_support", "priority": 0.75, "reason": "B12 supports nervous system health"}
        ],
        "follow_up_questions": ["Short-term or long-term memory?", "Any family history?"],
        "contraindication_flags": []
    },

    # ===== STRESS & MOOD =====
    "chronic_stress": {
        "display_name": "Chronic Stress",
        "category": "stress",
        "description": "Persistent feelings of being overwhelmed",
        "intents": [
            {"intent_id": "magnesium_stress_support", "priority": 0.90, "reason": "Magnesium is depleted by stress"},
            {"intent_id": "adaptogen_support", "priority": 0.85, "reason": "Adaptogens modulate stress response"},
            {"intent_id": "reduce_stress_response", "priority": 0.75, "reason": "Lifestyle interventions reduce stress"}
        ],
        "follow_up_questions": ["Work or personal stress?", "Physical symptoms?"],
        "contraindication_flags": []
    },
    "anxiety": {
        "display_name": "Anxiety & Nervousness",
        "category": "stress",
        "description": "Feelings of worry, nervousness, or unease",
        "intents": [
            {"intent_id": "magnesium_stress_support", "priority": 0.90, "reason": "Magnesium supports GABA and calmness"},
            {"intent_id": "omega3_brain_support", "priority": 0.80, "reason": "Omega-3s support mood regulation"},
            {"intent_id": "breathwork_practice", "priority": 0.75, "reason": "Breathwork activates parasympathetic system"}
        ],
        "follow_up_questions": ["Situational or constant?", "Physical symptoms like racing heart?"],
        "contraindication_flags": []
    },
    "low_mood": {
        "display_name": "Low Mood",
        "category": "mood",
        "description": "Persistent feelings of sadness or low motivation",
        "intents": [
            {"intent_id": "omega3_brain_support", "priority": 0.85, "reason": "Omega-3s support mood and serotonin"},
            {"intent_id": "vitamin_d_immune", "priority": 0.85, "reason": "Low vitamin D linked to mood issues"},
            {"intent_id": "b12_energy_support", "priority": 0.75, "reason": "B12 supports neurotransmitter synthesis"}
        ],
        "follow_up_questions": ["How long have you felt this way?", "Seasonal pattern?"],
        "contraindication_flags": []
    },

    # ===== DIGESTIVE ISSUES =====
    "bloating": {
        "display_name": "Bloating",
        "category": "digestion",
        "description": "Feeling of fullness or swelling in the abdomen",
        "intents": [
            {"intent_id": "digestive_enzyme_support", "priority": 0.85, "reason": "Enzymes support food breakdown"},
            {"intent_id": "probiotic_support", "priority": 0.80, "reason": "Probiotics support gut balance"},
            {"intent_id": "fiber_gut_health", "priority": 0.70, "reason": "Fiber supports healthy digestion"}
        ],
        "follow_up_questions": ["After specific foods?", "How often?"],
        "contraindication_flags": []
    },
    "irregular_digestion": {
        "display_name": "Irregular Digestion",
        "category": "digestion",
        "description": "Constipation, diarrhea, or irregular bowel movements",
        "intents": [
            {"intent_id": "probiotic_support", "priority": 0.85, "reason": "Probiotics support gut motility"},
            {"intent_id": "fiber_gut_health", "priority": 0.80, "reason": "Fiber regulates bowel movements"},
            {"intent_id": "magnesium_stress_support", "priority": 0.70, "reason": "Magnesium supports muscle relaxation"}
        ],
        "follow_up_questions": ["Constipation or diarrhea?", "Water intake?"],
        "contraindication_flags": []
    },
    "food_sensitivities": {
        "display_name": "Food Sensitivities",
        "category": "digestion",
        "description": "Discomfort after eating certain foods",
        "intents": [
            {"intent_id": "digestive_enzyme_support", "priority": 0.85, "reason": "Enzymes help break down problem foods"},
            {"intent_id": "probiotic_support", "priority": 0.80, "reason": "Probiotics support gut barrier"},
            {"intent_id": "gut_health_optimization", "priority": 0.75, "reason": "Gut health reduces sensitivities"}
        ],
        "follow_up_questions": ["Which foods?", "Symptoms experienced?"],
        "contraindication_flags": []
    },

    # ===== IMMUNE & RECOVERY =====
    "frequent_illness": {
        "display_name": "Frequent Colds/Illness",
        "category": "immunity",
        "description": "Getting sick more often than usual",
        "intents": [
            {"intent_id": "vitamin_d_immune", "priority": 0.90, "reason": "Vitamin D is critical for immune function"},
            {"intent_id": "zinc_immune_support", "priority": 0.85, "reason": "Zinc supports immune cell function"},
            {"intent_id": "vitamin_c_immune", "priority": 0.80, "reason": "Vitamin C supports immune response"}
        ],
        "follow_up_questions": ["How often do you get sick?", "Any chronic conditions?"],
        "contraindication_flags": ["hypercalcemia"]
    },
    "slow_recovery": {
        "display_name": "Slow Recovery",
        "category": "immunity",
        "description": "Takes longer than normal to recover from illness or exercise",
        "intents": [
            {"intent_id": "vitamin_d_immune", "priority": 0.85, "reason": "Vitamin D supports recovery"},
            {"intent_id": "zinc_immune_support", "priority": 0.80, "reason": "Zinc supports tissue repair"},
            {"intent_id": "protein_muscle_support", "priority": 0.75, "reason": "Protein supports recovery"}
        ],
        "follow_up_questions": ["From illness or exercise?", "How long does recovery take?"],
        "contraindication_flags": []
    },

    # ===== INFLAMMATION & PAIN =====
    "joint_pain": {
        "display_name": "Joint Pain/Stiffness",
        "category": "inflammation",
        "description": "Pain or stiffness in joints",
        "intents": [
            {"intent_id": "omega3_antiinflammatory", "priority": 0.90, "reason": "Omega-3s reduce inflammatory markers"},
            {"intent_id": "curcumin_inflammation", "priority": 0.85, "reason": "Curcumin is a potent anti-inflammatory"},
            {"intent_id": "collagen_joint_support", "priority": 0.80, "reason": "Collagen supports joint structure"}
        ],
        "follow_up_questions": ["Which joints?", "Worse in morning or after activity?"],
        "contraindication_flags": []
    },
    "muscle_soreness": {
        "display_name": "Muscle Soreness",
        "category": "inflammation",
        "description": "Persistent or excessive muscle soreness",
        "intents": [
            {"intent_id": "magnesium_stress_support", "priority": 0.85, "reason": "Magnesium supports muscle relaxation"},
            {"intent_id": "omega3_antiinflammatory", "priority": 0.80, "reason": "Omega-3s reduce inflammation"},
            {"intent_id": "protein_muscle_support", "priority": 0.75, "reason": "Protein supports muscle repair"}
        ],
        "follow_up_questions": ["Related to exercise?", "Specific areas?"],
        "contraindication_flags": []
    },
    "general_inflammation": {
        "display_name": "General Inflammation",
        "category": "inflammation",
        "description": "Signs of systemic inflammation",
        "intents": [
            {"intent_id": "omega3_antiinflammatory", "priority": 0.90, "reason": "Omega-3s are potent anti-inflammatories"},
            {"intent_id": "curcumin_inflammation", "priority": 0.85, "reason": "Curcumin reduces inflammatory markers"},
            {"intent_id": "anti_inflammatory_diet", "priority": 0.80, "reason": "Diet is foundational for inflammation"}
        ],
        "follow_up_questions": ["Any blood markers elevated?", "Chronic conditions?"],
        "contraindication_flags": []
    },

    # ===== HEART & CARDIOVASCULAR =====
    "heart_health_concern": {
        "display_name": "Heart Health Concerns",
        "category": "cardiovascular",
        "description": "Wanting to support cardiovascular health",
        "intents": [
            {"intent_id": "omega3_cardiovascular", "priority": 0.90, "reason": "Omega-3s support heart health"},
            {"intent_id": "coq10_heart_support", "priority": 0.85, "reason": "CoQ10 supports heart muscle"},
            {"intent_id": "magnesium_heart", "priority": 0.80, "reason": "Magnesium supports heart rhythm"}
        ],
        "follow_up_questions": ["Family history?", "Current blood pressure/cholesterol?"],
        "contraindication_flags": []
    },

    # ===== LIVER HEALTH =====
    "liver_support": {
        "display_name": "Liver Support Needed",
        "category": "liver",
        "description": "Elevated liver enzymes or wanting liver support",
        "intents": [
            {"intent_id": "milk_thistle_liver", "priority": 0.85, "reason": "Milk thistle supports liver function"},
            {"intent_id": "nac_liver_support", "priority": 0.80, "reason": "NAC supports glutathione production"},
            {"intent_id": "liver_supportive_diet", "priority": 0.75, "reason": "Diet supports liver health"}
        ],
        "follow_up_questions": ["Recent liver enzyme tests?", "Alcohol consumption?"],
        "contraindication_flags": ["hepatotoxic"]
    }
}


# ============================================================================
# LIFESTYLE SCHEMA
# Defines assessment questions for lifestyle factors
# Used by frontend to generate dynamic forms
# ============================================================================

LIFESTYLE_SCHEMA: Dict[str, Any] = {
    "version": "1.0.0",
    "description": "Lifestyle assessment schema for GenoMAX² protocol generation",
    "questions": [
        # ===== SLEEP ASSESSMENT =====
        {
            "id": "sleep_hours",
            "category": "sleep",
            "question": "How many hours of sleep do you typically get per night?",
            "type": "select",
            "options": [
                {"value": "less_than_5", "label": "Less than 5 hours", "flag": "severe_sleep_deficit"},
                {"value": "5_to_6", "label": "5-6 hours", "flag": "sleep_deficit"},
                {"value": "6_to_7", "label": "6-7 hours", "flag": "mild_sleep_deficit"},
                {"value": "7_to_8", "label": "7-8 hours", "flag": None},
                {"value": "8_to_9", "label": "8-9 hours", "flag": None},
                {"value": "more_than_9", "label": "More than 9 hours", "flag": "excessive_sleep"}
            ],
            "required": True
        },
        {
            "id": "sleep_quality",
            "category": "sleep",
            "question": "How would you rate your sleep quality?",
            "type": "scale",
            "min": 1,
            "max": 10,
            "labels": {"1": "Very Poor", "5": "Average", "10": "Excellent"},
            "thresholds": {"poor": 4, "average": 6, "good": 8},
            "required": True
        },
        {
            "id": "sleep_issues",
            "category": "sleep",
            "question": "Do you experience any of these sleep issues?",
            "type": "multiselect",
            "options": [
                {"value": "difficulty_falling_asleep", "label": "Difficulty falling asleep", "painpoint": "insomnia"},
                {"value": "waking_during_night", "label": "Waking during the night", "painpoint": "poor_sleep_quality"},
                {"value": "waking_too_early", "label": "Waking too early", "painpoint": "poor_sleep_quality"},
                {"value": "unrefreshed", "label": "Waking unrefreshed", "painpoint": "waking_unrefreshed"},
                {"value": "none", "label": "None of the above", "painpoint": None}
            ],
            "required": False
        },

        # ===== STRESS ASSESSMENT =====
        {
            "id": "stress_level",
            "category": "stress",
            "question": "How would you rate your current stress level?",
            "type": "scale",
            "min": 1,
            "max": 10,
            "labels": {"1": "Very Low", "5": "Moderate", "10": "Extremely High"},
            "thresholds": {"low": 3, "moderate": 6, "high": 8},
            "required": True
        },
        {
            "id": "stress_sources",
            "category": "stress",
            "question": "What are your main sources of stress?",
            "type": "multiselect",
            "options": [
                {"value": "work", "label": "Work/Career"},
                {"value": "relationships", "label": "Relationships"},
                {"value": "financial", "label": "Financial"},
                {"value": "health", "label": "Health concerns"},
                {"value": "family", "label": "Family responsibilities"},
                {"value": "other", "label": "Other"}
            ],
            "required": False
        },
        {
            "id": "stress_management",
            "category": "stress",
            "question": "Do you practice any stress management techniques?",
            "type": "multiselect",
            "options": [
                {"value": "meditation", "label": "Meditation"},
                {"value": "exercise", "label": "Exercise"},
                {"value": "breathing", "label": "Breathing exercises"},
                {"value": "therapy", "label": "Therapy/Counseling"},
                {"value": "hobbies", "label": "Hobbies"},
                {"value": "none", "label": "None currently"}
            ],
            "required": False
        },

        # ===== ACTIVITY ASSESSMENT =====
        {
            "id": "exercise_frequency",
            "category": "activity",
            "question": "How often do you exercise per week?",
            "type": "select",
            "options": [
                {"value": "never", "label": "Never", "flag": "sedentary"},
                {"value": "1_2_times", "label": "1-2 times", "flag": "low_activity"},
                {"value": "3_4_times", "label": "3-4 times", "flag": None},
                {"value": "5_6_times", "label": "5-6 times", "flag": None},
                {"value": "daily", "label": "Daily", "flag": "high_activity"}
            ],
            "required": True
        },
        {
            "id": "exercise_type",
            "category": "activity",
            "question": "What types of exercise do you do?",
            "type": "multiselect",
            "options": [
                {"value": "cardio", "label": "Cardio (running, cycling, swimming)"},
                {"value": "strength", "label": "Strength training"},
                {"value": "hiit", "label": "HIIT"},
                {"value": "yoga", "label": "Yoga/Pilates"},
                {"value": "sports", "label": "Sports"},
                {"value": "walking", "label": "Walking"},
                {"value": "other", "label": "Other"}
            ],
            "required": False
        },
        {
            "id": "daily_movement",
            "category": "activity",
            "question": "How would you describe your daily movement (outside of exercise)?",
            "type": "select",
            "options": [
                {"value": "mostly_sitting", "label": "Mostly sitting (desk job)", "flag": "sedentary_work"},
                {"value": "some_movement", "label": "Some movement throughout the day"},
                {"value": "active_job", "label": "Active job (on feet most of the day)"},
                {"value": "very_active", "label": "Very active (physical labor)"}
            ],
            "required": True
        },

        # ===== DIET ASSESSMENT =====
        {
            "id": "diet_type",
            "category": "diet",
            "question": "How would you describe your diet?",
            "type": "select",
            "options": [
                {"value": "standard", "label": "Standard/No restrictions"},
                {"value": "vegetarian", "label": "Vegetarian", "flag": "vegetarian"},
                {"value": "vegan", "label": "Vegan", "flag": "vegan"},
                {"value": "pescatarian", "label": "Pescatarian"},
                {"value": "keto", "label": "Keto/Low-carb"},
                {"value": "paleo", "label": "Paleo"},
                {"value": "mediterranean", "label": "Mediterranean"},
                {"value": "other", "label": "Other"}
            ],
            "required": True
        },
        {
            "id": "vegetable_intake",
            "category": "diet",
            "question": "How many servings of vegetables do you eat daily?",
            "type": "select",
            "options": [
                {"value": "0_1", "label": "0-1 servings", "flag": "low_vegetable_intake"},
                {"value": "2_3", "label": "2-3 servings"},
                {"value": "4_5", "label": "4-5 servings"},
                {"value": "6_plus", "label": "6+ servings"}
            ],
            "required": True
        },
        {
            "id": "fish_intake",
            "category": "diet",
            "question": "How often do you eat fatty fish (salmon, sardines, mackerel)?",
            "type": "select",
            "options": [
                {"value": "never", "label": "Never/Rarely", "flag": "low_omega3_dietary"},
                {"value": "monthly", "label": "A few times a month", "flag": "low_omega3_dietary"},
                {"value": "weekly", "label": "Once a week"},
                {"value": "2_3_weekly", "label": "2-3 times a week"},
                {"value": "daily", "label": "Daily or more"}
            ],
            "required": True
        },
        {
            "id": "processed_food",
            "category": "diet",
            "question": "How often do you eat processed or fast food?",
            "type": "select",
            "options": [
                {"value": "daily", "label": "Daily", "flag": "high_processed_food"},
                {"value": "several_weekly", "label": "Several times a week", "flag": "moderate_processed_food"},
                {"value": "weekly", "label": "About once a week"},
                {"value": "monthly", "label": "A few times a month"},
                {"value": "rarely", "label": "Rarely/Never"}
            ],
            "required": True
        },
        {
            "id": "water_intake",
            "category": "diet",
            "question": "How much water do you drink daily?",
            "type": "select",
            "options": [
                {"value": "less_than_4", "label": "Less than 4 glasses", "flag": "dehydration_risk"},
                {"value": "4_to_6", "label": "4-6 glasses"},
                {"value": "6_to_8", "label": "6-8 glasses"},
                {"value": "more_than_8", "label": "More than 8 glasses"}
            ],
            "required": True
        },

        # ===== SUBSTANCES =====
        {
            "id": "caffeine_intake",
            "category": "substances",
            "question": "How much caffeine do you consume daily?",
            "type": "select",
            "options": [
                {"value": "none", "label": "None"},
                {"value": "1_2_cups", "label": "1-2 cups of coffee/tea"},
                {"value": "3_4_cups", "label": "3-4 cups", "flag": "moderate_caffeine"},
                {"value": "5_plus", "label": "5+ cups", "flag": "high_caffeine"}
            ],
            "required": True
        },
        {
            "id": "alcohol_intake",
            "category": "substances",
            "question": "How often do you drink alcohol?",
            "type": "select",
            "options": [
                {"value": "never", "label": "Never"},
                {"value": "monthly", "label": "A few times a month"},
                {"value": "weekly", "label": "1-2 times a week"},
                {"value": "several_weekly", "label": "3-4 times a week", "flag": "regular_alcohol"},
                {"value": "daily", "label": "Daily", "flag": "daily_alcohol"}
            ],
            "required": True
        },
        {
            "id": "smoking",
            "category": "substances",
            "question": "Do you smoke or use tobacco?",
            "type": "select",
            "options": [
                {"value": "never", "label": "Never"},
                {"value": "former", "label": "Former smoker"},
                {"value": "occasional", "label": "Occasionally", "flag": "tobacco_use"},
                {"value": "regular", "label": "Regular smoker", "flag": "tobacco_use"}
            ],
            "required": True
        },

        # ===== SCREEN & LIGHT EXPOSURE =====
        {
            "id": "screen_before_bed",
            "category": "light_exposure",
            "question": "Do you use screens (phone, TV, computer) before bed?",
            "type": "select",
            "options": [
                {"value": "no", "label": "No, I avoid screens 1+ hour before bed"},
                {"value": "sometimes", "label": "Sometimes"},
                {"value": "usually", "label": "Usually, up to 30 mins before", "flag": "screen_exposure"},
                {"value": "always", "label": "Yes, right up until sleep", "flag": "high_screen_exposure"}
            ],
            "required": True
        },
        {
            "id": "morning_light",
            "category": "light_exposure",
            "question": "Do you get natural light exposure in the morning?",
            "type": "select",
            "options": [
                {"value": "yes_outdoor", "label": "Yes, outdoor sunlight"},
                {"value": "yes_indoor", "label": "Yes, bright indoor light"},
                {"value": "limited", "label": "Limited light exposure", "flag": "low_morning_light"},
                {"value": "no", "label": "No, minimal light", "flag": "low_morning_light"}
            ],
            "required": True
        }
    ],
    "categories": {
        "sleep": {"display_name": "Sleep", "icon": "moon", "order": 1},
        "stress": {"display_name": "Stress", "icon": "brain", "order": 2},
        "activity": {"display_name": "Activity", "icon": "activity", "order": 3},
        "diet": {"display_name": "Diet", "icon": "apple", "order": 4},
        "substances": {"display_name": "Substances", "icon": "coffee", "order": 5},
        "light_exposure": {"display_name": "Light & Screens", "icon": "sun", "order": 6}
    },
    "scoring": {
        "description": "Flags generated from responses inform constraint generation",
        "flag_categories": ["sleep", "stress", "nutrition", "activity", "circadian"]
    }
}
