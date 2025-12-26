# Painpoints Dictionary for GenoMAX2 Brain
# Maps user-reported symptoms to supplement intents

PAINPOINTS_DICTIONARY = {
    "fatigue": {
        "label": "Fatigue / Low Energy",
        "mapped_intents": {
            "b12_energy_support": 0.85,
            "iron_energy_support": 0.80,
            "coq10_cellular_energy": 0.75,
            "vitamin_d_immune": 0.70
        }
    },
    "sleep_issues": {
        "label": "Sleep Problems",
        "mapped_intents": {
            "magnesium_for_sleep": 0.90,
            "glycine_for_sleep": 0.75,
            "melatonin_sleep": 0.70
        }
    },
    "stress": {
        "label": "Stress / Anxiety",
        "mapped_intents": {
            "magnesium_stress_support": 0.85,
            "adaptogen_support": 0.75
        }
    },
    "brain_fog": {
        "label": "Brain Fog / Poor Focus",
        "mapped_intents": {
            "omega3_brain_support": 0.85,
            "lions_mane_cognition": 0.75,
            "b12_energy_support": 0.65
        }
    },
    "muscle_weakness": {
        "label": "Muscle Weakness / Cramps",
        "mapped_intents": {
            "magnesium_stress_support": 0.80,
            "vitamin_d_immune": 0.70,
            "creatine_performance": 0.65
        }
    },
    "joint_pain": {
        "label": "Joint Pain / Stiffness",
        "mapped_intents": {
            "omega3_antiinflammatory": 0.85,
            "collagen_joint_support": 0.80,
            "curcumin_inflammation": 0.75
        }
    },
    "digestive_issues": {
        "label": "Digestive Problems",
        "mapped_intents": {
            "probiotic_support": 0.85,
            "digestive_enzyme_support": 0.75,
            "fiber_gut_health": 0.70
        }
    },
    "frequent_illness": {
        "label": "Frequent Colds / Weak Immunity",
        "mapped_intents": {
            "vitamin_d_immune": 0.90,
            "zinc_immune_support": 0.85,
            "vitamin_c_immune": 0.75
        }
    },
    "hair_skin_nails": {
        "label": "Hair / Skin / Nail Issues",
        "mapped_intents": {
            "collagen_joint_support": 0.85,
            "zinc_immune_support": 0.70
        }
    },
    "heart_palpitations": {
        "label": "Heart Palpitations",
        "mapped_intents": {
            "magnesium_heart": 0.85,
            "omega3_cardiovascular": 0.75,
            "coq10_heart_support": 0.70
        }
    },
    "inflammation": {
        "label": "Chronic Inflammation",
        "mapped_intents": {
            "omega3_antiinflammatory": 0.90,
            "curcumin_inflammation": 0.85
        }
    },
    "blood_sugar": {
        "label": "Blood Sugar Issues",
        "mapped_intents": {
            "berberine_glucose": 0.85
        }
    }
}

LIFESTYLE_SCHEMA = {
    "questions": [
        {"field": "sleep_hours", "label": "Average sleep per night", "type": "number", "min": 0, "max": 12},
        {"field": "sleep_quality", "label": "Sleep quality (self-rated)", "type": "scale", "min": 1, "max": 10},
        {"field": "stress_level", "label": "Stress level", "type": "scale", "min": 1, "max": 10},
        {"field": "activity_level", "label": "Physical activity level", "type": "enum", "options": ["sedentary", "light", "moderate", "high"]},
        {"field": "caffeine_intake", "label": "Caffeine intake", "type": "enum", "options": ["none", "low", "medium", "high"]},
        {"field": "alcohol_intake", "label": "Alcohol consumption", "type": "enum", "options": ["none", "low", "medium", "high"]},
        {"field": "work_schedule", "label": "Work schedule", "type": "enum", "options": ["day", "night", "rotating"]},
        {"field": "meals_per_day", "label": "Meals per day", "type": "number", "min": 0, "max": 5},
        {"field": "sugar_intake", "label": "Added sugar intake", "type": "enum", "options": ["low", "medium", "high"]},
        {"field": "smoking", "label": "Smoking", "type": "boolean"}
    ]
}
