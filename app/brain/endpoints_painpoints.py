"""
Brain Painpoints and Lifestyle Schema Endpoints
GenoMAX² API v3.10.1

These endpoints provide painpoint mappings and lifestyle questionnaire schema
for frontend form generation and intent routing.
"""

from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA


def get_painpoints_response():
    """Get list of available painpoints with their mapped intents."""
    return {
        "count": len(PAINPOINTS_DICTIONARY),
        "painpoints": [
            {
                "id": key,
                "label": val.get("label"),
                "mapped_intents": list(val.get("mapped_intents", {}).keys())
            }
            for key, val in PAINPOINTS_DICTIONARY.items()
        ]
    }


def get_lifestyle_schema_response():
    """Get the lifestyle questionnaire schema."""
    return LIFESTYLE_SCHEMA
