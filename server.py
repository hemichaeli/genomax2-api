"""
GenoMAX2 API Server Entry Point v3.10.3
This file consolidates all endpoint imports for Railway deployment.

Use this file for Railway deployment:
  uvicorn server:app --host 0.0.0.0 --port $PORT
"""

# Import base API (registers core endpoints)
from api_server import app

# Import Brain endpoints (registers /painpoints, /lifestyle-schema)
from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA

# Import Admin endpoints (registers /migrate-dig-natura-inactive, /admin/module-status)
import admin_endpoints  # noqa: F401


# ===== PAINPOINTS AND LIFESTYLE SCHEMA ENDPOINTS =====

@app.get("/api/v1/brain/painpoints")
def list_painpoints():
    """List available painpoints with their mappings to supplement intents."""
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


@app.get("/api/v1/brain/lifestyle-schema")
def get_lifestyle_schema():
    """Get the lifestyle questionnaire schema for frontend forms."""
    return LIFESTYLE_SCHEMA


# For local development
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
