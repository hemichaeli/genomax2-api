"""
GenoMAX2 API Server Entry Point v3.11.1
Adds Bloodwork Engine v1 endpoints with error handling.

Use this file for Railway deployment:
  uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from api_server import app
from app.brain.painpoints_data import PAINPOINTS_DICTIONARY, LIFESTYLE_SCHEMA

# ===== BLOODWORK ENGINE V1 =====
try:
    from bloodwork_engine.api import register_bloodwork_endpoints
    register_bloodwork_endpoints(app)
    print("✅ Bloodwork Engine v1 endpoints registered successfully")
except Exception as e:
    print(f"❌ ERROR loading Bloodwork Engine: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()


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
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
