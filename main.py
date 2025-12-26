"""
GenoMAX2 API Server Entry Point
Includes the brain_router with /painpoints and /lifestyle-schema endpoints.

Use this file for Railway deployment:
  uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from api_server import app
from app.brain import brain_router

# Include the brain_router (has /painpoints and /lifestyle-schema)
app.include_router(brain_router)

# For local development
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
