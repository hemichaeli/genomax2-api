feat(api): v3.15.1 - Add Painpoints & Lifestyle Schema Endpoints (Issue #2)

This commit adds two new endpoints for frontend consumption:

1. GET /api/v1/brain/painpoints
   - Returns the painpoints dictionary mapping user-reported symptoms to supplement intents
   - Includes priority scores for each intent

2. GET /api/v1/brain/lifestyle-schema  
   - Returns the lifestyle assessment schema for frontend form generation
   - Defines questions for sleep, stress, activity, diet factors

Changes:
- Import PAINPOINTS_DICTIONARY and LIFESTYLE_SCHEMA from app.brain.painpoints_data
- Update version to 3.15.1
- Update brain_version to 1.5.1
- Add painpoints and lifestyle-schema to features list
- Add new endpoints under /api/v1/brain/ prefix

This closes Issue #2 and enables the frontend to dynamically render
the assessment wizard based on backend-defined schemas.