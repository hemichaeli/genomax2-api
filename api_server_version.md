"""
GenoMAX² API Server
Gender-Optimized Biological Operating System
Version 3.25.0 - Copy Cleanup System

v3.25.0:
- Add Copy Cleanup module for placeholder removal
- GET /api/v1/qa/copy/placeholders/list - List modules with placeholders
- POST /api/v1/copy/cleanup/dry-run - Preview cleanup
- POST /api/v1/copy/cleanup/execute - Execute with audit logging
- GET /api/v1/qa/copy/clean/count - Count placeholder-free modules
- Renders deterministic front/back labels and Shopify body
- Strips TBD/MISSING/REVIEW/PLACEHOLDER tokens
- Full audit trail via copy_cleanup_audit_v1 table

v3.24.0:
- Add Shopify Integration for product export
- Health check, readiness summary, dry-run, publish endpoints
- Idempotent upsert by handle with metafields

v3.21.0:
- Add Excel Override endpoints for catalog data sync
- POST /api/v1/catalog/override/preflight - Validate Excel structure
- POST /api/v1/catalog/override/dry-run - Compute diffs without changes
- POST /api/v1/catalog/override/execute - Execute override with confirm=True
- GET /api/v1/catalog/override/logs/{batch_id} - Retrieve audit logs
- Implements Option D: Primary ingredient classification + Aggregate safety
"""

# Note: Full file content maintained - only version bumped from 3.21.0 to 3.25.0
# This is a stub comment - the actual file is committed via GitHub MCP
