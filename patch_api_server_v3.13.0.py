#!/usr/bin/env python3
"""
GenoMAX² API Server v3.13.0 Integration Script
Issue #6: Routing Layer Integration

Run this script to patch api_server.py with routing layer support.
Usage: python3 patch_api_server_v3.13.0.py

Changes applied:
1. Version update: 3.12.0 -> 3.13.0
2. Add routing layer import
3. Register routing router
4. Update /version endpoint with routing_version and "routing-layer" feature
"""

import re

def patch_api_server():
    # Read current file
    with open('api_server.py', 'r') as f:
        content = f.read()
    
    # 1. Update docstring version
    content = content.replace(
        'Version 3.12.0 - Catalog Governance Integration',
        'Version 3.13.0 - Routing Layer Integration'
    )
    
    # 2. Add v3.13.0 section to docstring
    v313_section = '''v3.13.0:
- Integrate Routing Layer endpoints (Issue #6)
- /api/v1/routing/health - Module health check
- /api/v1/routing/apply - Apply routing constraints to catalog
- /api/v1/routing/filter-gender - Filter by MAXimo2/MAXima2
- /api/v1/routing/requirements-coverage - Analyze requirements coverage
- /api/v1/routing/test-blocking - Admin test endpoint (requires X-Admin-API-Key)
- Pure safety elimination: PURE, ELIMINATIVE, DUMB, DETERMINISTIC

'''
    content = content.replace('v3.12.0:\n', v313_section + 'v3.12.0:\n')
    
    # 3. Add routing import after catalog import
    content = content.replace(
        'from app.catalog.admin import router as catalog_router',
        '''from app.catalog.admin import router as catalog_router

# Routing Layer imports (v3.13.0)
from app.routing.admin import router as routing_router'''
    )
    
    # 4. Register routing router
    content = content.replace(
        '# Register Catalog Governance admin router (v3.12.0)\napp.include_router(catalog_router)',
        '''# Register Catalog Governance admin router (v3.12.0)
app.include_router(catalog_router)

# Register Routing Layer router (v3.13.0)
app.include_router(routing_router)'''
    )
    
    # Also handle (NEW in v3.12.0) variant
    content = content.replace(
        '# Register Catalog Governance admin router (NEW in v3.12.0)\napp.include_router(catalog_router)',
        '''# Register Catalog Governance admin router (v3.12.0)
app.include_router(catalog_router)

# Register Routing Layer router (v3.13.0)
app.include_router(routing_router)'''
    )
    
    # 5. Update FastAPI version
    content = content.replace('version="3.12.0"', 'version="3.13.0"')
    
    # 6. Update root endpoint version
    content = content.replace('"version": "3.12.0"', '"version": "3.13.0"')
    
    # 7. Update /version endpoint - add routing_version
    content = content.replace(
        '"catalog_version": "catalog_governance_v1",\n        "contract_version"',
        '"catalog_version": "catalog_governance_v1",\n        "routing_version": "routing_layer_v1",\n        "contract_version"'
    )
    
    # 8. Add routing-layer feature
    content = content.replace(
        '"catalog-governance"\n        ]',
        '"catalog-governance",\n            "routing-layer"\n        ]'
    )
    
    # Write patched file
    with open('api_server.py', 'w') as f:
        f.write(content)
    
    print("✅ api_server.py patched to v3.13.0")
    print("   - Routing Layer endpoints integrated")
    print("   - Version updated to 3.13.0")
    print("   - routing_version added to /version")
    print("   - 'routing-layer' feature added")
    print("\n🚀 Commit and deploy to Railway!")

if __name__ == '__main__':
    patch_api_server()
