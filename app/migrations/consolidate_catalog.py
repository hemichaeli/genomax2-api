"""
Migration 016: Consolidate Catalog Systems
Version: 3.40.0

PURPOSE:
- Add version_note field to catalog_products for traceability
- Mark legacy products with migration note
- Deprecate hardcoded SuplifulCatalogManager in favor of database CatalogWiring

CHANGES:
1. ALTER TABLE catalog_products ADD COLUMN version_note VARCHAR(100)
2. UPDATE all products with version_note = 'v3.40.0_legacy_consolidation'

RESULT: Single source of truth in catalog_products table (151 products, all TIER_1/TIER_2)
"""

from fastapi import APIRouter
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os

router = APIRouter(prefix="/api/v1/migrations", tags=["migrations"])

def get_db_connection():
    import psycopg2
    return psycopg2.connect(os.environ.get("DATABASE_URL"))

@router.post("/run/consolidate-catalog")
def run_consolidate_catalog():
    """Add version_note column and mark all products."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Step 1: Add version_note column if not exists
        cur.execute("""
            ALTER TABLE catalog_products 
            ADD COLUMN IF NOT EXISTS version_note VARCHAR(100)
        """)
        
        # Step 2: Update all products with migration note
        cur.execute("""
            UPDATE catalog_products 
            SET version_note = 'v3.40.0_legacy_consolidation'
            WHERE version_note IS NULL
        """)
        updated_count = cur.rowcount
        
        # Step 3: Verify
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE version_note IS NOT NULL) as with_note,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_3') as tier3
            FROM catalog_products
            WHERE governance_status = 'ACTIVE'
        """)
        stats = cur.fetchone()
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "016_consolidate_catalog",
            "version": "3.40.0",
            "changes": {
                "column_added": "version_note VARCHAR(100)",
                "products_updated": updated_count
            },
            "verification": {
                "total_active": stats['total'],
                "with_version_note": stats['with_note'],
                "tier1": stats['tier1'],
                "tier2": stats['tier2'],
                "tier3": stats['tier3']
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.get("/status/consolidate-catalog")
def check_consolidate_status():
    """Check consolidation migration status."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check if version_note column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'catalog_products' AND column_name = 'version_note'
        """)
        column_exists = cur.fetchone() is not None
        
        if column_exists:
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE version_note IS NOT NULL) as with_note,
                    COUNT(*) FILTER (WHERE governance_status = 'ACTIVE') as active
                FROM catalog_products
            """)
            stats = cur.fetchone()
        else:
            stats = {"total": 0, "with_note": 0, "active": 0}
        
        cur.close()
        conn.close()
        
        return {
            "migration": "016_consolidate_catalog",
            "version_note_column_exists": column_exists,
            "stats": dict(stats) if stats else {},
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {"status": "error", "error": str(e)}
