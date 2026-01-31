# =============================================================================
# Migration 016: os_environment Normalization
# Eliminates Universal - every product exists once per environment
# =============================================================================

import os
from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor

router = APIRouter(prefix="/api/v1/migrations", tags=["Migrations"])

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


# =============================================================================
# MIGRATION 016: os_environment Normalization
# =============================================================================

@router.post("/run/016-os-environment")
def run_migration_016() -> Dict[str, Any]:
    """
    Run migration 016: Normalize catalog_products to os_environment execution model.
    
    RULE: No Universal - every product exists once per environment (MAXimo² or MAXima²)
    
    Transformation:
      - male   -> 1 row: MAXimo²
      - female -> 1 row: MAXima²
      - unisex -> 2 rows: MAXimo² (-M suffix) + MAXima² (-F suffix)
    
    Current state: 76 MAXimo² + 74 MAXima² + 2 Universal = 152 products
    Target state:  78 MAXimo² + 76 MAXima² = 154 products
    
    NOTE: Temporarily disables triggers to bypass append-only governance for gx_catalog_id suffix changes.
    
    Safe to run multiple times (idempotent - checks if os_environment column exists and is populated).
    """
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    results = []
    
    try:
        cur = conn.cursor()
        
        # ===========================================
        # STEP 0: PRE-MIGRATION AUDIT
        # ===========================================
        cur.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(*) FILTER (WHERE sex_target = 'male') as male_count,
                COUNT(*) FILTER (WHERE sex_target = 'female') as female_count,
                COUNT(*) FILTER (WHERE sex_target = 'unisex') as unisex_count
            FROM catalog_products
        """)
        before_counts = dict(cur.fetchone())
        results.append(f"PRE-MIGRATION: {before_counts}")
        
        # Check if os_environment column already exists
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'catalog_products' AND column_name = 'os_environment'
        """)
        column_exists = cur.fetchone() is not None
        
        if column_exists:
            # Check if already populated
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE os_environment IS NOT NULL) as populated
                FROM catalog_products
            """)
            pop_check = dict(cur.fetchone())
            if pop_check['total'] == pop_check['populated'] and pop_check['total'] > 0:
                # Already migrated
                cur.execute("""
                    SELECT 
                        COUNT(*) FILTER (WHERE os_environment = 'MAXimo²') as maximo_count,
                        COUNT(*) FILTER (WHERE os_environment = 'MAXima²') as maxima_count,
                        COUNT(*) as total
                    FROM catalog_products
                """)
                final_counts = dict(cur.fetchone())
                conn.close()
                return {
                    "status": "skipped",
                    "migration": "016-os-environment",
                    "reason": "os_environment already populated",
                    "counts": final_counts
                }
        
        # ===========================================
        # STEP 1: ADD os_environment COLUMN
        # ===========================================
        if not column_exists:
            cur.execute("""
                ALTER TABLE catalog_products
                ADD COLUMN os_environment VARCHAR(20)
            """)
            results.append("Added os_environment column")
        
        # Add CHECK constraint (NO UNIVERSAL allowed)
        cur.execute("""
            ALTER TABLE catalog_products
            DROP CONSTRAINT IF EXISTS catalog_products_os_environment_check
        """)
        cur.execute("""
            ALTER TABLE catalog_products
            ADD CONSTRAINT catalog_products_os_environment_check 
            CHECK (os_environment IN ('MAXimo²', 'MAXima²'))
        """)
        results.append("Added CHECK constraint (MAXimo², MAXima² only)")
        
        # ===========================================
        # STEP 2: TEMPORARILY DISABLE TRIGGERS
        # (Required to modify gx_catalog_id which is normally immutable)
        # ===========================================
        cur.execute("""
            ALTER TABLE catalog_products DISABLE TRIGGER ALL
        """)
        results.append("Temporarily disabled triggers on catalog_products")
        
        # ===========================================
        # STEP 3: BACKFILL MALE -> MAXimo²
        # ===========================================
        cur.execute("""
            UPDATE catalog_products
            SET os_environment = 'MAXimo²'
            WHERE sex_target = 'male'
              AND os_environment IS NULL
            RETURNING gx_catalog_id
        """)
        male_updated = len(cur.fetchall())
        results.append(f"Backfilled {male_updated} male -> MAXimo²")
        
        # ===========================================
        # STEP 4: BACKFILL FEMALE -> MAXima²
        # ===========================================
        cur.execute("""
            UPDATE catalog_products
            SET os_environment = 'MAXima²'
            WHERE sex_target = 'female'
              AND os_environment IS NULL
            RETURNING gx_catalog_id
        """)
        female_updated = len(cur.fetchall())
        results.append(f"Backfilled {female_updated} female -> MAXima²")
        
        # ===========================================
        # STEP 5: HANDLE UNISEX -> DUPLICATE INTO BOTH ENVIRONMENTS
        # ===========================================
        
        # 5a. Get unisex products before modifying
        cur.execute("""
            SELECT gx_catalog_id, product_name
            FROM catalog_products
            WHERE sex_target = 'unisex'
              AND os_environment IS NULL
        """)
        unisex_products = [dict(r) for r in cur.fetchall()]
        results.append(f"Found {len(unisex_products)} unisex products to split")
        
        if len(unisex_products) > 0:
            # 5b. Update existing unisex rows to MAXimo² (add -M suffix)
            cur.execute("""
                UPDATE catalog_products
                SET 
                    os_environment = 'MAXimo²',
                    gx_catalog_id = gx_catalog_id || '-M'
                WHERE sex_target = 'unisex'
                  AND os_environment IS NULL
                RETURNING gx_catalog_id
            """)
            maximo_created = len(cur.fetchall())
            results.append(f"Updated {maximo_created} unisex -> MAXimo² with -M suffix")
            
            # 5c. Insert MAXima² duplicates for former unisex products
            cur.execute("""
                INSERT INTO catalog_products (
                    gx_catalog_id,
                    product_name,
                    product_url,
                    category,
                    sub_category,
                    short_description,
                    serving_info,
                    base_price,
                    currency,
                    evidence_tier,
                    governance_status,
                    block_reason,
                    ingredient_tags,
                    category_tags,
                    sex_target,
                    os_environment,
                    source_version,
                    shopify_enabled,
                    amazon_enabled,
                    tiktok_enabled,
                    created_at,
                    updated_at
                )
                SELECT
                    REPLACE(gx_catalog_id, '-M', '-F') as gx_catalog_id,
                    product_name,
                    product_url,
                    category,
                    sub_category,
                    short_description,
                    serving_info,
                    base_price,
                    currency,
                    evidence_tier,
                    governance_status,
                    block_reason,
                    ingredient_tags,
                    category_tags,
                    sex_target,
                    'MAXima²' as os_environment,
                    COALESCE(source_version, 'unknown') || '_dup_016',
                    shopify_enabled,
                    amazon_enabled,
                    tiktok_enabled,
                    created_at,
                    NOW() as updated_at
                FROM catalog_products
                WHERE os_environment = 'MAXimo²'
                  AND sex_target = 'unisex'
                  AND gx_catalog_id LIKE '%-M'
                ON CONFLICT (gx_catalog_id) DO NOTHING
                RETURNING gx_catalog_id
            """)
            maxima_created = len(cur.fetchall())
            results.append(f"Inserted {maxima_created} MAXima² duplicates with -F suffix")
        
        # ===========================================
        # STEP 6: RE-ENABLE TRIGGERS
        # ===========================================
        cur.execute("""
            ALTER TABLE catalog_products ENABLE TRIGGER ALL
        """)
        results.append("Re-enabled triggers on catalog_products")
        
        # ===========================================
        # STEP 7: VERIFY NO NULLs REMAIN
        # ===========================================
        cur.execute("""
            SELECT COUNT(*) as null_count
            FROM catalog_products
            WHERE os_environment IS NULL
        """)
        null_count = cur.fetchone()['null_count']
        
        if null_count > 0:
            conn.rollback()
            conn.close()
            raise HTTPException(
                status_code=500, 
                detail=f"Migration failed: {null_count} rows still have NULL os_environment"
            )
        
        # ===========================================
        # STEP 8: SET NOT NULL CONSTRAINT
        # ===========================================
        cur.execute("""
            ALTER TABLE catalog_products
            ALTER COLUMN os_environment SET NOT NULL
        """)
        results.append("Set os_environment to NOT NULL")
        
        # ===========================================
        # STEP 9: CREATE INDEXES
        # ===========================================
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_catalog_os_environment 
            ON catalog_products(os_environment)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_catalog_env_tier 
            ON catalog_products(os_environment, evidence_tier)
        """)
        results.append("Created indexes on os_environment")
        
        # ===========================================
        # STEP 10: UPDATE VIEW
        # ===========================================
        cur.execute("""
            CREATE OR REPLACE VIEW v_active_catalog AS
            SELECT 
                gx_catalog_id,
                product_name,
                category,
                evidence_tier,
                ingredient_tags,
                category_tags,
                os_environment,
                sex_target,
                base_price
            FROM catalog_products
            WHERE governance_status = 'ACTIVE'
            ORDER BY 
                os_environment,
                CASE evidence_tier 
                    WHEN 'TIER_1' THEN 1 
                    WHEN 'TIER_2' THEN 2 
                    ELSE 3 
                END,
                product_name
        """)
        results.append("Updated v_active_catalog view")
        
        # ===========================================
        # STEP 11: POST-MIGRATION AUDIT
        # ===========================================
        cur.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(*) FILTER (WHERE os_environment = 'MAXimo²') as maximo_count,
                COUNT(*) FILTER (WHERE os_environment = 'MAXima²') as maxima_count,
                COUNT(*) FILTER (WHERE sex_target = 'unisex') as former_unisex_now_split
            FROM catalog_products
        """)
        after_counts = dict(cur.fetchone())
        results.append(f"POST-MIGRATION: {after_counts}")
        
        # ===========================================
        # STEP 12: LOG TO GOVERNANCE STATS
        # ===========================================
        cur.execute("""
            INSERT INTO catalog_governance_stats (
                total_products, 
                tier1_count, 
                tier2_count, 
                tier3_count, 
                active_count, 
                blocked_count, 
                pending_count, 
                version
            )
            SELECT 
                COUNT(*) as total_products,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_1') as tier1_count,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_2') as tier2_count,
                COUNT(*) FILTER (WHERE evidence_tier = 'TIER_3') as tier3_count,
                COUNT(*) FILTER (WHERE governance_status = 'ACTIVE') as active_count,
                COUNT(*) FILTER (WHERE governance_status = 'BLOCKED') as blocked_count,
                COUNT(*) FILTER (WHERE governance_status = 'PENDING') as pending_count,
                'os_environment_v3.42.0' as version
            FROM catalog_products
        """)
        results.append("Logged governance stats snapshot")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "migration": "016-os-environment",
            "before": before_counts,
            "after": after_counts,
            "unisex_split": len(unisex_products),
            "steps": results,
            "validation": {
                "no_universal": True,
                "all_populated": True,
                "environments": ["MAXimo²", "MAXima²"]
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        try:
            # Re-enable triggers on error
            cur.execute("ALTER TABLE catalog_products ENABLE TRIGGER ALL")
            conn.rollback()
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Migration error: {str(e)}")


@router.get("/status/016-os-environment")
def check_migration_016() -> Dict[str, Any]:
    """Check if migration 016 has been applied."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Check if os_environment column exists
        cur.execute("""
            SELECT column_name, is_nullable FROM information_schema.columns 
            WHERE table_name = 'catalog_products' AND column_name = 'os_environment'
        """)
        column_info = cur.fetchone()
        column_exists = column_info is not None
        is_not_null = column_info['is_nullable'] == 'NO' if column_info else False
        
        if not column_exists:
            cur.close()
            conn.close()
            return {
                "migration": "016-os-environment",
                "applied": False,
                "reason": "os_environment column does not exist",
                "ready_to_run": True
            }
        
        # Check current counts
        cur.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(*) FILTER (WHERE os_environment = 'MAXimo²') as maximo_count,
                COUNT(*) FILTER (WHERE os_environment = 'MAXima²') as maxima_count,
                COUNT(*) FILTER (WHERE os_environment IS NULL) as null_count,
                COUNT(*) FILTER (WHERE sex_target = 'unisex') as unisex_count
            FROM catalog_products
        """)
        counts = dict(cur.fetchone())
        
        cur.close()
        conn.close()
        
        # Migration is applied if:
        # 1. Column exists
        # 2. No NULL values
        # 3. NOT NULL constraint is set
        applied = (
            column_exists and 
            counts['null_count'] == 0 and 
            is_not_null
        )
        
        return {
            "migration": "016-os-environment",
            "applied": applied,
            "column_exists": column_exists,
            "is_not_null": is_not_null,
            "counts": counts,
            "environments": ["MAXimo²", "MAXima²"],
            "note": "Applied when os_environment is NOT NULL and fully populated"
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Check error: {str(e)}")


@router.get("/preview/016-os-environment")
def preview_migration_016() -> Dict[str, Any]:
    """Preview what migration 016 will do without executing."""
    conn = get_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor()
        
        # Get current state
        cur.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(*) FILTER (WHERE sex_target = 'male') as male_count,
                COUNT(*) FILTER (WHERE sex_target = 'female') as female_count,
                COUNT(*) FILTER (WHERE sex_target = 'unisex') as unisex_count
            FROM catalog_products
        """)
        current = dict(cur.fetchone())
        
        # Get unisex products that will be split
        cur.execute("""
            SELECT gx_catalog_id, product_name, evidence_tier
            FROM catalog_products
            WHERE sex_target = 'unisex'
            ORDER BY gx_catalog_id
        """)
        unisex_products = [dict(r) for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        # Calculate expected outcome
        expected_maximo = current['male_count'] + current['unisex_count']
        expected_maxima = current['female_count'] + current['unisex_count']
        expected_total = expected_maximo + expected_maxima
        
        return {
            "migration": "016-os-environment",
            "current_state": current,
            "unisex_to_split": unisex_products,
            "expected_outcome": {
                "maximo_count": expected_maximo,
                "maxima_count": expected_maxima,
                "total_products": expected_total,
                "products_added": current['unisex_count']
            },
            "transformations": [
                f"{current['male_count']} male -> MAXimo²",
                f"{current['female_count']} female -> MAXima²",
                f"{current['unisex_count']} unisex -> {current['unisex_count']} MAXimo² + {current['unisex_count']} MAXima²"
            ]
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Preview error: {str(e)}")
