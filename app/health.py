"""
GenoMAX² Deployment Health Check
================================
Endpoint to verify deployment status and migration completion.
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import os
from datetime import datetime

router = APIRouter(prefix="/api/v1/health", tags=["Health"])


class HealthCheckResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: str
    api_version: str
    bloodwork_engine_version: str
    database_connected: bool
    migrations: Dict[str, Any]
    tables: Dict[str, bool]
    endpoints_registered: List[str]


def get_db_connection():
    """Get database connection."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    
    return psycopg2.connect(
        host=os.environ.get('PGHOST', 'localhost'),
        port=os.environ.get('PGPORT', '5432'),
        database=os.environ.get('PGDATABASE', 'railway'),
        user=os.environ.get('PGUSER', 'postgres'),
        password=os.environ.get('PGPASSWORD', ''),
        cursor_factory=RealDictCursor
    )


@router.get("/deployment", response_model=HealthCheckResponse)
def check_deployment_health():
    """
    Comprehensive deployment health check.
    
    Verifies:
    - Database connectivity
    - Migration status (specifically 013_bloodwork_v2_schema.sql)
    - Required tables exist
    - Bloodwork Engine v2.0 loaded
    """
    result = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "api_version": "3.28.0",
        "bloodwork_engine_version": "2.0.0",
        "database_connected": False,
        "migrations": {
            "checked": False,
            "migrations_table_exists": False,
            "total_executed": 0,
            "bloodwork_v2_migration": {
                "filename": "013_bloodwork_v2_schema.sql",
                "executed": False,
                "executed_at": None
            }
        },
        "tables": {
            "routing_constraint_blocks": False,
            "bloodwork_uploads": False,
            "bloodwork_results": False,
            "bloodwork_markers": False
        },
        "endpoints_registered": []
    }
    
    # Check bloodwork engine
    try:
        from bloodwork_engine import __version__ as bw_version
        result["bloodwork_engine_version"] = bw_version
    except:
        result["status"] = "degraded"
    
    # Check database
    try:
        conn = get_db_connection()
        result["database_connected"] = True
        cur = conn.cursor()
        
        # Check migrations table
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '_migrations'
            )
        """)
        result["migrations"]["migrations_table_exists"] = cur.fetchone()[0]
        
        if result["migrations"]["migrations_table_exists"]:
            # Get migration count
            cur.execute("SELECT COUNT(*) FROM _migrations WHERE success = true")
            result["migrations"]["total_executed"] = cur.fetchone()[0]
            result["migrations"]["checked"] = True
            
            # Check specific migration
            cur.execute("""
                SELECT filename, executed_at, success, error_message
                FROM _migrations 
                WHERE filename = '013_bloodwork_v2_schema.sql'
            """)
            row = cur.fetchone()
            if row:
                result["migrations"]["bloodwork_v2_migration"]["executed"] = row["success"]
                result["migrations"]["bloodwork_v2_migration"]["executed_at"] = str(row["executed_at"]) if row["executed_at"] else None
                if not row["success"]:
                    result["migrations"]["bloodwork_v2_migration"]["error"] = row["error_message"]
                    result["status"] = "unhealthy"
        
        # Check required tables
        for table in ["routing_constraint_blocks", "bloodwork_uploads", "bloodwork_results", "bloodwork_markers"]:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table,))
            result["tables"][table] = cur.fetchone()[0]
        
        # Check routing_constraint_blocks has data
        if result["tables"]["routing_constraint_blocks"]:
            cur.execute("SELECT COUNT(*) FROM routing_constraint_blocks")
            count = cur.fetchone()[0]
            result["tables"]["routing_constraint_blocks_count"] = count
            if count == 0:
                result["status"] = "degraded"
        
        cur.close()
        conn.close()
        
        # Check if all tables exist
        if not all(result["tables"].values()):
            result["status"] = "unhealthy"
            
    except Exception as e:
        result["status"] = "unhealthy"
        result["database_error"] = str(e)
    
    # List registered bloodwork endpoints
    result["endpoints_registered"] = [
        "/api/v1/bloodwork/status",
        "/api/v1/bloodwork/markers",
        "/api/v1/bloodwork/markers/{code}",
        "/api/v1/bloodwork/lab-profiles",
        "/api/v1/bloodwork/reference-ranges",
        "/api/v1/bloodwork/safety-gates",
        "/api/v1/bloodwork/computed-markers",
        "/api/v1/bloodwork/process",
        "/api/v1/bloodwork/reload"
    ]
    
    return result


@router.get("/migrations")
def check_migration_status():
    """
    Detailed migration status check.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get all migrations
        cur.execute("""
            SELECT filename, executed_at, success, error_message, checksum
            FROM _migrations 
            ORDER BY filename
        """)
        migrations = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "total": len(migrations),
            "successful": len([m for m in migrations if m["success"]]),
            "failed": len([m for m in migrations if not m["success"]]),
            "migrations": migrations
        }
        
    except Exception as e:
        return {"error": str(e)}


@router.post("/run-migration/{filename}")
def run_specific_migration(filename: str):
    """
    Run a specific migration file by name.
    Use with caution - primarily for fixing failed migrations.
    """
    from pathlib import Path
    
    try:
        conn = get_db_connection()
        
        # Find the migration file
        migrations_dir = Path(__file__).parent.parent / "migrations"
        migration_file = migrations_dir / filename
        
        if not migration_file.exists():
            return {"error": f"Migration file not found: {filename}"}
        
        content = migration_file.read_text()
        
        cur = conn.cursor()
        cur.execute(content)
        
        # Record the migration
        import hashlib
        checksum = hashlib.sha256(content.encode()).hexdigest()
        
        cur.execute("""
            INSERT INTO _migrations (filename, checksum, success)
            VALUES (%s, %s, true)
            ON CONFLICT (filename) DO UPDATE SET
                executed_at = NOW(),
                checksum = EXCLUDED.checksum,
                success = true,
                error_message = NULL
        """, (filename, checksum))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "filename": filename,
            "message": "Migration executed successfully"
        }
        
    except Exception as e:
        return {"error": str(e)}
