"""
Deployment Health Check Endpoint
================================
Returns comprehensive status of the deployed GenoMAX² API.
"""

from fastapi import APIRouter
from datetime import datetime
import os

router = APIRouter(prefix="/api/v1/health", tags=["Health"])


@router.get("/deployment")
def deployment_health():
    """
    Comprehensive deployment health check.
    Verifies all components are operational.
    """
    from api_server import get_db
    
    status = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "api_version": "3.28.0",
        "environment": os.environ.get("RAILWAY_ENVIRONMENT", "unknown"),
        "components": {}
    }
    
    # Check Bloodwork Engine v2
    try:
        from bloodwork_engine import __version__ as bw_version
        from bloodwork_engine.engine_v2 import get_engine, get_loader
        
        loader = get_loader()
        engine = get_engine()
        
        status["components"]["bloodwork_engine"] = {
            "status": "healthy",
            "version": bw_version,
            "markers": len(loader.allowed_marker_codes),
            "safety_gates": len(loader.get_safety_gates()),
            "ruleset_version": loader.ruleset_version
        }
    except Exception as e:
        status["components"]["bloodwork_engine"] = {
            "status": "error",
            "error": str(e)
        }
    
    # Check Database Connection
    try:
        conn = get_db()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            conn.close()
            status["components"]["database"] = {"status": "healthy"}
        else:
            status["components"]["database"] = {"status": "error", "error": "Connection failed"}
    except Exception as e:
        status["components"]["database"] = {"status": "error", "error": str(e)}
    
    # Check Migration Status
    try:
        conn = get_db()
        if conn:
            cur = conn.cursor()
            
            # Check _migrations table
            cur.execute("""
                SELECT filename, executed_at, success 
                FROM _migrations 
                ORDER BY filename DESC 
                LIMIT 5
            """)
            migrations = [
                {"filename": row[0], "executed_at": str(row[1]), "success": row[2]} 
                for row in cur.fetchall()
            ]
            
            # Check bloodwork tables exist
            tables_check = {}
            for table in ['routing_constraint_blocks', 'bloodwork_uploads', 'bloodwork_results', 'bloodwork_markers']:
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table}'
                    )
                """)
                tables_check[table] = cur.fetchone()[0]
            
            # Count routing constraints
            rcb_count = 0
            if tables_check.get('routing_constraint_blocks'):
                cur.execute("SELECT COUNT(*) FROM routing_constraint_blocks")
                rcb_count = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            status["components"]["migrations"] = {
                "status": "healthy",
                "recent_migrations": migrations,
                "bloodwork_v2_tables": tables_check,
                "routing_constraints_seeded": rcb_count
            }
        else:
            status["components"]["migrations"] = {"status": "error", "error": "No connection"}
    except Exception as e:
        status["components"]["migrations"] = {"status": "error", "error": str(e)}
    
    # Check OCR Parser
    try:
        from bloodwork_engine.ocr_parser import OCRParser
        status["components"]["ocr_parser"] = {"status": "available"}
    except Exception as e:
        status["components"]["ocr_parser"] = {"status": "unavailable", "error": str(e)}
    
    # Check Lab Adapters
    try:
        from bloodwork_engine.lab_adapters import list_providers
        providers = list_providers()
        status["components"]["lab_adapters"] = {
            "status": "available",
            "providers": providers
        }
    except Exception as e:
        status["components"]["lab_adapters"] = {"status": "unavailable", "error": str(e)}
    
    # Check Safety Router
    try:
        from bloodwork_engine.safety_router import SafetyRouter
        status["components"]["safety_router"] = {"status": "available"}
    except Exception as e:
        status["components"]["safety_router"] = {"status": "unavailable", "error": str(e)}
    
    # Overall status
    all_healthy = all(
        c.get("status") in ["healthy", "available"] 
        for c in status["components"].values()
    )
    status["overall_status"] = "healthy" if all_healthy else "degraded"
    
    return status


@router.get("/quick")
def quick_health():
    """Quick health check for load balancers."""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}


@router.get("/bloodwork-v2")
def bloodwork_v2_health():
    """Detailed Bloodwork Engine v2 health check."""
    try:
        from bloodwork_engine.engine_v2 import get_engine, get_loader, GateTier
        
        loader = get_loader()
        engine = get_engine()
        
        # Get gate counts by tier - FIX: iterate over .items() not just dict
        gates = loader.get_safety_gates()
        tier_counts = {}
        for gate_id, gate in gates.items():  # Use .items() to get key-value pairs
            tier = gate.get('tier', 1)
            tier_counts[f"tier_{tier}"] = tier_counts.get(f"tier_{tier}", 0) + 1
        
        # Test marker processing
        test_result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 100, "unit": "ng/mL"}],
            sex="male",
            age=35
        )
        
        return {
            "status": "healthy",
            "version": loader.ruleset_version,
            "markers": {
                "total_allowed": len(loader.allowed_marker_codes),
                "sample": list(loader.allowed_marker_codes)[:10]
            },
            "safety_gates": {
                "total": len(gates),
                "by_tier": tier_counts
            },
            "test_processing": {
                "success": True,
                "markers_processed": test_result.summary['total'],
                "routing_constraints": test_result.routing_constraints
            }
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }
