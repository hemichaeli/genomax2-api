"""
GenoMAX² Webhook API Endpoints
==============================
FastAPI endpoints for receiving lab result webhooks from Junction (Vital) and Lab Testing API.

These endpoints should be registered on the main app via:
    from bloodwork_engine.api_webhook_endpoints import register_webhook_endpoints
    register_webhook_endpoints(app)
"""

import os
import json
from typing import Optional
from starlette.requests import Request


def register_webhook_endpoints(app):
    """
    Register webhook endpoints on a FastAPI app.
    
    Usage:
        from bloodwork_engine.api_webhook_endpoints import register_webhook_endpoints
        register_webhook_endpoints(app)
    """
    
    # ---------------------------------------------------------
    # POST /api/v1/webhooks/vital
    # ---------------------------------------------------------
    @app.post("/api/v1/webhooks/vital", tags=["Webhooks"])
    async def vital_webhook(request: Request):
        """
        Junction/Vital results webhook endpoint.
        
        Receives lab result notifications from Junction (formerly Vital).
        Verifies signature and processes results through Bloodwork Engine.
        
        Headers:
        - X-Vital-Signature: HMAC-SHA256 signature for verification
        
        Events:
        - order.created, order.updated, order.completed, order.cancelled
        - results.ready, results.partial, results.critical
        """
        from bloodwork_engine.webhooks import (
            process_vital_webhook,
            verify_vital_signature
        )
        
        # Get raw body for signature verification
        raw_body = await request.body()
        signature = request.headers.get("X-Vital-Signature", "")
        ip_address = request.client.host if request.client else None
        
        # Verify signature
        if not verify_vital_signature(raw_body, signature):
            return {
                "success": False,
                "error": "INVALID_SIGNATURE",
                "message": "Webhook signature verification failed"
            }
        
        # Parse payload
        try:
            payload = json.loads(raw_body.decode())
        except json.JSONDecodeError as e:
            return {
                "success": False,
                "error": "INVALID_PAYLOAD",
                "message": f"Failed to parse JSON: {e}"
            }
        
        # Process webhook
        result = process_vital_webhook(
            payload=payload,
            signature_header=signature,
            ip_address=ip_address,
            auto_process=True
        )
        
        return {
            "success": result.success,
            "event_id": str(result.event_id) if result.event_id else None,
            "event_type": result.event_type,
            "message": result.message,
            "order_id": result.order_id,
            "error": result.error
        }
    
    # ---------------------------------------------------------
    # POST /api/v1/webhooks/labtestingapi
    # ---------------------------------------------------------
    @app.post("/api/v1/webhooks/labtestingapi", tags=["Webhooks"])
    async def lab_testing_api_webhook(request: Request):
        """
        Lab Testing API results webhook endpoint.
        
        Receives lab result notifications from Lab Testing API (Quest Diagnostics).
        Verifies signature and processes results through Bloodwork Engine.
        
        Headers:
        - X-Signature: HMAC-SHA256 signature for verification
        - X-Timestamp: Unix timestamp for replay protection
        
        Events:
        - order.created, order.updated
        - results.ready, results.critical
        """
        from bloodwork_engine.webhooks import (
            process_lab_testing_api_webhook,
            verify_lab_testing_api_signature
        )
        
        # Get raw body for signature verification
        raw_body = await request.body()
        signature = request.headers.get("X-Signature", "")
        timestamp = request.headers.get("X-Timestamp", "")
        ip_address = request.client.host if request.client else None
        
        # Verify signature
        if not verify_lab_testing_api_signature(raw_body, signature, timestamp):
            return {
                "success": False,
                "error": "INVALID_SIGNATURE",
                "message": "Webhook signature verification failed"
            }
        
        # Parse payload
        try:
            payload = json.loads(raw_body.decode())
        except json.JSONDecodeError as e:
            return {
                "success": False,
                "error": "INVALID_PAYLOAD",
                "message": f"Failed to parse JSON: {e}"
            }
        
        # Process webhook
        result = process_lab_testing_api_webhook(
            payload=payload,
            signature_header=signature,
            timestamp_header=timestamp,
            ip_address=ip_address,
            auto_process=True
        )
        
        return {
            "success": result.success,
            "event_id": str(result.event_id) if result.event_id else None,
            "event_type": result.event_type,
            "message": result.message,
            "order_id": result.order_id,
            "error": result.error
        }
    
    # ---------------------------------------------------------
    # GET /api/v1/webhooks/status
    # ---------------------------------------------------------
    @app.get("/api/v1/webhooks/status", tags=["Webhooks"])
    async def webhook_status():
        """
        Check webhook configuration status.
        
        Returns configuration status for all webhook providers
        and lists available webhook endpoints.
        """
        vital_secret = os.getenv("VITAL_WEBHOOK_SECRET")
        lta_secret = os.getenv("LAB_TESTING_API_WEBHOOK_SECRET")
        
        return {
            "status": "operational",
            "providers": {
                "vital": {
                    "configured": bool(vital_secret),
                    "secret_preview": f"{vital_secret[:8]}...{vital_secret[-4:]}" if vital_secret and len(vital_secret) > 12 else ("***" if vital_secret else None),
                    "environment": os.getenv("VITAL_ENVIRONMENT", "sandbox"),
                    "endpoint": "/api/v1/webhooks/vital",
                    "signature_header": "X-Vital-Signature",
                    "events": [
                        "order.created", "order.updated", "order.completed", "order.cancelled",
                        "results.ready", "results.partial", "results.critical"
                    ]
                },
                "lab_testing_api": {
                    "configured": bool(lta_secret),
                    "secret_preview": f"{lta_secret[:8]}...{lta_secret[-4:]}" if lta_secret and len(lta_secret) > 12 else ("***" if lta_secret else None),
                    "environment": os.getenv("LAB_TESTING_API_ENVIRONMENT", "production"),
                    "endpoint": "/api/v1/webhooks/labtestingapi",
                    "signature_header": "X-Signature",
                    "timestamp_header": "X-Timestamp",
                    "events": [
                        "order.created", "order.updated",
                        "results.ready", "results.critical"
                    ]
                }
            },
            "endpoints": [
                {
                    "path": "/api/v1/webhooks/vital",
                    "method": "POST",
                    "provider": "Junction (Vital)"
                },
                {
                    "path": "/api/v1/webhooks/labtestingapi", 
                    "method": "POST",
                    "provider": "Lab Testing API"
                },
                {
                    "path": "/api/v1/webhooks/status",
                    "method": "GET",
                    "provider": "Status check"
                }
            ],
            "setup_instructions": {
                "vital": {
                    "step_1": "Configure webhook URL in Junction dashboard: https://<your-domain>/api/v1/webhooks/vital",
                    "step_2": "Copy the webhook signing secret from Junction",
                    "step_3": "Set VITAL_WEBHOOK_SECRET environment variable"
                },
                "lab_testing_api": {
                    "step_1": "Configure webhook URL with Lab Testing API: https://<your-domain>/api/v1/webhooks/labtestingapi",
                    "step_2": "Set LAB_TESTING_API_WEBHOOK_SECRET environment variable"
                }
            }
        }
    
    # ---------------------------------------------------------
    # POST /api/v1/webhooks/test
    # ---------------------------------------------------------
    @app.post("/api/v1/webhooks/test", tags=["Webhooks"])
    async def test_webhook(
        provider: str = "vital",
        event_type: str = "results.ready"
    ):
        """
        Test webhook processing with a mock payload.
        
        For development/testing only. Creates a mock webhook event
        and processes it through the system.
        """
        from bloodwork_engine.webhooks import (
            process_vital_webhook,
            process_lab_testing_api_webhook
        )
        
        # Create mock payloads
        if provider == "vital":
            mock_payload = {
                "event_type": event_type,
                "data": {
                    "order_id": "test_order_123",
                    "user_id": "test_user_456",
                    "status": "completed",
                    "results": [
                        {"marker": "ferritin", "value": 85, "unit": "ng/mL"},
                        {"marker": "vitamin_d_25oh", "value": 45, "unit": "ng/mL"},
                        {"marker": "vitamin_b12", "value": 550, "unit": "pg/mL"}
                    ]
                },
                "created_at": "2025-01-25T12:00:00Z"
            }
            
            result = process_vital_webhook(
                payload=mock_payload,
                signature_header="test_signature",
                ip_address="127.0.0.1",
                auto_process=False  # Don't actually try to fetch from API
            )
        else:
            mock_payload = {
                "event": event_type,
                "data": {
                    "order_id": "lta_order_789",
                    "patient_external_id": "test_patient_012",
                    "status": "completed"
                },
                "timestamp": "2025-01-25T12:00:00Z"
            }
            
            result = process_lab_testing_api_webhook(
                payload=mock_payload,
                signature_header="test_signature",
                timestamp_header="1737806400",
                ip_address="127.0.0.1",
                auto_process=False
            )
        
        return {
            "test_mode": True,
            "provider": provider,
            "event_type": event_type,
            "mock_payload": mock_payload,
            "result": {
                "success": result.success,
                "event_id": result.event_id,
                "event_type": result.event_type,
                "message": result.message,
                "order_id": result.order_id,
                "error": result.error
            }
        }
    
    return app
