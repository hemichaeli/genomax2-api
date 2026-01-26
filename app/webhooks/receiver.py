"""
GenoMAX² Webhook Receiver
FastAPI endpoints for receiving lab test results via webhooks

Security:
- HMAC signature verification for Junction
- API key verification for Lab Testing API
- Rate limiting per source
"""

import hashlib
import hmac
import json
import os
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Request, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel

from .models import (
    JunctionWebhookPayload,
    LabTestingAPIWebhookPayload,
    WebhookEvent,
    WebhookEventType,
    LabResult,
    Biomarker,
    normalize_biomarker_code
)
from .processor import WebhookProcessor

router = APIRouter(prefix="/api/v1/webhooks", tags=["Webhooks"])

# Environment variables
JUNCTION_WEBHOOK_SECRET = os.getenv("JUNCTION_WEBHOOK_SECRET", "")
LAB_TESTING_API_KEY = os.getenv("LAB_TESTING_API_KEY", "")


class WebhookResponse(BaseModel):
    """Standard webhook response"""
    status: str
    event_id: Optional[str] = None
    message: str
    processed: bool = False


def verify_junction_signature(payload: bytes, signature: str) -> bool:
    """
    Verify Junction (Vital) webhook HMAC signature
    Ref: https://docs.tryvital.io/webhooks#webhook-security
    """
    if not JUNCTION_WEBHOOK_SECRET:
        return False
    
    expected = hmac.new(
        JUNCTION_WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(f"sha256={expected}", signature)


@router.get("/health")
async def webhook_health():
    """Webhook receiver health check"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "providers": {
            "junction": bool(JUNCTION_WEBHOOK_SECRET),
            "lab_testing_api": bool(LAB_TESTING_API_KEY)
        },
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/junction", response_model=WebhookResponse)
async def receive_junction_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_vital_signature: Optional[str] = Header(None, alias="X-Vital-Signature"),
    x_vital_timestamp: Optional[str] = Header(None, alias="X-Vital-Timestamp")
):
    """
    Receive webhooks from Junction (Vital) lab integration.
    
    Supported events:
    - labtest.order.created
    - labtest.order.updated
    - labtest.results.ready
    - labtest.results.critical
    
    Security: HMAC-SHA256 signature verification
    """
    # Get raw body for signature verification
    body = await request.body()
    
    # Verify signature if secret is configured
    if JUNCTION_WEBHOOK_SECRET:
        if not x_vital_signature:
            raise HTTPException(status_code=401, detail="Missing signature header")
        
        if not verify_junction_signature(body, x_vital_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Parse payload
    try:
        data = json.loads(body)
        payload = JunctionWebhookPayload(**data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {str(e)}")
    
    # Create webhook event
    event_type = WebhookEventType.UNKNOWN
    if payload.event_type == "labtest.results.ready":
        event_type = WebhookEventType.LABTEST_RESULTS_READY
    elif payload.event_type == "labtest.results.critical":
        event_type = WebhookEventType.LABTEST_RESULTS_CRITICAL
    elif payload.event_type == "labtest.order.created":
        event_type = WebhookEventType.LABTEST_ORDER_CREATED
    elif payload.event_type == "labtest.order.updated":
        event_type = WebhookEventType.LABTEST_ORDER_UPDATED
    
    event = WebhookEvent(
        event_id=f"junction_{payload.data.get('order_id', 'unknown')}_{datetime.utcnow().timestamp()}",
        event_type=event_type,
        source="junction",
        payload=data,
        signature=x_vital_signature,
        verified=bool(JUNCTION_WEBHOOK_SECRET and x_vital_signature)
    )
    
    # Process results in background if ready
    if event_type in [WebhookEventType.LABTEST_RESULTS_READY, WebhookEventType.LABTEST_RESULTS_CRITICAL]:
        processor = WebhookProcessor()
        background_tasks.add_task(
            processor.process_junction_results,
            event,
            payload
        )
    
    return WebhookResponse(
        status="received",
        event_id=event.event_id,
        message=f"Webhook {payload.event_type} received and queued",
        processed=event_type in [WebhookEventType.LABTEST_RESULTS_READY, WebhookEventType.LABTEST_RESULTS_CRITICAL]
    )


@router.post("/lab-testing-api", response_model=WebhookResponse)
async def receive_lab_testing_api_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    """
    Receive webhooks from Lab Testing API.
    
    Supported events:
    - order.completed
    - results.available
    
    Security: API key verification
    """
    # Verify API key if configured
    if LAB_TESTING_API_KEY:
        if not x_api_key or x_api_key != LAB_TESTING_API_KEY:
            raise HTTPException(status_code=401, detail="Invalid API key")
    
    # Parse payload
    try:
        body = await request.body()
        data = json.loads(body)
        payload = LabTestingAPIWebhookPayload(**data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid payload: {str(e)}")
    
    # Create webhook event
    event_type = WebhookEventType.UNKNOWN
    if payload.event == "results.available":
        event_type = WebhookEventType.LTA_RESULTS_AVAILABLE
    elif payload.event == "order.completed":
        event_type = WebhookEventType.LTA_ORDER_COMPLETED
    
    event = WebhookEvent(
        event_id=f"lta_{payload.order_id}_{datetime.utcnow().timestamp()}",
        event_type=event_type,
        source="lab_testing_api",
        payload=data,
        verified=bool(LAB_TESTING_API_KEY and x_api_key)
    )
    
    # Process results in background
    if event_type == WebhookEventType.LTA_RESULTS_AVAILABLE:
        processor = WebhookProcessor()
        background_tasks.add_task(
            processor.process_lab_testing_api_results,
            event,
            payload
        )
    
    return WebhookResponse(
        status="received",
        event_id=event.event_id,
        message=f"Webhook {payload.event} received and queued",
        processed=event_type == WebhookEventType.LTA_RESULTS_AVAILABLE
    )


@router.post("/test")
async def test_webhook_endpoint(request: Request):
    """
    Test endpoint for webhook integration testing.
    Accepts any payload and returns echo.
    """
    try:
        body = await request.body()
        data = json.loads(body) if body else {}
    except:
        data = {"raw": body.decode() if body else "empty"}
    
    return {
        "status": "test_received",
        "timestamp": datetime.utcnow().isoformat(),
        "headers": dict(request.headers),
        "payload": data
    }
