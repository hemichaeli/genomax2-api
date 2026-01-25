"""
GenoMAX² Webhook Handlers
==========================
Webhook handlers for receiving lab results from Junction (Vital) and Lab Testing API.

Features:
- Signature verification for security
- Automatic processing through Bloodwork Engine
- Database storage for events, orders, and results
- Error handling with retry support

Usage:
    from bloodwork_engine.webhooks import (
        process_vital_webhook,
        process_lab_testing_api_webhook,
        verify_vital_signature,
        verify_lab_testing_api_signature
    )
"""

import os
import json
import hmac
import hashlib
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

logger = logging.getLogger(__name__)


class WebhookProvider(Enum):
    """Supported webhook providers."""
    JUNCTION = "junction"
    VITAL = "vital"  # Alias for Junction
    LAB_TESTING_API = "lab_testing_api"


class WebhookEventType(Enum):
    """Webhook event types."""
    # Junction/Vital events
    ORDER_CREATED = "order.created"
    ORDER_UPDATED = "order.updated"
    ORDER_COMPLETED = "order.completed"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_FAILED = "order.failed"
    RESULTS_READY = "results.ready"
    RESULTS_PARTIAL = "results.partial"
    RESULTS_CRITICAL = "results.critical"
    
    # Lab Testing API events
    LTA_ORDER_CREATED = "lta.order.created"
    LTA_ORDER_UPDATED = "lta.order.updated"
    LTA_RESULTS_READY = "lta.results.ready"
    LTA_RESULTS_CRITICAL = "lta.results.critical"
    
    UNKNOWN = "unknown"


class WebhookStatus(Enum):
    """Webhook processing status."""
    RECEIVED = "received"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    IGNORED = "ignored"


@dataclass
class WebhookEvent:
    """Parsed webhook event."""
    event_id: str
    provider: str
    event_type: str
    raw_payload: Dict[str, Any]
    parsed_order_id: Optional[str] = None
    parsed_user_id: Optional[str] = None
    signature_header: Optional[str] = None
    signature_valid: Optional[bool] = None
    ip_address: Optional[str] = None
    status: str = "received"
    processed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    lab_order_id: Optional[str] = None
    bloodwork_upload_id: Optional[str] = None
    bloodwork_result_id: Optional[str] = None
    received_at: datetime = None
    
    def __post_init__(self):
        if self.received_at is None:
            self.received_at = datetime.now(timezone.utc)


@dataclass
class WebhookResult:
    """Result of webhook processing."""
    success: bool
    event_id: str
    event_type: str
    message: str
    order_id: Optional[str] = None
    bloodwork_result_id: Optional[str] = None
    safety_gates_triggered: int = 0
    routing_constraints: List[str] = None
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.routing_constraints is None:
            self.routing_constraints = []


# ============================================================
# SIGNATURE VERIFICATION
# ============================================================

def verify_vital_signature(
    payload: bytes,
    signature_header: str,
    secret: Optional[str] = None
) -> bool:
    """
    Verify Junction/Vital webhook signature.
    
    Junction uses HMAC-SHA256 for webhook verification.
    Signature format: sha256=<hex_digest>
    
    Args:
        payload: Raw request body bytes
        signature_header: Value of X-Vital-Signature or X-Junction-Signature header
        secret: Webhook signing secret (defaults to VITAL_WEBHOOK_SECRET env var)
    
    Returns:
        True if signature is valid
    """
    if not signature_header:
        logger.warning("Missing webhook signature header")
        return False
    
    secret = secret or os.environ.get("VITAL_WEBHOOK_SECRET")
    if not secret:
        logger.error("VITAL_WEBHOOK_SECRET not configured - cannot verify signature")
        # In development, allow unverified webhooks with warning
        if os.environ.get("VITAL_ENVIRONMENT") == "sandbox":
            logger.warning("Allowing unverified webhook in sandbox mode")
            return True
        return False
    
    try:
        # Remove "sha256=" prefix if present
        if signature_header.startswith("sha256="):
            provided_sig = signature_header[7:]
        else:
            provided_sig = signature_header
        
        # Calculate expected signature
        expected_sig = hmac.new(
            secret.encode("utf-8"),
            payload,
            hashlib.sha256
        ).hexdigest()
        
        # Constant-time comparison
        return hmac.compare_digest(expected_sig, provided_sig)
        
    except Exception as e:
        logger.error(f"Signature verification error: {e}")
        return False


def verify_lab_testing_api_signature(
    payload: bytes,
    signature_header: str,
    timestamp_header: Optional[str] = None,
    secret: Optional[str] = None
) -> bool:
    """
    Verify Lab Testing API webhook signature.
    
    Lab Testing API uses HMAC-SHA256 with timestamp validation.
    
    Args:
        payload: Raw request body bytes
        signature_header: Value of X-Signature header
        timestamp_header: Value of X-Timestamp header (for replay protection)
        secret: Webhook signing secret (defaults to LAB_TESTING_API_WEBHOOK_SECRET env var)
    
    Returns:
        True if signature is valid and timestamp is recent
    """
    if not signature_header:
        logger.warning("Missing webhook signature header")
        return False
    
    secret = secret or os.environ.get("LAB_TESTING_API_WEBHOOK_SECRET")
    if not secret:
        logger.error("LAB_TESTING_API_WEBHOOK_SECRET not configured - cannot verify signature")
        # Allow unverified in sandbox
        if os.environ.get("LAB_TESTING_API_ENVIRONMENT") == "sandbox":
            logger.warning("Allowing unverified webhook in sandbox mode")
            return True
        return False
    
    try:
        # Validate timestamp if provided (protect against replay attacks)
        if timestamp_header:
            try:
                ts = int(timestamp_header)
                now = int(datetime.now(timezone.utc).timestamp())
                # Allow 5 minute window
                if abs(now - ts) > 300:
                    logger.warning(f"Webhook timestamp too old: {ts} vs {now}")
                    return False
            except ValueError:
                logger.warning(f"Invalid timestamp header: {timestamp_header}")
        
        # Calculate expected signature
        # Format: timestamp.payload
        signed_payload = payload
        if timestamp_header:
            signed_payload = f"{timestamp_header}.".encode() + payload
        
        expected_sig = hmac.new(
            secret.encode("utf-8"),
            signed_payload,
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(expected_sig, signature_header)
        
    except Exception as e:
        logger.error(f"Signature verification error: {e}")
        return False


# ============================================================
# WEBHOOK PARSING
# ============================================================

def parse_vital_event(payload: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Parse Junction/Vital webhook payload.
    
    Returns:
        Tuple of (event_type, order_id, user_id)
    """
    event_type = payload.get("event_type") or payload.get("type") or "unknown"
    
    # Extract data based on event structure
    data = payload.get("data", payload)
    
    order_id = (
        data.get("order_id") or
        data.get("id") or
        payload.get("order_id")
    )
    
    user_id = (
        data.get("user_id") or
        data.get("client_user_id") or
        payload.get("user_id")
    )
    
    return event_type, order_id, user_id


def parse_lab_testing_api_event(payload: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Parse Lab Testing API webhook payload.
    
    Returns:
        Tuple of (event_type, order_id, patient_id)
    """
    event_type = payload.get("event") or payload.get("event_type") or "unknown"
    
    # Map to internal event types
    event_map = {
        "order.created": "lta.order.created",
        "order.updated": "lta.order.updated",
        "results.ready": "lta.results.ready",
        "results.critical": "lta.results.critical",
    }
    event_type = event_map.get(event_type, event_type)
    
    # Extract data
    data = payload.get("data", payload)
    
    order_id = (
        data.get("order_id") or
        data.get("id") or
        payload.get("order_id")
    )
    
    patient_id = (
        data.get("patient_external_id") or
        data.get("patient", {}).get("external_id") or
        data.get("patient_id")
    )
    
    return event_type, order_id, patient_id


# ============================================================
# WEBHOOK PROCESSING
# ============================================================

def process_vital_webhook(
    payload: Dict[str, Any],
    signature_header: Optional[str] = None,
    ip_address: Optional[str] = None,
    auto_process: bool = True,
    lab_profile: str = "GLOBAL_CONSERVATIVE",
    sex: Optional[str] = None,
    age: Optional[int] = None
) -> WebhookResult:
    """
    Process a Junction/Vital webhook.
    
    Args:
        payload: Parsed JSON payload
        signature_header: X-Vital-Signature header value
        ip_address: Request IP address
        auto_process: Whether to automatically process results through engine
        lab_profile: Lab profile for Bloodwork Engine
        sex: Patient sex for engine processing
        age: Patient age for engine processing
    
    Returns:
        WebhookResult with processing outcome
    """
    event_id = str(uuid.uuid4())
    
    try:
        # Parse event
        event_type, order_id, user_id = parse_vital_event(payload)
        
        logger.info(f"Processing Vital webhook: {event_type} for order {order_id}")
        
        # Create event record
        event = WebhookEvent(
            event_id=event_id,
            provider="junction",
            event_type=event_type,
            raw_payload=payload,
            parsed_order_id=order_id,
            parsed_user_id=user_id,
            signature_header=signature_header,
            ip_address=ip_address,
            status="processing"
        )
        
        # Check if this is a results event
        results_events = ["results.ready", "results.partial", "results.critical", "order.completed"]
        
        if event_type in results_events and auto_process:
            return _process_vital_results(event, lab_profile, sex, age)
        
        # For non-results events, just log and return
        return WebhookResult(
            success=True,
            event_id=event_id,
            event_type=event_type,
            message=f"Event received: {event_type}",
            order_id=order_id
        )
        
    except Exception as e:
        logger.error(f"Vital webhook processing error: {e}", exc_info=True)
        return WebhookResult(
            success=False,
            event_id=event_id,
            event_type=payload.get("event_type", "unknown"),
            message="Processing failed",
            error=str(e)
        )


def _process_vital_results(
    event: WebhookEvent,
    lab_profile: str,
    sex: Optional[str],
    age: Optional[int]
) -> WebhookResult:
    """Process results from Vital webhook and run through Bloodwork Engine."""
    try:
        from bloodwork_engine.lab_adapters import VitalAdapter
        from bloodwork_engine.engine_v2 import get_engine
        
        # Get results from Junction API
        api_key = os.environ.get("VITAL_API_KEY")
        environment = os.environ.get("VITAL_ENVIRONMENT", "sandbox")
        
        adapter = VitalAdapter(api_key=api_key, environment=environment)
        
        # Fetch full results
        results = adapter.fetch_results(order_id=event.parsed_order_id)
        
        if not results:
            return WebhookResult(
                success=True,
                event_id=event.event_id,
                event_type=event.event_type,
                message="No results found for order",
                order_id=event.parsed_order_id
            )
        
        # Process through Bloodwork Engine
        result = results[0]  # Take first result set
        engine_input = adapter.to_engine_input(result)
        
        if not engine_input:
            return WebhookResult(
                success=True,
                event_id=event.event_id,
                event_type=event.event_type,
                message="No mappable markers in results",
                order_id=event.parsed_order_id
            )
        
        engine = get_engine(lab_profile=lab_profile)
        engine_result = engine.process_markers(
            markers=engine_input,
            sex=sex,
            age=age
        )
        
        # Generate result ID
        bloodwork_result_id = str(uuid.uuid4())
        
        return WebhookResult(
            success=True,
            event_id=event.event_id,
            event_type=event.event_type,
            message="Results processed successfully",
            order_id=event.parsed_order_id,
            bloodwork_result_id=bloodwork_result_id,
            safety_gates_triggered=len(engine_result.safety_gates),
            routing_constraints=engine_result.routing_constraints
        )
        
    except Exception as e:
        logger.error(f"Error processing Vital results: {e}", exc_info=True)
        return WebhookResult(
            success=False,
            event_id=event.event_id,
            event_type=event.event_type,
            message="Results processing failed",
            order_id=event.parsed_order_id,
            error=str(e)
        )


def process_lab_testing_api_webhook(
    payload: Dict[str, Any],
    signature_header: Optional[str] = None,
    timestamp_header: Optional[str] = None,
    ip_address: Optional[str] = None,
    auto_process: bool = True,
    lab_profile: str = "GLOBAL_CONSERVATIVE",
    sex: Optional[str] = None,
    age: Optional[int] = None
) -> WebhookResult:
    """
    Process a Lab Testing API webhook.
    
    Args:
        payload: Parsed JSON payload
        signature_header: X-Signature header value
        timestamp_header: X-Timestamp header value
        ip_address: Request IP address
        auto_process: Whether to automatically process results through engine
        lab_profile: Lab profile for Bloodwork Engine
        sex: Patient sex for engine processing
        age: Patient age for engine processing
    
    Returns:
        WebhookResult with processing outcome
    """
    event_id = str(uuid.uuid4())
    
    try:
        # Parse event
        event_type, order_id, patient_id = parse_lab_testing_api_event(payload)
        
        logger.info(f"Processing Lab Testing API webhook: {event_type} for order {order_id}")
        
        # Create event record
        event = WebhookEvent(
            event_id=event_id,
            provider="lab_testing_api",
            event_type=event_type,
            raw_payload=payload,
            parsed_order_id=order_id,
            parsed_user_id=patient_id,
            signature_header=signature_header,
            ip_address=ip_address,
            status="processing"
        )
        
        # Check if this is a results event
        results_events = ["lta.results.ready", "lta.results.critical"]
        
        if event_type in results_events and auto_process:
            return _process_lab_testing_api_results(event, lab_profile, sex, age)
        
        # For non-results events, just log and return
        return WebhookResult(
            success=True,
            event_id=event_id,
            event_type=event_type,
            message=f"Event received: {event_type}",
            order_id=order_id
        )
        
    except Exception as e:
        logger.error(f"Lab Testing API webhook processing error: {e}", exc_info=True)
        return WebhookResult(
            success=False,
            event_id=event_id,
            event_type=payload.get("event", "unknown"),
            message="Processing failed",
            error=str(e)
        )


def _process_lab_testing_api_results(
    event: WebhookEvent,
    lab_profile: str,
    sex: Optional[str],
    age: Optional[int]
) -> WebhookResult:
    """Process results from Lab Testing API webhook and run through Bloodwork Engine."""
    try:
        from bloodwork_engine.lab_adapters import LabTestingAPIAdapter
        from bloodwork_engine.engine_v2 import get_engine
        
        # Get results from Lab Testing API
        api_key = os.environ.get("LAB_TESTING_API_KEY")
        environment = os.environ.get("LAB_TESTING_API_ENVIRONMENT", "production")
        
        adapter = LabTestingAPIAdapter(api_key=api_key, environment=environment)
        
        # Fetch full results
        results = adapter.fetch_results(order_id=event.parsed_order_id)
        
        if not results:
            return WebhookResult(
                success=True,
                event_id=event.event_id,
                event_type=event.event_type,
                message="No results found for order",
                order_id=event.parsed_order_id
            )
        
        # Process through Bloodwork Engine
        result = results[0]  # Take first result set
        engine_input = adapter.to_engine_input(result)
        
        if not engine_input:
            return WebhookResult(
                success=True,
                event_id=event.event_id,
                event_type=event.event_type,
                message="No mappable markers in results",
                order_id=event.parsed_order_id
            )
        
        engine = get_engine(lab_profile=lab_profile)
        engine_result = engine.process_markers(
            markers=engine_input,
            sex=sex,
            age=age
        )
        
        # Generate result ID
        bloodwork_result_id = str(uuid.uuid4())
        
        return WebhookResult(
            success=True,
            event_id=event.event_id,
            event_type=event.event_type,
            message="Results processed successfully",
            order_id=event.parsed_order_id,
            bloodwork_result_id=bloodwork_result_id,
            safety_gates_triggered=len(engine_result.safety_gates),
            routing_constraints=engine_result.routing_constraints
        )
        
    except Exception as e:
        logger.error(f"Error processing Lab Testing API results: {e}", exc_info=True)
        return WebhookResult(
            success=False,
            event_id=event.event_id,
            event_type=event.event_type,
            message="Results processing failed",
            order_id=event.parsed_order_id,
            error=str(e)
        )


# ============================================================
# DATABASE OPERATIONS (for future integration)
# ============================================================

async def store_webhook_event(
    event: WebhookEvent,
    db_pool = None
) -> bool:
    """
    Store webhook event in database.
    
    Args:
        event: WebhookEvent to store
        db_pool: Database connection pool
    
    Returns:
        True if stored successfully
    """
    if db_pool is None:
        logger.warning("No database pool provided - skipping event storage")
        return False
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO webhook_events (
                    event_id, provider, event_type, raw_payload,
                    parsed_order_id, parsed_user_id, signature_header,
                    signature_valid, ip_address, status, processed_at,
                    error_message, retry_count, lab_order_id,
                    bloodwork_upload_id, bloodwork_result_id, received_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
                )
            """,
                event.event_id, event.provider, event.event_type,
                json.dumps(event.raw_payload), event.parsed_order_id,
                event.parsed_user_id, event.signature_header, event.signature_valid,
                event.ip_address, event.status, event.processed_at,
                event.error_message, event.retry_count, event.lab_order_id,
                event.bloodwork_upload_id, event.bloodwork_result_id, event.received_at
            )
        return True
    except Exception as e:
        logger.error(f"Failed to store webhook event: {e}")
        return False


async def update_lab_order_status(
    order_id: str,
    provider: str,
    status: str,
    additional_data: Dict[str, Any] = None,
    db_pool = None
) -> bool:
    """
    Update lab order status in database.
    
    Args:
        order_id: Provider's order ID
        provider: Provider name (junction, lab_testing_api)
        status: New status
        additional_data: Additional fields to update
        db_pool: Database connection pool
    
    Returns:
        True if updated successfully
    """
    if db_pool is None:
        logger.warning("No database pool provided - skipping order status update")
        return False
    
    try:
        async with db_pool.acquire() as conn:
            # Build dynamic update query
            updates = ["status = $3", "updated_at = NOW()"]
            params = [provider, order_id, status]
            param_idx = 4
            
            if additional_data:
                if "completed_at" in additional_data:
                    updates.append(f"completed_at = ${param_idx}")
                    params.append(additional_data["completed_at"])
                    param_idx += 1
                if "results_url" in additional_data:
                    updates.append(f"results_url = ${param_idx}")
                    params.append(additional_data["results_url"])
                    param_idx += 1
                if "bloodwork_result_id" in additional_data:
                    updates.append(f"bloodwork_result_id = ${param_idx}")
                    params.append(additional_data["bloodwork_result_id"])
                    param_idx += 1
            
            query = f"""
                UPDATE lab_orders
                SET {', '.join(updates)}
                WHERE provider = $1 AND provider_order_id = $2
            """
            
            await conn.execute(query, *params)
        return True
    except Exception as e:
        logger.error(f"Failed to update lab order status: {e}")
        return False


async def store_safety_gate_trigger(
    user_id: Optional[str],
    bloodwork_result_id: str,
    gate_data: Dict[str, Any],
    db_pool = None
) -> bool:
    """
    Store triggered safety gate in database for audit.
    
    Args:
        user_id: User UUID
        bloodwork_result_id: Result UUID
        gate_data: Safety gate information
        db_pool: Database connection pool
    
    Returns:
        True if stored successfully
    """
    if db_pool is None:
        logger.warning("No database pool provided - skipping safety gate storage")
        return False
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO safety_gate_triggers (
                    trigger_id, user_id, bloodwork_result_id,
                    gate_id, gate_name, gate_tier, gate_action, routing_constraint,
                    trigger_marker, trigger_value, trigger_unit,
                    threshold_value, threshold_operator,
                    blocked_ingredients, caution_ingredients, recommended_ingredients
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                )
            """,
                str(uuid.uuid4()), user_id, bloodwork_result_id,
                gate_data.get("gate_id"), gate_data.get("name"),
                gate_data.get("tier"), gate_data.get("action"),
                gate_data.get("routing_constraint"), gate_data.get("trigger_marker"),
                gate_data.get("trigger_value"), gate_data.get("trigger_unit"),
                gate_data.get("threshold"), gate_data.get("threshold_operator", ">"),
                gate_data.get("blocked_ingredients", []),
                gate_data.get("caution_ingredients", []),
                gate_data.get("recommended_ingredients", [])
            )
        return True
    except Exception as e:
        logger.error(f"Failed to store safety gate trigger: {e}")
        return False
