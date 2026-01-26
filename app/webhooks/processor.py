"""
GenoMAX² Webhook Processor
Processes lab results and triggers Brain orchestration

Flow:
1. Receive webhook with lab results
2. Normalize biomarker codes and units
3. Create bloodwork_input payload
4. Call orchestrate/v2 endpoint
5. Store results and notify user
"""

import os
import json
import hashlib
import httpx
from datetime import datetime
from typing import Optional, Dict, Any, List
from .models import (
    WebhookEvent,
    JunctionWebhookPayload,
    LabTestingAPIWebhookPayload,
    LabResult,
    Biomarker,
    normalize_biomarker_code,
    convert_unit
)

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://web-production-7110.up.railway.app")
DATABASE_URL = os.getenv("DATABASE_URL", "")


class WebhookProcessor:
    """
    Processes incoming lab webhooks and triggers Brain orchestration.
    Implements "Blood does not negotiate" principle.
    """
    
    def __init__(self):
        self.api_base_url = API_BASE_URL
    
    async def process_junction_results(
        self, 
        event: WebhookEvent, 
        payload: JunctionWebhookPayload
    ) -> Dict[str, Any]:
        """
        Process Junction (Vital) lab results.
        
        Junction data structure:
        {
            "event_type": "labtest.results.ready",
            "data": {
                "order_id": "...",
                "user_id": "...",
                "results": [
                    {"marker": "25-hydroxyvitamin-d", "value": 45.2, "unit": "ng/mL", ...}
                ]
            }
        }
        """
        data = payload.data
        order_id = data.get("order_id", "unknown")
        user_id = data.get("client_user_id") or data.get("user_id", "unknown")
        
        # Extract and normalize biomarkers
        raw_results = data.get("results", [])
        markers = []
        
        for result in raw_results:
            try:
                code = normalize_biomarker_code(result.get("marker", ""))
                value = float(result.get("value", 0))
                unit = result.get("unit", "")
                
                # Skip invalid results
                if not code or value == 0:
                    continue
                
                markers.append({
                    "code": code,
                    "value": value,
                    "unit": unit
                })
            except (ValueError, TypeError) as e:
                # Log but continue processing other markers
                print(f"[WebhookProcessor] Skipping invalid marker: {result} - {e}")
                continue
        
        if not markers:
            return {
                "status": "no_valid_markers",
                "order_id": order_id,
                "message": "No valid biomarkers found in results"
            }
        
        # Determine sex from user profile or metadata
        sex = data.get("metadata", {}).get("sex") or data.get("sex") or "male"
        age = data.get("metadata", {}).get("age") or data.get("age") or 35
        
        # Create bloodwork_input for orchestrate/v2
        bloodwork_input = {
            "markers": markers,
            "lab_profile": "GLOBAL_CONSERVATIVE",
            "sex": sex,
            "age": int(age)
        }
        
        # Trigger orchestration
        result = await self._trigger_orchestration(
            user_id=user_id,
            bloodwork_input=bloodwork_input,
            source="junction",
            order_id=order_id
        )
        
        # Store webhook event
        await self._store_webhook_event(event, result)
        
        return result
    
    async def process_lab_testing_api_results(
        self,
        event: WebhookEvent,
        payload: LabTestingAPIWebhookPayload
    ) -> Dict[str, Any]:
        """
        Process Lab Testing API results.
        
        Lab Testing API data structure:
        {
            "event": "results.available",
            "order_id": "...",
            "patient_id": "...",
            "results": [
                {"test_id": "...", "test_name": "Vitamin D", "result_value": "45.2", "result_unit": "ng/mL"}
            ]
        }
        """
        order_id = payload.order_id
        user_id = payload.patient_id or "unknown"
        
        # Extract and normalize biomarkers
        raw_results = payload.results or []
        markers = []
        
        for result in raw_results:
            try:
                # Map test name to code
                test_name = result.get("test_name", "").lower()
                code = normalize_biomarker_code(test_name)
                
                # Parse value (may be string)
                value_str = str(result.get("result_value", "0"))
                value = float(value_str.replace(",", "").replace("<", "").replace(">", ""))
                
                unit = result.get("result_unit", "")
                
                if not code or value == 0:
                    continue
                
                markers.append({
                    "code": code,
                    "value": value,
                    "unit": unit
                })
            except (ValueError, TypeError) as e:
                print(f"[WebhookProcessor] Skipping invalid marker: {result} - {e}")
                continue
        
        if not markers:
            return {
                "status": "no_valid_markers",
                "order_id": order_id,
                "message": "No valid biomarkers found in results"
            }
        
        # Create bloodwork_input
        bloodwork_input = {
            "markers": markers,
            "lab_profile": "GLOBAL_CONSERVATIVE",
            "sex": "male",  # Lab Testing API may not provide this
            "age": 35
        }
        
        # Trigger orchestration
        result = await self._trigger_orchestration(
            user_id=user_id,
            bloodwork_input=bloodwork_input,
            source="lab_testing_api",
            order_id=order_id
        )
        
        # Store webhook event
        await self._store_webhook_event(event, result)
        
        return result
    
    async def _trigger_orchestration(
        self,
        user_id: str,
        bloodwork_input: Dict[str, Any],
        source: str,
        order_id: str
    ) -> Dict[str, Any]:
        """
        Trigger Brain orchestration with bloodwork input.
        Uses internal API call to orchestrate/v2.
        """
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.api_base_url}/api/v1/brain/orchestrate/v2",
                    json={
                        "bloodwork_input": bloodwork_input,
                        "selected_goals": ["optimize"],  # Default goal
                        "assessment_context": {
                            "gender": bloodwork_input.get("sex", "male"),
                            "age": bloodwork_input.get("age", 35)
                        },
                        "metadata": {
                            "source": source,
                            "order_id": order_id,
                            "user_id": user_id,
                            "webhook_triggered": True
                        }
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return {
                        "status": "orchestration_triggered",
                        "run_id": data.get("run_id"),
                        "order_id": order_id,
                        "source": source,
                        "markers_processed": len(bloodwork_input.get("markers", [])),
                        "routing_constraints": data.get("routing_constraints", {})
                    }
                else:
                    return {
                        "status": "orchestration_failed",
                        "order_id": order_id,
                        "source": source,
                        "error": response.text,
                        "status_code": response.status_code
                    }
                    
        except Exception as e:
            return {
                "status": "orchestration_error",
                "order_id": order_id,
                "source": source,
                "error": str(e)
            }
    
    async def _store_webhook_event(
        self,
        event: WebhookEvent,
        result: Dict[str, Any]
    ) -> None:
        """
        Store webhook event in database for audit trail.
        Uses append-only pattern per governance requirements.
        """
        # For now, log to stdout. In production, this would insert to webhook_events table.
        log_entry = {
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "source": event.source,
            "timestamp": event.timestamp.isoformat(),
            "verified": event.verified,
            "result_status": result.get("status"),
            "run_id": result.get("run_id"),
            "processed_at": datetime.utcnow().isoformat()
        }
        print(f"[WebhookProcessor] Event stored: {json.dumps(log_entry)}")


# === Database Schema for Webhook Events ===
"""
CREATE TABLE IF NOT EXISTS webhook_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    source VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    signature VARCHAR(255),
    verified BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    result_status VARCHAR(50),
    result_run_id UUID,
    result_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Indexes
    INDEX idx_webhook_events_source (source),
    INDEX idx_webhook_events_type (event_type),
    INDEX idx_webhook_events_created (created_at)
);

-- Audit trigger for append-only pattern
CREATE OR REPLACE FUNCTION webhook_events_audit()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'DELETE not allowed on webhook_events (append-only)';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        -- Only allow updating result fields
        IF OLD.event_id != NEW.event_id OR OLD.payload != NEW.payload THEN
            RAISE EXCEPTION 'Cannot modify immutable fields on webhook_events';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER webhook_events_audit_trigger
BEFORE UPDATE OR DELETE ON webhook_events
FOR EACH ROW EXECUTE FUNCTION webhook_events_audit();
"""
