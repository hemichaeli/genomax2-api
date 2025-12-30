"""
GenoMAX² Telemetry Emitter (Issue #9)
Singleton emitter for collecting telemetry events from all layers.

Usage:
    from app.telemetry import TelemetryEmitter
    
    emitter = TelemetryEmitter.get_instance()
    emitter.emit_routing_block(run_id, "BLOCK_IRON", count=1)
"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from threading import Lock
import uuid

from .models import (
    TelemetryRun,
    TelemetryEvent,
    TelemetryEventType,
    AgeBucket,
    ConfidenceLevel,
)


class TelemetryEmitter:
    """
    Singleton emitter for telemetry events.
    Thread-safe, non-blocking (fails silently).
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._db_url = os.getenv("DATABASE_URL")
        self._enabled = os.getenv("TELEMETRY_ENABLED", "true").lower() == "true"
        self._buffer: List[TelemetryEvent] = []
        self._buffer_lock = Lock()
        self._max_buffer = 100
        
    @classmethod
    def get_instance(cls) -> "TelemetryEmitter":
        """Get singleton instance."""
        return cls()
    
    def _get_conn(self):
        """Get database connection."""
        if not self._db_url:
            return None
        try:
            return psycopg2.connect(self._db_url, cursor_factory=RealDictCursor)
        except Exception as e:
            print(f"[Telemetry] DB connection failed: {e}")
            return None
    
    def _ensure_tables(self, conn) -> bool:
        """Ensure telemetry tables exist."""
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_runs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    run_id UUID NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    
                    api_version VARCHAR(20),
                    bloodwork_version VARCHAR(20),
                    catalog_version VARCHAR(50),
                    routing_version VARCHAR(50),
                    matching_version VARCHAR(50),
                    explainability_version VARCHAR(50),
                    
                    sex VARCHAR(10),
                    age_bucket VARCHAR(20),
                    
                    has_bloodwork BOOLEAN DEFAULT FALSE,
                    intents_count INTEGER DEFAULT 0,
                    matched_items_count INTEGER DEFAULT 0,
                    unmatched_intents_count INTEGER DEFAULT 0,
                    blocked_skus_count INTEGER DEFAULT 0,
                    auto_blocked_skus_count INTEGER DEFAULT 0,
                    caution_flags_count INTEGER DEFAULT 0,
                    confidence_level VARCHAR(20)
                );
                
                CREATE INDEX IF NOT EXISTS idx_telemetry_runs_created_at 
                    ON telemetry_runs(created_at);
                CREATE INDEX IF NOT EXISTS idx_telemetry_runs_confidence 
                    ON telemetry_runs(confidence_level);
                CREATE INDEX IF NOT EXISTS idx_telemetry_runs_bloodwork 
                    ON telemetry_runs(has_bloodwork);
                CREATE INDEX IF NOT EXISTS idx_telemetry_runs_run_id 
                    ON telemetry_runs(run_id);
                
                CREATE TABLE IF NOT EXISTS telemetry_events (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    run_id UUID NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    
                    event_type VARCHAR(50) NOT NULL,
                    code VARCHAR(255) NOT NULL,
                    count INTEGER DEFAULT 1,
                    metadata JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_telemetry_events_run_id 
                    ON telemetry_events(run_id);
                CREATE INDEX IF NOT EXISTS idx_telemetry_events_type 
                    ON telemetry_events(event_type);
                CREATE INDEX IF NOT EXISTS idx_telemetry_events_created_at 
                    ON telemetry_events(created_at);
                CREATE INDEX IF NOT EXISTS idx_telemetry_events_code 
                    ON telemetry_events(code);
                
                CREATE TABLE IF NOT EXISTS telemetry_daily_rollups (
                    day DATE PRIMARY KEY,
                    total_runs INTEGER DEFAULT 0,
                    pct_has_bloodwork FLOAT DEFAULT 0,
                    pct_low_confidence FLOAT DEFAULT 0,
                    avg_unmatched_intents FLOAT DEFAULT 0,
                    avg_blocked_skus FLOAT DEFAULT 0,
                    top_block_reasons JSONB,
                    top_missing_fields JSONB,
                    top_unknown_ingredients JSONB,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            conn.commit()
            cur.close()
            return True
        except Exception as e:
            print(f"[Telemetry] Table creation failed: {e}")
            return False
    
    # ===== RUN-LEVEL EMISSIONS =====
    
    def start_run(
        self,
        run_id: str,
        sex: Optional[str] = None,
        age: Optional[int] = None,
        has_bloodwork: bool = False,
        api_version: str = "3.16.0",
        bloodwork_version: str = "1.0",
    ) -> Optional[str]:
        """Start telemetry for a run. Returns telemetry_run_id."""
        if not self._enabled:
            return None
            
        conn = self._get_conn()
        if not conn:
            return None
            
        try:
            self._ensure_tables(conn)
            cur = conn.cursor()
            
            telemetry_id = str(uuid.uuid4())
            age_bucket = AgeBucket.from_age(age).value
            
            cur.execute("""
                INSERT INTO telemetry_runs 
                (id, run_id, sex, age_bucket, has_bloodwork, api_version, bloodwork_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                telemetry_id,
                run_id,
                sex,
                age_bucket,
                has_bloodwork,
                api_version,
                bloodwork_version,
            ))
            conn.commit()
            cur.close()
            conn.close()
            return telemetry_id
        except Exception as e:
            print(f"[Telemetry] start_run failed: {e}")
            try:
                conn.close()
            except:
                pass
            return None
    
    def complete_run(
        self,
        run_id: str,
        intents_count: int = 0,
        matched_items_count: int = 0,
        unmatched_intents_count: int = 0,
        blocked_skus_count: int = 0,
        auto_blocked_skus_count: int = 0,
        caution_flags_count: int = 0,
        confidence_level: str = "unknown",
    ):
        """Update run with final counts."""
        if not self._enabled:
            return
            
        conn = self._get_conn()
        if not conn:
            return
            
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE telemetry_runs SET
                    intents_count = %s,
                    matched_items_count = %s,
                    unmatched_intents_count = %s,
                    blocked_skus_count = %s,
                    auto_blocked_skus_count = %s,
                    caution_flags_count = %s,
                    confidence_level = %s
                WHERE run_id = %s
            """, (
                intents_count,
                matched_items_count,
                unmatched_intents_count,
                blocked_skus_count,
                auto_blocked_skus_count,
                caution_flags_count,
                confidence_level,
                run_id,
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[Telemetry] complete_run failed: {e}")
            try:
                conn.close()
            except:
                pass
    
    # ===== EVENT EMISSIONS =====
    
    def _emit_event(
        self,
        run_id: str,
        event_type: TelemetryEventType,
        code: str,
        count: int = 1,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Internal event emission."""
        if not self._enabled:
            return
            
        # Sanitize metadata - NO PII
        safe_metadata = {}
        if metadata:
            # Only allow safe keys
            safe_keys = {"reason", "layer", "severity", "marker_code", "gate_name"}
            safe_metadata = {k: v for k, v in metadata.items() if k in safe_keys}
        
        conn = self._get_conn()
        if not conn:
            return
            
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO telemetry_events 
                (run_id, event_type, code, count, metadata)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                run_id,
                event_type.value if isinstance(event_type, TelemetryEventType) else event_type,
                code,
                count,
                json.dumps(safe_metadata) if safe_metadata else None,
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"[Telemetry] emit_event failed: {e}")
            try:
                conn.close()
            except:
                pass
    
    # ===== CATALOG GOVERNANCE EVENTS =====
    
    def emit_catalog_auto_block(self, run_id: str, reason_code: str, count: int = 1):
        """Emit when catalog governance auto-blocks a SKU."""
        self._emit_event(
            run_id,
            TelemetryEventType.CATALOG_AUTO_BLOCK,
            reason_code,
            count,
            {"layer": "catalog_governance"},
        )
    
    def emit_unknown_ingredient(self, run_id: str, ingredient_name: str, count: int = 1):
        """Emit when an unknown ingredient is encountered."""
        # Hash or truncate ingredient name to avoid PII leakage
        safe_code = ingredient_name[:50] if ingredient_name else "unknown"
        self._emit_event(
            run_id,
            TelemetryEventType.UNKNOWN_INGREDIENT,
            safe_code,
            count,
            {"layer": "catalog_governance"},
        )
    
    # ===== ROUTING LAYER EVENTS =====
    
    def emit_routing_block(self, run_id: str, block_reason: str, count: int = 1):
        """Emit when routing layer blocks a target."""
        self._emit_event(
            run_id,
            TelemetryEventType.ROUTING_BLOCK,
            block_reason,
            count,
            {"layer": "routing"},
        )
    
    def emit_safety_gate_triggered(
        self, 
        run_id: str, 
        gate_name: str, 
        marker_code: str,
        count: int = 1
    ):
        """Emit when a safety gate is triggered."""
        self._emit_event(
            run_id,
            TelemetryEventType.SAFETY_GATE_TRIGGERED,
            gate_name,
            count,
            {"layer": "bloodwork", "marker_code": marker_code, "gate_name": gate_name},
        )
    
    # ===== MATCHING LAYER EVENTS =====
    
    def emit_unmatched_intent(self, run_id: str, intent_id: str, count: int = 1):
        """Emit when an intent cannot be matched to any SKU."""
        self._emit_event(
            run_id,
            TelemetryEventType.MATCHING_UNMATCHED_INTENT,
            intent_id,
            count,
            {"layer": "matching"},
        )
    
    def emit_requirement_unfulfilled(
        self, 
        run_id: str, 
        requirement_code: str, 
        count: int = 1
    ):
        """Emit when a requirement cannot be fulfilled."""
        self._emit_event(
            run_id,
            TelemetryEventType.MATCHING_REQUIREMENT_UNFULFILLED,
            requirement_code,
            count,
            {"layer": "matching"},
        )
    
    # ===== EXPLAINABILITY EVENTS =====
    
    def emit_low_confidence(self, run_id: str, reason: str, count: int = 1):
        """Emit when explainability reports low confidence."""
        self._emit_event(
            run_id,
            TelemetryEventType.LOW_CONFIDENCE,
            reason,
            count,
            {"layer": "explainability"},
        )
    
    # ===== BLOODWORK EVENTS =====
    
    def emit_marker_missing(self, run_id: str, marker_code: str, count: int = 1):
        """Emit when a requested marker is missing from bloodwork."""
        self._emit_event(
            run_id,
            TelemetryEventType.BLOODWORK_MARKER_MISSING,
            marker_code,
            count,
            {"layer": "bloodwork"},
        )
    
    def emit_unit_conversion(self, run_id: str, marker_code: str, count: int = 1):
        """Emit when unit conversion is applied."""
        self._emit_event(
            run_id,
            TelemetryEventType.UNIT_CONVERSION_APPLIED,
            marker_code,
            count,
            {"layer": "bloodwork"},
        )


# Convenience function
def get_emitter() -> TelemetryEmitter:
    """Get telemetry emitter singleton."""
    return TelemetryEmitter.get_instance()
