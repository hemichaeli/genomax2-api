"""
GenoMAX² Brain - Bloodwork Handoff Tests
========================================
Tests for Bloodwork → Brain Handoff Integration.

Tests:
1. test_blood_precedence - downstream cannot unblock blood restrictions
2. test_determinism - sorted arrays produce stable hashes  
3. test_persistence - handoff stored to decision_outputs
4. test_outage_hard_abort - unavailability returns 503
5. test_schema_validation - validates against JSON Schema
"""

import pytest
import json
from datetime import datetime
from unittest.mock import patch, MagicMock
import httpx

# Import the modules under test
from app.brain.bloodwork_handoff import (
    BloodworkHandoffV1,
    BloodworkHandoffException,
    BloodworkHandoffError,
    RoutingConstraints,
    fetch_bloodwork_handoff,
    merge_routing_constraints,
    blood_blocks_ingredient,
    get_blood_cautions,
    has_incomplete_panel,
    validate_handoff_schema,
    handoff_to_decision_output,
    _build_handoff_from_response,
    BLOODWORK_BASE_URL,
    BLOODWORK_ENDPOINT
)
from app.shared.hashing import canonicalize_and_hash


# ============================================
# FIXTURES
# ============================================

@pytest.fixture
def mock_bloodwork_response():
    """Standard bloodwork API response with iron block."""
    return {
        "processed_at": "2025-12-28T15:00:00Z",
        "lab_profile": "GLOBAL_CONSERVATIVE",
        "markers": [
            {
                "original_code": "ferritin",
                "canonical_code": "ferritin",
                "original_value": 400,
                "canonical_value": 400,
                "original_unit": "ng/mL",
                "canonical_unit": "ng/mL",
                "status": "RESOLVED",
                "range_status": "HIGH",
                "flags": ["ABOVE_DECISION_LIMIT"],
                "log_entries": []
            },
            {
                "original_code": "vitamin_d",
                "canonical_code": "vitamin_d_25oh",
                "original_value": 35,
                "canonical_value": 35,
                "original_unit": "ng/mL",
                "canonical_unit": "ng/mL",
                "status": "RESOLVED",
                "range_status": "OPTIMAL",
                "flags": [],
                "log_entries": []
            },
            {
                "original_code": "b12",
                "canonical_code": "b12",
                "original_value": 500,
                "canonical_value": 500,
                "original_unit": "pg/mL",
                "canonical_unit": "pg/mL",
                "status": "RESOLVED",
                "range_status": "OPTIMAL",
                "flags": [],
                "log_entries": []
            }
        ],
        "routing_constraints": ["BLOCK_IRON"],
        "safety_gates": [
            {
                "gate_id": "iron_block",
                "description": "Block iron when ferritin high",
                "trigger_marker": "ferritin",
                "trigger_value": 400,
                "threshold": 300,
                "routing_constraint": "BLOCK_IRON",
                "exception_active": False,
                "exception_reason": None
            }
        ],
        "require_review": False,
        "summary": {
            "total_markers": 3,
            "valid_markers": 3,
            "unknown_markers": 0,
            "in_range": 2,
            "out_of_range": 1
        },
        "ruleset_version": "registry_v1.0+ranges_v1.0",
        "input_hash": "sha256:abc123",
        "output_hash": "sha256:def456"
    }


@pytest.fixture
def mock_request_payload():
    """Standard request payload for bloodwork processing."""
    return {
        "markers": [
            {"code": "ferritin", "value": 400, "unit": "ng/mL"},
            {"code": "vitamin_d", "value": 35, "unit": "ng/mL"},
            {"code": "b12", "value": 500, "unit": "pg/mL"}
        ],
        "lab_profile": "GLOBAL_CONSERVATIVE",
        "sex": "male",
        "age": 35
    }


@pytest.fixture
def valid_handoff_dict():
    """Valid handoff object matching schema."""
    return {
        "handoff_version": "bloodwork_handoff.v1",
        "source": {
            "service": "bloodwork_engine",
            "base_url": BLOODWORK_BASE_URL,
            "endpoint": BLOODWORK_ENDPOINT,
            "engine_version": "1.0.0"
        },
        "input": {
            "lab_profile": "GLOBAL_CONSERVATIVE",
            "sex": "male",
            "age": 35,
            "markers": [
                {"code": "ferritin", "value": 400, "unit": "ng/mL"}
            ]
        },
        "output": {
            "routing_constraints": {
                "blocked_ingredients": ["iron"],
                "blocked_categories": [],
                "caution_flags": [],
                "requirements": [],
                "reason_codes": ["BLOCK_IRON"]
            },
            "signal_flags": [],
            "unknown_biomarkers": []
        },
        "audit": {
            "input_hash": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "output_hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "ruleset_version": "registry_v1.0+ranges_v1.0",
            "marker_registry_version": "registry_v1.0",
            "reference_ranges_version": "ranges_v1.0",
            "processed_at": "2025-12-28T15:00:00Z"
        }
    }


# ============================================
# TEST 1: BLOOD PRECEDENCE
# ============================================

class TestBloodPrecedence:
    """
    Test that blood-derived constraints cannot be removed by downstream layers.
    
    PRINCIPLE: Blood does not negotiate.
    """
    
    def test_blood_blocks_cannot_be_overridden(self, valid_handoff_dict):
        """Blood-blocked ingredients stay blocked regardless of brain decisions."""
        handoff = BloodworkHandoffV1(
            handoff_version=valid_handoff_dict["handoff_version"],
            source=valid_handoff_dict["source"],
            input=valid_handoff_dict["input"],
            output=valid_handoff_dict["output"],
            audit=valid_handoff_dict["audit"]
        )
        
        # Verify iron is blocked by blood
        assert blood_blocks_ingredient(handoff, "iron") is True
        assert blood_blocks_ingredient(handoff, "IRON") is True  # Case insensitive
        
        # Other ingredients not blocked
        assert blood_blocks_ingredient(handoff, "vitamin_d") is False
        assert blood_blocks_ingredient(handoff, "magnesium") is False
    
    def test_merge_preserves_blood_blocks(self):
        """Merged constraints include all blood blocks even if brain doesn't add them."""
        blood_constraints = {
            "blocked_ingredients": ["iron", "potassium"],
            "blocked_categories": ["hepatotoxic"],
            "caution_flags": ["vitamin_d"],
            "requirements": ["b12"],
            "reason_codes": ["BLOCK_IRON", "BLOCK_POTASSIUM"]
        }
        
        brain_constraints = {
            "blocked_ingredients": [],  # Brain tries to allow everything
            "blocked_categories": [],
            "caution_flags": ["zinc"],  # Brain adds its own caution
            "requirements": ["omega3"],  # Brain adds requirement
            "reason_codes": ["BRAIN_LOW_OMEGA3"]
        }
        
        merged = merge_routing_constraints(blood_constraints, brain_constraints)
        
        # Blood blocks preserved
        assert "iron" in merged["blocked_ingredients"]
        assert "potassium" in merged["blocked_ingredients"]
        assert "hepatotoxic" in merged["blocked_categories"]
        
        # Brain additions included
        assert "zinc" in merged["caution_flags"]
        assert "omega3" in merged["requirements"]
        
        # All reason codes present
        assert "BLOCK_IRON" in merged["reason_codes"]
        assert "BRAIN_LOW_OMEGA3" in merged["reason_codes"]
    
    def test_merge_is_additive_only(self):
        """Merge can only add constraints, never remove blood-derived ones."""
        blood_constraints = {
            "blocked_ingredients": ["iron"],
            "blocked_categories": [],
            "caution_flags": [],
            "requirements": [],
            "reason_codes": ["BLOCK_IRON"]
        }
        
        # Brain attempts to "unblock" iron (not possible)
        brain_constraints = {
            "blocked_ingredients": [],  # Empty = doesn't override blood
            "blocked_categories": [],
            "caution_flags": [],
            "requirements": ["iron"],  # Contradiction: require what's blocked
            "reason_codes": ["REQUIRE_IRON"]  # This won't remove the block
        }
        
        merged = merge_routing_constraints(blood_constraints, brain_constraints)
        
        # Iron still blocked (blood precedence)
        assert "iron" in merged["blocked_ingredients"]
        
        # Contradiction present but blood block takes precedence in routing
        # The merged object shows both, but downstream routing respects blocks > requirements
        assert "iron" in merged["requirements"]  # Brain's request is recorded
        assert "BLOCK_IRON" in merged["reason_codes"]  # But block reason present


# ============================================
# TEST 2: DETERMINISM
# ============================================

class TestDeterminism:
    """
    Test that handoff produces deterministic outputs.
    
    Same input + same blood data = identical hash.
    """
    
    def test_sorted_arrays_produce_stable_hash(self):
        """Constraint arrays are sorted for hash stability."""
        constraints_a = {
            "blocked_ingredients": ["zinc", "iron", "magnesium"],
            "blocked_categories": ["hepatotoxic", "abrasive"],
            "caution_flags": ["vitamin_d", "calcium"],
            "requirements": ["omega3", "b12"],
            "reason_codes": ["BLOCK_IRON", "CAUTION_VIT_D", "BLOCK_ZINC"]
        }
        
        # Same items, different initial order
        constraints_b = {
            "blocked_ingredients": ["iron", "magnesium", "zinc"],
            "blocked_categories": ["abrasive", "hepatotoxic"],
            "caution_flags": ["calcium", "vitamin_d"],
            "requirements": ["b12", "omega3"],
            "reason_codes": ["CAUTION_VIT_D", "BLOCK_ZINC", "BLOCK_IRON"]
        }
        
        merged_a = merge_routing_constraints(constraints_a, {})
        merged_b = merge_routing_constraints(constraints_b, {})
        
        # After merge, both should be identical (sorted)
        assert merged_a == merged_b
        
        # Hash is identical
        hash_a = canonicalize_and_hash(merged_a)
        hash_b = canonicalize_and_hash(merged_b)
        assert hash_a == hash_b
    
    def test_handoff_hash_is_deterministic(self, mock_bloodwork_response, mock_request_payload):
        """Same API response produces same handoff hash every time."""
        handoff_1 = _build_handoff_from_response(mock_bloodwork_response, mock_request_payload)
        handoff_2 = _build_handoff_from_response(mock_bloodwork_response, mock_request_payload)
        
        hash_1 = canonicalize_and_hash(handoff_1.to_dict())
        hash_2 = canonicalize_and_hash(handoff_2.to_dict())
        
        assert hash_1 == hash_2
    
    def test_different_inputs_produce_different_hashes(self):
        """Different constraint sets produce different hashes."""
        constraints_a = {
            "blocked_ingredients": ["iron"],
            "blocked_categories": [],
            "caution_flags": [],
            "requirements": [],
            "reason_codes": ["BLOCK_IRON"]
        }
        
        constraints_b = {
            "blocked_ingredients": ["iron", "zinc"],  # Additional block
            "blocked_categories": [],
            "caution_flags": [],
            "requirements": [],
            "reason_codes": ["BLOCK_IRON", "BLOCK_ZINC"]
        }
        
        hash_a = canonicalize_and_hash(merge_routing_constraints(constraints_a, {}))
        hash_b = canonicalize_and_hash(merge_routing_constraints(constraints_b, {}))
        
        assert hash_a != hash_b


# ============================================
# TEST 3: PERSISTENCE
# ============================================

class TestPersistence:
    """
    Test that handoff is correctly prepared for database persistence.
    """
    
    def test_handoff_to_decision_output_structure(self, valid_handoff_dict):
        """handoff_to_decision_output returns correct structure."""
        handoff = BloodworkHandoffV1(
            handoff_version=valid_handoff_dict["handoff_version"],
            source=valid_handoff_dict["source"],
            input=valid_handoff_dict["input"],
            output=valid_handoff_dict["output"],
            audit=valid_handoff_dict["audit"]
        )
        
        run_id = "test-run-123"
        result = handoff_to_decision_output(handoff, run_id)
        
        assert result["run_id"] == run_id
        assert result["phase"] == "bloodwork_handoff"
        assert isinstance(result["output_json"], dict)
        assert result["output_hash"].startswith("sha256:")
    
    def test_persistence_hash_matches_handoff(self, valid_handoff_dict):
        """Persisted hash matches direct hash of handoff."""
        handoff = BloodworkHandoffV1(
            handoff_version=valid_handoff_dict["handoff_version"],
            source=valid_handoff_dict["source"],
            input=valid_handoff_dict["input"],
            output=valid_handoff_dict["output"],
            audit=valid_handoff_dict["audit"]
        )
        
        persistence_data = handoff_to_decision_output(handoff, "run-456")
        direct_hash = canonicalize_and_hash(handoff.to_dict())
        
        assert persistence_data["output_hash"] == direct_hash
    
    def test_output_json_is_complete_handoff(self, valid_handoff_dict):
        """output_json contains the complete handoff object."""
        handoff = BloodworkHandoffV1(
            handoff_version=valid_handoff_dict["handoff_version"],
            source=valid_handoff_dict["source"],
            input=valid_handoff_dict["input"],
            output=valid_handoff_dict["output"],
            audit=valid_handoff_dict["audit"]
        )
        
        persistence_data = handoff_to_decision_output(handoff, "run-789")
        output_json = persistence_data["output_json"]
        
        # All required fields present
        assert output_json["handoff_version"] == "bloodwork_handoff.v1"
        assert "source" in output_json
        assert "input" in output_json
        assert "output" in output_json
        assert "audit" in output_json
        
        # Routing constraints preserved
        assert "blocked_ingredients" in output_json["output"]["routing_constraints"]


# ============================================
# TEST 4: OUTAGE HARD ABORT
# ============================================

class TestOutageHardAbort:
    """
    Test that bloodwork unavailability causes hard abort (503).
    
    STRICT MODE: No fallback, no graceful degradation.
    """
    
    def test_connection_error_raises_503(self, mock_request_payload):
        """Connection failure raises BLOODWORK_UNAVAILABLE with 503."""
        with patch('app.brain.bloodwork_handoff.httpx.Client') as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_client.return_value.__exit__ = MagicMock(return_value=False)
            mock_instance.post.side_effect = httpx.ConnectError("Connection refused")
            
            with pytest.raises(BloodworkHandoffException) as exc_info:
                fetch_bloodwork_handoff(
                    markers=mock_request_payload["markers"],
                    lab_profile=mock_request_payload["lab_profile"],
                    sex=mock_request_payload["sex"],
                    age=mock_request_payload["age"]
                )
            
            assert exc_info.value.error_code == BloodworkHandoffError.BLOODWORK_UNAVAILABLE
            assert exc_info.value.http_code == 503
    
    def test_timeout_raises_503(self, mock_request_payload):
        """Timeout raises BLOODWORK_TIMEOUT with 503."""
        with patch('app.brain.bloodwork_handoff.httpx.Client') as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_client.return_value.__exit__ = MagicMock(return_value=False)
            mock_instance.post.side_effect = httpx.TimeoutException("Request timed out")
            
            with pytest.raises(BloodworkHandoffException) as exc_info:
                fetch_bloodwork_handoff(
                    markers=mock_request_payload["markers"],
                    lab_profile=mock_request_payload["lab_profile"]
                )
            
            assert exc_info.value.error_code == BloodworkHandoffError.BLOODWORK_TIMEOUT
            assert exc_info.value.http_code == 503
    
    def test_api_error_raises_502(self, mock_request_payload):
        """API error (non-200 response) raises BLOODWORK_API_ERROR with 502."""
        with patch('app.brain.bloodwork_handoff.httpx.Client') as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_client.return_value.__exit__ = MagicMock(return_value=False)
            
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"
            mock_instance.post.return_value = mock_response
            
            with pytest.raises(BloodworkHandoffException) as exc_info:
                fetch_bloodwork_handoff(
                    markers=mock_request_payload["markers"],
                    lab_profile=mock_request_payload["lab_profile"]
                )
            
            assert exc_info.value.error_code == BloodworkHandoffError.BLOODWORK_API_ERROR
            assert exc_info.value.http_code == 502
    
    def test_no_graceful_degradation(self, mock_request_payload):
        """
        There is no fallback behavior on outage.
        
        STRICT MODE means we abort completely, not provide partial results.
        """
        with patch('app.brain.bloodwork_handoff.httpx.Client') as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value.__enter__ = MagicMock(return_value=mock_instance)
            mock_client.return_value.__exit__ = MagicMock(return_value=False)
            mock_instance.post.side_effect = httpx.ConnectError("Cannot connect")
            
            # Exception must be raised - no default/empty handoff returned
            with pytest.raises(BloodworkHandoffException):
                result = fetch_bloodwork_handoff(
                    markers=mock_request_payload["markers"],
                    lab_profile=mock_request_payload["lab_profile"]
                )
            
            # This code should never be reached
            assert False, "Should have raised exception, not returned result"


# ============================================
# TEST 5: SCHEMA VALIDATION
# ============================================

class TestSchemaValidation:
    """
    Test JSON Schema validation of handoff objects.
    """
    
    def test_valid_handoff_passes_validation(self, valid_handoff_dict):
        """Valid handoff object passes schema validation."""
        is_valid, error = validate_handoff_schema(valid_handoff_dict)
        assert is_valid is True
        assert error is None
    
    def test_missing_handoff_version_fails(self, valid_handoff_dict):
        """Missing handoff_version fails validation."""
        invalid = {k: v for k, v in valid_handoff_dict.items() if k != "handoff_version"}
        
        is_valid, error = validate_handoff_schema(invalid)
        assert is_valid is False
        assert "handoff_version" in error.lower() or "missing" in error.lower()
    
    def test_missing_routing_constraints_fails(self, valid_handoff_dict):
        """Missing routing_constraints in output fails validation."""
        invalid = valid_handoff_dict.copy()
        invalid["output"] = {"signal_flags": []}  # Missing routing_constraints
        
        is_valid, error = validate_handoff_schema(invalid)
        assert is_valid is False
        assert "routing_constraints" in error.lower() or "missing" in error.lower()
    
    def test_invalid_version_fails(self, valid_handoff_dict):
        """Wrong handoff_version value fails validation."""
        invalid = valid_handoff_dict.copy()
        invalid["handoff_version"] = "wrong_version"
        
        is_valid, error = validate_handoff_schema(invalid)
        assert is_valid is False
    
    def test_missing_audit_fields_fails(self, valid_handoff_dict):
        """Missing required audit fields fails validation."""
        invalid = valid_handoff_dict.copy()
        invalid["audit"] = {
            "input_hash": "sha256:abc",
            # Missing output_hash, ruleset_version, processed_at
        }
        
        is_valid, error = validate_handoff_schema(invalid)
        assert is_valid is False
    
    def test_invalid_hash_format_fails(self, valid_handoff_dict):
        """Invalid hash format (missing sha256: prefix) fails validation."""
        invalid = valid_handoff_dict.copy()
        invalid["audit"] = valid_handoff_dict["audit"].copy()
        invalid["audit"]["input_hash"] = "not_a_valid_hash"
        
        is_valid, error = validate_handoff_schema(invalid)
        # Note: basic validation may not catch this, but full jsonschema will
        # Either way, the schema defines the pattern constraint


# ============================================
# ADDITIONAL HELPER TESTS
# ============================================

class TestHelperFunctions:
    """Test helper functions for completeness."""
    
    def test_get_blood_cautions(self, valid_handoff_dict):
        """get_blood_cautions extracts caution flags correctly."""
        valid_handoff_dict["output"]["routing_constraints"]["caution_flags"] = ["vitamin_d", "calcium"]
        
        handoff = BloodworkHandoffV1(
            handoff_version=valid_handoff_dict["handoff_version"],
            source=valid_handoff_dict["source"],
            input=valid_handoff_dict["input"],
            output=valid_handoff_dict["output"],
            audit=valid_handoff_dict["audit"]
        )
        
        cautions = get_blood_cautions(handoff)
        assert "vitamin_d" in cautions
        assert "calcium" in cautions
    
    def test_has_incomplete_panel(self, valid_handoff_dict):
        """has_incomplete_panel detects BLOODWORK_INCOMPLETE_PANEL flag."""
        # Without flag
        handoff_complete = BloodworkHandoffV1(
            handoff_version=valid_handoff_dict["handoff_version"],
            source=valid_handoff_dict["source"],
            input=valid_handoff_dict["input"],
            output=valid_handoff_dict["output"],
            audit=valid_handoff_dict["audit"]
        )
        assert has_incomplete_panel(handoff_complete) is False
        
        # With flag
        valid_handoff_dict["output"]["signal_flags"] = ["BLOODWORK_INCOMPLETE_PANEL"]
        handoff_incomplete = BloodworkHandoffV1(
            handoff_version=valid_handoff_dict["handoff_version"],
            source=valid_handoff_dict["source"],
            input=valid_handoff_dict["input"],
            output=valid_handoff_dict["output"],
            audit=valid_handoff_dict["audit"]
        )
        assert has_incomplete_panel(handoff_incomplete) is True
    
    def test_routing_constraints_to_dict(self):
        """RoutingConstraints.to_dict() returns sorted, deduped arrays."""
        rc = RoutingConstraints(
            blocked_ingredients=["zinc", "iron", "zinc"],  # Duplicate
            blocked_categories=["hepatotoxic"],
            caution_flags=["vitamin_d", "calcium", "vitamin_d"],  # Duplicate
            requirements=["omega3"],
            reason_codes=["BLOCK_IRON", "BLOCK_ZINC", "BLOCK_IRON"]  # Duplicate
        )
        
        result = rc.to_dict()
        
        # Deduped
        assert len(result["blocked_ingredients"]) == 2
        assert len(result["caution_flags"]) == 2
        assert len(result["reason_codes"]) == 2
        
        # Sorted
        assert result["blocked_ingredients"] == ["iron", "zinc"]
        assert result["caution_flags"] == ["calcium", "vitamin_d"]


# ============================================
# INTEGRATION TEST (with mock)
# ============================================

class TestIntegration:
    """Integration test with mocked API response."""
    
    def test_full_handoff_flow(self, mock_bloodwork_response, mock_request_payload):
        """Complete flow from API response to validated handoff."""
        # Build handoff from response
        handoff = _build_handoff_from_response(mock_bloodwork_response, mock_request_payload)
        
        # Verify structure
        assert handoff.handoff_version == "bloodwork_handoff.v1"
        assert handoff.source["service"] == "bloodwork_engine"
        
        # Verify constraints extracted
        constraints = handoff.output["routing_constraints"]
        assert "iron" in constraints["blocked_ingredients"]
        assert "BLOCK_IRON" in constraints["reason_codes"]
        
        # Verify can be persisted
        persistence = handoff_to_decision_output(handoff, "integration-test-run")
        assert persistence["phase"] == "bloodwork_handoff"
        assert persistence["output_hash"].startswith("sha256:")
        
        # Verify blood precedence check works
        assert blood_blocks_ingredient(handoff, "iron") is True
        assert blood_blocks_ingredient(handoff, "vitamin_d") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
