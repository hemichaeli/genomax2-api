"""
GenoMAX² Brain - Orchestrate V2 Bloodwork Integration Tests
============================================================
Tests for the BloodworkInputV2 integration into Brain Orchestrate.

Tests:
1. test_bloodwork_input_model_validation - Pydantic model validates correctly
2. test_integration_calls_bloodwork_engine - Verifies handoff flow
3. test_merged_constraints_preserve_blood_blocks - Blood precedence
4. test_unavailability_raises_exception - Strict mode 503
5. test_persistence_data_structure - Correct persistence format
"""

import pytest
from unittest.mock import patch, MagicMock
import httpx

from app.brain.orchestrate_v2_bloodwork import (
    BloodworkInputV2,
    MarkerInput,
    BloodworkIntegrationResult,
    orchestrate_with_bloodwork_input,
    build_bloodwork_error_response
)
from app.brain.bloodwork_handoff import (
    BloodworkHandoffV1,
    BloodworkHandoffException,
    BloodworkHandoffError
)


# ============================================
# FIXTURES
# ============================================

@pytest.fixture
def valid_bloodwork_input():
    """Valid BloodworkInputV2 for testing."""
    return BloodworkInputV2(
        markers=[
            MarkerInput(code="ferritin", value=400, unit="ng/mL"),
            MarkerInput(code="vitamin_d", value=35, unit="ng/mL"),
            MarkerInput(code="b12", value=500, unit="pg/mL")
        ],
        lab_profile="GLOBAL_CONSERVATIVE",
        sex="male",
        age=35
    )


@pytest.fixture
def mock_handoff():
    """Mock BloodworkHandoffV1 with iron block."""
    return BloodworkHandoffV1(
        handoff_version="bloodwork_handoff.v1",
        source={
            "service": "bloodwork_engine",
            "base_url": "https://web-production-97b74.up.railway.app",
            "endpoint": "/api/v1/bloodwork/process",
            "engine_version": "1.0.0"
        },
        input={
            "lab_profile": "GLOBAL_CONSERVATIVE",
            "sex": "male",
            "age": 35,
            "markers": [{"code": "ferritin", "value": 400, "unit": "ng/mL"}]
        },
        output={
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
        audit={
            "input_hash": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "output_hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "ruleset_version": "registry_v1.0+ranges_v1.0",
            "marker_registry_version": "registry_v1.0",
            "reference_ranges_version": "ranges_v1.0",
            "processed_at": "2025-12-28T16:00:00Z"
        }
    )


# ============================================
# TEST 1: PYDANTIC MODEL VALIDATION
# ============================================

class TestBloodworkInputV2Model:
    """Test BloodworkInputV2 Pydantic model validation."""
    
    def test_valid_input_accepted(self, valid_bloodwork_input):
        """Valid input passes validation."""
        assert len(valid_bloodwork_input.markers) == 3
        assert valid_bloodwork_input.lab_profile == "GLOBAL_CONSERVATIVE"
        assert valid_bloodwork_input.sex == "male"
        assert valid_bloodwork_input.age == 35
    
    def test_markers_required(self):
        """Empty markers array rejected."""
        with pytest.raises(ValueError):
            BloodworkInputV2(markers=[])
    
    def test_invalid_sex_rejected(self):
        """Invalid sex value rejected."""
        with pytest.raises(ValueError):
            BloodworkInputV2(
                markers=[MarkerInput(code="ferritin", value=100, unit="ng/mL")],
                sex="unknown"
            )
    
    def test_age_bounds_enforced(self):
        """Age must be 0-150."""
        with pytest.raises(ValueError):
            BloodworkInputV2(
                markers=[MarkerInput(code="ferritin", value=100, unit="ng/mL")],
                age=200
            )
        
        with pytest.raises(ValueError):
            BloodworkInputV2(
                markers=[MarkerInput(code="ferritin", value=100, unit="ng/mL")],
                age=-5
            )
    
    def test_to_markers_list_conversion(self, valid_bloodwork_input):
        """to_markers_list() converts to correct format."""
        markers_list = valid_bloodwork_input.to_markers_list()
        
        assert len(markers_list) == 3
        assert markers_list[0] == {"code": "ferritin", "value": 400, "unit": "ng/mL"}
        assert all(isinstance(m, dict) for m in markers_list)
    
    def test_defaults_applied(self):
        """Default values applied correctly."""
        minimal = BloodworkInputV2(
            markers=[MarkerInput(code="ferritin", value=100, unit="ng/mL")]
        )
        
        assert minimal.lab_profile == "GLOBAL_CONSERVATIVE"
        assert minimal.sex is None
        assert minimal.age is None


# ============================================
# TEST 2: INTEGRATION CALLS BLOODWORK ENGINE
# ============================================

class TestIntegrationFlow:
    """Test the orchestrate_with_bloodwork_input flow."""
    
    def test_successful_integration(self, valid_bloodwork_input, mock_handoff):
        """Successful integration returns correct result."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.return_value = mock_handoff
            
            result = orchestrate_with_bloodwork_input(
                bloodwork_input=valid_bloodwork_input,
                run_id="test-run-001"
            )
            
            assert result.success is True
            assert result.handoff is not None
            assert result.http_code == 200
            assert "iron" in result.merged_constraints["blocked_ingredients"]
    
    def test_calls_fetch_with_correct_params(self, valid_bloodwork_input, mock_handoff):
        """fetch_bloodwork_handoff called with correct parameters."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.return_value = mock_handoff
            
            orchestrate_with_bloodwork_input(bloodwork_input=valid_bloodwork_input)
            
            mock_fetch.assert_called_once()
            call_kwargs = mock_fetch.call_args.kwargs
            
            assert call_kwargs["lab_profile"] == "GLOBAL_CONSERVATIVE"
            assert call_kwargs["sex"] == "male"
            assert call_kwargs["age"] == 35
            assert len(call_kwargs["markers"]) == 3
    
    def test_generates_run_id_if_not_provided(self, valid_bloodwork_input, mock_handoff):
        """Run ID is generated if not provided."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.return_value = mock_handoff
            
            result = orchestrate_with_bloodwork_input(bloodwork_input=valid_bloodwork_input)
            
            assert result.persistence_data is not None
            assert result.persistence_data["run_id"] is not None
            assert len(result.persistence_data["run_id"]) == 36  # UUID format


# ============================================
# TEST 3: BLOOD CONSTRAINTS TAKE PRECEDENCE
# ============================================

class TestBloodPrecedence:
    """Test that blood constraints are always preserved."""
    
    def test_blood_blocks_preserved_in_merge(self, valid_bloodwork_input, mock_handoff):
        """Blood blocks remain even if brain has empty constraints."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.return_value = mock_handoff
            
            result = orchestrate_with_bloodwork_input(
                bloodwork_input=valid_bloodwork_input,
                brain_constraints={
                    "blocked_ingredients": [],  # Brain allows everything
                    "blocked_categories": [],
                    "caution_flags": [],
                    "requirements": ["iron"],  # Brain requires iron (contradiction)
                    "reason_codes": ["BRAIN_LOW_IRON"]
                }
            )
            
            # Blood block preserved
            assert "iron" in result.merged_constraints["blocked_ingredients"]
            
            # Brain's requirement also recorded (but block takes precedence in routing)
            assert "iron" in result.merged_constraints["requirements"]
            
            # Both reason codes present
            assert "BLOCK_IRON" in result.merged_constraints["reason_codes"]
            assert "BRAIN_LOW_IRON" in result.merged_constraints["reason_codes"]
    
    def test_brain_additions_merged(self, valid_bloodwork_input, mock_handoff):
        """Brain can add additional constraints."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.return_value = mock_handoff
            
            result = orchestrate_with_bloodwork_input(
                bloodwork_input=valid_bloodwork_input,
                brain_constraints={
                    "blocked_ingredients": ["zinc"],  # Brain adds a block
                    "blocked_categories": [],
                    "caution_flags": ["vitamin_d"],  # Brain adds caution
                    "requirements": [],
                    "reason_codes": ["BRAIN_BLOCK_ZINC", "BRAIN_CAUTION_VIT_D"]
                }
            )
            
            # Blood + brain blocks merged
            assert "iron" in result.merged_constraints["blocked_ingredients"]
            assert "zinc" in result.merged_constraints["blocked_ingredients"]
            
            # Brain caution added
            assert "vitamin_d" in result.merged_constraints["caution_flags"]


# ============================================
# TEST 4: UNAVAILABILITY RAISES EXCEPTION
# ============================================

class TestStrictModeErrors:
    """Test strict mode error handling (no graceful degradation)."""
    
    def test_connection_error_raises_exception(self, valid_bloodwork_input):
        """Connection failure raises BloodworkHandoffException."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.side_effect = BloodworkHandoffException(
                BloodworkHandoffError.BLOODWORK_UNAVAILABLE,
                "Cannot connect to Bloodwork Engine",
                http_code=503
            )
            
            with pytest.raises(BloodworkHandoffException) as exc_info:
                orchestrate_with_bloodwork_input(bloodwork_input=valid_bloodwork_input)
            
            assert exc_info.value.error_code == BloodworkHandoffError.BLOODWORK_UNAVAILABLE
            assert exc_info.value.http_code == 503
    
    def test_timeout_raises_exception(self, valid_bloodwork_input):
        """Timeout raises BloodworkHandoffException."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.side_effect = BloodworkHandoffException(
                BloodworkHandoffError.BLOODWORK_TIMEOUT,
                "Request timed out",
                http_code=503
            )
            
            with pytest.raises(BloodworkHandoffException) as exc_info:
                orchestrate_with_bloodwork_input(bloodwork_input=valid_bloodwork_input)
            
            assert exc_info.value.error_code == BloodworkHandoffError.BLOODWORK_TIMEOUT
            assert exc_info.value.http_code == 503
    
    def test_invalid_schema_raises_exception(self, valid_bloodwork_input):
        """Invalid schema raises BloodworkHandoffException."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.side_effect = BloodworkHandoffException(
                BloodworkHandoffError.BLOODWORK_INVALID_HANDOFF,
                "Schema validation failed",
                http_code=502
            )
            
            with pytest.raises(BloodworkHandoffException) as exc_info:
                orchestrate_with_bloodwork_input(bloodwork_input=valid_bloodwork_input)
            
            assert exc_info.value.error_code == BloodworkHandoffError.BLOODWORK_INVALID_HANDOFF
            assert exc_info.value.http_code == 502
    
    def test_no_fallback_result(self, valid_bloodwork_input):
        """No default/fallback result on error - exception must propagate."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.side_effect = BloodworkHandoffException(
                BloodworkHandoffError.BLOODWORK_UNAVAILABLE,
                "Service down",
                http_code=503
            )
            
            # Must raise, not return a fallback result
            with pytest.raises(BloodworkHandoffException):
                result = orchestrate_with_bloodwork_input(bloodwork_input=valid_bloodwork_input)
            
            # This line should never be reached
            assert False, "Should have raised exception"


# ============================================
# TEST 5: PERSISTENCE DATA STRUCTURE
# ============================================

class TestPersistenceData:
    """Test persistence data structure is correct."""
    
    def test_persistence_data_structure(self, valid_bloodwork_input, mock_handoff):
        """Persistence data has correct structure."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.return_value = mock_handoff
            
            result = orchestrate_with_bloodwork_input(
                bloodwork_input=valid_bloodwork_input,
                run_id="test-run-persist"
            )
            
            persistence = result.persistence_data
            
            assert persistence["run_id"] == "test-run-persist"
            assert persistence["phase"] == "bloodwork_handoff"
            assert isinstance(persistence["output_json"], dict)
            assert persistence["output_hash"].startswith("sha256:")
    
    def test_persistence_contains_full_handoff(self, valid_bloodwork_input, mock_handoff):
        """Persistence output_json contains complete handoff."""
        with patch('app.brain.orchestrate_v2_bloodwork.fetch_bloodwork_handoff') as mock_fetch:
            mock_fetch.return_value = mock_handoff
            
            result = orchestrate_with_bloodwork_input(
                bloodwork_input=valid_bloodwork_input
            )
            
            output_json = result.persistence_data["output_json"]
            
            assert output_json["handoff_version"] == "bloodwork_handoff.v1"
            assert "source" in output_json
            assert "input" in output_json
            assert "output" in output_json
            assert "audit" in output_json
            assert "routing_constraints" in output_json["output"]


# ============================================
# ERROR RESPONSE BUILDER TESTS
# ============================================

class TestErrorResponseBuilder:
    """Test build_bloodwork_error_response helper."""
    
    def test_builds_correct_error_structure(self):
        """Error response has correct structure."""
        exception = BloodworkHandoffException(
            BloodworkHandoffError.BLOODWORK_UNAVAILABLE,
            "Service unavailable",
            http_code=503
        )
        
        response = build_bloodwork_error_response(exception)
        
        assert response["success"] is False
        assert response["error"]["code"] == "BLOODWORK_UNAVAILABLE"
        assert response["error"]["message"] == "Service unavailable"
        assert response["error"]["type"] == "BLOODWORK_HANDOFF_ERROR"
        assert response["http_status"] == 503
    
    def test_handles_different_error_codes(self):
        """Different error codes produce correct responses."""
        for error_code, http_code in [
            (BloodworkHandoffError.BLOODWORK_UNAVAILABLE, 503),
            (BloodworkHandoffError.BLOODWORK_TIMEOUT, 503),
            (BloodworkHandoffError.BLOODWORK_API_ERROR, 502),
            (BloodworkHandoffError.BLOODWORK_INVALID_HANDOFF, 502),
        ]:
            exception = BloodworkHandoffException(error_code, "Test", http_code=http_code)
            response = build_bloodwork_error_response(exception)
            
            assert response["error"]["code"] == error_code.value
            assert response["http_status"] == http_code


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
