"""
GenoMAX² Bloodwork Engine v1.0 - Regression Tests
=================================================
Comprehensive tests for bloodwork processing, classification, and safety gates.

Test Categories:
1. Marker classification tests (in_range, low, high, critical)
2. Safety gate activation tests
3. Unit conversion tests
4. Determinism verification tests
5. Edge case tests
"""

import pytest
import json
from pathlib import Path

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from bloodwork_engine.engine import (
    BloodworkEngine, 
    BloodworkDataLoader,
    MarkerStatus,
    RangeStatus,
    get_engine,
    get_loader
)


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the data loader singleton before each test."""
    BloodworkDataLoader.reset()
    yield
    BloodworkDataLoader.reset()


@pytest.fixture
def engine():
    """Get a fresh BloodworkEngine instance."""
    return get_engine(lab_profile="GLOBAL_CONSERVATIVE")


@pytest.fixture
def loader():
    """Get the data loader."""
    return get_loader()


# ============================================================
# TEST: DATA LOADING & VALIDATION
# ============================================================

class TestDataLoading:
    """Tests for data loading and validation."""
    
    def test_marker_registry_loads(self, loader):
        """Verify marker registry loads with correct count."""
        assert len(loader.allowed_marker_codes) == 13
        assert "ferritin" in loader.allowed_marker_codes
        assert "vitamin_d_25oh" in loader.allowed_marker_codes
    
    def test_reference_ranges_loads(self, loader):
        """Verify reference ranges load with ranges defined."""
        assert loader.range_count > 0, "Reference ranges should be populated"
        assert loader.range_count >= 13, "Should have at least one range per marker"
    
    def test_ruleset_version(self, loader):
        """Verify ruleset version is properly constructed."""
        version = loader.ruleset_version
        assert "registry_v" in version
        assert "ranges_v" in version
    
    def test_safety_gates_defined(self, loader):
        """Verify safety gates are defined."""
        gates = loader.get_safety_gates()
        assert "iron_block" in gates
        assert "vitamin_d_caution" in gates
        assert "hepatic_caution" in gates
        assert "renal_caution" in gates
        assert "acute_inflammation" in gates


# ============================================================
# TEST: MARKER CLASSIFICATION - FERRITIN
# ============================================================

class TestFerritinClassification:
    """Tests for ferritin marker classification."""
    
    def test_ferritin_in_range_male(self, engine):
        """Male ferritin in optimal range (50-200)."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 100, "unit": "ng/mL"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.IN_RANGE
        assert marker.canonical_value == 100
    
    def test_ferritin_low_male(self, engine):
        """Male ferritin below optimal (<50)."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 40, "unit": "ng/mL"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.LOW
    
    def test_ferritin_high_male(self, engine):
        """Male ferritin above optimal (>200)."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 250, "unit": "ng/mL"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.HIGH
    
    def test_ferritin_critical_low_male(self, engine):
        """Male ferritin critically low (<12)."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 8, "unit": "ng/mL"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.CRITICAL_LOW
        assert "CRITICAL:ferritin:CRITICAL_LOW" in result.routing_constraints
    
    def test_ferritin_critical_high_male(self, engine):
        """Male ferritin critically high (>500)."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 600, "unit": "ng/mL"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.CRITICAL_HIGH
    
    def test_ferritin_in_range_female(self, engine):
        """Female ferritin in optimal range (30-150)."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 80, "unit": "ng/mL"}],
            sex="female"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.IN_RANGE


# ============================================================
# TEST: MARKER CLASSIFICATION - VITAMIN D
# ============================================================

class TestVitaminDClassification:
    """Tests for vitamin D marker classification."""
    
    def test_vitamin_d_in_range(self, engine):
        """Vitamin D in optimal range (40-80)."""
        result = engine.process_markers(
            markers=[{"code": "vitamin_d_25oh", "value": 50, "unit": "ng/mL"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.IN_RANGE
    
    def test_vitamin_d_low(self, engine):
        """Vitamin D below optimal (<40)."""
        result = engine.process_markers(
            markers=[{"code": "vitamin_d_25oh", "value": 30, "unit": "ng/mL"}]
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.LOW
    
    def test_vitamin_d_critical_low(self, engine):
        """Vitamin D critically low (<10)."""
        result = engine.process_markers(
            markers=[{"code": "vitamin_d_25oh", "value": 8, "unit": "ng/mL"}]
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.CRITICAL_LOW


# ============================================================
# TEST: MARKER CLASSIFICATION - LIVER ENZYMES
# ============================================================

class TestLiverEnzymes:
    """Tests for ALT and AST classification."""
    
    def test_alt_in_range_male(self, engine):
        """Male ALT in range (7-40)."""
        result = engine.process_markers(
            markers=[{"code": "alt", "value": 25, "unit": "U/L"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.IN_RANGE
    
    def test_alt_high_male(self, engine):
        """Male ALT above optimal (>40)."""
        result = engine.process_markers(
            markers=[{"code": "alt", "value": 55, "unit": "U/L"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.HIGH
    
    def test_ast_in_range_female(self, engine):
        """Female AST in range (10-32)."""
        result = engine.process_markers(
            markers=[{"code": "ast", "value": 20, "unit": "U/L"}],
            sex="female"
        )
        marker = result.markers[0]
        assert marker.range_status == RangeStatus.IN_RANGE


# ============================================================
# TEST: SAFETY GATE - IRON BLOCK
# ============================================================

class TestIronBlockGate:
    """Tests for iron block safety gate."""
    
    def test_iron_block_triggered_male(self, engine):
        """Iron block gate triggers when male ferritin > 300."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 350, "unit": "ng/mL"}],
            sex="male"
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "iron_block" in gate_ids
        assert "BLOCK_IRON" in result.routing_constraints
    
    def test_iron_block_not_triggered_normal(self, engine):
        """Iron block gate does not trigger with normal ferritin."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 100, "unit": "ng/mL"}],
            sex="male"
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "iron_block" not in gate_ids
    
    def test_iron_block_exception_with_acute_crp(self, engine):
        """Iron block has exception when CRP indicates acute inflammation."""
        result = engine.process_markers(
            markers=[
                {"code": "ferritin", "value": 400, "unit": "ng/mL"},
                {"code": "hs_crp", "value": 5.0, "unit": "mg/L"}  # Acute inflammation
            ],
            sex="male"
        )
        iron_gate = next((g for g in result.safety_gates if g.gate_id == "iron_block"), None)
        assert iron_gate is not None
        assert iron_gate.exception_active == True
        assert "Acute inflammation" in iron_gate.exception_reason
        # BLOCK_IRON should NOT be in routing constraints due to exception
        assert "BLOCK_IRON" not in result.routing_constraints


# ============================================================
# TEST: SAFETY GATE - VITAMIN D CAUTION
# ============================================================

class TestVitaminDCautionGate:
    """Tests for vitamin D caution safety gate."""
    
    def test_vitamin_d_caution_triggered(self, engine):
        """Vitamin D caution triggers when calcium > 10.5."""
        result = engine.process_markers(
            markers=[{"code": "calcium_serum", "value": 10.8, "unit": "mg/dL"}]
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "vitamin_d_caution" in gate_ids
        assert "CAUTION_VITAMIN_D" in result.routing_constraints
    
    def test_vitamin_d_caution_not_triggered(self, engine):
        """Vitamin D caution does not trigger with normal calcium."""
        result = engine.process_markers(
            markers=[{"code": "calcium_serum", "value": 9.5, "unit": "mg/dL"}]
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "vitamin_d_caution" not in gate_ids


# ============================================================
# TEST: SAFETY GATE - HEPATIC CAUTION
# ============================================================

class TestHepaticCautionGate:
    """Tests for hepatic caution safety gate."""
    
    def test_hepatic_caution_triggered_by_alt(self, engine):
        """Hepatic caution triggers when ALT > threshold."""
        result = engine.process_markers(
            markers=[{"code": "alt", "value": 60, "unit": "U/L"}],
            sex="male"
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "hepatic_caution" in gate_ids
        assert "CAUTION_HEPATOTOXIC" in result.routing_constraints
    
    def test_hepatic_caution_triggered_by_ast(self, engine):
        """Hepatic caution triggers when AST > threshold."""
        result = engine.process_markers(
            markers=[{"code": "ast", "value": 55, "unit": "U/L"}],
            sex="male"
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "hepatic_caution" in gate_ids
    
    def test_hepatic_caution_not_triggered(self, engine):
        """Hepatic caution does not trigger with normal enzymes."""
        result = engine.process_markers(
            markers=[
                {"code": "alt", "value": 25, "unit": "U/L"},
                {"code": "ast", "value": 22, "unit": "U/L"}
            ],
            sex="male"
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "hepatic_caution" not in gate_ids


# ============================================================
# TEST: SAFETY GATE - RENAL CAUTION
# ============================================================

class TestRenalCautionGate:
    """Tests for renal caution safety gate."""
    
    def test_renal_caution_triggered_by_low_egfr(self, engine):
        """Renal caution triggers when eGFR < 60."""
        result = engine.process_markers(
            markers=[{"code": "egfr", "value": 55, "unit": "mL/min/1.73m2"}]
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "renal_caution" in gate_ids
        assert "CAUTION_RENAL" in result.routing_constraints
    
    def test_renal_caution_triggered_by_high_creatinine(self, engine):
        """Renal caution triggers when creatinine > threshold."""
        result = engine.process_markers(
            markers=[{"code": "creatinine", "value": 1.5, "unit": "mg/dL"}],
            sex="male"
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "renal_caution" in gate_ids
    
    def test_renal_caution_not_triggered(self, engine):
        """Renal caution does not trigger with normal kidney function."""
        result = engine.process_markers(
            markers=[
                {"code": "egfr", "value": 95, "unit": "mL/min/1.73m2"},
                {"code": "creatinine", "value": 1.0, "unit": "mg/dL"}
            ],
            sex="male"
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "renal_caution" not in gate_ids


# ============================================================
# TEST: SAFETY GATE - ACUTE INFLAMMATION
# ============================================================

class TestAcuteInflammationGate:
    """Tests for acute inflammation safety gate."""
    
    def test_acute_inflammation_triggered(self, engine):
        """Acute inflammation flag triggers when hs-CRP > 3."""
        result = engine.process_markers(
            markers=[{"code": "hs_crp", "value": 5.0, "unit": "mg/L"}]
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "acute_inflammation" in gate_ids
        assert "FLAG_ACUTE_INFLAMMATION" in result.routing_constraints
    
    def test_acute_inflammation_not_triggered(self, engine):
        """Acute inflammation does not trigger with low CRP."""
        result = engine.process_markers(
            markers=[{"code": "hs_crp", "value": 0.5, "unit": "mg/L"}]
        )
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "acute_inflammation" not in gate_ids


# ============================================================
# TEST: DETERMINISM
# ============================================================

class TestDeterminism:
    """Tests for deterministic output."""
    
    def test_same_input_same_output(self, engine):
        """Same input produces identical output hash."""
        markers = [
            {"code": "ferritin", "value": 100, "unit": "ng/mL"},
            {"code": "vitamin_d_25oh", "value": 50, "unit": "ng/mL"}
        ]
        
        result1 = engine.process_markers(markers=markers, sex="male", age=35)
        result2 = engine.process_markers(markers=markers, sex="male", age=35)
        
        assert result1.input_hash == result2.input_hash
        assert result1.output_hash == result2.output_hash
    
    def test_different_input_different_hash(self, engine):
        """Different input produces different hash."""
        result1 = engine.process_markers(
            markers=[{"code": "ferritin", "value": 100, "unit": "ng/mL"}],
            sex="male"
        )
        result2 = engine.process_markers(
            markers=[{"code": "ferritin", "value": 200, "unit": "ng/mL"}],
            sex="male"
        )
        
        assert result1.input_hash != result2.input_hash
        assert result1.output_hash != result2.output_hash


# ============================================================
# TEST: UNIT CONVERSION
# ============================================================

class TestUnitConversion:
    """Tests for unit conversion."""
    
    def test_vitamin_d_nmol_to_ng(self, engine):
        """Convert vitamin D from nmol/L to ng/mL."""
        result = engine.process_markers(
            markers=[{"code": "vitamin_d_25oh", "value": 125, "unit": "nmol/L"}]
        )
        marker = result.markers[0]
        assert marker.conversion_applied == True
        assert marker.canonical_unit == "ng/mL"
        # 125 nmol/L * 0.4 = 50 ng/mL
        assert abs(marker.canonical_value - 50) < 0.1
    
    def test_ferritin_ug_l_to_ng_ml(self, engine):
        """Ferritin µg/L to ng/mL (1:1 conversion)."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 100, "unit": "µg/L"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.canonical_value == 100  # 1:1 conversion


# ============================================================
# TEST: MISSING RANGE - NOW SHOULD NOT APPEAR
# ============================================================

class TestNoMissingRange:
    """Verify MISSING_RANGE no longer appears for defined markers."""
    
    def test_no_missing_range_for_ferritin(self, engine):
        """Ferritin should not return MISSING_RANGE."""
        result = engine.process_markers(
            markers=[{"code": "ferritin", "value": 100, "unit": "ng/mL"}],
            sex="male"
        )
        marker = result.markers[0]
        assert marker.range_status != RangeStatus.MISSING_RANGE
    
    def test_no_missing_range_for_all_markers(self, engine, loader):
        """None of the 13 markers should return MISSING_RANGE."""
        for code in loader.allowed_marker_codes:
            marker_def = loader.get_marker_definition(code)
            unit = marker_def["canonical_unit"]
            
            result = engine.process_markers(
                markers=[{"code": code, "value": 50, "unit": unit}],
                sex="male",  # Use male for testosterone
                age=35
            )
            marker = result.markers[0]
            
            # Should have a range status other than MISSING_RANGE
            assert marker.range_status != RangeStatus.MISSING_RANGE, \
                f"Marker {code} returned MISSING_RANGE"


# ============================================================
# TEST: UNKNOWN MARKER
# ============================================================

class TestUnknownMarker:
    """Tests for unknown marker handling."""
    
    def test_unknown_marker_flagged(self, engine):
        """Unknown markers are properly flagged."""
        result = engine.process_markers(
            markers=[{"code": "unknown_biomarker", "value": 50, "unit": "mg/dL"}]
        )
        marker = result.markers[0]
        assert marker.status == MarkerStatus.UNKNOWN
        assert marker.range_status == RangeStatus.REQUIRE_REVIEW
        assert "UNKNOWN_MARKER" in marker.flags


# ============================================================
# TEST: COMPLETE PANEL PROCESSING
# ============================================================

class TestCompletePanelProcessing:
    """Tests for processing complete biomarker panels."""
    
    def test_metabolic_panel_male(self, engine):
        """Process a complete metabolic panel for male."""
        markers = [
            {"code": "ferritin", "value": 150, "unit": "ng/mL"},
            {"code": "vitamin_d_25oh", "value": 55, "unit": "ng/mL"},
            {"code": "calcium_serum", "value": 9.5, "unit": "mg/dL"},
            {"code": "hs_crp", "value": 0.8, "unit": "mg/L"},
            {"code": "fasting_glucose", "value": 90, "unit": "mg/dL"},
            {"code": "hba1c", "value": 5.2, "unit": "%"},
            {"code": "creatinine", "value": 1.0, "unit": "mg/dL"},
            {"code": "egfr", "value": 95, "unit": "mL/min/1.73m2"},
            {"code": "alt", "value": 28, "unit": "U/L"},
            {"code": "ast", "value": 25, "unit": "U/L"},
            {"code": "magnesium_serum", "value": 2.1, "unit": "mg/dL"},
            {"code": "hemoglobin", "value": 15.5, "unit": "g/dL"},
            {"code": "total_testosterone", "value": 550, "unit": "ng/dL"}
        ]
        
        result = engine.process_markers(markers=markers, sex="male", age=35)
        
        # Verify all markers processed
        assert result.summary["total"] == 13
        assert result.summary["valid"] == 13
        assert result.summary["unknown"] == 0
        
        # With all values in optimal range, should have no safety gates
        assert len(result.safety_gates) == 0
        
        # All should be in range
        assert result.summary["in_range"] == 13
    
    def test_abnormal_panel_triggers_gates(self, engine):
        """Process panel with abnormal values triggers safety gates."""
        markers = [
            {"code": "ferritin", "value": 400, "unit": "ng/mL"},  # High - iron block
            {"code": "calcium_serum", "value": 10.8, "unit": "mg/dL"},  # High - vit D caution
            {"code": "alt", "value": 65, "unit": "U/L"},  # High - hepatic caution
            {"code": "egfr", "value": 50, "unit": "mL/min/1.73m2"},  # Low - renal caution
            {"code": "hs_crp", "value": 0.5, "unit": "mg/L"}  # Normal (no acute inflammation)
        ]
        
        result = engine.process_markers(markers=markers, sex="male", age=45)
        
        # Should trigger multiple safety gates
        gate_ids = [g.gate_id for g in result.safety_gates]
        assert "iron_block" in gate_ids
        assert "vitamin_d_caution" in gate_ids
        assert "hepatic_caution" in gate_ids
        assert "renal_caution" in gate_ids
        
        # Verify routing constraints
        assert "BLOCK_IRON" in result.routing_constraints
        assert "CAUTION_VITAMIN_D" in result.routing_constraints
        assert "CAUTION_HEPATOTOXIC" in result.routing_constraints
        assert "CAUTION_RENAL" in result.routing_constraints


# ============================================================
# RUN TESTS
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
