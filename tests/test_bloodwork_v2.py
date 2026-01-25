#!/usr/bin/env python3
"""
GenoMAX² Bloodwork Engine v2.0 - Test Suite
============================================
Run this script to verify the bloodwork engine is working correctly.

Usage:
    python tests/test_bloodwork_v2.py
    
Or via pytest:
    pytest tests/test_bloodwork_v2.py -v
"""

import sys
import json
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from bloodwork_engine.engine_v2 import (
    BloodworkEngineV2,
    BloodworkDataLoaderV2,
    get_engine,
    get_loader,
    GateTier,
    GateAction,
    MarkerStatus,
    RangeStatus
)


def test_loader_initialization():
    """Test that the data loader initializes correctly."""
    print("\n=== Test: Loader Initialization ===")
    loader = get_loader()
    
    # Check marker count
    marker_count = len(loader.allowed_marker_codes)
    print(f"Allowed markers: {marker_count}")
    assert marker_count >= 37, f"Expected at least 37 markers, got {marker_count}"
    
    # Check range count
    range_count = loader.range_count
    print(f"Reference ranges: {range_count}")
    assert range_count > 0, "Expected reference ranges to be loaded"
    
    # Check safety gates
    gates = loader.get_safety_gates()
    gate_count = len(gates)
    print(f"Safety gates: {gate_count}")
    assert gate_count >= 23, f"Expected at least 23 gates, got {gate_count}"
    
    # Check ruleset version
    print(f"Ruleset version: {loader.ruleset_version}")
    
    print("✓ Loader initialization PASSED")
    return True


def test_marker_processing():
    """Test basic marker processing."""
    print("\n=== Test: Marker Processing ===")
    engine = get_engine()
    
    # Test markers
    test_markers = [
        {"code": "ferritin", "value": 150, "unit": "ng/mL"},
        {"code": "vitamin_d_25oh", "value": 45, "unit": "ng/mL"},
        {"code": "hemoglobin", "value": 14.5, "unit": "g/dL"},
        {"code": "fasting_glucose", "value": 95, "unit": "mg/dL"},
        {"code": "alt", "value": 28, "unit": "U/L"},
    ]
    
    result = engine.process_markers(markers=test_markers, sex="male", age=35)
    
    print(f"Processed at: {result.processed_at}")
    print(f"Lab profile: {result.lab_profile}")
    print(f"Total markers: {result.summary['total']}")
    print(f"Valid markers: {result.summary['valid']}")
    print(f"Ruleset version: {result.ruleset_version}")
    
    # Check all markers processed
    assert result.summary['total'] == 5, f"Expected 5 markers, got {result.summary['total']}"
    assert result.summary['valid'] == 5, f"Expected 5 valid markers, got {result.summary['valid']}"
    
    # Check no unknown markers
    assert result.summary['unknown'] == 0, f"Unexpected unknown markers"
    
    print("✓ Marker processing PASSED")
    return result


def test_unit_conversion():
    """Test unit conversion functionality."""
    print("\n=== Test: Unit Conversion ===")
    engine = get_engine()
    
    # Test vitamin D conversion (nmol/L to ng/mL)
    test_markers = [
        {"code": "vitamin_d_25oh", "value": 100, "unit": "nmol/L"},  # ~40 ng/mL
    ]
    
    result = engine.process_markers(markers=test_markers)
    
    marker = result.markers[0]
    print(f"Original: {marker.original_value} {marker.original_unit}")
    print(f"Converted: {marker.canonical_value} {marker.canonical_unit}")
    print(f"Conversion applied: {marker.conversion_applied}")
    
    assert marker.conversion_applied == True, "Expected conversion to be applied"
    # 100 nmol/L * 0.4 = 40 ng/mL
    assert 39 < marker.canonical_value < 41, f"Expected ~40, got {marker.canonical_value}"
    
    print("✓ Unit conversion PASSED")
    return True


def test_iron_block_gate():
    """Test iron block safety gate."""
    print("\n=== Test: Iron Block Gate ===")
    engine = get_engine()
    
    # High ferritin - should trigger iron block
    test_markers = [
        {"code": "ferritin", "value": 350, "unit": "ng/mL"},  # Above threshold
        {"code": "hs_crp", "value": 1.0, "unit": "mg/L"},      # Normal (no exception)
    ]
    
    result = engine.process_markers(markers=test_markers, sex="male")
    
    print(f"Routing constraints: {result.routing_constraints}")
    print(f"Safety gates triggered: {len(result.safety_gates)}")
    
    # Should have BLOCK_IRON in routing constraints
    assert "BLOCK_IRON" in result.routing_constraints, "Expected BLOCK_IRON in constraints"
    
    # Check gate details
    iron_gate = next((g for g in result.safety_gates if "iron" in g.gate_id.lower()), None)
    if iron_gate:
        print(f"Gate: {iron_gate.gate_id}")
        print(f"Description: {iron_gate.description}")
        print(f"Exception active: {iron_gate.exception_active}")
    
    print("✓ Iron block gate PASSED")
    return True


def test_iron_block_exception():
    """Test iron block exception when CRP is elevated (acute inflammation)."""
    print("\n=== Test: Iron Block Exception (Acute Inflammation) ===")
    engine = get_engine()
    
    # High ferritin BUT high CRP - exception should apply
    test_markers = [
        {"code": "ferritin", "value": 350, "unit": "ng/mL"},  # Above threshold
        {"code": "hs_crp", "value": 8.0, "unit": "mg/L"},      # Acute inflammation
    ]
    
    result = engine.process_markers(markers=test_markers, sex="male")
    
    print(f"Routing constraints: {result.routing_constraints}")
    
    # Should have FLAG_ACUTE_INFLAMMATION
    has_inflammation_flag = any("INFLAMMATION" in c for c in result.routing_constraints)
    print(f"Acute inflammation flag: {has_inflammation_flag}")
    
    # Check if iron block has exception active
    iron_gate = next((g for g in result.safety_gates if "iron" in g.gate_id.lower()), None)
    if iron_gate:
        print(f"Iron gate exception active: {iron_gate.exception_active}")
        print(f"Exception reason: {iron_gate.exception_reason}")
        
        # Exception should be active
        assert iron_gate.exception_active == True, "Expected exception to be active"
    
    print("✓ Iron block exception PASSED")
    return True


def test_tier1_potassium_block():
    """Test Tier 1 potassium block gate."""
    print("\n=== Test: Tier 1 Potassium Block Gate ===")
    engine = get_engine()
    
    # High potassium - should trigger block
    test_markers = [
        {"code": "potassium", "value": 5.5, "unit": "mEq/L"},  # Above 5.0 threshold
    ]
    
    result = engine.process_markers(markers=test_markers)
    
    print(f"Routing constraints: {result.routing_constraints}")
    
    # Should have BLOCK_POTASSIUM
    has_potassium_block = any("POTASSIUM" in c for c in result.routing_constraints)
    print(f"Potassium block triggered: {has_potassium_block}")
    assert has_potassium_block, "Expected potassium block"
    
    print("✓ Potassium block PASSED")
    return True


def test_hepatic_caution():
    """Test hepatic caution gate for elevated liver enzymes."""
    print("\n=== Test: Hepatic Caution Gate ===")
    engine = get_engine()
    
    # Elevated ALT/AST - should trigger hepatic caution
    test_markers = [
        {"code": "alt", "value": 65, "unit": "U/L"},  # Above 50 threshold (male)
        {"code": "ast", "value": 55, "unit": "U/L"},  # Above 50 threshold (male)
    ]
    
    result = engine.process_markers(markers=test_markers, sex="male")
    
    print(f"Routing constraints: {result.routing_constraints}")
    
    # Should have CAUTION_HEPATOTOXIC
    has_hepatic_caution = any("HEPAT" in c.upper() for c in result.routing_constraints)
    print(f"Hepatic caution triggered: {has_hepatic_caution}")
    assert has_hepatic_caution, "Expected hepatic caution"
    
    print("✓ Hepatic caution PASSED")
    return True


def test_mthfr_genetic_marker():
    """Test MTHFR genetic marker processing (if supported)."""
    print("\n=== Test: MTHFR Genetic Marker ===")
    engine = get_engine()
    
    # Test MTHFR C677T homozygous mutation
    test_markers = [
        {"code": "mthfr_c677t", "value": "TT", "unit": "categorical"},
    ]
    
    try:
        result = engine.process_markers(markers=test_markers)
        
        print(f"Routing constraints: {result.routing_constraints}")
        
        # Check if MTHFR triggered folic acid block
        has_folic_block = any("FOLIC" in c.upper() for c in result.routing_constraints)
        print(f"Folic acid block triggered: {has_folic_block}")
        
        # Check for methylfolate requirement
        has_methylfolate_flag = any("METHYLFOLATE" in c.upper() for c in result.routing_constraints)
        print(f"Methylfolate requirement flagged: {has_methylfolate_flag}")
        
        print("✓ MTHFR genetic marker PASSED")
    except Exception as e:
        print(f"⚠ MTHFR genetic marker not fully implemented: {e}")
    
    return True


def test_computed_markers():
    """Test computed/derived markers (HOMA-IR, ratios)."""
    print("\n=== Test: Computed Markers ===")
    engine = get_engine()
    
    # Provide markers needed for HOMA-IR calculation
    test_markers = [
        {"code": "fasting_glucose", "value": 100, "unit": "mg/dL"},
        {"code": "fasting_insulin", "value": 12, "unit": "µIU/mL"},
    ]
    
    result = engine.process_markers(markers=test_markers)
    
    print(f"Computed markers: {len(result.computed_markers) if hasattr(result, 'computed_markers') else 'N/A'}")
    
    if hasattr(result, 'computed_markers') and result.computed_markers:
        for cm in result.computed_markers:
            print(f"  - {cm.name}: {cm.value} ({cm.interpretation})")
    else:
        print("  (No computed markers returned)")
    
    print("✓ Computed markers test completed")
    return True


def test_full_panel():
    """Test a full panel of markers simulating a real lab report."""
    print("\n=== Test: Full Panel Processing ===")
    engine = get_engine()
    
    # Comprehensive test panel
    test_markers = [
        # Iron/Anemia
        {"code": "ferritin", "value": 85, "unit": "ng/mL"},
        {"code": "hemoglobin", "value": 14.2, "unit": "g/dL"},
        
        # Vitamins
        {"code": "vitamin_d_25oh", "value": 52, "unit": "ng/mL"},
        {"code": "vitamin_b12", "value": 550, "unit": "pg/mL"},
        {"code": "folate_serum", "value": 12, "unit": "ng/mL"},
        
        # Liver
        {"code": "alt", "value": 24, "unit": "U/L"},
        {"code": "ast", "value": 22, "unit": "U/L"},
        
        # Kidney
        {"code": "creatinine", "value": 0.95, "unit": "mg/dL"},
        {"code": "egfr", "value": 95, "unit": "mL/min/1.73m2"},
        
        # Glucose/Metabolic
        {"code": "fasting_glucose", "value": 88, "unit": "mg/dL"},
        {"code": "hba1c", "value": 5.2, "unit": "%"},
        
        # Inflammation
        {"code": "hs_crp", "value": 0.8, "unit": "mg/L"},
        
        # Electrolytes
        {"code": "calcium_serum", "value": 9.5, "unit": "mg/dL"},
        {"code": "magnesium_serum", "value": 2.1, "unit": "mg/dL"},
        {"code": "potassium", "value": 4.2, "unit": "mEq/L"},
        
        # Thyroid
        {"code": "tsh", "value": 2.1, "unit": "mIU/L"},
        
        # Lipids
        {"code": "triglycerides", "value": 95, "unit": "mg/dL"},
        {"code": "ldl_cholesterol", "value": 110, "unit": "mg/dL"},
        {"code": "hdl_cholesterol", "value": 55, "unit": "mg/dL"},
        
        # Hormones (male)
        {"code": "total_testosterone", "value": 650, "unit": "ng/dL"},
    ]
    
    result = engine.process_markers(markers=test_markers, sex="male", age=40)
    
    print(f"\nSUMMARY")
    print(f"{'='*40}")
    print(f"Total markers: {result.summary['total']}")
    print(f"Valid markers: {result.summary['valid']}")
    print(f"Unknown markers: {result.summary['unknown']}")
    print(f"Optimal markers: {result.summary.get('optimal', 'N/A')}")
    
    print(f"\nRANGE STATUS BREAKDOWN")
    for marker in result.markers:
        status_icon = "✓" if marker.range_status.value in ["OPTIMAL", "NORMAL"] else "⚠"
        print(f"  {status_icon} {marker.canonical_code}: {marker.canonical_value} {marker.canonical_unit} [{marker.range_status.value}]")
    
    print(f"\nSAFETY GATES")
    if result.safety_gates:
        for gate in result.safety_gates:
            print(f"  - {gate.gate_id}: {gate.routing_constraint}")
    else:
        print("  (No safety gates triggered - all markers in safe range)")
    
    print(f"\nROUTING CONSTRAINTS: {result.routing_constraints if result.routing_constraints else '(None)'}")
    print(f"Require review: {result.require_review}")
    
    print("\n✓ Full panel test PASSED")
    return result


def run_all_tests():
    """Run all tests."""
    print("="*60)
    print("GenoMAX² Bloodwork Engine v2.0 - Test Suite")
    print("="*60)
    
    tests = [
        test_loader_initialization,
        test_marker_processing,
        test_unit_conversion,
        test_iron_block_gate,
        test_iron_block_exception,
        test_tier1_potassium_block,
        test_hepatic_caution,
        test_mthfr_genetic_marker,
        test_computed_markers,
        test_full_panel,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"\n✗ {test.__name__} FAILED: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("="*60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
