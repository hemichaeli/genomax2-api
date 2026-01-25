"""
GenoMAX² Bloodwork Engine v2.0 - Pipeline Test
===============================================
Demonstrates the complete bloodwork-to-routing pipeline:
1. Process bloodwork markers through BloodworkEngineV2
2. Extract routing constraints via SafetyRouter  
3. Filter products based on constraints

Run with: python -m bloodwork_engine.test_pipeline
"""

import json
from datetime import date

# Import from our package
from bloodwork_engine.engine_v2 import BloodworkEngineV2, get_engine
from bloodwork_engine.safety_router import SafetyRouter, create_static_router


def test_iron_overload_scenario():
    """
    Test Scenario 1: Iron Overload
    - Ferritin elevated (420 ng/mL in male)
    - Should trigger GATE_001 BLOCK_IRON
    - Should block all iron supplements
    """
    print("\n" + "="*60)
    print("SCENARIO 1: Iron Overload (Male)")
    print("="*60)
    
    engine = get_engine()
    
    # Simulate bloodwork with elevated ferritin
    markers = {
        "ferritin": {"value": 420, "unit": "ng/mL"},  # High - should trigger iron block
        "hemoglobin": {"value": 15.5, "unit": "g/dL"},  # Normal
        "hs_crp": {"value": 0.8, "unit": "mg/L"},  # Normal - no inflammation deferral
    }
    
    result = engine.process_markers(
        markers=markers,
        sex="male",
        age=45,
        lab_profile="GLOBAL_CONSERVATIVE"
    )
    
    print(f"\nEngine Version: {result.engine_version}")
    print(f"Ruleset: {result.ruleset_version}")
    print(f"\nProcessed Markers: {len(result.processed_markers)}")
    
    # Check gates
    print(f"\nSafety Gates Triggered:")
    for gate in result.safety_gates:
        status = "🚫 ACTIVE" if gate.get('status') == 'ACTIVE' else "✓ inactive"
        print(f"  {gate.get('gate_code', 'N/A')}: {gate.get('constraint_code', 'N/A')} [{status}]")
    
    # Get routing constraints
    router = create_static_router()
    constraints = router.get_routing_constraints(result)
    
    print(f"\nRouting Constraints:")
    print(f"  Blocked ingredients: {constraints.blocked_ingredients}")
    print(f"  Caution ingredients: {list(constraints.caution_ingredients.keys())}")
    print(f"  Recommended: {list(constraints.recommended_ingredients.keys())}")
    
    # Test product filtering
    test_products = [
        {"id": "PROD_001", "name": "Iron Complex", "ingredients": ["iron_bisglycinate", "vitamin_c"]},
        {"id": "PROD_002", "name": "B-Complex", "ingredients": ["methylcobalamin", "methylfolate", "pyridoxal_5_phosphate"]},
        {"id": "PROD_003", "name": "Ferrous Fumarate", "ingredients": ["ferrous_fumarate"]},
        {"id": "PROD_004", "name": "Vitamin D3", "ingredients": ["cholecalciferol"]},
    ]
    
    filtered = router.filter_products(test_products, constraints)
    
    print(f"\nProduct Filtering Results:")
    for p in filtered:
        status = "✓ ALLOWED" if p.is_allowed else "🚫 BLOCKED"
        print(f"  {p.product_name}: {status}")
        if p.blocked_ingredients:
            print(f"    Blocked ingredients: {p.blocked_ingredients}")
        if p.recommended_ingredients:
            print(f"    Recommended: {p.recommended_ingredients}")
    
    return result, constraints


def test_methylation_mthfr_scenario():
    """
    Test Scenario 2: MTHFR Homozygous + Elevated Homocysteine
    - MTHFR C677T = TT (homozygous)
    - Elevated homocysteine
    - Should trigger methylfolate requirement and block folic acid
    """
    print("\n" + "="*60)
    print("SCENARIO 2: MTHFR TT + Elevated Homocysteine")
    print("="*60)
    
    engine = get_engine()
    
    markers = {
        "mthfr_c677t": {"value": "TT", "unit": "genotype"},  # Homozygous
        "homocysteine": {"value": 14.5, "unit": "µmol/L"},  # Elevated (>10)
        "vitamin_b12": {"value": 280, "unit": "pg/mL"},  # Low-normal
        "folate_serum": {"value": 8.2, "unit": "ng/mL"},  # Normal
    }
    
    result = engine.process_markers(
        markers=markers,
        sex="female",
        age=38,
        lab_profile="GLOBAL_CONSERVATIVE"
    )
    
    print(f"\nProcessed Markers:")
    for pm in result.processed_markers:
        print(f"  {pm.code}: {pm.value} {pm.unit} - {pm.status}")
    
    print(f"\nSafety Gates:")
    active_gates = [g for g in result.safety_gates if g.get('status') == 'ACTIVE']
    for gate in active_gates:
        print(f"  🚫 {gate.get('gate_code')}: {gate.get('constraint_code')}")
    
    router = create_static_router()
    constraints = router.get_routing_constraints(result)
    
    print(f"\nKey Constraints:")
    print(f"  BLOCKED: {constraints.blocked_ingredients}")
    print(f"  RECOMMENDED: {list(constraints.recommended_ingredients.keys())}")
    
    # Test with prenatal vitamins
    test_products = [
        {"id": "PRE_001", "name": "Standard Prenatal (Folic Acid)", "ingredients": ["folic_acid", "iron_bisglycinate", "vitamin_d3"]},
        {"id": "PRE_002", "name": "Methylated Prenatal", "ingredients": ["methylfolate", "methylcobalamin", "iron_bisglycinate"]},
        {"id": "PRE_003", "name": "B-Complex Methyl", "ingredients": ["methylfolate", "methylcobalamin", "pyridoxal_5_phosphate"]},
    ]
    
    filtered = router.filter_products(test_products, constraints)
    
    print(f"\nPrenatal Vitamin Filtering:")
    for p in filtered:
        status = "✓ ALLOWED" if p.is_allowed else "🚫 BLOCKED"
        blocked_reason = f" - Contains folic acid" if "folic_acid" in p.blocked_ingredients else ""
        print(f"  {p.product_name}: {status}{blocked_reason}")
        print(f"    Priority Score: {p.priority_score}")
    
    return result, constraints


def test_hepatotoxic_ashwagandha_block():
    """
    Test Scenario 3: Elevated Liver Enzymes
    - ALT elevated
    - Should trigger hepatotoxic caution
    - Should BLOCK ashwagandha (permanent block per GenoMAX² policy)
    """
    print("\n" + "="*60)
    print("SCENARIO 3: Elevated Liver Enzymes (Ashwagandha Block)")
    print("="*60)
    
    engine = get_engine()
    
    markers = {
        "alt": {"value": 62, "unit": "U/L"},  # Elevated (>50 male)
        "ast": {"value": 48, "unit": "U/L"},  # Borderline
    }
    
    result = engine.process_markers(
        markers=markers,
        sex="male",
        age=42,
        lab_profile="GLOBAL_CONSERVATIVE"
    )
    
    router = create_static_router()
    constraints = router.get_routing_constraints(result)
    
    print(f"\nHepatic Gate Status:")
    for gate in result.safety_gates:
        if 'HEPAT' in gate.get('constraint_code', '').upper():
            print(f"  {gate.get('gate_code')}: {gate.get('constraint_code')} - {gate.get('status')}")
    
    print(f"\nBlocked Ingredients: {constraints.blocked_ingredients}")
    print(f"Caution Ingredients: {list(constraints.caution_ingredients.keys())}")
    
    # Test stress/adaptogen products
    test_products = [
        {"id": "STRESS_001", "name": "Ashwagandha Complex", "ingredients": ["ashwagandha", "rhodiola"]},
        {"id": "STRESS_002", "name": "Rhodiola Stress", "ingredients": ["rhodiola", "vitamin_b5"]},
        {"id": "STRESS_003", "name": "Kava Calm", "ingredients": ["kava", "passionflower"]},
        {"id": "STRESS_004", "name": "L-Theanine Focus", "ingredients": ["l_theanine", "vitamin_b6"]},
    ]
    
    filtered = router.filter_products(test_products, constraints)
    
    print(f"\nStress/Adaptogen Products:")
    for p in filtered:
        status = "✓ ALLOWED" if p.is_allowed else "🚫 BLOCKED"
        caution = " ⚠️ CAUTION" if p.caution_ingredients else ""
        print(f"  {p.product_name}: {status}{caution}")
        if p.block_reasons:
            print(f"    Reason: {p.block_reasons[0]}")
        if p.caution_reasons:
            print(f"    Caution: {p.caution_reasons[0]}")
    
    return result, constraints


def test_insulin_resistance_scenario():
    """
    Test Scenario 4: Insulin Resistance (HOMA-IR Computed)
    - Elevated fasting insulin
    - Elevated fasting glucose
    - Should calculate HOMA-IR and flag insulin support
    """
    print("\n" + "="*60)
    print("SCENARIO 4: Insulin Resistance (Computed HOMA-IR)")
    print("="*60)
    
    engine = get_engine()
    
    markers = {
        "fasting_glucose": {"value": 108, "unit": "mg/dL"},  # Elevated
        "fasting_insulin": {"value": 18, "unit": "µIU/mL"},  # High
        "hba1c": {"value": 5.9, "unit": "%"},  # Prediabetic range
        "triglycerides": {"value": 180, "unit": "mg/dL"},  # Elevated
    }
    
    result = engine.process_markers(
        markers=markers,
        sex="male",
        age=52,
        lab_profile="GLOBAL_CONSERVATIVE"
    )
    
    print(f"\nComputed Markers:")
    if hasattr(result, 'computed_markers') and result.computed_markers:
        for cm in result.computed_markers:
            print(f"  {cm.name}: {cm.value:.2f} ({cm.interpretation})")
    
    print(f"\nRelevant Gates:")
    for gate in result.safety_gates:
        if gate.get('status') == 'ACTIVE':
            print(f"  🚫 {gate.get('gate_code')}: {gate.get('constraint_code')}")
    
    router = create_static_router()
    constraints = router.get_routing_constraints(result)
    
    print(f"\nRecommended for Insulin Support:")
    for ing, reason in constraints.recommended_ingredients.items():
        print(f"  ✓ {ing}: {reason}")
    
    return result, constraints


def test_complex_scenario():
    """
    Test Scenario 5: Complex Multi-Constraint Case
    - Multiple active gates
    - Conflicting constraints resolved
    """
    print("\n" + "="*60)
    print("SCENARIO 5: Complex Multi-Constraint Case")
    print("="*60)
    
    engine = get_engine()
    
    # Complex bloodwork with multiple issues
    markers = {
        "ferritin": {"value": 28, "unit": "ng/mL"},  # Low (but not anemia)
        "hemoglobin": {"value": 11.2, "unit": "g/dL"},  # Low (anemia threshold)
        "vitamin_b12": {"value": 220, "unit": "pg/mL"},  # Deficient
        "omega3_index": {"value": 3.2, "unit": "%"},  # Deficient (<4%)
        "homocysteine": {"value": 12, "unit": "µmol/L"},  # Elevated
        "tsh": {"value": 5.2, "unit": "mIU/L"},  # Elevated (hypothyroid)
        "platelet_count": {"value": 85, "unit": "x10^3/µL"},  # Low
    }
    
    result = engine.process_markers(
        markers=markers,
        sex="female",
        age=45,
        lab_profile="GLOBAL_CONSERVATIVE"
    )
    
    print(f"\nAll Active Gates:")
    active = [g for g in result.safety_gates if g.get('status') == 'ACTIVE']
    for gate in active:
        action = gate.get('action', 'UNKNOWN')
        print(f"  {action}: {gate.get('constraint_code')}")
    
    router = create_static_router()
    constraints = router.get_routing_constraints(result)
    
    print(f"\nFinal Routing Summary:")
    print(f"  Total Blocked: {len(constraints.blocked_ingredients)}")
    print(f"  Total Cautions: {len(constraints.caution_ingredients)}")
    print(f"  Total Recommended: {len(constraints.recommended_ingredients)}")
    
    print(f"\n  Blocked: {constraints.blocked_ingredients}")
    print(f"  Cautions: {list(constraints.caution_ingredients.keys())}")
    print(f"  Recommended: {list(constraints.recommended_ingredients.keys())}")
    
    # Note: IDA flag should override iron block
    print(f"\n  Note: Low Hgb + Low Ferritin triggers IDA flag,")
    print(f"        which should OVERRIDE iron block for this user.")
    
    return result, constraints


def main():
    """Run all test scenarios."""
    print("\n" + "#"*60)
    print("# GenoMAX² Bloodwork Engine v2.0 - Pipeline Test")
    print("# 'Blood does not negotiate'")
    print("#"*60)
    
    # Run all scenarios
    test_iron_overload_scenario()
    test_methylation_mthfr_scenario()
    test_hepatotoxic_ashwagandha_block()
    test_insulin_resistance_scenario()
    test_complex_scenario()
    
    print("\n" + "="*60)
    print("All scenarios completed successfully!")
    print("="*60)


if __name__ == "__main__":
    main()
