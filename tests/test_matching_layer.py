"""
Matching Layer Tests (Issue #7)

Tests for intent-to-SKU matching logic.

Tests validate:
- Gender filtering (MAXimo² / MAXima²)
- Intent-to-ingredient matching
- Match score calculation
- Requirement fulfillment
- Caution warning propagation
- Deterministic output
- Edge cases

Version: matching_layer_v1
"""

import pytest
from typing import List

from app.matching.models import (
    MatchingInput,
    MatchingResult,
    ProtocolItem,
    UnmatchedIntent,
    MatchingAudit,
    AllowedSKUInput,
    IntentInput,
    UserContext,
)
from app.matching.match import (
    resolve_matching,
    filter_by_gender,
    calculate_match_score,
    match_intents_to_skus,
    fulfill_requirements,
)


# ============================================================================
# Test Fixtures
# ============================================================================

def make_sku(
    sku_id: str,
    product_name: str,
    ingredient_tags: List[str],
    gender_line: str = "UNISEX",
    caution_flags: List[str] = None,
    evidence_tier: str = "TIER_1",
) -> AllowedSKUInput:
    """Helper to create test SKUs."""
    return AllowedSKUInput(
        sku_id=sku_id,
        product_name=product_name,
        ingredient_tags=ingredient_tags,
        category_tags=[],
        gender_line=gender_line,
        evidence_tier=evidence_tier,
        caution_flags=caution_flags or [],
        caution_reasons=[f"CAUTION_{f.upper()}" for f in (caution_flags or [])],
        fulfills_requirements=[],
    )


def make_intent(
    code: str,
    priority: int,
    targets: List[str],
) -> IntentInput:
    """Helper to create test intents."""
    return IntentInput(
        code=code,
        priority=priority,
        ingredient_targets=targets,
        source="goal",
    )


# ============================================================================
# Gender Filter Tests
# ============================================================================

class TestGenderFilter:
    """Test gender-based SKU filtering."""
    
    def test_maximo_sees_male_and_unisex(self):
        """MAXimo² users should see male and unisex SKUs only."""
        skus = [
            make_sku("M1", "Male Product", ["b12"], gender_line="MAXimo2"),
            make_sku("F1", "Female Product", ["b12"], gender_line="MAXima2"),
            make_sku("U1", "Unisex Product", ["b12"], gender_line="UNISEX"),
        ]
        context = UserContext(sex="male", product_line="MAXimo2")
        
        filtered = filter_by_gender(skus, context)
        
        sku_ids = [s.sku_id for s in filtered]
        assert "M1" in sku_ids
        assert "U1" in sku_ids
        assert "F1" not in sku_ids
    
    def test_maxima_sees_female_and_unisex(self):
        """MAXima² users should see female and unisex SKUs only."""
        skus = [
            make_sku("M1", "Male Product", ["b12"], gender_line="MAXimo2"),
            make_sku("F1", "Female Product", ["b12"], gender_line="MAXima2"),
            make_sku("U1", "Unisex Product", ["b12"], gender_line="UNISEX"),
        ]
        context = UserContext(sex="female", product_line="MAXima2")
        
        filtered = filter_by_gender(skus, context)
        
        sku_ids = [s.sku_id for s in filtered]
        assert "F1" in sku_ids
        assert "U1" in sku_ids
        assert "M1" not in sku_ids
    
    def test_null_gender_line_included(self):
        """SKUs with null/empty gender_line should be included."""
        skus = [
            make_sku("N1", "No Gender", ["b12"], gender_line=None),
            make_sku("E1", "Empty Gender", ["b12"], gender_line=""),
        ]
        context = UserContext(sex="male")
        
        filtered = filter_by_gender(skus, context)
        
        assert len(filtered) == 2
    
    def test_context_derives_product_line(self):
        """UserContext should auto-derive product_line from sex."""
        male_context = UserContext(sex="male")
        female_context = UserContext(sex="female")
        
        assert male_context.product_line == "MAXimo2"
        assert female_context.product_line == "MAXima2"


# ============================================================================
# Match Score Tests
# ============================================================================

class TestMatchScore:
    """Test match score calculation."""
    
    def test_perfect_match(self):
        """All intent targets covered = score 1.0."""
        sku_ings = {"b12", "iron", "d3"}
        intent_targets = {"b12", "iron"}
        
        score, overlap = calculate_match_score(sku_ings, intent_targets)
        
        assert score == 1.0
        assert overlap == {"b12", "iron"}
    
    def test_partial_match(self):
        """Partial coverage = score < 1.0."""
        sku_ings = {"b12", "d3"}
        intent_targets = {"b12", "iron", "zinc"}
        
        score, overlap = calculate_match_score(sku_ings, intent_targets)
        
        assert score == pytest.approx(1/3, abs=0.01)
        assert overlap == {"b12"}
    
    def test_no_match(self):
        """No overlap = score 0."""
        sku_ings = {"b12", "d3"}
        intent_targets = {"iron", "zinc"}
        
        score, overlap = calculate_match_score(sku_ings, intent_targets)
        
        assert score == 0.0
        assert overlap == set()
    
    def test_empty_targets(self):
        """Empty intent targets = score 0."""
        sku_ings = {"b12", "d3"}
        intent_targets = set()
        
        score, overlap = calculate_match_score(sku_ings, intent_targets)
        
        assert score == 0.0


# ============================================================================
# Intent Matching Tests
# ============================================================================

class TestIntentMatching:
    """Test intent-to-SKU matching."""
    
    def test_intent_matches_sku(self):
        """Intent with matching ingredients finds SKU."""
        skus = [
            make_sku("S1", "Energy Boost", ["b12", "coq10"]),
        ]
        intents = [
            make_intent("INTENT_ENERGY", 1, ["b12", "coq10"]),
        ]
        
        candidates, unmatched = match_intents_to_skus(skus, intents)
        
        assert len(unmatched) == 0
        assert "S1" in candidates
        assert len(candidates["S1"].intents) == 1
    
    def test_unmatched_intent_tracked(self):
        """Intent with no matching SKUs goes to unmatched list."""
        skus = [
            make_sku("S1", "Sleep Aid", ["melatonin", "magnesium"]),
        ]
        intents = [
            make_intent("INTENT_ENERGY", 1, ["b12", "coq10"]),
        ]
        
        candidates, unmatched = match_intents_to_skus(skus, intents)
        
        assert len(unmatched) == 1
        assert unmatched[0].code == "INTENT_ENERGY"
    
    def test_multiple_skus_match_same_intent(self):
        """Multiple SKUs can match the same intent."""
        skus = [
            make_sku("S1", "Product A", ["b12", "d3"]),
            make_sku("S2", "Product B", ["b12", "iron"]),
        ]
        intents = [
            make_intent("INTENT_ENERGY", 1, ["b12"]),
        ]
        
        candidates, unmatched = match_intents_to_skus(skus, intents)
        
        assert len(unmatched) == 0
        assert len(candidates["S1"].intents) == 1
        assert len(candidates["S2"].intents) == 1
    
    def test_case_insensitive_matching(self):
        """Ingredient matching should be case-insensitive."""
        skus = [
            make_sku("S1", "Product", ["B12", "CoQ10"]),
        ]
        intents = [
            make_intent("INTENT_ENERGY", 1, ["b12", "coq10"]),
        ]
        
        candidates, unmatched = match_intents_to_skus(skus, intents)
        
        assert len(unmatched) == 0


# ============================================================================
# Requirement Fulfillment Tests
# ============================================================================

class TestRequirementFulfillment:
    """Test bloodwork requirement fulfillment."""
    
    def test_requirement_fulfilled(self):
        """SKU with required ingredient is marked as requirement."""
        skus = [make_sku("S1", "B12 Boost", ["b12", "iron"])]
        
        # Create candidates dict
        from app.matching.match import MatchCandidate
        candidates = {"S1": MatchCandidate(sku=skus[0])}
        
        fulfilled, unfulfilled = fulfill_requirements(
            candidates,
            requirements=["b12"]
        )
        
        assert "b12" in fulfilled
        assert len(unfulfilled) == 0
        assert candidates["S1"].is_requirement
    
    def test_unfulfilled_requirement_tracked(self):
        """Requirements without matching SKU go to unfulfilled list."""
        skus = [make_sku("S1", "Iron Only", ["iron"])]
        
        from app.matching.match import MatchCandidate
        candidates = {"S1": MatchCandidate(sku=skus[0])}
        
        fulfilled, unfulfilled = fulfill_requirements(
            candidates,
            requirements=["b12", "omega3"]
        )
        
        assert "b12" in unfulfilled
        assert "omega3" in unfulfilled
        assert len(fulfilled) == 0


# ============================================================================
# Caution Warning Tests
# ============================================================================

class TestCautionWarnings:
    """Test caution flag propagation."""
    
    def test_caution_flags_become_warnings(self):
        """SKU caution flags should appear in protocol warnings."""
        skus = [
            make_sku(
                "S1", "Iron Supplement", ["iron"],
                caution_flags=["iron_high", "ferritin_elevated"]
            ),
        ]
        intents = [
            make_intent("INTENT_IRON", 1, ["iron"]),
        ]
        context = UserContext(sex="male")
        
        input_data = MatchingInput(
            allowed_skus=skus,
            prioritized_intents=intents,
            user_context=context,
            requirements=[],
        )
        
        result = resolve_matching(input_data)
        
        assert len(result.protocol) == 1
        assert len(result.protocol[0].warnings) > 0
        assert any("IRON_HIGH" in w for w in result.protocol[0].warnings)


# ============================================================================
# Determinism Tests
# ============================================================================

class TestDeterminism:
    """Test that output is deterministic."""
    
    def test_same_input_same_hash(self):
        """Same input should produce same output hash."""
        skus = [
            make_sku("S1", "Product A", ["b12"]),
            make_sku("S2", "Product B", ["iron"]),
        ]
        intents = [
            make_intent("I1", 1, ["b12"]),
            make_intent("I2", 2, ["iron"]),
        ]
        context = UserContext(sex="male")
        
        input_data = MatchingInput(
            allowed_skus=skus,
            prioritized_intents=intents,
            user_context=context,
        )
        
        result1 = resolve_matching(input_data)
        result2 = resolve_matching(input_data)
        
        assert result1.match_hash == result2.match_hash
    
    def test_protocol_sorted_by_priority(self):
        """Protocol items should be sorted by priority rank."""
        skus = [
            make_sku("S1", "Low Priority", ["zinc"]),
            make_sku("S2", "High Priority", ["b12"]),
        ]
        intents = [
            make_intent("I1", 10, ["zinc"]),  # Lower priority
            make_intent("I2", 1, ["b12"]),     # Higher priority
        ]
        context = UserContext(sex="male")
        
        input_data = MatchingInput(
            allowed_skus=skus,
            prioritized_intents=intents,
            user_context=context,
        )
        
        result = resolve_matching(input_data)
        
        # First item should be the one matching higher priority intent
        assert result.protocol[0].sku_id == "S2"
        assert result.protocol[0].priority_rank == 1


# ============================================================================
# Edge Case Tests
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_empty_allowed_skus(self):
        """Empty SKU list should return empty protocol."""
        intents = [make_intent("I1", 1, ["b12"])]
        context = UserContext(sex="male")
        
        input_data = MatchingInput(
            allowed_skus=[],
            prioritized_intents=intents,
            user_context=context,
        )
        
        result = resolve_matching(input_data)
        
        assert len(result.protocol) == 0
        assert len(result.unmatched_intents) == 1
    
    def test_empty_intents(self):
        """Empty intent list with requirements should still fulfill."""
        skus = [make_sku("S1", "B12 Product", ["b12"])]
        context = UserContext(sex="male")
        
        input_data = MatchingInput(
            allowed_skus=skus,
            prioritized_intents=[],
            user_context=context,
            requirements=["b12"],
        )
        
        result = resolve_matching(input_data)
        
        assert len(result.protocol) == 1
        assert result.protocol[0].reason == "requirement"
    
    def test_both_intent_and_requirement(self):
        """SKU matching both intent and requirement gets reason 'both'."""
        skus = [make_sku("S1", "B12 Product", ["b12"])]
        intents = [make_intent("I1", 1, ["b12"])]
        context = UserContext(sex="male")
        
        input_data = MatchingInput(
            allowed_skus=skus,
            prioritized_intents=intents,
            user_context=context,
            requirements=["b12"],
        )
        
        result = resolve_matching(input_data)
        
        assert len(result.protocol) == 1
        assert result.protocol[0].reason == "both"
    
    def test_audit_counts_correct(self):
        """Audit should have correct counts."""
        skus = [
            make_sku("S1", "Product A", ["b12"]),
            make_sku("S2", "Product B", ["iron"], gender_line="MAXima2"),
        ]
        intents = [
            make_intent("I1", 1, ["b12"]),
            make_intent("I2", 2, ["zinc"]),  # Won't match
        ]
        context = UserContext(sex="male")
        
        input_data = MatchingInput(
            allowed_skus=skus,
            prioritized_intents=intents,
            user_context=context,
            requirements=["omega3"],  # Won't be fulfilled
        )
        
        result = resolve_matching(input_data)
        
        assert result.audit.total_allowed_skus == 2
        assert result.audit.gender_filtered_count == 1  # Only S1 (male)
        assert result.audit.intents_processed == 2
        assert result.audit.intents_matched == 1
        assert result.audit.intents_unmatched == 1
        assert "omega3" in result.audit.requirements_unfulfilled


# ============================================================================
# Integration Tests
# ============================================================================

class TestFullPipeline:
    """Test complete matching pipeline."""
    
    def test_full_matching_flow(self):
        """Test complete flow: filter → match → requirements → output."""
        skus = [
            make_sku("M1", "Male Energy", ["b12", "coq10"], gender_line="MAXimo2"),
            make_sku("M2", "Male Sleep", ["melatonin", "magnesium"], gender_line="MAXimo2"),
            make_sku("F1", "Female Vitality", ["iron", "folate"], gender_line="MAXima2"),
            make_sku("U1", "Universal D3", ["d3"], gender_line="UNISEX"),
        ]
        intents = [
            make_intent("ENERGY", 1, ["b12", "coq10"]),
            make_intent("IMMUNITY", 2, ["d3", "zinc"]),
        ]
        context = UserContext(sex="male")
        requirements = ["d3"]
        
        input_data = MatchingInput(
            allowed_skus=skus,
            prioritized_intents=intents,
            user_context=context,
            requirements=requirements,
        )
        
        result = resolve_matching(input_data)
        
        # Should have matched items
        assert len(result.protocol) >= 2
        
        # D3 requirement should be fulfilled
        assert "d3" in result.audit.requirements_fulfilled
        
        # Female product should not be in protocol
        protocol_ids = [p.sku_id for p in result.protocol]
        assert "F1" not in protocol_ids
        
        # Hash should be deterministic
        assert result.match_hash.startswith("sha256:")
        
        # Version should be set
        assert result.version == "matching_layer_v1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
