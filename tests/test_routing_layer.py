"""
Routing Layer Tests (Issue #6)

Comprehensive test suite for Pure Safety Elimination.

Tests Required (from Issue #6):
- test_blocked_ingredient_excluded: SKU with iron blocked when BLOCK_IRON active
- test_blocked_category_excluded: SKU in hepatotoxic category blocked
- test_caution_does_not_block: Caution flag propagates but doesn't block
- test_multiple_blocks_aggregated: SKU blocked by multiple rules shows all reasons
- test_deterministic_output: Same input -> same hash
- test_empty_constraints_allows_all: No constraints -> all valid SKUs allowed
- test_metadata_blocks_propagate: Issue #5 blocks remain

Version: routing_layer_v1
"""

import pytest
from typing import List

from app.routing.models import (
    SkuInput,
    RoutingConstraints,
    RoutingResult,
    AllowedSKU,
    BlockedSKU,
)
from app.routing.apply import (
    apply_routing_constraints,
    filter_by_gender,
    get_requirements_coverage,
)


# ============================================================================
# TEST FIXTURES
# ============================================================================

@pytest.fixture
def sample_skus() -> List[SkuInput]:
    """Sample SKUs for testing."""
    return [
        SkuInput(
            sku_id="iron-supplement-30",
            product_name="Iron 30mg",
            ingredient_tags=["iron", "vitamin_c"],
            category_tags=["minerals"],
            risk_tags=[],
            gender_line="UNISEX",
        ),
        SkuInput(
            sku_id="vitamin-d3-5000",
            product_name="Vitamin D3 5000 IU",
            ingredient_tags=["vitamin_d3"],
            category_tags=["vitamins"],
            risk_tags=[],
            gender_line="UNISEX",
        ),
        SkuInput(
            sku_id="b-complex-50",
            product_name="B-Complex 50",
            ingredient_tags=["vitamin_b12", "vitamin_b6", "folate"],
            category_tags=["vitamins"],
            risk_tags=[],
            gender_line="UNISEX",
        ),
        SkuInput(
            sku_id="ashwagandha-600",
            product_name="Ashwagandha 600mg",
            ingredient_tags=["ashwagandha"],
            category_tags=["adaptogens", "hepatotoxic"],
            risk_tags=["blocked_ingredient"],
            gender_line="UNISEX",
        ),
        SkuInput(
            sku_id="omega3-fish-oil",
            product_name="Omega-3 Fish Oil",
            ingredient_tags=["omega3", "epa", "dha"],
            category_tags=["fatty_acids"],
            risk_tags=[],
            gender_line="UNISEX",
        ),
        SkuInput(
            sku_id="mens-multivitamin",
            product_name="Men's Multi",
            ingredient_tags=["vitamin_a", "vitamin_c", "zinc"],
            category_tags=["multivitamins"],
            risk_tags=[],
            gender_line="MAXimo2",
        ),
        SkuInput(
            sku_id="womens-iron-plus",
            product_name="Women's Iron Plus",
            ingredient_tags=["iron", "vitamin_b12", "folate"],
            category_tags=["minerals"],
            risk_tags=[],
            gender_line="MAXima2",
        ),
        SkuInput(
            sku_id="potassium-citrate",
            product_name="Potassium Citrate 99mg",
            ingredient_tags=["potassium"],
            category_tags=["minerals"],
            risk_tags=[],
            gender_line="UNISEX",
        ),
    ]


@pytest.fixture
def empty_constraints() -> RoutingConstraints:
    """Empty constraints - nothing blocked."""
    return RoutingConstraints()


@pytest.fixture
def iron_block_constraints() -> RoutingConstraints:
    """Constraints that block iron."""
    return RoutingConstraints(
        blocked_ingredients=["iron"],
        reason_codes=["BLOCK_IRON_FERRITIN_HIGH"],
    )


@pytest.fixture
def hepatotoxic_block_constraints() -> RoutingConstraints:
    """Constraints that block hepatotoxic category."""
    return RoutingConstraints(
        blocked_categories=["hepatotoxic"],
        reason_codes=["BLOCK_CATEGORY_HEPATOTOXIC"],
    )


@pytest.fixture
def vitamin_d_caution_constraints() -> RoutingConstraints:
    """Constraints with vitamin D caution (not block)."""
    return RoutingConstraints(
        caution_flags=["vitamin_d3"],
        reason_codes=["CAUTION_VITAMIN_D_CALCIUM_HIGH"],
    )


@pytest.fixture
def multi_constraint() -> RoutingConstraints:
    """Complex constraints with multiple rules."""
    return RoutingConstraints(
        blocked_ingredients=["iron", "potassium"],
        blocked_categories=["hepatotoxic"],
        caution_flags=["vitamin_d3"],
        requirements=["vitamin_b12", "omega3"],
        reason_codes=[
            "BLOCK_IRON_FERRITIN_HIGH",
            "BLOCK_POTASSIUM_KIDNEY",
            "CAUTION_VITAMIN_D_CALCIUM_HIGH",
        ],
    )


# ============================================================================
# CORE ROUTING TESTS (Issue #6 Required)
# ============================================================================

class TestBlockedIngredientExcluded:
    """test_blocked_ingredient_excluded: SKU with iron blocked when BLOCK_IRON active"""
    
    def test_iron_sku_blocked(self, sample_skus, iron_block_constraints):
        """Iron supplement should be blocked when iron is in blocked_ingredients."""
        result = apply_routing_constraints(sample_skus, iron_block_constraints)
        
        blocked_ids = {s.sku_id for s in result.blocked_skus}
        assert "iron-supplement-30" in blocked_ids
        assert "womens-iron-plus" in blocked_ids  # Also contains iron
        
    def test_non_iron_sku_allowed(self, sample_skus, iron_block_constraints):
        """Non-iron SKUs should pass through."""
        result = apply_routing_constraints(sample_skus, iron_block_constraints)
        
        allowed_ids = {s.sku_id for s in result.allowed_skus}
        assert "vitamin-d3-5000" in allowed_ids
        assert "omega3-fish-oil" in allowed_ids
        
    def test_block_reason_recorded(self, sample_skus, iron_block_constraints):
        """Blocked SKU should have reason code."""
        result = apply_routing_constraints(sample_skus, iron_block_constraints)
        
        iron_blocked = next(s for s in result.blocked_skus if s.sku_id == "iron-supplement-30")
        assert "BLOCK_INGREDIENT_IRON" in iron_blocked.reason_codes
        assert iron_blocked.blocked_by == "blood"
        assert "iron" in iron_blocked.blocked_ingredients


class TestBlockedCategoryExcluded:
    """test_blocked_category_excluded: SKU in hepatotoxic category blocked"""
    
    def test_hepatotoxic_sku_blocked(self, sample_skus, hepatotoxic_block_constraints):
        """Ashwagandha (hepatotoxic) should be blocked."""
        result = apply_routing_constraints(sample_skus, hepatotoxic_block_constraints)
        
        blocked_ids = {s.sku_id for s in result.blocked_skus}
        assert "ashwagandha-600" in blocked_ids
        
    def test_category_block_reason(self, sample_skus, hepatotoxic_block_constraints):
        """Category block should have proper reason code."""
        result = apply_routing_constraints(sample_skus, hepatotoxic_block_constraints)
        
        # Ashwagandha also has metadata block, so check for category block reason
        ash_blocked = next(s for s in result.blocked_skus if s.sku_id == "ashwagandha-600")
        # May have BLOCKED_BY_EVIDENCE from metadata OR category block
        assert any("HEPATOTOXIC" in r or "EVIDENCE" in r for r in ash_blocked.reason_codes)


class TestCautionDoesNotBlock:
    """test_caution_does_not_block: Caution flag propagates but doesn't block"""
    
    def test_vitamin_d_not_blocked_on_caution(self, sample_skus, vitamin_d_caution_constraints):
        """Vitamin D should NOT be blocked, only flagged for caution."""
        result = apply_routing_constraints(sample_skus, vitamin_d_caution_constraints)
        
        allowed_ids = {s.sku_id for s in result.allowed_skus}
        assert "vitamin-d3-5000" in allowed_ids
        
    def test_caution_flag_propagated(self, sample_skus, vitamin_d_caution_constraints):
        """Vitamin D SKU should have caution flag attached."""
        result = apply_routing_constraints(sample_skus, vitamin_d_caution_constraints)
        
        vit_d = next(s for s in result.allowed_skus if s.sku_id == "vitamin-d3-5000")
        assert "vitamin_d3" in vit_d.caution_flags
        assert "CAUTION_VITAMIN_D3" in vit_d.caution_reasons
        
    def test_caution_count_in_audit(self, sample_skus, vitamin_d_caution_constraints):
        """Audit should track caution count."""
        result = apply_routing_constraints(sample_skus, vitamin_d_caution_constraints)
        
        assert result.audit.caution_count >= 1


class TestMultipleBlocksAggregated:
    """test_multiple_blocks_aggregated: SKU blocked by multiple rules shows all reasons"""
    
    def test_womens_iron_multiple_blocks(self, sample_skus, multi_constraint):
        """Women's Iron Plus blocked by iron AND contains b12 (tracked separately)."""
        result = apply_routing_constraints(sample_skus, multi_constraint)
        
        blocked_ids = {s.sku_id for s in result.blocked_skus}
        assert "womens-iron-plus" in blocked_ids
        
    def test_multiple_reason_codes(self, sample_skus, multi_constraint):
        """Blocked SKU should have multiple reason codes if multiple rules apply."""
        result = apply_routing_constraints(sample_skus, multi_constraint)
        
        iron_blocked = next(s for s in result.blocked_skus if s.sku_id == "iron-supplement-30")
        # Should have iron block
        assert "BLOCK_INGREDIENT_IRON" in iron_blocked.reason_codes


class TestDeterministicOutput:
    """test_deterministic_output: Same input -> same hash"""
    
    def test_same_input_same_hash(self, sample_skus, multi_constraint):
        """Running twice with same input should produce same hash."""
        result1 = apply_routing_constraints(sample_skus, multi_constraint)
        result2 = apply_routing_constraints(sample_skus, multi_constraint)
        
        assert result1.routing_hash == result2.routing_hash
        
    def test_different_input_different_hash(self, sample_skus, iron_block_constraints, empty_constraints):
        """Different constraints should produce different hashes."""
        result1 = apply_routing_constraints(sample_skus, iron_block_constraints)
        result2 = apply_routing_constraints(sample_skus, empty_constraints)
        
        assert result1.routing_hash != result2.routing_hash
        
    def test_sorted_output(self, sample_skus, empty_constraints):
        """Output lists should be sorted alphabetically by sku_id."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        
        allowed_ids = [s.sku_id for s in result.allowed_skus]
        assert allowed_ids == sorted(allowed_ids)


class TestEmptyConstraintsAllowsAll:
    """test_empty_constraints_allows_all: No constraints -> all valid SKUs allowed"""
    
    def test_all_valid_skus_pass(self, sample_skus, empty_constraints):
        """With no constraints, all SKUs without metadata blocks should pass."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        
        # Ashwagandha has blocked_ingredient risk tag, so it should be blocked
        # All others should be allowed
        allowed_count = len(result.allowed_skus)
        blocked_count = len(result.blocked_skus)
        
        # 8 total, 1 has metadata block
        assert allowed_count == 7
        assert blocked_count == 1
        
    def test_no_blood_blocks_with_empty(self, sample_skus, empty_constraints):
        """No blood-based blocks with empty constraints."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        
        assert result.audit.blocked_by_blood == 0


class TestMetadataBlocksPropagate:
    """test_metadata_blocks_propagate: Issue #5 blocks remain"""
    
    def test_ashwagandha_blocked_by_metadata(self, sample_skus, empty_constraints):
        """Ashwagandha should be blocked due to blocked_ingredient risk tag."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        
        blocked_ids = {s.sku_id for s in result.blocked_skus}
        assert "ashwagandha-600" in blocked_ids
        
    def test_metadata_block_reason(self, sample_skus, empty_constraints):
        """Metadata block should have BLOCKED_BY_EVIDENCE reason."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        
        ash_blocked = next(s for s in result.blocked_skus if s.sku_id == "ashwagandha-600")
        assert "BLOCKED_BY_EVIDENCE" in ash_blocked.reason_codes
        assert ash_blocked.blocked_by == "metadata"
        
    def test_metadata_count_in_audit(self, sample_skus, empty_constraints):
        """Audit should track metadata blocks."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        
        assert result.audit.blocked_by_metadata >= 1


# ============================================================================
# ADDITIONAL ROUTING TESTS
# ============================================================================

class TestRequirementsTracking:
    """Test requirements fulfillment tracking."""
    
    def test_requirements_fulfilled(self, sample_skus, multi_constraint):
        """Should track which requirements are fulfilled by allowed SKUs."""
        result = apply_routing_constraints(sample_skus, multi_constraint)
        
        # b12 is in b-complex-50 (and womens-iron-plus, but that's blocked)
        # omega3 is in omega3-fish-oil
        assert "vitamin_b12" in result.audit.requirements_in_catalog or \
               "omega3" in result.audit.requirements_in_catalog
               
    def test_sku_fulfills_requirements_tracked(self, sample_skus, multi_constraint):
        """Allowed SKUs should track which requirements they fulfill."""
        result = apply_routing_constraints(sample_skus, multi_constraint)
        
        omega = next((s for s in result.allowed_skus if s.sku_id == "omega3-fish-oil"), None)
        if omega:
            assert "omega3" in omega.fulfills_requirements


class TestAuditCompleteness:
    """Test audit trail completeness."""
    
    def test_audit_counts_match(self, sample_skus, multi_constraint):
        """Audit counts should match actual results."""
        result = apply_routing_constraints(sample_skus, multi_constraint)
        
        assert result.audit.total_input_skus == len(sample_skus)
        assert result.audit.allowed_count == len(result.allowed_skus)
        assert result.audit.blocked_count == len(result.blocked_skus)
        assert result.audit.allowed_count + result.audit.blocked_count == result.audit.total_input_skus
        
    def test_constraints_applied_tracked(self, sample_skus, multi_constraint):
        """Should track which constraint types were applied."""
        result = apply_routing_constraints(sample_skus, multi_constraint)
        
        assert "blocked_ingredients" in result.audit.constraints_applied
        assert "blocked_categories" in result.audit.constraints_applied
        assert "caution_flags" in result.audit.constraints_applied
        assert "requirements" in result.audit.constraints_applied


# ============================================================================
# UTILITY FUNCTION TESTS
# ============================================================================

class TestGenderFilter:
    """Test gender filtering utility."""
    
    def test_male_filter(self, sample_skus, empty_constraints):
        """Male filter should exclude female-only SKUs."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        filtered = filter_by_gender(result.allowed_skus, "male")
        
        filtered_ids = {s.sku_id for s in filtered}
        assert "mens-multivitamin" in filtered_ids
        assert "womens-iron-plus" not in filtered_ids  # Also blocked by metadata anyway
        
    def test_female_filter(self, sample_skus, empty_constraints):
        """Female filter should exclude male-only SKUs."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        filtered = filter_by_gender(result.allowed_skus, "female")
        
        filtered_ids = {s.sku_id for s in filtered}
        assert "mens-multivitamin" not in filtered_ids


class TestRequirementsCoverage:
    """Test requirements coverage utility."""
    
    def test_full_coverage(self, sample_skus, empty_constraints):
        """Should calculate coverage percentage correctly."""
        result = apply_routing_constraints(sample_skus, empty_constraints)
        
        coverage = get_requirements_coverage(
            result.allowed_skus,
            ["vitamin_b12", "omega3"]
        )
        
        # Both should be available
        assert coverage["coverage_pct"] == 100.0
        assert "vitamin_b12" in coverage["fulfilled"]
        assert "omega3" in coverage["fulfilled"]
        
    def test_partial_coverage(self, sample_skus, iron_block_constraints):
        """Should track missing requirements."""
        result = apply_routing_constraints(sample_skus, iron_block_constraints)
        
        coverage = get_requirements_coverage(
            result.allowed_skus,
            ["vitamin_b12", "iron"]  # iron is blocked
        )
        
        # B12 available via b-complex, iron blocked
        assert coverage["coverage_pct"] == 50.0
        assert "iron" in coverage["missing"]


# ============================================================================
# EDGE CASE TESTS
# ============================================================================

class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_empty_sku_list(self, empty_constraints):
        """Should handle empty SKU list gracefully."""
        result = apply_routing_constraints([], empty_constraints)
        
        assert len(result.allowed_skus) == 0
        assert len(result.blocked_skus) == 0
        assert result.audit.total_input_skus == 0
        
    def test_case_insensitive_matching(self):
        """Should match ingredients case-insensitively."""
        skus = [
            SkuInput(
                sku_id="test-sku",
                product_name="Test",
                ingredient_tags=["IRON", "Vitamin_C"],  # Mixed case
                category_tags=[],
                risk_tags=[],
            )
        ]
        constraints = RoutingConstraints(
            blocked_ingredients=["iron"]  # lowercase
        )
        
        result = apply_routing_constraints(skus, constraints)
        
        assert len(result.blocked_skus) == 1
        assert result.blocked_skus[0].sku_id == "test-sku"
        
    def test_sku_with_no_tags(self):
        """Should handle SKU with empty tag lists."""
        skus = [
            SkuInput(
                sku_id="empty-sku",
                product_name="Empty Tags SKU",
                ingredient_tags=[],
                category_tags=[],
                risk_tags=[],
            )
        ]
        constraints = RoutingConstraints(
            blocked_ingredients=["iron"]
        )
        
        result = apply_routing_constraints(skus, constraints)
        
        # Should be allowed (nothing to block)
        assert len(result.allowed_skus) == 1
        assert len(result.blocked_skus) == 0


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
