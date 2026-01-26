"""
Test Suite for Constraint Translator
=====================================
Validates deterministic translation of bloodwork constraints.

Run with: pytest tests/test_constraint_translator.py -v
"""

import pytest
from app.brain.constraint_translator import (
    ConstraintTranslator,
    TranslatedConstraints,
    translate_constraints,
    get_translator,
    merge_constraints,
    is_ingredient_blocked,
    get_block_reason,
    CONSTRAINT_MAPPINGS,
    __version__,
)


class TestConstraintTranslatorBasic:
    """Basic functionality tests."""
    
    def test_version_exists(self):
        """Verify version is defined."""
        assert __version__ == "1.0.0"
    
    def test_translator_initialization(self):
        """Verify translator initializes correctly."""
        translator = ConstraintTranslator()
        assert translator.version == __version__
        assert len(translator.mappings) == len(CONSTRAINT_MAPPINGS)
    
    def test_empty_constraints(self):
        """Empty input produces empty output."""
        result = translate_constraints([])
        assert result.is_empty()
        assert len(result.reason_codes) == 0
    
    def test_singleton_translator(self):
        """get_translator returns same instance."""
        t1 = get_translator()
        t2 = get_translator()
        assert t1 is t2


class TestIronBlockConstraint:
    """Tests for BLOCK_IRON constraint."""
    
    def test_block_iron_ingredients(self):
        """BLOCK_IRON blocks all iron forms."""
        result = translate_constraints(["BLOCK_IRON"])
        
        assert "iron" in result.blocked_ingredients
        assert "iron_bisglycinate" in result.blocked_ingredients
        assert "ferrous_sulfate" in result.blocked_ingredients
        assert "heme_iron" in result.blocked_ingredients
    
    def test_block_iron_targets(self):
        """BLOCK_IRON sets correct targets."""
        result = translate_constraints(["BLOCK_IRON"])
        assert "iron_supplementation" in result.blocked_targets
    
    def test_block_iron_reason(self):
        """BLOCK_IRON includes reason code."""
        result = translate_constraints(["BLOCK_IRON"])
        assert "BLOOD_BLOCK_IRON" in result.reason_codes


class TestHepaticConstraint:
    """Tests for CAUTION_HEPATOTOXIC constraint."""
    
    def test_hepatic_blocks_ashwagandha(self):
        """CAUTION_HEPATOTOXIC blocks ashwagandha permanently."""
        result = translate_constraints(["CAUTION_HEPATOTOXIC"])
        assert "ashwagandha" in result.blocked_ingredients
    
    def test_hepatic_caution_flags(self):
        """CAUTION_HEPATOTOXIC sets caution flags."""
        result = translate_constraints(["CAUTION_HEPATOTOXIC"])
        assert "hepatic_sensitive" in result.caution_flags
        assert "liver_function_impaired" in result.caution_flags


class TestMethylationConstraints:
    """Tests for methylation-related constraints."""
    
    def test_mthfr_blocks_folic_acid(self):
        """MTHFR variant blocks synthetic folic acid."""
        result = translate_constraints(["FLAG_METHYLFOLATE_REQUIRED"])
        assert "folic_acid" in result.blocked_ingredients
    
    def test_mthfr_recommends_methylfolate(self):
        """MTHFR variant recommends active folate forms."""
        result = translate_constraints(["FLAG_METHYLFOLATE_REQUIRED"])
        assert "methylfolate" in result.recommended_ingredients
        assert "5_mthf" in result.recommended_ingredients
    
    def test_methylation_support_recommendations(self):
        """FLAG_METHYLATION_SUPPORT recommends B vitamins."""
        result = translate_constraints(["FLAG_METHYLATION_SUPPORT"])
        assert "methylcobalamin" in result.recommended_ingredients
        assert "pyridoxal_5_phosphate" in result.recommended_ingredients


class TestRenalConstraint:
    """Tests for CAUTION_RENAL constraint."""
    
    def test_renal_caution_flags(self):
        """CAUTION_RENAL sets appropriate flags."""
        result = translate_constraints(["CAUTION_RENAL"])
        assert "renal_sensitive" in result.caution_flags
        assert "kidney_function_impaired" in result.caution_flags
    
    def test_renal_reason_code(self):
        """CAUTION_RENAL includes reason code."""
        result = translate_constraints(["CAUTION_RENAL"])
        assert "BLOOD_CAUTION_RENAL" in result.reason_codes


class TestPostMIConstraint:
    """Tests for BLOCK_POST_MI constraint (VINTAGE MI trial)."""
    
    def test_post_mi_blocks_arginine(self):
        """Post-MI blocks L-Arginine per VINTAGE MI trial."""
        result = translate_constraints(["BLOCK_POST_MI"])
        assert "l_arginine" in result.blocked_ingredients
        assert "arginine" in result.blocked_ingredients
    
    def test_post_mi_reason_includes_trial(self):
        """Post-MI references VINTAGE MI trial."""
        result = translate_constraints(["BLOCK_POST_MI"])
        assert "VINTAGE_MI_TRIAL" in result.reason_codes


class TestCombinedConstraints:
    """Tests for multiple combined constraints."""
    
    def test_multiple_blocks_combine(self):
        """Multiple block constraints combine correctly."""
        result = translate_constraints(["BLOCK_IRON", "BLOCK_POTASSIUM"])
        
        # Iron blocked
        assert "iron" in result.blocked_ingredients
        assert "ferrous_sulfate" in result.blocked_ingredients
        
        # Potassium blocked
        assert "potassium" in result.blocked_ingredients
        assert "potassium_citrate" in result.blocked_ingredients
    
    def test_block_and_caution_combine(self):
        """Block and caution constraints combine correctly."""
        result = translate_constraints(["BLOCK_IRON", "CAUTION_HEPATOTOXIC"])
        
        assert "iron" in result.blocked_ingredients
        assert "ashwagandha" in result.blocked_ingredients
        assert "hepatic_sensitive" in result.caution_flags
    
    def test_recommended_removed_if_blocked(self):
        """Recommended ingredients are removed if also blocked."""
        # FLAG_ANEMIA recommends iron, but BLOCK_IRON should remove it
        result = translate_constraints(["FLAG_ANEMIA", "BLOCK_IRON"])
        
        # Iron should NOT be in recommended (it's blocked)
        assert "iron_bisglycinate" not in result.recommended_ingredients
        
        # But it should be in blocked
        assert "iron" in result.blocked_ingredients


class TestDeterminism:
    """Tests for deterministic behavior."""
    
    def test_same_input_same_output(self):
        """Same input always produces same output."""
        result1 = translate_constraints(["BLOCK_IRON", "CAUTION_RENAL"])
        result2 = translate_constraints(["BLOCK_IRON", "CAUTION_RENAL"])
        
        assert result1.output_hash == result2.output_hash
        assert result1.blocked_ingredients == result2.blocked_ingredients
    
    def test_order_independent(self):
        """Order of constraint codes doesn't affect output."""
        result1 = translate_constraints(["BLOCK_IRON", "CAUTION_RENAL"])
        result2 = translate_constraints(["CAUTION_RENAL", "BLOCK_IRON"])
        
        assert result1.output_hash == result2.output_hash
    
    def test_duplicate_codes_handled(self):
        """Duplicate codes are deduplicated."""
        result = translate_constraints(["BLOCK_IRON", "BLOCK_IRON", "BLOCK_IRON"])
        
        # Should have same result as single code
        single_result = translate_constraints(["BLOCK_IRON"])
        assert result.output_hash == single_result.output_hash


class TestUnknownConstraints:
    """Tests for handling unknown constraint codes."""
    
    def test_unknown_code_recorded(self):
        """Unknown codes are recorded in reason_codes."""
        result = translate_constraints(["UNKNOWN_CONSTRAINT_XYZ"])
        assert "UNKNOWN_CONSTRAINT_UNKNOWN_CONSTRAINT_XYZ" in result.reason_codes
    
    def test_unknown_mixed_with_known(self):
        """Unknown codes don't break known code processing."""
        result = translate_constraints(["BLOCK_IRON", "UNKNOWN_CODE"])
        
        # Known code should work
        assert "iron" in result.blocked_ingredients
        
        # Unknown should be recorded
        assert "UNKNOWN_CONSTRAINT_UNKNOWN_CODE" in result.reason_codes


class TestHelperFunctions:
    """Tests for helper functions."""
    
    def test_is_ingredient_blocked(self):
        """is_ingredient_blocked works correctly."""
        constraints = translate_constraints(["BLOCK_IRON"])
        
        assert is_ingredient_blocked("iron", constraints) is True
        assert is_ingredient_blocked("IRON", constraints) is True  # Case insensitive
        assert is_ingredient_blocked("vitamin_c", constraints) is False
    
    def test_get_block_reason(self):
        """get_block_reason returns correct reason."""
        constraints = translate_constraints(["BLOCK_IRON"])
        
        reason = get_block_reason("iron", constraints)
        assert reason == "BLOOD_BLOCK_IRON"
        
        reason_none = get_block_reason("vitamin_c", constraints)
        assert reason_none is None


class TestMergeConstraints:
    """Tests for merge_constraints function."""
    
    def test_merge_adds_constraints(self):
        """Merging adds new constraints."""
        bloodwork = translate_constraints(["BLOCK_IRON"])
        
        other = {
            "blocked_ingredients": ["new_ingredient"],
            "caution_flags": ["new_flag"],
        }
        
        merged = merge_constraints(bloodwork, other)
        
        # Original constraints preserved
        assert "iron" in merged.blocked_ingredients
        
        # New constraints added
        assert "new_ingredient" in merged.blocked_ingredients
        assert "new_flag" in merged.caution_flags
    
    def test_merge_cannot_remove_blocks(self):
        """Merging cannot remove bloodwork blocks."""
        bloodwork = translate_constraints(["BLOCK_IRON"])
        
        # Attempting to "unblock" via merge should fail
        merged = merge_constraints(bloodwork, {})
        
        # Iron still blocked
        assert "iron" in merged.blocked_ingredients


class TestSerialization:
    """Tests for serialization."""
    
    def test_to_dict_sorted(self):
        """to_dict produces sorted, deterministic output."""
        result = translate_constraints(["BLOCK_IRON", "CAUTION_RENAL"])
        d = result.to_dict()
        
        # Lists should be sorted
        assert d["blocked_ingredients"] == sorted(d["blocked_ingredients"])
        assert d["caution_flags"] == sorted(d["caution_flags"])
        assert d["reason_codes"] == sorted(d["reason_codes"])
    
    def test_to_dict_has_all_fields(self):
        """to_dict includes all required fields."""
        result = translate_constraints(["BLOCK_IRON"])
        d = result.to_dict()
        
        required_fields = [
            "blocked_ingredients",
            "blocked_categories",
            "blocked_targets",
            "caution_flags",
            "reason_codes",
            "recommended_ingredients",
            "translator_version",
            "input_hash",
            "output_hash",
            "translated_at",
        ]
        
        for field in required_fields:
            assert field in d


class TestQAScenarios:
    """Integration tests matching QA matrix scenarios."""
    
    def test_scenario_iron_overload(self):
        """QA: Iron overload (ferritin=400) → BLOCK_IRON."""
        result = translate_constraints(["BLOCK_IRON"])
        
        assert len(result.blocked_ingredients) >= 5
        assert "iron_bisglycinate" in result.blocked_ingredients
        assert "BLOOD_BLOCK_IRON" in result.reason_codes
    
    def test_scenario_hepatic_risk(self):
        """QA: Hepatic risk (ALT=85) → CAUTION_HEPATOTOXIC."""
        result = translate_constraints(["CAUTION_HEPATOTOXIC"])
        
        assert "ashwagandha" in result.blocked_ingredients
        assert "hepatic_sensitive" in result.caution_flags
    
    def test_scenario_renal_impairment(self):
        """QA: Renal impairment (eGFR<60) → CAUTION_RENAL."""
        result = translate_constraints(["CAUTION_RENAL"])
        
        assert "renal_sensitive" in result.caution_flags
        assert "kidney_function_impaired" in result.caution_flags
    
    def test_scenario_hyperthyroid(self):
        """QA: Hyperthyroid (TSH<0.4) → FLAG_HYPERTHYROID."""
        result = translate_constraints(["FLAG_HYPERTHYROID"])
        
        assert "iodine" in result.blocked_ingredients
        assert "kelp" in result.blocked_ingredients
        assert "hyperthyroid" in result.caution_flags
    
    def test_scenario_full_combined(self):
        """QA: Combined scenario with multiple constraints."""
        result = translate_constraints([
            "BLOCK_IRON",
            "CAUTION_HEPATOTOXIC",
            "FLAG_METHYLATION_SUPPORT",
            "CAUTION_RENAL",
        ])
        
        # Blocks
        assert "iron" in result.blocked_ingredients
        assert "ashwagandha" in result.blocked_ingredients
        
        # Cautions
        assert "hepatic_sensitive" in result.caution_flags
        assert "renal_sensitive" in result.caution_flags
        
        # Recommendations
        assert "methylcobalamin" in result.recommended_ingredients
        
        # Multiple reason codes
        assert len(result.reason_codes) >= 4
