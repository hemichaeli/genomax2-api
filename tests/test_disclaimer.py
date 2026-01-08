"""
Tests for DSHEA Disclaimer Utilities
Covers singular/plural prefix selection and rendering logic.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from app.shared.disclaimer import (
    choose_disclaimer_prefix,
    render_disclaimer_block,
    should_require_disclaimer,
    DSHEA_DISCLAIMER_SINGULAR,
    DSHEA_DISCLAIMER_PLURAL,
)


class TestChooseDisclaimerPrefix:
    """Tests for choose_disclaimer_prefix(claim_count)"""
    
    def test_single_claim_returns_singular(self):
        """claim_count == 1 -> singular text"""
        result = choose_disclaimer_prefix(1)
        assert result == DSHEA_DISCLAIMER_SINGULAR
        assert result.startswith("This statement")
        assert "These statements" not in result
    
    def test_multiple_claims_returns_plural(self):
        """claim_count > 1 -> plural text"""
        result = choose_disclaimer_prefix(2)
        assert result == DSHEA_DISCLAIMER_PLURAL
        assert result.startswith("These statements")
        assert "This statement has" not in result
    
    def test_many_claims_returns_plural(self):
        """claim_count = 5 -> plural text"""
        result = choose_disclaimer_prefix(5)
        assert result == DSHEA_DISCLAIMER_PLURAL
    
    def test_zero_claims_returns_plural_defensive(self):
        """claim_count = 0 -> plural (defensive default)"""
        result = choose_disclaimer_prefix(0)
        assert result == DSHEA_DISCLAIMER_PLURAL
    
    def test_negative_claims_returns_plural_defensive(self):
        """claim_count < 0 -> plural (defensive default)"""
        result = choose_disclaimer_prefix(-1)
        assert result == DSHEA_DISCLAIMER_PLURAL
    
    def test_disclaimer_contains_fda_reference(self):
        """Both versions mention FDA"""
        assert "Food and Drug Administration" in DSHEA_DISCLAIMER_SINGULAR
        assert "Food and Drug Administration" in DSHEA_DISCLAIMER_PLURAL
    
    def test_disclaimer_contains_disease_clause(self):
        """Both versions contain disease prevention clause"""
        disease_clause = "not intended to diagnose, treat, cure, or prevent any disease"
        assert disease_clause in DSHEA_DISCLAIMER_SINGULAR
        assert disease_clause in DSHEA_DISCLAIMER_PLURAL


class TestRenderDisclaimerBlock:
    """Tests for render_disclaimer_block()"""
    
    def test_supplement_single_claim(self):
        """SUPPLEMENT with 1 claim -> formatted singular"""
        result = render_disclaimer_block(
            disclaimer_text="ignored",  # Uses claim-aware text
            disclaimer_symbol="*",
            claim_count=1,
            applicability="SUPPLEMENT"
        )
        assert result is not None
        assert result.startswith("* This statement")
    
    def test_supplement_multiple_claims(self):
        """SUPPLEMENT with 3 claims -> formatted plural"""
        result = render_disclaimer_block(
            disclaimer_text="ignored",
            disclaimer_symbol="*",
            claim_count=3,
            applicability="SUPPLEMENT"
        )
        assert result is not None
        assert result.startswith("* These statements")
    
    def test_supplement_custom_symbol(self):
        """Custom symbol is prepended"""
        result = render_disclaimer_block(
            disclaimer_text="ignored",
            disclaimer_symbol="†",
            claim_count=1,
            applicability="SUPPLEMENT"
        )
        assert result.startswith("† This statement")
    
    def test_topical_returns_none(self):
        """TOPICAL products return None (no DSHEA required)"""
        result = render_disclaimer_block(
            disclaimer_text="anything",
            disclaimer_symbol="*",
            claim_count=1,
            applicability="TOPICAL"
        )
        assert result is None
    
    def test_topical_with_multiple_claims_still_none(self):
        """TOPICAL always returns None regardless of claims"""
        result = render_disclaimer_block(
            disclaimer_text="anything",
            disclaimer_symbol="*",
            claim_count=5,
            applicability="TOPICAL"
        )
        assert result is None


class TestShouldRequireDisclaimer:
    """Tests for should_require_disclaimer() used by QA gate"""
    
    def test_supplement_requires_disclaimer(self):
        """SUPPLEMENT -> True"""
        assert should_require_disclaimer("SUPPLEMENT") is True
    
    def test_topical_does_not_require_disclaimer(self):
        """TOPICAL -> False"""
        assert should_require_disclaimer("TOPICAL") is False
    
    def test_unknown_value_does_not_require(self):
        """Unknown values -> False (defensive)"""
        assert should_require_disclaimer("UNKNOWN") is False
        assert should_require_disclaimer("") is False
    
    def test_case_sensitive(self):
        """Values are case-sensitive (DB constraint enforces uppercase)"""
        assert should_require_disclaimer("supplement") is False
        assert should_require_disclaimer("Supplement") is False


class TestDisclaimerTextIntegrity:
    """Tests to ensure disclaimer text is not accidentally modified"""
    
    def test_singular_text_exact(self):
        """Singular text matches expected DSHEA language"""
        expected = (
            "This statement has not been evaluated by the Food and Drug Administration. "
            "This product is not intended to diagnose, treat, cure, or prevent any disease."
        )
        assert DSHEA_DISCLAIMER_SINGULAR == expected
    
    def test_plural_text_exact(self):
        """Plural text matches expected DSHEA language"""
        expected = (
            "These statements have not been evaluated by the Food and Drug Administration. "
            "This product is not intended to diagnose, treat, cure, or prevent any disease."
        )
        assert DSHEA_DISCLAIMER_PLURAL == expected
    
    def test_only_prefix_differs(self):
        """Singular and plural differ only in first word"""
        singular_rest = DSHEA_DISCLAIMER_SINGULAR.split(" ", 2)[2]
        plural_rest = DSHEA_DISCLAIMER_PLURAL.split(" ", 2)[2]
        assert singular_rest == plural_rest


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
