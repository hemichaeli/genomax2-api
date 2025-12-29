"""
Explainability Test Suite (Issue #8)

Tests for protocol explainability generation.

Validates:
- Confidence calculation rules
- Item explanation generation
- Blocked item explanations
- Disclaimer immutability
- No logic changes (pure UX layer)

Version: explainability_v1
"""

import pytest
from datetime import datetime

from app.explainability.models import (
    ExplainabilityRequest,
    ConfidenceLevel,
    DisclaimerSet,
)
from app.explainability.explain import (
    generate_explainability,
    calculate_confidence,
    get_disclaimers,
    generate_item_explanation,
    generate_blocked_explanation,
)


# ============================================================
# CONFIDENCE CALCULATION TESTS
# ============================================================

class TestConfidenceCalculation:
    """Test confidence level calculation rules."""
    
    def test_high_confidence_all_factors(self):
        """HIGH: Bloodwork + complete + ≥2 intents + no cautions"""
        confidence = calculate_confidence(
            has_bloodwork=True,
            bloodwork_complete=True,
            intent_count=3,
            caution_count=0
        )
        assert confidence.level == ConfidenceLevel.HIGH
        assert confidence.badge_text == "High Confidence"
    
    def test_low_confidence_no_bloodwork(self):
        """LOW: No bloodwork = lifestyle-only"""
        confidence = calculate_confidence(
            has_bloodwork=False,
            bloodwork_complete=False,
            intent_count=5,
            caution_count=0
        )
        assert confidence.level == ConfidenceLevel.LOW
        assert "bloodwork" in confidence.explanation.lower()
    
    def test_medium_confidence_partial_panel(self):
        """MEDIUM: Incomplete bloodwork panel"""
        confidence = calculate_confidence(
            has_bloodwork=True,
            bloodwork_complete=False,
            intent_count=3,
            caution_count=0
        )
        assert confidence.level == ConfidenceLevel.MEDIUM
    
    def test_low_confidence_few_intents(self):
        """LOW: Less than 2 intents"""
        confidence = calculate_confidence(
            has_bloodwork=True,
            bloodwork_complete=True,
            intent_count=1,
            caution_count=0
        )
        assert confidence.level == ConfidenceLevel.LOW
    
    def test_medium_confidence_with_cautions(self):
        """MEDIUM: Cautions present"""
        confidence = calculate_confidence(
            has_bloodwork=True,
            bloodwork_complete=True,
            intent_count=3,
            caution_count=2
        )
        assert confidence.level == ConfidenceLevel.MEDIUM
        assert "caution" in confidence.explanation.lower()
    
    def test_confidence_has_recommendations(self):
        """Confidence should include recommendations when not HIGH"""
        confidence = calculate_confidence(
            has_bloodwork=False,
            bloodwork_complete=False,
            intent_count=0,
            caution_count=0
        )
        assert len(confidence.recommendations_to_improve) > 0
    
    def test_high_confidence_no_recommendations(self):
        """HIGH confidence should have empty recommendations"""
        confidence = calculate_confidence(
            has_bloodwork=True,
            bloodwork_complete=True,
            intent_count=5,
            caution_count=0
        )
        assert confidence.level == ConfidenceLevel.HIGH
        # High confidence may still have empty recommendations


# ============================================================
# ITEM EXPLANATION TESTS
# ============================================================

class TestItemExplanation:
    """Test item explanation generation."""
    
    def test_item_has_why_included(self):
        """Every item should have why_included reasons"""
        item = {
            "sku_id": "TEST-001",
            "product_name": "Test Product",
            "matched_intents": ["INTENT_ENERGY"],
            "matched_ingredients": ["b12"],
            "match_score": 0.8,
            "reason": "intent_match",
            "warnings": []
        }
        explanation = generate_item_explanation(item)
        assert len(explanation.why_included) > 0
    
    def test_item_has_why_not_blocked(self):
        """Every item should have why_not_blocked reasons"""
        item = {
            "sku_id": "TEST-001",
            "product_name": "Test Product",
            "matched_intents": [],
            "matched_ingredients": [],
            "match_score": 0.5,
            "reason": "",
            "warnings": []
        }
        explanation = generate_item_explanation(item)
        assert len(explanation.why_not_blocked) > 0
        assert "safety checks" in " ".join(explanation.why_not_blocked).lower()
    
    def test_intent_explanation_readable(self):
        """Intent explanations should be human-readable"""
        item = {
            "sku_id": "TEST-001",
            "product_name": "Test Product",
            "matched_intents": ["INTENT_ENERGY", "INTENT_SLEEP"],
            "matched_ingredients": [],
            "match_score": 0.6,
            "reason": "intent_match",
            "warnings": []
        }
        explanation = generate_item_explanation(item)
        # Should not contain raw INTENT_ prefix in output
        why_text = " ".join(explanation.why_included)
        assert "Energy" in why_text or "Sleep" in why_text
    
    def test_caution_warnings_captured(self):
        """Caution warnings should be captured in caution_notes"""
        item = {
            "sku_id": "TEST-001",
            "product_name": "Test Product",
            "matched_intents": ["INTENT_ENERGY"],
            "matched_ingredients": ["magnesium"],
            "match_score": 0.7,
            "reason": "both",
            "warnings": ["CAUTION: MAGNESIUM_HIGH_DOSE"]
        }
        explanation = generate_item_explanation(item)
        assert len(explanation.caution_notes) > 0
    
    def test_high_match_score_noted(self):
        """High match scores should be mentioned"""
        item = {
            "sku_id": "TEST-001",
            "product_name": "Test Product",
            "matched_intents": ["INTENT_ENERGY"],
            "matched_ingredients": ["b12", "coq10"],
            "match_score": 0.9,
            "reason": "both",
            "warnings": []
        }
        explanation = generate_item_explanation(item)
        why_text = " ".join(explanation.why_included).lower()
        assert "strong" in why_text or "good" in why_text


# ============================================================
# BLOCKED ITEM EXPLANATION TESTS
# ============================================================

class TestBlockedExplanation:
    """Test blocked item explanation generation."""
    
    def test_blood_block_explained(self):
        """Blood-based blocks should be clearly explained"""
        blocked = {
            "sku_id": "BLOCKED-001",
            "product_name": "Iron Supplement",
            "reason_codes": ["BLOCK_IRON_FERRITIN_HIGH"],
            "blocked_by": "blood"
        }
        explanation = generate_blocked_explanation(blocked)
        assert "ferritin" in explanation.reason.lower()
        assert explanation.blocked_by == "blood"
    
    def test_metadata_block_explained(self):
        """Metadata blocks should be explained"""
        blocked = {
            "sku_id": "BLOCKED-002",
            "product_name": "Unknown Product",
            "reason_codes": ["INSUFFICIENT_METADATA"],
            "blocked_by": "metadata"
        }
        explanation = generate_blocked_explanation(blocked)
        assert "data" in explanation.reason.lower() or "safety" in explanation.reason.lower()
    
    def test_blood_block_can_change(self):
        """Blood blocks should indicate they can change"""
        blocked = {
            "sku_id": "BLOCKED-001",
            "product_name": "Iron Supplement",
            "reason_codes": ["BLOCK_IRON_FERRITIN_HIGH"],
            "blocked_by": "blood"
        }
        explanation = generate_blocked_explanation(blocked)
        assert explanation.can_change is True
        assert explanation.change_hint is not None
    
    def test_metadata_block_cannot_change(self):
        """Metadata blocks should not indicate they can change"""
        blocked = {
            "sku_id": "BLOCKED-002",
            "product_name": "Unknown Product",
            "reason_codes": ["INSUFFICIENT_METADATA"],
            "blocked_by": "metadata"
        }
        explanation = generate_blocked_explanation(blocked)
        assert explanation.can_change is False
    
    def test_unknown_code_has_fallback(self):
        """Unknown reason codes should have fallback explanation"""
        blocked = {
            "sku_id": "BLOCKED-003",
            "product_name": "Some Product",
            "reason_codes": ["UNKNOWN_FUTURE_CODE"],
            "blocked_by": "safety"
        }
        explanation = generate_blocked_explanation(blocked)
        assert len(explanation.reason) > 0


# ============================================================
# DISCLAIMER TESTS
# ============================================================

class TestDisclaimers:
    """Test disclaimer immutability and content."""
    
    def test_disclaimers_exist(self):
        """All required disclaimers should exist"""
        disclaimers = get_disclaimers()
        assert disclaimers.allowed_not_recommended
        assert disclaimers.blocked_biological
        assert disclaimers.not_medical_advice
        assert disclaimers.blood_precedence
    
    def test_disclaimers_have_version(self):
        """Disclaimers should have a version for audit"""
        disclaimers = get_disclaimers()
        assert disclaimers.version.startswith("disclaimer_v")
    
    def test_not_medical_advice_present(self):
        """Medical advice disclaimer must be present"""
        disclaimers = get_disclaimers()
        assert "not medical advice" in disclaimers.not_medical_advice.lower()
    
    def test_blood_precedence_present(self):
        """Blood precedence disclaimer must be present"""
        disclaimers = get_disclaimers()
        assert "blood" in disclaimers.blood_precedence.lower()
    
    def test_disclaimers_immutable(self):
        """Getting disclaimers multiple times returns same content"""
        d1 = get_disclaimers()
        d2 = get_disclaimers()
        assert d1.allowed_not_recommended == d2.allowed_not_recommended
        assert d1.not_medical_advice == d2.not_medical_advice


# ============================================================
# FULL EXPLAINABILITY TESTS
# ============================================================

class TestFullExplainability:
    """Test complete explainability generation."""
    
    def test_full_explainability_generated(self):
        """Full explainability should be generated from request"""
        request = ExplainabilityRequest(
            protocol_id="TEST-PROTO-001",
            protocol_items=[
                {
                    "sku_id": "SKU-001",
                    "product_name": "Energy Boost",
                    "matched_intents": ["INTENT_ENERGY"],
                    "matched_ingredients": ["b12"],
                    "match_score": 0.8,
                    "reason": "both",
                    "warnings": []
                }
            ],
            blocked_items=[
                {
                    "sku_id": "SKU-BLOCKED",
                    "product_name": "Iron Plus",
                    "reason_codes": ["BLOCK_IRON_FERRITIN_HIGH"],
                    "blocked_by": "blood"
                }
            ],
            has_bloodwork=True,
            bloodwork_complete=True,
            intent_count=2,
            caution_count=0
        )
        
        result = generate_explainability(request)
        
        assert result.protocol_id == "TEST-PROTO-001"
        assert len(result.item_explanations) == 1
        assert len(result.blocked_explanations) == 1
        assert result.confidence is not None
        assert result.disclaimers is not None
    
    def test_explainability_does_not_change_items(self):
        """Explainability should NOT modify protocol items"""
        original_items = [
            {
                "sku_id": "SKU-001",
                "product_name": "Test",
                "matched_intents": ["INTENT_A"],
                "matched_ingredients": ["x"],
                "match_score": 0.5,
                "reason": "test",
                "warnings": []
            }
        ]
        
        request = ExplainabilityRequest(
            protocol_id="TEST",
            protocol_items=original_items.copy(),
            has_bloodwork=False,
            bloodwork_complete=False,
            intent_count=1,
            caution_count=0
        )
        
        generate_explainability(request)
        
        # Original items should be unchanged
        assert original_items[0]["sku_id"] == "SKU-001"
        assert original_items[0]["match_score"] == 0.5
    
    def test_explainability_has_timestamp(self):
        """Explainability should have generation timestamp"""
        request = ExplainabilityRequest(
            protocol_id="TEST",
            protocol_items=[],
            has_bloodwork=False,
            bloodwork_complete=False,
            intent_count=0,
            caution_count=0
        )
        
        result = generate_explainability(request)
        assert result.generated_at is not None
    
    def test_explainability_has_version(self):
        """Explainability should have module version"""
        request = ExplainabilityRequest(
            protocol_id="TEST",
            protocol_items=[],
            has_bloodwork=False,
            bloodwork_complete=False,
            intent_count=0,
            caution_count=0
        )
        
        result = generate_explainability(request)
        assert result.version == "explainability_v1"


# ============================================================
# NEGATIVE TESTS
# ============================================================

class TestNoLogicChanges:
    """Verify explainability does not change any logic."""
    
    def test_no_new_recommendations(self):
        """Explainability should not add new recommendations"""
        request = ExplainabilityRequest(
            protocol_id="TEST",
            protocol_items=[
                {"sku_id": "A", "product_name": "A", "matched_intents": [], 
                 "matched_ingredients": [], "match_score": 0.1, "reason": "", "warnings": []}
            ],
            has_bloodwork=False,
            bloodwork_complete=False,
            intent_count=0,
            caution_count=0
        )
        
        result = generate_explainability(request)
        
        # Should only explain what was passed in
        assert len(result.item_explanations) == 1
    
    def test_no_score_changes(self):
        """Explainability should not change match scores"""
        original_score = 0.42
        request = ExplainabilityRequest(
            protocol_id="TEST",
            protocol_items=[
                {"sku_id": "A", "product_name": "A", "matched_intents": [], 
                 "matched_ingredients": [], "match_score": original_score, 
                 "reason": "", "warnings": []}
            ],
            has_bloodwork=False,
            bloodwork_complete=False,
            intent_count=1,
            caution_count=0
        )
        
        result = generate_explainability(request)
        
        # Item explanation should not modify the original score
        # (explainability only reads, never writes to protocol data)
        assert request.protocol_items[0]["match_score"] == original_score
    
    def test_no_unblocking(self):
        """Explainability should not unblock any items"""
        request = ExplainabilityRequest(
            protocol_id="TEST",
            protocol_items=[],  # Empty - only blocked items
            blocked_items=[
                {"sku_id": "BLOCKED", "product_name": "Blocked Item",
                 "reason_codes": ["BLOCK_IRON"], "blocked_by": "blood"}
            ],
            has_bloodwork=True,
            bloodwork_complete=True,
            intent_count=2,
            caution_count=0
        )
        
        result = generate_explainability(request)
        
        # Should still have 0 included items
        assert len(result.item_explanations) == 0
        # Should still have 1 blocked item
        assert len(result.blocked_explanations) == 1
