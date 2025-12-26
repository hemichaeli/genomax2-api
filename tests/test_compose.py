"""
Tests for GenoMAX² Compose Phase

Tests verify:
1. Painpoints generate intents correctly
2. Lifestyle modifies priority only (never creates intents)
3. Blood constraints always override
4. Deterministic outputs (same input = same output)
"""

import pytest
import sys
import os

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from app.brain.compose import (
    PainpointInput,
    LifestyleInput,
    Intent,
    generate_intents_from_painpoints,
    apply_lifestyle_modifiers,
    apply_blood_constraints,
    compose,
    compose_result_to_dict
)


class TestPainpointsToIntents:
    """Test painpoints → intents generation."""
    
    def test_single_painpoint_generates_intents(self):
        """Single painpoint should generate mapped intents."""
        painpoints = [PainpointInput(id="fatigue", severity=2)]
        intents, applied, audit = generate_intents_from_painpoints(painpoints)
        
        assert len(intents) > 0
        assert "fatigue" in applied
        assert any(i.id == "increase_energy" for i in intents)
    
    def test_severity_affects_priority(self):
        """Higher severity should result in higher priority."""
        low = [PainpointInput(id="fatigue", severity=1)]
        high = [PainpointInput(id="fatigue", severity=3)]
        
        low_intents, _, _ = generate_intents_from_painpoints(low)
        high_intents, _, _ = generate_intents_from_painpoints(high)
        
        low_energy = next(i for i in low_intents if i.id == "increase_energy")
        high_energy = next(i for i in high_intents if i.id == "increase_energy")
        
        assert high_energy.priority > low_energy.priority
    
    def test_priority_capped(self):
        """Priority should never exceed max_priority_cap."""
        painpoints = [PainpointInput(id="fatigue", severity=3)]
        intents, _, _ = generate_intents_from_painpoints(painpoints)
        
        for intent in intents:
            assert intent.priority <= intent.max_priority_cap
    
    def test_unknown_painpoint_logged(self):
        """Unknown painpoint should be logged, not crash."""
        painpoints = [PainpointInput(id="not_a_real_painpoint", severity=2)]
        intents, applied, audit = generate_intents_from_painpoints(painpoints)
        
        assert len(intents) == 0
        assert any("Unknown painpoint" in log for log in audit)
    
    def test_multiple_painpoints_merge(self):
        """Multiple painpoints targeting same intent should merge."""
        painpoints = [
            PainpointInput(id="fatigue", severity=2),
            PainpointInput(id="stress", severity=3)
        ]
        intents, applied, _ = generate_intents_from_painpoints(painpoints)
        
        # Both map to stress_management
        stress_intents = [i for i in intents if i.id == "stress_management"]
        assert len(stress_intents) == 1  # Should be merged, not duplicated


class TestLifestyleModifiers:
    """Test lifestyle → priority modification."""
    
    def get_base_lifestyle(self) -> LifestyleInput:
        """Return neutral lifestyle input."""
        return LifestyleInput(
            sleep_hours=7,
            sleep_quality=7,
            stress_level=5,
            activity_level="moderate",
            caffeine_intake="low",
            alcohol_intake="none",
            work_schedule="day",
            meals_per_day=3,
            sugar_intake="low",
            smoking=False
        )
    
    def test_lifestyle_cannot_create_intents(self):
        """Lifestyle should NEVER create new intents."""
        lifestyle = LifestyleInput(
            sleep_hours=4,  # Triggers poor_sleep rule
            sleep_quality=3,
            stress_level=9,  # Triggers high_stress rule
            activity_level="sedentary",
            caffeine_intake="high",
            alcohol_intake="high",
            work_schedule="night",
            meals_per_day=1,
            sugar_intake="high",
            smoking=True
        )
        
        # Start with NO intents
        empty_intents = []
        result, rules, conf, audit = apply_lifestyle_modifiers(empty_intents, lifestyle)
        
        # Should still have no intents
        assert len(result) == 0
        assert any("cannot create intents" in log for log in audit)
    
    def test_lifestyle_modifies_existing_intent(self):
        """Lifestyle should modify priority of existing intents."""
        lifestyle = LifestyleInput(
            sleep_hours=4,  # Triggers poor_sleep_duration
            sleep_quality=7,
            stress_level=5,
            activity_level="moderate",
            caffeine_intake="low",
            alcohol_intake="none",
            work_schedule="day",
            meals_per_day=3,
            sugar_intake="low",
            smoking=False
        )
        
        intents = [Intent(id="improve_sleep", priority=0.5, source="painpoint")]
        result, rules, conf, audit = apply_lifestyle_modifiers(intents, lifestyle)
        
        assert "poor_sleep_duration" in rules
        sleep_intent = result[0]
        assert sleep_intent.priority != 0.5  # Should be modified
    
    def test_high_stress_applies_confidence_penalty(self):
        """High stress rule should reduce confidence."""
        lifestyle = self.get_base_lifestyle()
        lifestyle.stress_level = 9  # Triggers high_stress
        
        intents = [Intent(id="stress_management", priority=0.5, source="painpoint")]
        result, rules, conf, audit = apply_lifestyle_modifiers(intents, lifestyle)
        
        assert result[0].confidence < 1.0  # Penalty applied
    
    def test_smoking_triggers_quit_intent_modifier(self):
        """Smoking should trigger quit_smoking modifier (if intent exists)."""
        lifestyle = self.get_base_lifestyle()
        lifestyle.smoking = True
        
        # Only modifies if intent already exists
        intents = [Intent(id="quit_smoking", priority=0.3, source="goal")]
        result, rules, conf, audit = apply_lifestyle_modifiers(intents, lifestyle)
        
        assert "smoking_active" in rules


class TestBloodConstraints:
    """Test blood constraints override behavior."""
    
    def test_blood_blocks_intent(self):
        """Blood block should remove intent entirely."""
        intents = [
            Intent(id="increase_energy", priority=0.8, source="painpoint"),
            Intent(id="stress_management", priority=0.6, source="painpoint")
        ]
        
        blood_blocks = {"blocked_intents": ["increase_energy"]}
        result, audit = apply_blood_constraints(intents, blood_blocks)
        
        assert len(result) == 1
        assert result[0].id == "stress_management"
        assert any("BLOCK" in log for log in audit)
    
    def test_blood_forces_intent_priority(self):
        """Blood requirement should maximize intent priority."""
        intents = [Intent(id="vitamin_d", priority=0.3, source="painpoint")]
        
        blood_blocks = {"required_intents": ["vitamin_d"]}
        result, audit = apply_blood_constraints(intents, blood_blocks)
        
        assert result[0].priority == result[0].max_priority_cap
        assert result[0].source == "blood"
    
    def test_no_blood_constraints_passes_through(self):
        """Without constraints, all intents pass through."""
        intents = [
            Intent(id="a", priority=0.5, source="painpoint"),
            Intent(id="b", priority=0.6, source="painpoint")
        ]
        
        result, audit = apply_blood_constraints(intents, None)
        
        assert len(result) == 2


class TestComposeIntegration:
    """Integration tests for full compose pipeline."""
    
    def test_compose_deterministic(self):
        """Same input should produce same output."""
        painpoints = [PainpointInput(id="fatigue", severity=2)]
        lifestyle = LifestyleInput(
            sleep_hours=6, sleep_quality=6, stress_level=6,
            activity_level="light", caffeine_intake="low",
            alcohol_intake="none", work_schedule="day",
            meals_per_day=3, sugar_intake="low", smoking=False
        )
        
        result1 = compose(painpoints_input=painpoints, lifestyle_input=lifestyle)
        result2 = compose(painpoints_input=painpoints, lifestyle_input=lifestyle)
        
        # Convert to dict for comparison
        dict1 = compose_result_to_dict(result1)
        dict2 = compose_result_to_dict(result2)
        
        assert dict1["intents"] == dict2["intents"]
    
    def test_compose_full_pipeline(self):
        """Full pipeline: painpoints → lifestyle → blood."""
        painpoints = [
            PainpointInput(id="fatigue", severity=2),
            PainpointInput(id="stress", severity=2)
        ]
        lifestyle = LifestyleInput(
            sleep_hours=5, sleep_quality=5, stress_level=8,
            activity_level="sedentary", caffeine_intake="high",
            alcohol_intake="none", work_schedule="night",
            meals_per_day=3, sugar_intake="low", smoking=False
        )
        blood_blocks = {"blocked_intents": ["exercise"]}
        
        result = compose(
            painpoints_input=painpoints,
            lifestyle_input=lifestyle,
            blood_blocks=blood_blocks
        )
        
        # Should have intents
        assert len(result.intents) > 0
        
        # Exercise should be blocked
        assert not any(i.id == "exercise" for i in result.intents)
        
        # Should have audit trail
        assert len(result.audit_log) > 0
        
        # Should track applied rules
        assert len(result.painpoints_applied) > 0
    
    def test_compose_painpoints_only(self):
        """Compose with only painpoints."""
        painpoints = [PainpointInput(id="hair_loss", severity=3)]
        
        result = compose(painpoints_input=painpoints)
        
        assert len(result.intents) > 0
        assert "hair_loss" in result.painpoints_applied
    
    def test_compose_goal_intents_merge(self):
        """Goal intents should merge with painpoint intents."""
        painpoints = [PainpointInput(id="fatigue", severity=1)]
        goals = [Intent(id="increase_energy", priority=0.9, source="goal")]
        
        result = compose(painpoints_input=painpoints, goal_intents=goals)
        
        energy = next(i for i in result.intents if i.id == "increase_energy")
        assert energy.priority == 0.9  # Goal priority wins (higher)


class TestSerializationAndAudit:
    """Test audit trail and serialization."""
    
    def test_compose_result_serializable(self):
        """ComposeResult should serialize to dict."""
        painpoints = [PainpointInput(id="fatigue", severity=2)]
        result = compose(painpoints_input=painpoints)
        
        dict_result = compose_result_to_dict(result)
        
        assert "intents" in dict_result
        assert "audit_log" in dict_result
        assert isinstance(dict_result["intents"], list)
    
    def test_audit_log_contains_all_operations(self):
        """Audit log should track all modifications."""
        painpoints = [PainpointInput(id="stress", severity=2)]
        lifestyle = LifestyleInput(
            sleep_hours=5, sleep_quality=5, stress_level=9,
            activity_level="moderate", caffeine_intake="low",
            alcohol_intake="none", work_schedule="day",
            meals_per_day=3, sugar_intake="low", smoking=False
        )
        
        result = compose(painpoints_input=painpoints, lifestyle_input=lifestyle)
        
        # Should have NEW entries (from painpoints)
        assert any("[NEW]" in log for log in result.audit_log)
        # Should have MOD entries (from lifestyle)
        assert any("[MOD]" in log or "[SKIP]" in log for log in result.audit_log)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
