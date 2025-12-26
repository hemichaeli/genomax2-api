"""
GenoMAX² Brain Resolver Tests
Unit tests for Contract schemas, Resolver logic, and Mocks.

Run with:
    pytest tests/test_resolver.py -v

Coverage:
    pytest tests/test_resolver.py --cov=app.brain --cov-report=html
"""

import pytest
import json
from datetime import datetime

# Import contracts
from app.brain.contracts import (
    CONTRACT_VERSION,
    AssessmentContext,
    RoutingConstraints,
    ProtocolIntents,
    ProtocolIntentItem,
    ResolverInput,
    ResolverOutput,
    ResolverAudit,
    TargetDetail,
    GenderEnum,
    empty_routing_constraints,
    empty_protocol_intents,
)

# Import resolver
from app.brain.resolver import (
    RESOLVER_VERSION,
    compute_hash,
    merge_constraints,
    merge_intents,
    resolve_all,
    filter_blocked_intents,
    get_active_intents,
    validate_resolver_output,
)

# Import mocks
from app.brain.mocks import (
    bloodwork_mock,
    lifestyle_mock,
    goals_mock,
    create_test_assessment_context,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def sample_assessment_context():
    """Sample AssessmentContext for testing."""
    return AssessmentContext(
        protocol_id="test-protocol-001",
        run_id="test-run-001",
        gender="male",
        age=35,
        height_cm=180.0,
        weight_kg=75.0,
        meds=["metformin"],
        conditions=[],
        allergies=[],
        flags={},
    )


@pytest.fixture
def sample_bloodwork_constraints():
    """Sample bloodwork constraints."""
    return RoutingConstraints(
        blocked_targets=["iron_boost", "ferritin_elevated"],
        caution_targets=["vitamin_d_high"],
        allowed_targets=["magnesium", "omega3", "vitamin_c"],
        blocked_ingredients=["iron", "ashwagandha"],
        has_critical_flags=False,
        global_flags=[],
        target_details={
            "iron_boost": TargetDetail(
                gate_status="blocked",
                reason="Ferritin elevated (350 ng/mL)",
                blocking_biomarkers=["ferritin"],
                caution_biomarkers=[],
                source="bloodwork"
            )
        },
    )


@pytest.fixture
def sample_lifestyle_constraints():
    """Sample lifestyle constraints."""
    return RoutingConstraints(
        blocked_targets=["hepatotoxic_supplements"],
        caution_targets=["stimulant_herbs"],
        allowed_targets=["magnesium", "omega3", "probiotics"],
        blocked_ingredients=["kava"],
        has_critical_flags=False,
        global_flags=["LIFESTYLE:SHIFT_WORK"],
        target_details={},
    )


@pytest.fixture
def sample_goals_intents():
    """Sample intents from goals."""
    return ProtocolIntents(
        lifestyle=[
            {"intent_id": "improve_sleep_quality", "priority": 0.85},
            {"intent_id": "reduce_stress_response", "priority": 0.70},
        ],
        nutrition=[
            {"intent_id": "blood_sugar_stability", "priority": 0.75},
        ],
        supplements=[
            ProtocolIntentItem(
                intent_id="magnesium_for_sleep",
                target_id="sleep_quality",
                priority=0.80,
                source_goal="sleep",
                blocked=False,
            ),
            ProtocolIntentItem(
                intent_id="omega3_brain_support",
                target_id="cognitive_function",
                priority=0.75,
                source_goal="focus",
                blocked=False,
            ),
        ],
    )


@pytest.fixture
def sample_painpoint_intents():
    """Sample intents from painpoints."""
    return ProtocolIntents(
        lifestyle=[
            {"intent_id": "improve_sleep_quality", "priority": 0.90},  # Higher than goals
        ],
        nutrition=[],
        supplements=[
            ProtocolIntentItem(
                intent_id="magnesium_for_sleep",
                target_id="sleep_quality",
                priority=0.90,  # Higher than goals
                source_painpoint="poor_sleep",
                blocked=False,
            ),
            ProtocolIntentItem(
                intent_id="b12_energy_support",
                target_id="energy_metabolism",
                priority=0.85,
                source_painpoint="fatigue",
                blocked=False,
            ),
        ],
    )


# =============================================================================
# CONTRACT SCHEMA TESTS
# =============================================================================

class TestContractSchemas:
    """Test Pydantic schema validation."""
    
    def test_assessment_context_valid(self, sample_assessment_context):
        """AssessmentContext with valid data should pass validation."""
        assert sample_assessment_context.gender == "male"
        assert sample_assessment_context.age == 35
        assert sample_assessment_context.contract_version == CONTRACT_VERSION
    
    def test_assessment_context_gender_normalization(self):
        """Gender should be normalized to enum values."""
        ctx1 = AssessmentContext(
            protocol_id="p1", run_id="r1", gender="MALE"
        )
        assert ctx1.gender == "male"
        
        ctx2 = AssessmentContext(
            protocol_id="p2", run_id="r2", gender="Female"
        )
        assert ctx2.gender == "female"
        
        ctx3 = AssessmentContext(
            protocol_id="p3", run_id="r3", gender="m"
        )
        assert ctx3.gender == "male"
    
    def test_assessment_context_invalid_age(self):
        """Age outside valid range should fail validation."""
        with pytest.raises(Exception):
            AssessmentContext(
                protocol_id="p1", run_id="r1", gender="male", age=150
            )
    
    def test_routing_constraints_valid(self, sample_bloodwork_constraints):
        """RoutingConstraints with valid data should pass validation."""
        assert "iron_boost" in sample_bloodwork_constraints.blocked_targets
        assert "iron" in sample_bloodwork_constraints.blocked_ingredients
    
    def test_routing_constraints_empty(self):
        """Empty RoutingConstraints should be valid."""
        empty = empty_routing_constraints()
        assert empty.blocked_targets == []
        assert empty.caution_targets == []
        assert empty.has_critical_flags is False
    
    def test_protocol_intent_item_valid(self):
        """ProtocolIntentItem with valid data should pass validation."""
        intent = ProtocolIntentItem(
            intent_id="test_intent",
            target_id="test_target",
            priority=0.85,
        )
        assert intent.priority == 0.85
        assert intent.blocked is False
    
    def test_protocol_intent_item_invalid_priority(self):
        """Priority outside 0-1 range should fail validation."""
        with pytest.raises(Exception):
            ProtocolIntentItem(
                intent_id="test",
                target_id="test",
                priority=1.5,  # Invalid: > 1.0
            )
    
    def test_protocol_intents_valid(self, sample_goals_intents):
        """ProtocolIntents with valid data should pass validation."""
        assert len(sample_goals_intents.supplements) == 2
        assert len(sample_goals_intents.lifestyle) == 2
    
    def test_resolver_input_valid(self, sample_assessment_context, sample_bloodwork_constraints):
        """ResolverInput with valid data should pass validation."""
        resolver_input = ResolverInput(
            assessment_context=sample_assessment_context,
            bloodwork_constraints=sample_bloodwork_constraints,
            raw_goals=["sleep", "energy"],
        )
        assert resolver_input.contract_version == CONTRACT_VERSION
        assert len(resolver_input.raw_goals) == 2


# =============================================================================
# RESOLVER LOGIC TESTS
# =============================================================================

class TestMergeConstraints:
    """Test constraint merge logic."""
    
    def test_blocked_targets_union(self, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """Blocked targets should be UNION of both sources."""
        merged, stats = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        
        # All blocked from both should be present
        assert "iron_boost" in merged.blocked_targets  # From bloodwork
        assert "ferritin_elevated" in merged.blocked_targets  # From bloodwork
        assert "hepatotoxic_supplements" in merged.blocked_targets  # From lifestyle
    
    def test_caution_targets_union(self, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """Caution targets should be UNION of both sources."""
        merged, stats = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        
        assert "vitamin_d_high" in merged.caution_targets  # From bloodwork
        assert "stimulant_herbs" in merged.caution_targets  # From lifestyle
    
    def test_allowed_targets_intersection(self, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """Allowed targets should be INTERSECTION when both provided."""
        merged, stats = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        
        # Both have magnesium and omega3 in allowed
        assert "magnesium" in merged.allowed_targets
        assert "omega3" in merged.allowed_targets
        
        # Only bloodwork has vitamin_c, only lifestyle has probiotics
        assert "vitamin_c" not in merged.allowed_targets
        assert "probiotics" not in merged.allowed_targets
    
    def test_blocked_ingredients_union(self, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """Blocked ingredients should be UNION."""
        merged, stats = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        
        assert "iron" in merged.blocked_ingredients  # From bloodwork
        assert "ashwagandha" in merged.blocked_ingredients  # From bloodwork
        assert "kava" in merged.blocked_ingredients  # From lifestyle
    
    def test_critical_flags_or(self):
        """has_critical_flags should be OR of both sources."""
        bw = RoutingConstraints(has_critical_flags=True)
        ls = RoutingConstraints(has_critical_flags=False)
        merged, _ = merge_constraints(bw, ls)
        assert merged.has_critical_flags is True
        
        bw2 = RoutingConstraints(has_critical_flags=False)
        ls2 = RoutingConstraints(has_critical_flags=False)
        merged2, _ = merge_constraints(bw2, ls2)
        assert merged2.has_critical_flags is False
    
    def test_global_flags_union(self, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """Global flags should be UNION."""
        merged, stats = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        
        assert "LIFESTYLE:SHIFT_WORK" in merged.global_flags
    
    def test_deterministic_output(self, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """Same inputs should produce identical outputs."""
        merged1, _ = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        merged2, _ = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        
        assert merged1.model_dump() == merged2.model_dump()
    
    def test_audit_stats(self, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """Audit stats should be correct."""
        merged, stats = merge_constraints(sample_bloodwork_constraints, sample_lifestyle_constraints)
        
        assert stats["bloodwork_blocked_count"] == 2
        assert stats["lifestyle_blocked_count"] == 1
        assert stats["merged_blocked_count"] == 3


class TestMergeIntents:
    """Test intent merge logic."""
    
    def test_supplement_union_by_intent_id(self, sample_goals_intents, sample_painpoint_intents):
        """Supplements should be UNION by intent_id."""
        merged, stats = merge_intents(sample_goals_intents, sample_painpoint_intents)
        
        intent_ids = [i.intent_id for i in merged.supplements]
        assert "magnesium_for_sleep" in intent_ids
        assert "omega3_brain_support" in intent_ids
        assert "b12_energy_support" in intent_ids
    
    def test_max_priority_on_conflict(self, sample_goals_intents, sample_painpoint_intents):
        """On conflict, higher priority should win."""
        merged, stats = merge_intents(sample_goals_intents, sample_painpoint_intents)
        
        # magnesium_for_sleep: goals=0.80, painpoints=0.90
        mag_intent = next(i for i in merged.supplements if i.intent_id == "magnesium_for_sleep")
        assert mag_intent.priority == 0.90  # Painpoint priority wins
    
    def test_lifestyle_dedupe(self, sample_goals_intents, sample_painpoint_intents):
        """Lifestyle intents should be deduped by intent_id."""
        merged, stats = merge_intents(sample_goals_intents, sample_painpoint_intents)
        
        # improve_sleep_quality appears in both, should only appear once with max priority
        sleep_intents = [i for i in merged.lifestyle if i.get("intent_id") == "improve_sleep_quality"]
        assert len(sleep_intents) == 1
        assert sleep_intents[0]["priority"] == 0.90  # Max of 0.85 and 0.90
    
    def test_sorted_by_priority_desc(self, sample_goals_intents, sample_painpoint_intents):
        """Merged supplements should be sorted by priority desc."""
        merged, stats = merge_intents(sample_goals_intents, sample_painpoint_intents)
        
        priorities = [i.priority for i in merged.supplements]
        assert priorities == sorted(priorities, reverse=True)
    
    def test_deterministic_output(self, sample_goals_intents, sample_painpoint_intents):
        """Same inputs should produce identical outputs."""
        merged1, _ = merge_intents(sample_goals_intents, sample_painpoint_intents)
        merged2, _ = merge_intents(sample_goals_intents, sample_painpoint_intents)
        
        assert merged1.model_dump() == merged2.model_dump()
    
    def test_dedup_stats(self, sample_goals_intents, sample_painpoint_intents):
        """Dedup stats should be correct."""
        merged, stats = merge_intents(sample_goals_intents, sample_painpoint_intents)
        
        # Goals: 2 supplements, Painpoints: 2 supplements
        # magnesium_for_sleep is duplicate -> 3 unique
        assert stats["intents_deduplicated"] == 1
        assert stats["merged_intents_count"] == 3


class TestResolveAll:
    """Test full resolution pipeline."""
    
    def test_resolve_all_basic(self, sample_assessment_context):
        """resolve_all should produce valid output."""
        resolver_input = ResolverInput(
            assessment_context=sample_assessment_context,
            bloodwork_constraints=empty_routing_constraints(),
            lifestyle_constraints=empty_routing_constraints(),
            raw_goals=["sleep"],
        )
        
        output = resolve_all(resolver_input)
        
        assert output.protocol_id == sample_assessment_context.protocol_id
        assert output.contract_version == CONTRACT_VERSION
        assert output.audit.resolver_version == RESOLVER_VERSION
    
    def test_resolve_all_with_constraints(self, sample_assessment_context, sample_bloodwork_constraints, sample_lifestyle_constraints):
        """resolve_all should correctly merge constraints."""
        resolver_input = ResolverInput(
            assessment_context=sample_assessment_context,
            bloodwork_constraints=sample_bloodwork_constraints,
            lifestyle_constraints=sample_lifestyle_constraints,
        )
        
        output = resolve_all(resolver_input)
        
        # Check merged constraints
        assert "iron_boost" in output.resolved_constraints.blocked_targets
        assert "hepatotoxic_supplements" in output.resolved_constraints.blocked_targets
    
    def test_resolve_all_with_intents(self, sample_assessment_context, sample_goals_intents, sample_painpoint_intents):
        """resolve_all should correctly merge intents."""
        resolver_input = ResolverInput(
            assessment_context=sample_assessment_context,
            bloodwork_constraints=empty_routing_constraints(),
            lifestyle_constraints=empty_routing_constraints(),
            goals_intents=sample_goals_intents,
            painpoint_intents=sample_painpoint_intents,
        )
        
        output = resolve_all(resolver_input)
        
        # Check merged intents
        intent_ids = [i.intent_id for i in output.resolved_intents.supplements]
        assert "magnesium_for_sleep" in intent_ids
        assert "b12_energy_support" in intent_ids
    
    def test_resolve_all_audit_hashes(self, sample_assessment_context):
        """Audit should contain valid hashes."""
        resolver_input = ResolverInput(
            assessment_context=sample_assessment_context,
        )
        
        output = resolve_all(resolver_input)
        
        assert output.audit.input_hash.startswith("sha256:")
        assert output.audit.output_hash.startswith("sha256:")
        assert len(output.audit.input_hash) == 71  # sha256: + 64 hex chars
    
    def test_resolve_all_deterministic(self, sample_assessment_context):
        """Same inputs should produce same output hash."""
        resolver_input = ResolverInput(
            assessment_context=sample_assessment_context,
            raw_goals=["sleep", "energy"],
        )
        
        output1 = resolve_all(resolver_input)
        output2 = resolve_all(resolver_input)
        
        assert output1.audit.input_hash == output2.audit.input_hash
        # Note: output_hash may differ due to timestamp in audit


# =============================================================================
# MOCK ENGINE TESTS
# =============================================================================

class TestBloodworkMock:
    """Test bloodwork mock engine."""
    
    def test_empty_context_returns_empty_constraints(self):
        """Empty context should return empty constraints."""
        ctx = create_test_assessment_context()
        constraints = bloodwork_mock(ctx)
        
        assert constraints.blocked_targets == []
        assert constraints.caution_targets == []
    
    def test_warfarin_adds_caution(self):
        """Warfarin should add caution targets."""
        ctx = create_test_assessment_context(meds=["warfarin"])
        constraints = bloodwork_mock(ctx)
        
        assert "vitamin_k" in constraints.caution_targets
        assert "omega3_high_dose" in constraints.caution_targets
    
    def test_metformin_adds_caution(self):
        """Metformin should add caution for berberine."""
        ctx = create_test_assessment_context(meds=["metformin"])
        constraints = bloodwork_mock(ctx)
        
        assert "berberine_glucose" in constraints.caution_targets
    
    def test_hemochromatosis_blocks_iron(self):
        """Hemochromatosis should block iron."""
        ctx = create_test_assessment_context(conditions=["hemochromatosis"])
        constraints = bloodwork_mock(ctx)
        
        assert "iron_boost" in constraints.blocked_targets
        assert "iron" in constraints.blocked_ingredients
        assert constraints.has_critical_flags is True
    
    def test_liver_disease_blocks_hepatotoxic(self):
        """Liver disease should block hepatotoxic supplements."""
        ctx = create_test_assessment_context(conditions=["liver_disease"])
        constraints = bloodwork_mock(ctx)
        
        assert "ashwagandha" in constraints.blocked_ingredients
        assert "kava" in constraints.blocked_ingredients
    
    def test_multiple_conditions_cumulative(self):
        """Multiple conditions should have cumulative effect."""
        ctx = create_test_assessment_context(
            meds=["warfarin", "metformin"],
            conditions=["hemochromatosis"]
        )
        constraints = bloodwork_mock(ctx)
        
        # From warfarin
        assert "vitamin_k" in constraints.caution_targets
        # From metformin
        assert "berberine_glucose" in constraints.caution_targets
        # From hemochromatosis
        assert "iron_boost" in constraints.blocked_targets


class TestGoalsMock:
    """Test goals mock engine."""
    
    def test_sleep_goal_generates_intents(self):
        """Sleep goal should generate sleep-related intents."""
        intents = goals_mock(["sleep"])
        
        intent_ids = [i.intent_id for i in intents.supplements]
        assert "magnesium_for_sleep" in intent_ids
        assert "glycine_for_sleep" in intent_ids
    
    def test_multiple_goals_combined(self):
        """Multiple goals should combine intents."""
        intents = goals_mock(["sleep", "energy"])
        
        intent_ids = [i.intent_id for i in intents.supplements]
        # From sleep
        assert "magnesium_for_sleep" in intent_ids
        # From energy
        assert "b12_energy_support" in intent_ids
    
    def test_painpoints_higher_priority(self):
        """Painpoints should generate higher priority intents."""
        intents = goals_mock([], ["fatigue"])
        
        b12_intent = next(i for i in intents.supplements if i.intent_id == "b12_energy_support")
        assert b12_intent.priority == 0.90  # Painpoint priority
    
    def test_lifestyle_intents_generated(self):
        """Goals should generate lifestyle intents."""
        intents = goals_mock(["sleep", "stress"])
        
        lifestyle_ids = [i.get("intent_id") for i in intents.lifestyle]
        assert "improve_sleep_quality" in lifestyle_ids
        assert "reduce_stress_response" in lifestyle_ids
    
    def test_nutrition_intents_generated(self):
        """Goals should generate nutrition intents."""
        intents = goals_mock(["energy", "gut"])
        
        nutrition_ids = [i.get("intent_id") for i in intents.nutrition]
        assert "blood_sugar_stability" in nutrition_ids
        assert "fiber_diversity" in nutrition_ids
    
    def test_unknown_goal_ignored(self):
        """Unknown goals should be silently ignored."""
        intents = goals_mock(["unknown_goal_xyz"])
        
        assert len(intents.supplements) == 0


# =============================================================================
# HELPER FUNCTION TESTS
# =============================================================================

class TestHelperFunctions:
    """Test helper functions."""
    
    def test_filter_blocked_intents(self, sample_goals_intents, sample_bloodwork_constraints):
        """Blocked targets should mark intents as blocked."""
        # Add a blocked target that matches an intent
        constraints = RoutingConstraints(
            blocked_targets=["sleep_quality"],  # Matches magnesium_for_sleep target
        )
        
        filtered = filter_blocked_intents(sample_goals_intents, constraints)
        
        mag_intent = next(i for i in filtered.supplements if i.intent_id == "magnesium_for_sleep")
        assert mag_intent.blocked is True
        
        omega_intent = next(i for i in filtered.supplements if i.intent_id == "omega3_brain_support")
        assert omega_intent.blocked is False
    
    def test_get_active_intents(self, sample_goals_intents):
        """get_active_intents should filter out blocked intents."""
        # Create intents with one blocked
        intents = ProtocolIntents(
            supplements=[
                ProtocolIntentItem(intent_id="active1", target_id="t1", priority=0.8, blocked=False),
                ProtocolIntentItem(intent_id="blocked1", target_id="t2", priority=0.7, blocked=True),
                ProtocolIntentItem(intent_id="active2", target_id="t3", priority=0.6, blocked=False),
            ]
        )
        
        active = get_active_intents(intents)
        
        assert len(active) == 2
        assert all(not i.blocked for i in active)
    
    def test_validate_resolver_output_valid(self, sample_assessment_context):
        """Valid output should pass validation."""
        resolver_input = ResolverInput(assessment_context=sample_assessment_context)
        output = resolve_all(resolver_input)
        
        is_valid, errors = validate_resolver_output(output)
        
        assert is_valid is True
        assert errors == []
    
    def test_validate_resolver_output_overlap(self, sample_assessment_context):
        """Overlapping blocked/allowed should fail validation."""
        output = ResolverOutput(
            protocol_id="test",
            resolved_constraints=RoutingConstraints(
                blocked_targets=["target1"],
                allowed_targets=["target1"],  # Same as blocked
            ),
            resolved_intents=empty_protocol_intents(),
            assessment_context=sample_assessment_context,
            audit=ResolverAudit(
                resolved_at="2025-01-01T00:00:00Z",
                input_hash="sha256:abc",
                output_hash="sha256:def",
            ),
        )
        
        is_valid, errors = validate_resolver_output(output)
        
        assert is_valid is False
        assert any("blocked and allowed" in e for e in errors)


class TestComputeHash:
    """Test hash computation."""
    
    def test_deterministic_hash(self):
        """Same data should produce same hash."""
        data = {"key": "value", "number": 42}
        
        hash1 = compute_hash(data)
        hash2 = compute_hash(data)
        
        assert hash1 == hash2
    
    def test_different_data_different_hash(self):
        """Different data should produce different hash."""
        data1 = {"key": "value1"}
        data2 = {"key": "value2"}
        
        hash1 = compute_hash(data1)
        hash2 = compute_hash(data2)
        
        assert hash1 != hash2
    
    def test_hash_format(self):
        """Hash should be in sha256:xxxx format."""
        data = {"test": True}
        hash_value = compute_hash(data)
        
        assert hash_value.startswith("sha256:")
        assert len(hash_value) == 71  # sha256: (7) + 64 hex chars


# =============================================================================
# END-TO-END TESTS
# =============================================================================

class TestEndToEnd:
    """End-to-end integration tests."""
    
    def test_full_pipeline_mock(self):
        """Full pipeline with mocks should work end-to-end."""
        # Create context
        ctx = create_test_assessment_context(
            protocol_id="e2e-test-001",
            run_id="e2e-run-001",
            gender="female",
            age=32,
            meds=["metformin"],
            conditions=[],
        )
        
        # Generate constraints from mock
        bloodwork_constraints = bloodwork_mock(ctx)
        lifestyle_constraints = lifestyle_mock(ctx)
        
        # Generate intents from mock
        goals_intents = goals_mock(["sleep", "energy"], ["fatigue"])
        
        # Build resolver input
        resolver_input = ResolverInput(
            assessment_context=ctx,
            bloodwork_constraints=bloodwork_constraints,
            lifestyle_constraints=lifestyle_constraints,
            goals_intents=goals_intents,
            painpoint_intents=empty_protocol_intents(),
            raw_goals=["sleep", "energy"],
            raw_painpoints=["fatigue"],
        )
        
        # Resolve
        output = resolve_all(resolver_input)
        
        # Validate
        is_valid, errors = validate_resolver_output(output)
        assert is_valid, f"Validation failed: {errors}"
        
        # Check constraints merged (metformin -> berberine caution)
        assert "berberine_glucose" in output.resolved_constraints.caution_targets
        
        # Check intents present
        intent_ids = [i.intent_id for i in output.resolved_intents.supplements]
        assert "magnesium_for_sleep" in intent_ids
        assert "b12_energy_support" in intent_ids
        
        # Check audit
        assert output.audit.resolver_version == RESOLVER_VERSION
        assert output.audit.input_hash.startswith("sha256:")
    
    def test_complex_scenario_multiple_conditions(self):
        """Complex scenario with multiple medications and conditions."""
        ctx = create_test_assessment_context(
            protocol_id="complex-001",
            run_id="complex-run-001",
            gender="male",
            age=55,
            meds=["warfarin", "metformin", "statins"],
            conditions=["kidney_disease"],
        )
        
        bloodwork_constraints = bloodwork_mock(ctx)
        
        # Verify multiple constraints applied
        assert "vitamin_k" in bloodwork_constraints.caution_targets  # warfarin
        assert "berberine_glucose" in bloodwork_constraints.caution_targets  # metformin
        assert "coq10_depletion" in bloodwork_constraints.caution_targets  # statins
        assert "creatine_performance" in bloodwork_constraints.blocked_targets  # kidney
        assert bloodwork_constraints.has_critical_flags is True  # kidney disease
