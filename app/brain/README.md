# GenoMAX² Brain - Resolver System

## Overview

The Brain Resolver is the integration layer that deterministically connects:

```
Bloodwork Engine + Lifestyle Engine + Goals/Painpoints → Intents + Constraints → Route → SKU Plan
```

This module provides the **Contract** (canonical schemas) and **Resolver** (merge logic) that enable swapping mock engines with real engines without refactors.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              RESOLVER LAYER                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐     │
│   │  Bloodwork   │    │   Lifestyle  │    │   Goals/Painpoints   │     │
│   │   Engine     │    │    Engine    │    │       Engine         │     │
│   │   (mock)     │    │    (mock)    │    │       (mock)         │     │
│   └──────┬───────┘    └──────┬───────┘    └──────────┬───────────┘     │
│          │                   │                       │                  │
│          ▼                   ▼                       ▼                  │
│   RoutingConstraints  RoutingConstraints      ProtocolIntents          │
│          │                   │                       │                  │
│          └─────────┬─────────┘                       │                  │
│                    │                                 │                  │
│                    ▼                                 │                  │
│            merge_constraints()                       │                  │
│                    │                                 │                  │
│                    ▼                                 ▼                  │
│         RoutingConstraints              merge_intents()                 │
│         (resolved)                           │                          │
│                    │                         ▼                          │
│                    │              ProtocolIntents                       │
│                    │              (resolved)                            │
│                    │                         │                          │
│                    └─────────────────────────┘                          │
│                                │                                        │
│                                ▼                                        │
│                        ResolverOutput                                   │
│                                │                                        │
└────────────────────────────────┼────────────────────────────────────────┘
                                 │
                                 ▼
                        /api/v1/brain/route
                                 │
                                 ▼
                            SKU Plan
```

## Contract v1.0

### Schemas

| Schema | Purpose |
|--------|---------|
| `AssessmentContext` | Protocol/run metadata + user context (gender, age, meds) |
| `RoutingConstraints` | Blocked/caution/allowed targets + global flags |
| `ProtocolIntentItem` | Single supplement intent with priority (0-1) |
| `ProtocolIntents` | All intents: lifestyle, nutrition, supplements |
| `ResolverInput` | All inputs to the resolver |
| `ResolverOutput` | Deterministic merge output with audit trail |

### Version

All schemas include `contract_version: "1.0"`. Breaking changes require version bump.

## Resolver Rules

### Constraint Merge (`merge_constraints`)

| Field | Rule |
|-------|------|
| `blocked_targets` | UNION(bloodwork, lifestyle) |
| `caution_targets` | UNION(bloodwork, lifestyle) |
| `blocked_ingredients` | UNION(bloodwork, lifestyle) |
| `allowed_targets` | INTERSECTION if both provided, else whichever non-empty |
| `has_critical_flags` | bloodwork OR lifestyle |
| `global_flags` | UNION(bloodwork, lifestyle) |
| `target_details` | MERGE with bloodwork taking precedence |

### Intent Merge (`merge_intents`)

| Category | Rule |
|----------|------|
| `supplements` | UNION by intent_id; on conflict keep MAX(priority) |
| `lifestyle` | CONCATENATE, dedupe by intent_id, keep max priority |
| `nutrition` | CONCATENATE, dedupe by intent_id, keep max priority |

### Determinism Guarantee

**Same inputs → Same outputs**

- No randomness
- No time-based conditions (except audit timestamps)
- Sorted outputs for stability
- Hashed inputs/outputs for verification

## Usage

### Basic Usage

```python
from app.brain.contracts import ResolverInput, AssessmentContext
from app.brain.resolver import resolve_all
from app.brain.mocks import bloodwork_mock, goals_mock

# Create context
ctx = AssessmentContext(
    protocol_id="proto-001",
    run_id="run-001",
    gender="male",
    age=35,
    meds=["metformin"],
)

# Get constraints from mock (replace with real engine later)
constraints = bloodwork_mock(ctx)

# Get intents from mock
intents = goals_mock(["sleep", "energy"])

# Resolve
resolver_input = ResolverInput(
    assessment_context=ctx,
    bloodwork_constraints=constraints,
    goals_intents=intents,
)

output = resolve_all(resolver_input)
print(output.resolved_constraints.blocked_targets)
print([i.intent_id for i in output.resolved_intents.supplements])
```

### Replace Mocks with Real Engines

```python
# Before (mock)
from app.brain.mocks import bloodwork_mock
constraints = bloodwork_mock(ctx)

# After (real engine)
from app.brain.bloodwork_engine import analyze_bloodwork
constraints = analyze_bloodwork(ctx, bloodwork_signal)
```

The contract is stable - no other code changes needed.

## API Endpoint

### POST /api/v1/brain/resolve

**Input:**
```json
{
    "protocol_id": "uuid",
    "assessment_context": { ... },
    "bloodwork_constraints": { ... },
    "lifestyle_constraints": { ... },
    "raw_goals": ["sleep", "energy"],
    "raw_painpoints": ["fatigue"]
}
```

**Output:**
```json
{
    "protocol_id": "uuid",
    "resolved_constraints": {
        "blocked_targets": [...],
        "caution_targets": [...],
        "blocked_ingredients": [...]
    },
    "resolved_intents": {
        "supplements": [
            {"intent_id": "...", "target_id": "...", "priority": 0.85}
        ]
    },
    "audit": {
        "resolver_version": "1.0.0",
        "input_hash": "sha256:...",
        "output_hash": "sha256:..."
    }
}
```

## File Structure

```
app/brain/
├── __init__.py         # Exports
├── contracts.py        # Pydantic schemas (Contract v1.0)
├── resolver.py         # Deterministic merge logic
├── mocks.py           # Mock engines for testing
├── models.py          # Legacy models (to be deprecated)
├── orchestrate.py     # Orchestration logic
└── README.md          # This file

tests/
└── test_resolver.py   # Comprehensive test suite
```

## Testing

```bash
# Run all resolver tests
pytest tests/test_resolver.py -v

# With coverage
pytest tests/test_resolver.py --cov=app.brain --cov-report=html
```

## Migration Path

### Phase 0 (Current)
✅ Contract + Resolver + Mocks + `/api/v1/brain/resolve`

### Phase 1 (Next)
🔜 Bloodwork Engine (logic only, outputs RoutingConstraints)

### Phase 2
🔜 Goals/Painpoints Engine (outputs ProtocolIntents)

### Phase 3
🔜 Lifestyle Engine (outputs RoutingConstraints)

### Phase 4
🔜 Lab Integrations (adapters that produce standard outputs)

## Key Principles

1. **Safety First**: Blocked targets/ingredients are always UNION (additive safety)
2. **Deterministic**: Same inputs = same outputs, always
3. **Auditable**: Every output includes hashes and stats
4. **Swappable**: Mocks can be replaced with real engines without refactor
5. **Versioned**: Contract version enables future migrations
