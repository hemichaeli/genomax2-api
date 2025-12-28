# Bloodwork → Brain Handoff Integration Specification

## Version: 1.0.0 (Strict Mode)

This document defines the canonical integration between Bloodwork Engine and Brain Orchestrator.

---

## Core Principle

> **Blood does not negotiate.**

If bloodwork is required and unavailable, orchestration MUST hard-abort.

---

## Canonical Handoff Object

```json
{
  "handoff_version": "bloodwork_handoff.v1",
  "source": {
    "service": "bloodwork_engine",
    "base_url": "https://web-production-97b74.up.railway.app",
    "endpoint": "/api/v1/bloodwork/process",
    "engine_version": "1.0.0"
  },
  "input": {
    "lab_profile": "GLOBAL_CONSERVATIVE",
    "sex": "male",
    "age": 35,
    "markers": [
      { "code": "ferritin", "value": 400, "unit": "ng/mL" }
    ]
  },
  "output": {
    "routing_constraints": {
      "blocked_ingredients": ["iron"],
      "blocked_categories": [],
      "caution_flags": [],
      "requirements": [],
      "reason_codes": ["BLOCK_IRON_FERRITIN_HIGH"]
    },
    "signal_flags": [],
    "unknown_biomarkers": []
  },
  "audit": {
    "input_hash": "sha256:...",
    "output_hash": "sha256:...",
    "ruleset_version": "registry_v1.0+ranges_v1.0",
    "marker_registry_version": "registry_v1.0",
    "reference_ranges_version": "ranges_v1.0",
    "processed_at": "ISO-8601"
  }
}
```

---

## Error Handling

| Condition | Action | HTTP Code |
|-----------|--------|-----------|
| Bloodwork unavailable | Hard abort | 503 |
| Incomplete panel | Proceed with flag | 200 |
| Invalid handoff schema | Hard abort | 500 |

---

## Merge Rules

For `blocked_ingredients`, `blocked_categories`, `caution_flags`, `requirements`, `reason_codes`:

1. Union all sources
2. Deduplicate
3. Alphabetical sort (determinism)

**Precedence**: Blood constraints cannot be removed or overridden.
