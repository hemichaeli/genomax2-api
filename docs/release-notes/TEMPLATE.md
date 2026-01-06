# GenoMAX² Release Notes Template

Use this template for all future release notes.  
Save as: `docs/release-notes/vX.Y.Z-<short-name>.md`

---

## Naming Convention

```
v<MAJOR>.<MINOR>.<PATCH>-<short-name>.md
```

Examples:
- `v3.21.0-excel-override.md`
- `v3.22.0-supliful-api.md`
- `v3.23.0-design-export.md`

---

## Template

```markdown
# GenoMAX² Release Notes  
## vX.Y.Z – <Short Descriptive Title>

**Release Date:** YYYY-MM-DD  
**Scope:** <Domain / System Area>  
**Status:** Deployed / Partial / Experimental

---

## Overview

Short paragraph explaining *why* this release exists and what problem it solves.

---

## What's New

- Bullet list of new capabilities

---

## What Changed

- Behavioral or architectural changes
- Backwards compatibility notes if relevant

---

## What's Live

- Explicit list of what is production-ready

---

## New / Updated API Endpoints

List endpoints with one-line descriptions.

---

## Verification

Describe how correctness is verified:
- QA checks
- Batch IDs
- Metrics
- PASS/FAIL expectations

---

## Known Limitations

Clear list of what is intentionally not ready yet.

---

## Impacted Workflows

- Designers
- Research
- Backend
- Ops

---

## Next Gates

Concrete checklist of what unlocks the next stage.

---

## Summary

One paragraph executive summary.
```

---

## Guidelines

### Release Notes vs Changelog

| Release Notes | Changelog / Commits |
|---------------|---------------------|
| **Why** and **what it means** | **How** it was done |
| Business impact | Technical implementation |
| Human-readable | Machine-parseable |
| Per milestone | Per commit/PR |

**Do not mix these.**

### When to Write a Release Note

- New capability deployed to production
- Breaking change or behavioral change
- Major infrastructure milestone
- QA/audit system changes

### When NOT to Write a Release Note

- Bug fixes (use commit messages)
- Refactoring without behavior change
- Documentation updates
- Dependency bumps

---

## Checklist Before Publishing

- [ ] Version number matches Railway deployment
- [ ] Release date is accurate
- [ ] All new endpoints are documented
- [ ] Verification section has concrete proof (batch ID, metrics)
- [ ] Known limitations are explicit
- [ ] Next gates are actionable
