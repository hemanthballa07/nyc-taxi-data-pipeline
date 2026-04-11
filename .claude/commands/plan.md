---
name: plan
description: Create an implementation plan before writing code. Use before starting any new feature.
disable-model-invocation: true
---

Before writing any code for: $ARGUMENTS

1. **Read current state** (mandatory):
   - `docs/architecture.md` — what exists now, current state tables
   - `docs/plan.md` — what phase we're in, what's already done
   - `docs/changelog.md` — recent changes for context
   - Relevant skill files in `.claude/skills/`

2. **Check for an existing design** in `docs/plans/` — if one exists for this topic, follow it. Don't re-plan what's already been designed.

3. **If no design exists**, write a plan to `docs/plans/<topic>.md` with:
   - **Goal**: one sentence on what this accomplishes
   - **Files to create or modify**: exact paths
   - **Dependencies**: what must exist before this works
   - **Implementation steps**: numbered checklist
   - **Tests to write**: what validations prove this works
   - **Definition of done**: how we know it's complete

4. **Present the plan** and wait for approval before implementing.

5. After implementation, update living docs:
   - `docs/changelog.md` — log what was built
   - `docs/plan.md` — check off completed tasks
   - `docs/architecture.md` — update Current State and Files Registry
