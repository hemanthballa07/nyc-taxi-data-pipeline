---
name: brainstorm
description: "You MUST use this before any significant new feature — building ingestion, adding dbt models, creating DAGs, or changing architecture. Explores intent, requirements, and design before implementation."
---

# Brainstorm Skill

Help turn data engineering ideas into clear designs through structured dialogue before any code is written.

<HARD-GATE>
Do NOT write any code, create any files, or run any commands until you have presented a design and the user has approved it. This applies regardless of how simple the task seems.
</HARD-GATE>

## Process (follow in order)

### 1. Read Context First
Before asking anything, read:
- `docs/architecture.md` — current state, what exists, what doesn't
- `docs/plan.md` — what phase we're in, what's next
- `docs/changelog.md` — recent changes for context

### 2. Restate the Problem
Summarize what we're trying to solve in one sentence. If it's vague, ask ONE clarifying question. Only one question per message.

### 3. Check Scope
Is this actually one task, or multiple independent pieces? If multiple, flag it:
> "This is really 3 separate things: X, Y, and Z. I'd recommend tackling X first since Y and Z depend on it. Want to start with X?"

### 4. List 2-3 Options
For each option:
- **What**: the approach in 1-2 sentences
- **Pros**: why it's attractive
- **Cons**: risks or downsides
- **Effort**: rough time (hours)

Lead with your recommendation and say why.

### 5. Present Design
Once the approach is agreed, present the design:
- What files will be created/modified
- Key logic or SQL patterns involved
- How it connects to existing components
- What tests are needed
- What could go wrong

Keep each section proportional to its complexity. A simple seed file gets one sentence. A fact table join strategy gets a paragraph.

### 6. Get Approval
Ask: "Does this design look right? Any changes before I start building?"

**Do NOT proceed until the user says yes.**

### 7. Write It Down
Save the approved design to `docs/plans/<topic>.md` and commit it. This becomes the implementation spec.

### 8. Transition
Only after approval: begin implementing, following the design you just wrote.

## Decision-Making Principles
- We're a student portfolio project, not a Fortune 500 company
- Simplicity wins over sophistication
- "Good enough and done" beats "perfect and unfinished"
- Prefer approaches that teach transferable skills
- YAGNI ruthlessly — remove anything not needed for the current phase

## Good Topics for Brainstorming
- Data modeling (star schema grain, SCD types, how to handle nulls)
- Pipeline design (incremental vs full refresh, idempotency strategy)
- Tool selection (which library, which approach to bulk loading)
- Testing strategy (what to test, what's overkill for a portfolio project)
- Performance (indexing, partitioning, materialization choices)
- Scope management ("should I add X to this project?")

## Anti-Patterns
- Don't over-engineer: if the answer is obvious, say so and move on
- Don't recommend tools the user hasn't learned without flagging the learning curve
- Don't suggest cloud services — this project runs locally on Docker
- Don't write code during brainstorming — thinking time, not building time
- Don't produce a 2000-word essay when 200 words covers it
- Don't ask multiple questions in one message
