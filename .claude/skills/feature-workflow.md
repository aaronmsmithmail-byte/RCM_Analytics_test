---
name: feature-workflow
description: >
  Structured 6-stage development workflow for new features and changes.
  Use when the user says "new feature", "start work on", "implement",
  "add feature", "build", or any request to create/modify functionality.
  Also trigger when starting any non-trivial coding task.
---

# Feature Development Workflow

Follow these 6 stages in order. Do not skip stages. Pause at approval and
review gates for user input.

---

## Stage 1: PLAN

Before writing any code:

1. **Understand the request** — ask clarifying questions if scope is ambiguous
2. **Identify affected files** — use CLAUDE.md's architecture overview and recipes
3. **Write a plan** covering:
   - What changes (functional description)
   - Which files to create/modify (with line references if applicable)
   - Acceptance criteria (how to know it's done)
   - Test plan (what tests to add)
   - Documentation impact (README, CLAUDE.md, .env.example)
4. **Check standards** — reference `.claude/skills/standards.md` for:
   - Naming conventions for new functions/tables/columns
   - Client module patterns if adding a new service
   - Testing requirements for the type of change
5. **Track with TodoWrite** — create a task list from the plan

---

## Stage 2: APPROVE

**STOP and wait for user approval before coding.**

Present the plan to the user. Use `ExitPlanMode` if in plan mode, or present
the plan as a summary and ask explicitly:

> "Here's the plan for [feature]. Should I proceed?"

Do NOT start coding until the user approves. If the user requests changes,
update the plan and re-present.

---

## Stage 3: CODE

Implement against the approved plan:

1. **Follow CLAUDE.md recipes** for the type of change (new KPI, new tab, new entity, etc.)
2. **Follow standards** from `.claude/skills/standards.md` — naming, types, patterns
3. **Update TodoWrite** — mark each task as in_progress → completed
4. **Write tests** alongside the code (not as an afterthought)
5. **Update documentation** as you go:
   - CLAUDE.md test counts
   - README if dependencies/setup/architecture changed
   - .env.example if new env vars added

---

## Stage 4: VERIFY

Run all automated gates. All must pass before proceeding to review.

Invoke the `verify-gates` skill, or run manually:

```bash
make verify    # or run individually:
pytest tests/ -q                                          # all tests pass
ruff check src/ tests/ app.py generate_sample_data.py     # zero lint violations
```

Also verify:
- [ ] New functions have tests (2+ for query_* functions)
- [ ] CLAUDE.md test count matches `pytest tests/ -q | tail -1`
- [ ] README updated if needed (deps, setup, architecture)
- [ ] .env.example updated if new env vars added

If any gate fails, fix the issue and re-run. Do NOT proceed to review with
failing gates.

---

## Stage 5: REVIEW

Two-step review: local pre-commit review, then (optionally) PR review after push.

### Step A: Plan Compliance (manual check)
Before running automated review, verify the implementation matches the plan:
- All planned files were modified
- All acceptance criteria from Stage 2 are met
- No unplanned scope creep

### Step B: Pre-Commit Review (local)
Run the `/pre-commit-review` skill. It performs 4 phases in one pass:
1. **Simplify** — remove unnecessary abstractions
2. **Structural review** — security, performance, types
3. **Standards validation** — check against `standards.md` (naming, SQL, imports, security)
4. **Quality checks** — efficiency, dead code, error handling, documentation

> `/pre-commit-review` is the **only** review needed before committing.
> It includes standards validation — do NOT run a separate standards check.

### Step C: PR Review (after push, if creating a PR)
After committing and pushing, use one of these commands for GitHub PR review:
- `/code-review` — focused bug-finding with parallel agents (best for final validation)
- `/review-pr` — comprehensive multi-agent review (tests, errors, types, comments)

> These commands are for **PR review on GitHub**, not for local pre-commit checks.

Fix any 🔴 (must fix) issues before merging. 🟡 (should fix) issues are at your discretion.

---

## Stage 6: DEPLOY

After review passes with no blocking issues:

1. **Commit** with a descriptive message following this format:
   ```
   Short summary of what changed (imperative mood)

   - Bullet points with details
   - Reference the feature/issue if applicable

   Test count: N → M
   ```

2. **Push** to the feature branch

3. **Create PR** (if user requests) with:
   - Summary section (what and why)
   - Test plan section (how to verify)

---

## Quick Reference

| Stage | Action | Gate |
|-------|--------|------|
| 1. Plan | Write plan, identify files | — |
| 2. Approve | **User approves plan** | User says "proceed" |
| 3. Code | Implement + tests + docs | — |
| 4. Verify | `make verify` (tests + lint) | All pass |
| 5. Review | Pre-commit + standards + agents | No critical issues |
| 6. Deploy | Commit + push + PR | — |
