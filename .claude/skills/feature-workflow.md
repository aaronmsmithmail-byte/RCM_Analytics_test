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

Multi-phase technical review. Run all applicable reviews:

### Phase A: Plan Compliance
Verify the implementation matches the approved plan from Stage 2:
- All planned files were modified
- All acceptance criteria are met
- No unplanned scope creep

### Phase B: Pre-Commit Review
Run the `pre-commit-review` skill which covers:
- Simplification pass (remove unnecessary abstractions)
- Structural review (security, performance, types)
- Quality checks (efficiency, dead code, error handling, documentation)

### Phase C: Standards Validation
Check changes against `.claude/skills/standards.md`:
- Data modeling conventions (table/column naming, types)
- Python naming (functions, classes, constants)
- Import ordering and package patterns
- Client module pattern (if applicable)
- SQL conventions (parameterized queries, CTE pattern)
- Security (no hardcoded secrets, SQL injection prevention)

### Phase D: Specialized Reviews (as applicable)
Launch the relevant agents based on what changed:
- **Code changed**: `code-reviewer` agent
- **Error handling changed**: `silent-failure-hunter` agent
- **Tests added/modified**: `pr-test-analyzer` agent
- **Types added/modified**: `type-design-analyzer` agent
- **Comments added**: `comment-analyzer` agent

Present the review results to the user. If there are critical or important
issues, fix them before proceeding.

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
