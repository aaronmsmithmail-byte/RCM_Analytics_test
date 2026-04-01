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
5. **Update documentation and metadata** as you go:
   - CLAUDE.md test counts and module descriptions
   - README: tab count, metadata page count, test count, dependencies, setup
   - .env.example if new env vars added
   - Metadata pages in `src/metadata_pages.py` if the change affects data lineage, knowledge graph, semantic layer, or business process flow
   - `_TABLE_CATALOG` and `_KG_NODES` in `metadata_pages.py` if new tables/entities added

---

## Stage 4: VERIFY

Run all verification gates. All must pass before proceeding to review.

> **Quick check:** `make verify` runs Gates 1 + 2 (tests + lint).
> **Full CI check:** `make ci` runs Gates 1 + 2 + security scanning.
> Gates 1, 2, and part of 5 also run automatically via GitHub Actions on every push/PR.
> The full 5-gate check below adds coverage, docs, and standards.

### Gate 1: Tests

```bash
pytest tests/ -q
```

Pass criteria: Zero failures. Note the test count for Gate 4.

### Gate 2: Linting

```bash
ruff check src/ tests/ app.py generate_sample_data.py
```

Pass criteria: Zero violations. Run `ruff check --fix` for auto-fixable issues.

### Gate 3: New Code Has Tests

1. `git diff --name-only` to find modified source files
2. For each modified `src/*.py`, check if new `def` statements were added
3. New `query_*` functions need at least 2 tests (per CLAUDE.md)
4. New public functions need at least 1 test

### Gate 4: Documentation & Metadata Current

1. CLAUDE.md test count matches actual `pytest tests/ -q | tail -1`
2. README test count, tab count, and metadata page count are all current
3. If `requirements.txt` changed, README Dependencies table is updated
4. If new `os.environ.get()` calls added, they're in `.env.example`
5. If new tables/entities added: `_TABLE_CATALOG`, `_KG_NODES`, and lineage diagram in `metadata_pages.py` are updated
6. If new KPIs/tabs added: Business Process page reference table and Data Catalog are consistent

### Gate 5: Standards Compliance

Quick-check the diff against key standards from `.claude/skills/standards.md`:

1. New functions follow prefix conventions (`query_*`, `get_*`, `is_*`, `build_*`)
2. New queries use parameterized `?` placeholders (no f-strings with user input)
3. No hardcoded API keys, passwords, or secrets in code

If any gate fails, fix the issue and re-run. Do NOT proceed with failing gates.

---

## Stage 5: REVIEW

Two-step review: local pre-commit review, then (optionally) PR review after push.

### Step A: Plan Compliance

Before running automated review, verify the implementation matches the plan:
- All planned files were modified
- All acceptance criteria from Stage 2 are met
- No unplanned scope creep

### Step B: Pre-Commit Review (local)

Run all four phases on changed files (use `git diff --name-only` to identify them):

**Phase 1 — Simplify:**
- Remove unnecessary abstractions or helpers used only once
- Simplify overly complex logic
- Eliminate premature DRY — three similar lines beat a confusing abstraction
- Remove defensive checks guaranteed by types or internal code

Apply simplifications directly. Note what changed.

**Phase 2 — Structural Review:**
Check for security, performance, type safety, and convention issues.

**Phase 3 — Standards Validation:**
Check changed code against `.claude/skills/standards.md`:
- Data modeling: Table/column naming follows conventions
- Python naming: Correct function prefixes, PascalCase classes, SCREAMING_SNAKE constants
- Imports: Correct order (stdlib → third-party → local), no unused imports
- SQL: Parameterized `?` placeholders, `build_filter_cte()` pattern
- Client modules: Health check with TTL cache, returns `None` when unavailable
- Testing: New public functions have tests, new `query_*` functions have 2+
- Security: No hardcoded API keys, no SQL injection via f-strings
- Error handling: External calls in try/except, graceful fallback

**Phase 4 — Quality Checks:**
- Efficiency: Redundant computations, N+1 queries, missing caching
- Dead code: Unused imports/variables/functions, commented-out code
- Error handling: try/catch around external calls, no silent swallowing
- Comments: Google-style docstrings, "why" not "what", no outdated comments

**Verdict:** One of:
- **Ready to commit** — no blocking issues
- **Commit with caution** — minor issues noted but nothing blocking
- **Fix before committing** — must-fix issues found

### Step C: PR Review (after push, if creating a PR)

Use `/review-pr` for comprehensive multi-agent PR review (tests, errors, comments, code quality).

Fix any critical issues before merging.

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
| 4. Verify | `make verify` + 5-gate check | All pass |
| 5. Review | Pre-commit review (4 phases) | No critical issues |
| 6. Deploy | Commit + push + PR | — |
