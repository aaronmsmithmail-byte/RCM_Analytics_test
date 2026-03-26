---
name: pre-commit-review
description: >
  Comprehensive pre-commit code review that chains simplify, structural review,
  standards validation, and quality checks into a single pass. This is the LOCAL
  review skill — run before committing. For GitHub PR reviews after pushing,
  use /code-review or /review-pr instead.
  Use when the user says "pre-commit", "review before commit", "final review",
  "check before I commit", "ready to commit", or after completing a feature.
---

# Pre-Commit Review

Run all four phases in order before reporting findings. Do not skip phases.
This is Stage 5 of the development workflow (see `.claude/skills/feature-workflow.md`).

## Phase 1: Simplify

Run the built-in /simplify skill on all changed files (use `git diff --name-only` to identify them).
Focus on:
- Removing unnecessary abstractions or helper functions that are only used once
- Simplifying overly complex logic
- Eliminating premature DRY — three similar lines are better than a confusing abstraction
- Removing defensive checks on values that are guaranteed by types or internal code

Apply the simplifications directly. Note what was changed for the final summary.

## Phase 2: Structural Review

Run the built-in /review skill on the staged/changed code.
Let it check for security, performance, type safety, and convention issues.
Capture its findings for the final summary.

## Phase 3: Standards Validation

Check all changed code against `.claude/skills/standards.md`. For each changed file, verify:

- **Data modeling** (if SQL/schema changed): Table naming follows medallion convention, column naming follows patterns (snake_case, `{entity}_id`, `is_{property}`, `{name}_amount`)
- **Python naming**: Functions use correct prefix (`query_*`, `get_*`, `is_*`, `build_*`, `_private`), classes are PascalCase, constants are SCREAMING_SNAKE_CASE
- **Imports**: Correct order (stdlib → third-party → local), no unused imports
- **SQL**: Parameterized `?` placeholders, `build_filter_cte()` pattern, DuckDB date functions
- **Client modules** (if new client): Health check with TTL cache, returns `None` when unavailable
- **Testing**: New public functions have tests, new `query_*` functions have 2+ tests
- **Security**: No hardcoded API keys/secrets, no SQL injection via f-strings
- **Error handling**: External calls wrapped in try/except, graceful fallback (empty result or `None`)

## Phase 4: Quality Checks

After Phases 1–3, review the current state of the code against these criteria:

### Efficiency
- Redundant computations, unnecessary loops, or operations that could be simplified
- N+1 query patterns, repeated database/API calls, missing caching opportunities
- Do NOT over-optimize — only flag things that meaningfully impact readability or performance

### Dead Code
- Unused imports, variables, functions, and components
- Commented-out code blocks that should be removed or restored
- Unreachable code paths (after returns, impossible conditions)
- TODO/FIXME comments on code that is already done

### Error Handling
- Try/catch blocks around external calls (APIs, file I/O, database)
- Errors are logged or surfaced meaningfully — no silent swallowing
- Client modules return `None` when service unavailable (per standards.md)
- Metadata queries return empty DataFrame on error (per standards.md)

### Code Comments & Documentation
- Functions have Google-style docstrings (Args/Returns)
- Complex logic has inline comments explaining the reasoning
- No misleading or outdated comments that no longer match the code
- Comment the "why", not the "what"

## Output

Present a single unified report:

### Summary
Total issues by severity across all phases. If the code is clean, say so.

### Phase 1 — Simplifications Applied
List what was changed and why (or "No simplifications needed").

### Phase 2 — Structural Review
Summarize findings from the built-in review (or "No issues found").

### Phase 3 — Standards Validation
List any standards violations found, referencing the specific standard from standards.md.

### Phase 4 — Quality Checks
Group findings by file. For each issue:

**[CATEGORY] File:Line — Brief description**
Severity: 🔴 Must fix | 🟡 Should fix | 🟢 Suggestion
What: One sentence.
Fix: Recommended change.

### Verdict
State one of:
- ✅ **Ready to commit** — no blocking issues
- ⚠️ **Commit with caution** — 🟡 issues noted but nothing blocking
- 🛑 **Fix before committing** — 🔴 issues found that must be resolved

Do not invent issues to justify a failing verdict. If the code is solid, say so.
