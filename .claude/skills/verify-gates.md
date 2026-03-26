---
name: verify-gates
description: >
  Run all automated verification gates before committing or reviewing code.
  Use when the user says "verify", "run gates", "check gates", "pre-deploy check",
  "are we ready", or after completing coding work. Also run this automatically
  as part of the feature-workflow Stage 4.
---

# Verification Gates

Run all gates below in order. Report results as a pass/fail checklist.
**All gates must pass before code review or commit.**

> **`make verify`** is the fast version (Gates 1 + 2 only — tests + lint).
> This skill runs the full 5-gate check including coverage, docs, and standards.

---

## Gate 1: Tests

```bash
pytest tests/ -q
```

**Pass criteria:** Zero failures. Note the test count for Gate 5.

If tests fail:
- Show the failing test names and error summaries
- Do NOT proceed — fix failures first

---

## Gate 2: Linting

```bash
ruff check src/ tests/ app.py generate_sample_data.py
```

**Pass criteria:** "All checks passed!" with zero violations.

If violations found:
- Run `ruff check --fix` for auto-fixable issues
- Manually fix remaining issues
- Re-run to confirm zero violations

---

## Gate 3: New Code Has Tests

Check if any new public functions were added without corresponding tests:

1. `git diff --name-only` to find modified source files
2. For each modified `src/*.py` file, check if new `def` statements were added
3. For new `query_*` functions, verify at least 2 tests exist (per CLAUDE.md)
4. For new public functions, verify at least 1 test exists

**Pass criteria:** All new public functions have test coverage.

---

## Gate 4: Documentation Current

Check if documentation needs updating:

1. **CLAUDE.md test count**: Compare the count in CLAUDE.md with actual `pytest tests/ -q | tail -1`
2. **README.md**: If `requirements.txt` changed, verify Dependencies table is updated
3. **.env.example**: If new `os.environ.get()` calls were added, verify they're in `.env.example`

**Pass criteria:** All documentation matches current state.

---

## Gate 5: Standards Compliance

Quick-check the diff against key standards from `.claude/skills/standards.md`:

1. **Naming**: New functions follow prefix conventions (query_*, get_*, is_*, build_*)
2. **SQL**: New queries use parameterized `?` placeholders (no f-strings with user input)
3. **Imports**: New imports are in correct order (ruff already checks this in Gate 2)
4. **Security**: No hardcoded API keys, passwords, or secrets in code

**Pass criteria:** No standards violations in changed code.

---

## Output

Present results as a checklist:

```
## Verification Gates

- [x] Gate 1: Tests — N passed, 0 failed
- [x] Gate 2: Linting — All checks passed
- [x] Gate 3: Test coverage — All new functions covered
- [x] Gate 4: Documentation — Counts match, deps current
- [x] Gate 5: Standards — No violations

**Result: ✅ All gates passed — ready for review**
```

Or if any gate fails:

```
## Verification Gates

- [x] Gate 1: Tests — N passed, 0 failed
- [ ] Gate 2: Linting — 3 violations found (see details)
- [x] Gate 3: Test coverage — All new functions covered
- [ ] Gate 4: Documentation — CLAUDE.md test count outdated (318 → 334)
- [x] Gate 5: Standards — No violations

**Result: 🛑 2 gates failed — fix before proceeding**
```
