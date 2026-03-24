---
name: pre-commit-review
description: >
  Comprehensive pre-commit code review that chains simplify, review, and custom
  quality checks into a single pass. Use when the user says "pre-commit",
  "review before commit", "final review", "check before I commit", "ready to commit",
  or after completing a feature. Also trigger when the user says "review my code"
  or "clean up my code" in the context of finishing work.
---

# Pre-Commit Review

Run all three phases in order before reporting findings. Do not skip phases.

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

## Phase 3: Custom Quality Checks

After Phases 1 and 2, review the current state of the code against these four criteria:

### Efficiency
- Redundant computations, unnecessary loops, or operations that could be simplified
- N+1 query patterns, repeated database/API calls, missing caching opportunities
- Unnecessary re-renders in React components (missing memoization, unstable references)
- Do NOT over-optimize — only flag things that meaningfully impact readability or performance

### Dead Code
- Unused imports, variables, functions, and components
- Commented-out code blocks that should be removed or restored
- Unreachable code paths (after returns, impossible conditions)
- Unused dependencies in package.json / requirements.txt
- TODO/FIXME comments on code that is already done

### Error Handling
- Try/catch blocks around external calls (APIs, file I/O, database)
- Errors are logged or surfaced meaningfully — no silent swallowing
- No empty catch blocks or generic catch-all handlers that hide real issues
- Async/await functions have proper error handling (no unhandled promise rejections)
- User-facing errors provide helpful messages, not stack traces

### Code Comments & Documentation
- Functions have a brief description of what they do and why
- Complex logic has inline comments explaining the reasoning
- No misleading or outdated comments that no longer match the code
- Exported functions/components have parameter documentation
- Do NOT demand comments on self-explanatory one-liners — comment the "why", not the "what"

## Output

Present a single unified report:

### Summary
Total issues by severity across all phases. If the code is clean, say so.

### Phase 1 — Simplifications Applied
List what was changed and why (or "No simplifications needed").

### Phase 2 — Structural Review
Summarize findings from the built-in review (or "No issues found").

### Phase 3 — Quality Checks
Group findings by file. For each issue:

**[CATEGORY] File:Line — Brief description**
Severity: 🔴 Must fix | 🟡 Should fix | 🟢 Suggestion
What: One sentence.
Fix: Recommended change.

### Verdict
State one of:
- ✅ **Ready to commit** — no blocking issues
- ⚠️ **Commit with caution** — minor issues noted but nothing blocking
- 🛑 **Fix before committing** — blocking issues found

Do not invent issues to justify a failing verdict. If the code is solid, say so.
