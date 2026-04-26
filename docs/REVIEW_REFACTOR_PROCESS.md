# Review & Refactor Process

This document is the playbook for recurring review/refactor cycles on this codebase. When the user asks for a review, refactor pass, or "let's clean things up", follow this process. The standards being applied (what counts as a bug, what counts as idiomatic) live in [PROJECT_SPEC.md](../PROJECT_SPEC.md) and `CLAUDE.md`. This document is about *how* to run the cycle, not *what* to enforce.

## When to use this

Triggers:
- "Review the code"
- "Refactor X"
- "Make sure everything is clean before moving on"
- "Check that we're following best practices"
- After landing a multi-feature batch (e.g. 2-3 chart types in a row)

If the user just asks "review this function", that's a one-shot — don't run the full cycle.

## The cycle

Five phases. Don't skip them, but compress when scope is small.

### 1. Frame the scope

Before reading code, agree with the user on:
- **Surface area**: which spec features / which packages / which commit range
- **Out-of-scope concerns**: usually performance early, sometimes security
- **Definition of done**: build/vet/tests pass, or stricter (new tests added, doc updated)

If unclear, ask one targeted `AskUserQuestion`. Do not start exploring before scope is settled — exploration without scope produces noise.

### 2. Delegate the audit

Spawn a single `general-purpose` agent with a prompt that:
- Names the source-of-truth doc (`PROJECT_SPEC.md`) and which sections apply
- Lists every directory the agent must read (don't say "review the codebase" — list the paths)
- Specifies report headings: **Bugs / Spec gaps / DRY / Idiomatic Go / SQL safety / Test coverage**
- Caps length (~800-1200 words) and demands `file:line` citations
- Tells the agent to skip "this is well done" sections — only report what needs attention
- Flags signs of an unfinished refactor as a separate observation

Why an agent: a thorough review touches 30+ files. Doing it in the main loop pollutes context and slows everything that follows. The agent's report comes back as one tool result; relay highlights to the user.

### 3. Triage with the user

Take the agent's report and:
- Sort findings by **impact** (correctness > security > spec gaps > DRY > style)
- Within each tier, sort by **blast radius** (one function → many files)
- Present the top N (typically 5-10) as a recommended order, not a backlog dump
- Ask the user which to tackle this session — they may want only the critical bugs, or the whole thing

Don't propose to fix everything in one go. A 30-item refactor across one session produces a diff nobody will review properly.

### 4. Plan + implement (per item or per cluster)

For each accepted item:
- **Trivial fix** (one file, < 20 lines): just do it, mention in summary
- **Non-trivial** (multiple files or design tradeoffs): use `EnterPlanMode`, write the plan, get approval, implement
- **Cross-cutting refactor** (e.g. "logging everywhere"): always plan-mode. These are the ones that go wrong silently.

After each item:
- Run `go build ./... && go vet ./... && go test ./...`
- Update the relevant doc if the change shifts a convention (e.g. "we now use slog everywhere" → CLAUDE.md)

### 5. Verify and close

End-of-session:
- Confirm build/vet/tests pass
- Brief summary: what was found, what was fixed, what was deferred
- If items were deferred, ask whether to track them somewhere (followup file? next session?)
- Do not auto-commit. The user commits.

## Anti-patterns to avoid

These are mistakes I've made before. Watch for them.

- **Reviewing without a frame.** "Look at everything" produces a 50-item list nobody acts on. Always scope first.
- **Skipping the audit agent for "small" reviews.** If it's small enough to skip the agent, it's small enough to skip the cycle. Just answer the question.
- **Fixing as I review.** Tempting, but it breaks the triage step. Read the report, present it, *then* fix.
- **Treating the report as a checklist to grind through.** It's a menu. Pick what matters this session.
- **Forgetting to verify the agent's claims.** The agent reads excerpts. Before quoting "broken at line 63", read line 63. Especially for any claim that says "this is dead code" or "this is unreachable" — verify before deleting.
- **Bundling unrelated fixes into one commit.** Each accepted item should be its own commit (or coherent cluster). Mixing them defeats the user's ability to review.
- **Reverting recent intentional changes.** If a system-reminder says a file was modified intentionally, the change stays unless explicitly asked to revert.
- **Adding a "nice to have" the user didn't ask for.** Refactor scope = what was triaged. New ideas go to a follow-up list.

## What to look for (delegated to spec)

The `what` is in `PROJECT_SPEC.md` (Code Standards + Design Principles sections) and `CLAUDE.md` (architecture + conventions). When the spec is silent, default to:
- The standard library over dependencies
- Errors over panics
- Parameterized SQL; identifiers via `internal/sqlsafe`
- `slog` for logging; never `fmt.Println` for diagnostics
- Doc comments on every exported symbol
- Table-driven tests for pure functions

If the spec contradicts the code, the spec wins — but flag it to the user; sometimes the spec is wrong.

## Test discipline

Tests are part of the cycle, not a separate phase. When refactoring:
- Pure functions touched → add a table-driven test before changing behavior
- Repository functions touched → at minimum smoke-test build + vet; integration tests need a real DB and are usually out of scope per session
- Don't write tests just to hit coverage. Write tests for behavior that would actually break in the next refactor.

## Commit hygiene

User commits, not me. But when staging changes for a commit, group them so the diff tells a story:
- One commit per finding, or one per cluster of related findings
- Commit message: what changed and why, not how
- If the cycle produces 10+ commits, pause and ask whether to squash some

## A note on this document

If this process feels heavy for the work in front of you, it probably is. Compress accordingly — but compress *steps*, not *care*. The frame, the triage, and the verify steps are non-negotiable; the audit agent and the per-item planning scale with scope.
