# Agent Guide (`dsh-liaison`)

## Purpose & Architecture

`dsh-liaison` lets an external orchestrator (such as Codex) delegate coding work to a running DeepSeek Harness (`dsh`), inspect results, handle approvals/questions, and send rework back to the same session while the user can watch or close `dsh Web` at any time.

- Product requirements: [docs/requirements.md](docs/requirements.md)
- Architecture & delivery order: [docs/design.md](docs/design.md)
- Baseline: clean repository with pinned unmodified `deepseek-harness/` (`4b399c39a6d1479cd84ca591b4d7c88e24d38a96`).

## Core Rules (Preventing AI Slop & Over-Engineering)

1. **Build first, integrate after.**
   - Finish the working plugin + CLI before investing in heavy live-`dsh` or Playwright integration harnesses.
   - Keep default `verify` fast (seconds): `tsc --noEmit`, `oxlint`, and focused unit tests. Keep real `dsh web` and browser tests in a separate `test:integration` script so later PRs do not pay a compounding slow-test tax.
   - Do not build complex monkey-patched failure-injection fixtures (e.g., `SIGUSR2` mux-drop simulators) for low-probability local-loopback edge cases.
2. **Reuse upstream and standard libraries.**
   - Use `commander`, `zod`, Node 24 built-in `node:sqlite` (`DatabaseSync`) and `node:http` over `0600` UDS, `AbortSignal.timeout`/`any`, and linked public `@deepseek-ai/*` packages (see [docs/design.md](docs/design.md)).
   - Rely on `dsh` for session history, same-session `requestId` deduplication, `workspace-write` isolation, and multi-client interaction cancellation. Never build an external `dsh` process manager, `/proc` scanner, parallel retry-key alias table, or launch-token file on disk.
   - Keep `deepseek-harness/` unmodified.
3. **Report evidence truthfully.**
   - Distinguish `proven`, `mismatch`, and `indeterminate` when upstream evidence cannot settle a model check or background child wait.
   - Do not claim persistent plugin installation or `dsh Desktop` sharing as verified until exercised on a real run.

## Workflow & Review Discipline

- **Plan before coding**: For each issue, after its predecessor merges into `main`, AGY proposes a concise code-level plan against current `main` (exact upstream APIs, file layout, and test scope) and aligns with the lead before dispatching implementation to Flash.
- **One PR at a time**: Execute sequentially in issue order; keep PRs focused on the current issue.
- **Pragmatic review**: Block merge only on substantive correctness, security, data-integrity, or contract failures. Do not stall PRs on stylistic nits or speculative edge-case hardening; split worthwhile follow-ups into separate issues.
- **Scope ownership**: The user owns substantive goal and scope changes. Record agreed scope adjustments explicitly in an issue comment rather than silently rewriting issues.
