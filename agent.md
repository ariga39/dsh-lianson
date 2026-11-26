# Agent Guide (`dsh-liaison`)

## Project Purpose

`dsh-liaison` connects an external orchestrator (such as Codex) to a running DeepSeek Harness (`dsh`) server so coding delegations can be created, driven, observed, adjudicated, and reworked programmatically while a user can inspect the same live sessions in `dsh Web` without disrupting execution or spending orchestrator tokens summarizing work.

Product requirements live in [docs/requirements.md](docs/requirements.md). The current `main` baseline ships the upstream bootstrap, Slice 0 keyless smoke ([docs/slice0-smoke.md](docs/slice0-smoke.md)), and the in-process plugin with `status`/`ping`; delegation, observation, and interaction commands below are planned sequential work. Earlier documents (`docs/development-plan.md`, `docs/design-brief.md`) capture a historical external-daemon design; follow open issues and this guide where they differ.

## Architecture (In-Process `dsh` Plugin + Stateless CLI)

- **In-process Cordis plugin**: Runs inside `dsh` (zero upstream modifications), serves a user-only `0600` Unix domain socket (`node:http` + `zod`) protected by a lifetime non-blocking kernel `flock`, persists delegation bindings and prompt receipts in `node:sqlite`, and connects loopback in-memory via public `@deepseek-ai/dsh-api-gateway/client` + typed `/remote` packages (`interactions: 'answer'`) so approvals and questions flow through Gateway `$events` alongside any open browser.
- **Stateless CLI**: `commander` CLI sending HTTP requests over the Unix socket and emitting redacted JSON (with read-only SQLite fallback for offline delegation lookup).
- **No external `dsh` process manager**: Production code does not spawn, daemonize, `/proc`-scan, or signal external `dsh` processes, and never writes Host launch tokens to disk. Isolated automated tests spawn their own `dsh web --no-open --patch ...` child and reap its `ChildProcess` handle directly.
- **Pending runtime verification**: Persistent installation (`dsh plugin` / `$DSH_HOME/cordis.patch.yml`) and `dsh Desktop` sharing are source-confirmed mechanisms, not yet runtime-verified. Do not claim them as verified until exercised on a real run.

## Preventing AI Slop

1. **Reuse upstream and standard libraries instead of reinventing infrastructure.**
   - Use `commander` for CLI parsing, `zod` for runtime schema validation (CLI, HTTP/UDS, SQLite), Node 24 built-in `node:sqlite` (`DatabaseSync`, WAL, short synchronous transactions, with RPC and external I/O outside transactions, short `busy_timeout`), `node:http` over UDS + `AbortSignal.timeout`/`any`, and linked public `@deepseek-ai/*` packages (`dsh-api-gateway/client`, `dsh-client-connection/client`, `dsh-typert-registry`, `/remote` namespaces, `dsh-home-paths`, `dsh-session-title`, `dsh-sandbox`, `dsh-host-webserver`, `dsh-cmdline`, `node-addon-system/flock`).
   - Rely on Host-native same-session `requestId` deduplication (`hasPromptRequest`) and upstream `workspace-write` sandbox isolation. Use one business identity per command (`--delegation-id` for `create`, `--request-id` for `prompt`); do not add parallel retry-key alias tables or scan event history to reimplement prompt deduplication.
   - Keep `deepseek-harness/` unmodified at `4b399c39a6d1479cd84ca591b4d7c88e24d38a96`.
2. **Build only for the current merged dependency.**
   - Implement strictly what the current issue requires on top of merged `main`. Do not prebuild tables, routes, or abstractions for future issues.
3. **Report evidence truthfully.**
   - Distinguish `proven`, `mismatch`, and `indeterminate` when upstream evidence cannot settle a model check or background child wait.

## Workflow, Roles, and TDD

- **Per-task planning gate**: For **every** task, only after the predecessor PR is merged into `main`, AGY proposes an implementation plan against the actual code on `main`. The lead discusses the plan with AGY until both explicitly agree **before** dispatching implementation to Flash.
- **One PR at a time**: Execute strictly sequentially—never start the next issue until its predecessor merges.
- **Behavior-first red→green TDD**: Write a failing test covering the observable behavior first, implement the minimal solution reusing existing libraries and upstream APIs, then re-verify with `corepack pnpm run verify` (`tsc --noEmit`, type-aware `oxlint`, `node:test`).
- **Five-merge checkpoint**: Pause after every 5 merged PRs and consult AGY to check accumulated complexity, test speed, and fit with the remaining roadmap.
- **Ownership of scope**: The user owns substantive goal and scope changes. Never silently edit an issue body or expand scope mid-PR; record agreed boundary updates in an explicit issue comment first.

## Review Discipline

- Judge every review finding against actual user impact and source evidence.
- Substantive correctness, security, data-integrity, or contract failures block merge and must be fixed.
- When review iterations pile up, split actionable non-blocking nits into separate follow-up issues rather than bloating the current PR.
- Judge simplicity by eliminated mechanisms and reused libraries, not arbitrary line-count caps.
