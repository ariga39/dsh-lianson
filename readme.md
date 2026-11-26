# dsh-liaison

`dsh-liaison` connects an external orchestrator (such as Codex) to a running [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) (`dsh`) server so coding delegations can be created, driven, observed, adjudicated, and reworked programmatically while a person can open `dsh Web` at any time to view the exact same live session.

## Architecture and Anti-Reinvention Principle

`dsh-liaison` intentionally avoids AI-generated infrastructure reinventions by staying thin on top of upstream `dsh` and standard libraries:

- **In-process `dsh` Cordis plugin** (zero upstream edits) exposing a user-only `0600` Unix domain socket (`node:http` + `zod`), persisting liaison-owned delegation bindings in Node 24's built-in `node:sqlite`, and connecting via upstream's public `@deepseek-ai/dsh-api-gateway/client` and typed `/remote` packages.
- **Stateless `commander` CLI** talking over the local Unix socket (with read-only SQLite fallback for offline delegation lookup).
- **Direct upstream reuse**: relies on Host-native same-session `requestId` deduplication and upstream's `workspace-write` sandbox isolation rather than building a second daemon, external Host supervisor, custom sandbox, or parallel retry-key layer.

## Current Baseline vs. Planned Capability

- **Available now on `main` (`4f78093`)**:
  - Clean-checkout bootstrap (`scripts/bootstrap.ts`) pinning unmodified upstream `deepseek-harness` at `4b399c39a6d1479cd84ca591b4d7c88e24d38a96`.
  - **Slice 0 keyless E2E smoke** verifying external session creation, duplicate `requestId` handling, mid-turn Playwright `dsh Web` viewing during a pending question and active stream, safe browser close, and contiguous history ([docs/slice0-smoke.md](docs/slice0-smoke.md)).
- **Planned (not yet shipped; delivered strictly sequentially)**:
  1. In-process plugin with single-owner UDS HTTP endpoint and stateless `commander` CLI (`status`, `ping`).
  2. SQLite delegation bindings (`create`, `get`, `list`, `prompt`, `verify-model`) with worktree isolation and single-key idempotent prompts.
  3. Observation (`status`, segmented `wait`, contiguous `events`, `subagentCatalog`).
  4. Bound-session approvals and user questions (`interaction list|approve|reject|answer`) coexisting with `dsh Web`.
  5. Mid-turn `steer`, `queue` edits, bounded `stop`, and original-session `rework`.
  6. CLI JSON contract tests, verified persistent plugin installation, and operator runbook.
  7. Keyless two-delegation routing rehearsal with live 138-observation verification.
  8. Real-world acceptance with live model, real review, unattended and Web-observed proof, and live 138-observation SHA binding.

> Persistent installation (`dsh plugin` / `$DSH_HOME/cordis.patch.yml`) and `dsh Desktop` sharing are source-confirmed upstream mechanisms still pending runtime verification; they are not yet verified on this baseline.

## Prerequisites and Commands

Supported test host: Linux x64 with Node `24.22.3`, pnpm `11.28.5` via Corepack, and Chromium runtime prerequisites provided by the host.

```sh
corepack pnpm run smoke:slice0   # bootstrap pinned upstream, then run full verify
corepack pnpm run verify         # typecheck + lint + test
corepack pnpm run typecheck      # tsc --noEmit
corepack pnpm run lint           # type-aware Oxlint
corepack pnpm run test           # node --test tests/*.test.ts
```

## Issues and Documentation

- **Active roadmap**: See [open GitHub issues](https://github.com/ariga39/dsh-lianson/issues?q=is%3Aissue+is%3Aopen) (worked strictly one PR at a time).
- **Contributor / agent workflow**: See the [agent guide](agent.md).
- **Requirements and current smoke**: [docs/requirements.md](docs/requirements.md) and [docs/slice0-smoke.md](docs/slice0-smoke.md).
- **Historical design docs**: [docs/design-brief.md](docs/design-brief.md) and [docs/development-plan.md](docs/development-plan.md) reflect an earlier external-daemon design prior to the in-process plugin pivot; use their upstream references as background, and treat the open issues and [agent guide](agent.md) as authoritative where they differ.
