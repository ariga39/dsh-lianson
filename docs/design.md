# Design (`dsh-liaison`)

## Architecture

`dsh-liaison` is a thin adapter between an external orchestrator (such as Codex) and a running DeepSeek Harness (`dsh`) server. It modifies zero lines in pinned `deepseek-harness/` (`4b399c39a6d1479cd84ca591b4d7c88e24d38a96`).

```text
Codex / User
  └─ stateless CLI (commander + node:http over 0600 UDS, offline read-only SQLite)
        │  Unix domain socket ($XDG_RUNTIME_DIR/dsh-liaison/dsh-liaison.sock)
        ▼
In-process Cordis plugin inside dsh (dsh web / dsh Desktop)
  ├─ HTTP-over-UDS server (node:http + zod, single-owner non-blocking flock)
  ├─ Delegation ledger (node:sqlite WAL, short synchronous transactions)
  └─ Loopback Gateway client (@deepseek-ai/dsh-api-gateway/client, interactions: 'answer')
       └─ Talks to dsh via public /remote RPCs & $events (coexists with dsh Web)
```

### Why this shape
- **No external daemon or `dsh` process manager**: `dsh` is already a long-lived Cordis server. Running inside it removes process spawning, `/proc` scanning, PID signaling, and launch-token files on disk.
- **Loopback Gateway client inside the plugin**: Keeps zero coupling to private `dsh` internals while reusing typed `/remote` RPCs and Gateway's multi-client `approval/request` / `user-questions/request` arbitration (so `dsh Web` and Codex cancel each other's prompts cleanly).
- **`0600` Unix domain socket + stateless CLI**: OS UID access control without extra tokens or TCP ports, plus clean JSON output and offline read-only lookup when `dsh` is stopped.

### What `dsh` owns (never reimplement)
- Session history, streaming, and `dsh Web` viewing.
- Same-session prompt deduplication (`requestId` / `hasPromptRequest`).
- Workspace and sandbox isolation (`workspace-write`).
- Multi-client interaction arbitration (`waterfall` forwarding and mutual cancellation).

## Key Upstream Integration Points

- **Plugin entry URL**: Query `webServer` actively on readiness via `ctx.emit(webServer, 'webserver/entry-url', baseUrl, entries)` (matching upstream `remote-connection`), exchange `/connect` in memory, and allow lazy retry if initial connect fails. Never persist launch tokens or cookies.
- **Exact `DSH_HOME`**: Use `resolveDshHome()` from `@deepseek-ai/dsh-home-paths` (not `$host.home`, which returns `os.homedir()`).
- **Reused packages**: `commander`, `zod`, `ws`, Node 24 built-in `node:sqlite` (`DatabaseSync`) and `node:http`, plus `@deepseek-ai/dsh-api-gateway/client`, `@deepseek-ai/dsh-client-connection/client`, `@deepseek-ai/dsh-typert-registry`, `/remote` namespaces, `dsh-session-title` (`normalizeSessionTitle`), `dsh-sandbox` (`writableRoots`), and `node-addon-system/flock`.
- **Unbound sessions**: Register `ctx.typert.contexts.registerClient('agent', ...)` only for bound delegations; return `undefined` for all other sessions so Gateway yields `{ kind: 'next' }` without interfering with unrelated user sessions.

## Data Model & Command Surface

Two SQLite tables (`WAL`, short `busy_timeout`, no RPC/I/O inside transactions):
- **`delegations`**: `delegation_id` (unique), `session_id` (unique), `cwd`, `title`, `model`, `model_verification` (`proven` | `mismatch` | `indeterminate`), `status`, timestamps.
- **`prompt_receipts`**: `(delegation_id, request_id)` (unique), `before_cursor`, `prompt_hash`, timestamp.

CLI commands (all emit single redacted JSON documents):
- `status`: Plugin health, `dshHome`, origin, loopback state.
- `create` / `get` / `list` / `prompt` / `verify-model`: Create and drive delegations using one business key per command (`--delegation-id` for `create`, `--request-id` for `prompt`). Persist the minted `session_id` before remote `create` and persist `(before_cursor, prompt_hash)` before `session.prompt` so retries reuse the same `session_id` and preserve original cursor/conflict checks (upstream duplicate `requestId` returns only `{ accepted: true }`). `get`/`list` fall back to read-only SQLite when `dsh` is offline.
- `wait` / `events`: Segmented waiting across 30s windows, contiguous event reads, and `subagentCatalog` inspection (reporting `indeterminate` truthfully when upstream cannot attribute a child turn).
- `interaction list|approve|reject|answer`: Handle pending approvals and user questions on bound sessions without auto-approving unknown requests.
- `steer` / `queue` / `stop` / `rework`: Mid-turn guidance, queue edits, time-bounded stop (clear queue, cancel, and check quiet state within an overall time budget, reporting known or unknown state on expiry), and original-session rework (`unarchiveSession` + prompt on the original `sessionId`).

## Delivery Strategy: Build First, Integrate After

1. **Build the complete tool first with fast feedback**: Implement the plugin, SQLite ledger, and full CLI across two focused functional PRs. Keep the inner loop fast (`tsc --noEmit`, `oxlint`, and unit tests using lightweight fakes/mocks). Do **not** build heavy live-`dsh` harnesses, Playwright suites, or synthetic failure-injection fixtures (such as monkey-patched servers or `SIGUSR2` drop simulators) before the tool actually works.
2. **Run real integration & acceptance once the tool is built**: After all commands exist, add a dedicated `test:integration` suite against real `dsh web` (reusing a single Host where possible) and Playwright `dsh Web`, verify persistent plugin installation (`dsh plugin` / `$DSH_HOME/cordis.patch.yml`) and `dsh Desktop` behavior truthfully, and finish with the live-model / 138-route acceptance scenario from [requirements.md](requirements.md). Keep slow integration/browser suites separate from fast unit checks.
