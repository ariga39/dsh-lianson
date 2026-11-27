# dsh-liaison

The goal of dsh-liaison is to let Codex delegate coding work to DeepSeek Harness (dsh), inspect the results, and send review feedback back to the same session. A user can open dsh Web to see what the agent is doing and close it while the work continues.

The design relies on dsh for session history, prompt deduplication, and workspace isolation. The liaison layer only needs to connect an assignment to its dsh session and expose the controls Codex uses.

[Project requirements](docs/requirements.md) · [Design](docs/design.md) · [Open issues](https://github.com/ariga39/dsh-lianson/issues) · [Contributor guide](AGENTS.md)

## Development

Node 24 and the pinned `deepseek-harness/` checkout (`4b399c39a6d1479cd84ca591b4d7c88e24d38a96`) are required.

```sh
corepack pnpm install
corepack pnpm run bootstrap   # asserts the pin and links the public @deepseek-ai packages
corepack pnpm run verify      # typecheck, lint, fast node:test unit suite
corepack pnpm run test:integration  # real dsh web + Playwright, kept out of the fast loop
```

See the [operator runbook](docs/runbook.md) for persistent installation, delegation, Web observation, and rework.

`bootstrap` builds only ignored upstream `lib/` artifacts when they are missing. Tracked files in `deepseek-harness/` are never changed.

## Plugin

Load `dsh-liaison` as an ordinary Cordis plugin in the dsh Web or Desktop profile. It injects `webServer`, serves a `0600` Unix-domain socket, acquires the authenticated entry URL through `webserver/entry-url`, and connects a loopback Gateway client with `interactions: 'answer'`. Tokens and cookies stay in memory.

The socket is `$XDG_RUNTIME_DIR/dsh-liaison/dsh-liaison.sock`, or `$DSH_LIAISON_RUNTIME_DIR/dsh-liaison.sock` when that override is set. The ledger is `$DSH_HOME/data/dsh-liaison/ledger.sqlite3`, or `$DSH_LIAISON_LEDGER` when set.

## CLI

Every command prints one redacted JSON document. `get` and `list` fall back to a read-only ledger read when the plugin is not running.

```sh
dsh-liaison status
dsh-liaison create --delegation-id task-1 --cwd "$PWD" --title "Task one" --model provider/model
dsh-liaison get --delegation-id task-1
dsh-liaison list
dsh-liaison prompt --delegation-id task-1 --request-id review-1 --text "Please add the missing test"
dsh-liaison prompt --delegation-id task-1 --file instruction.md
dsh-liaison verify-model --delegation-id task-1 --request-id review-1
dsh-liaison wait --delegation-id task-1 --timeout-ms 120000
dsh-liaison events --delegation-id task-1 --after-seq -1
dsh-liaison interaction list --delegation-id task-1
dsh-liaison interaction approve --interaction-id interaction-...
dsh-liaison interaction reject --interaction-id interaction-...
dsh-liaison interaction answer --interaction-id interaction-... \
  --answers '[{"id":"q1","selected":["yes"]}]'
dsh-liaison steer --delegation-id task-1 --text "Focus on the failing test"
dsh-liaison queue --delegation-id task-1 --item-id message-... --action remove
dsh-liaison stop --delegation-id task-1 --timeout-ms 30000
dsh-liaison rework --delegation-id task-1 --request-id review-2 --text "Address the review finding"
```

`--cwd` must be an ordinary isolated directory. Harness state and the shared temp areas (including ancestors of Harness state) are rejected; ordinary sibling worktrees are accepted.

`create` and `prompt` persist their business key before the remote call. Repeating `--delegation-id` reuses the stored session when parameters match, and repeating `--request-id` reuses the original pre-prompt cursor when content matches. A different value under the same key is reported as a conflict. `verify-model` reports `proven`, `mismatch`, or `indeterminate` from the persisted `request/header`, never from intent. An unscoped check records its verdict for the latest turn it observes as the session-wide summary; a `--request-id` check is historical and leaves that summary unchanged. `get` and `list` show the last session-wide verification, which may be stale until another unscoped check, including when they fall back to the offline ledger.

`wait` reads Host waits in bounded windows and reports `completed`, `input-required`, `running`, `queued`, `timeout`, or `indeterminate`. `events` returns the exact contiguous range and fails with `events/sequence-gap` instead of hiding a hole; its `subagentCatalog` comes from Session projections. Approvals are never auto-approved: `approval/request` and `user-questions/request` are buffered only for bound delegation sessions and settle through `interaction approve|reject|answer`. A timed question that outlived its foreground wait can be answered later by passing `--delegation-id` and `--call-id` to `interaction answer`. `steer` and `queue` use the Host's steering and pending-inbox mutations, `stop` clears queued input and checks quiet state inside one overall time budget, and `rework` unarchives the original session before submitting follow-up work to its stored `sessionId`.
