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
```

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
```

`--cwd` must be an ordinary isolated directory. Harness state and the shared temp areas (including ancestors of Harness state) are rejected; ordinary sibling worktrees are accepted.

`create` and `prompt` persist their business key before the remote call. Repeating `--delegation-id` reuses the stored session when parameters match, and repeating `--request-id` reuses the original pre-prompt cursor when content matches. A different value under the same key is reported as a conflict. `verify-model` reports `proven`, `mismatch`, or `indeterminate` from the persisted `request/header`, never from intent.
