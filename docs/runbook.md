# Operator runbook

Verified against the pinned `deepseek-harness/` checkout with Node 24. The live
`test:integration` run exercised the real Host boot, the
`create`/`prompt`/`wait`/`events`/`verify-model` flow, two-worktree isolation,
approval and question settlement through both the CLI and Web, steer, queue
edit, stop, rework across an abrupt restart, and persistent installation.
Commands outside that exercised surface are marked as such.

## Fast checks versus live integration

```sh
corepack pnpm run verify            # typecheck, lint, fast unit suite
corepack pnpm run test:integration  # real dsh web + Playwright, no model key
```

`test:integration` is intentionally separate from `verify`. It launches real
Host processes, git worktrees, and Chromium, so it is slower and should not run
in the inner loop. The scripted model speaks the shipped provider stream, so no
external model credential is used.

## Install the plugin persistently

Ensure the pinned upstream artifacts exist, then install this checkout as a
profile dependency with the real `dsh plugin` command:

```sh
corepack pnpm run bootstrap
dsh plugin --profile web add "link:$PWD" --offline
```

`dsh plugin` runs `pnpm` from `PATH`. On a machine where only corepack is
available, expose a shim such as:

```sh
mkdir -p ~/.local/bin
printf '#!/bin/sh\nexec corepack pnpm "$@"\n' > ~/.local/bin/pnpm
chmod +x ~/.local/bin/pnpm
export PATH="$HOME/.local/bin:$PATH"
```

The install prints `dsh-liaison declares no dsh.bundle — installed as a plain
dependency, not a profile layer`. That is expected for this package. Mount it
as a persistent home-level plugin entry in `$DSH_HOME/cordis.patch.yml`:

```yaml
- insert:
    - id: dsh-liaison
      name: dsh-liaison
```

Start the Web profile normally. The entry is loaded on every later start:

```sh
DSH_HOME=/path/to/home dsh web --no-open --host 127.0.0.1 --port 0
```

For a disposable development run, `dsh web --no-open --patch <overlay.yml>` with
an insert pointing at `src/plugin.ts` is also verified. Persistent installation
is the `dsh plugin` + home-patch form above.

## Check the plugin

```sh
dsh-liaison status
```

The socket is `$XDG_RUNTIME_DIR/dsh-liaison/dsh-liaison.sock` (or
`$DSH_LIAISON_RUNTIME_DIR/dsh-liaison.sock`). The runtime directory is mode
`0700`; the socket is `0600`. One plugin owns the socket at a time. After an
abrupt Host exit, the next Host removes the stale socket and binds normally.

The CLI joins the Host's asynchronous authentication handshake on the first
command, so a command issued immediately after Host readiness does not fail with
`dsh exposes no authenticated entry URL`. `wait` and `stop` include that
handshake inside their own command deadline, so a slow first connection does
not extend either budget.

## Delegate work

Use a real workspace directory, not Harness state and not a directory inside a
globally shared temp area. Sibling git worktrees are supported.

```sh
dsh-liaison create --delegation-id task-a --cwd /path/worktree-a \
  --title "Task A" --model provider/model
dsh-liaison prompt --delegation-id task-a --request-id review-1 \
  --text "Implement the change"
dsh-liaison wait --delegation-id task-a --timeout-ms 120000
dsh-liaison events --delegation-id task-a
dsh-liaison verify-model --delegation-id task-a --request-id review-1
```

`verify-model` without `--request-id` checks the latest turn and records that
verdict as the delegation's session-wide summary. With `--request-id`, it
checks that historical turn without changing the summary. `get` and `list` show
the last session-wide summary; it may be stale until another unscoped check,
including when they fall back to the offline ledger.

Repeat `create` with the same delegation id and parameters to reuse the stored
session. Repeat `prompt` with the same request id and text to reuse its original
cursor. Different content under the same key is reported as a conflict.

Create independent delegations for independent worktrees. `workspace-write`
keeps each Session inside its own workspace except for globally shared platform
temp areas and any deployment-configured additional writable roots; those roots
are writable by every Session. Writes outside the effective root set require an
approval. The integration suite verifies two sibling worktrees and an attempted
cross-worktree write in a non-shared directory.

## Observe and answer

Open the URL printed by `dsh web`. The session appears under its workspace and
title. Prior history is visible, and an open page receives subsequent live
streaming. Closing the page does not stop execution, and never opening it is
supported.

Approvals and questions are never auto-approved:

```sh
dsh-liaison interaction list --delegation-id task-a
dsh-liaison interaction approve --interaction-id interaction-...
dsh-liaison interaction reject --interaction-id interaction-...
dsh-liaison interaction answer --interaction-id interaction-... \
  --answers '[{"id":"q1","selected":["Yes"]}]'
```

`interaction list`, `approve`, and `answer` were exercised live. `reject` is
listed for operator completeness but was not part of the live suite.

When both the CLI and a Web page have the same prompt, settling it from either
side cancels the other side's pending prompt. The integration suite covers both
directions and the same flow with no browser ever opened.

## Guide, stop, and rework

```sh
dsh-liaison steer --delegation-id task-a --text "Focus on the failing test"
dsh-liaison queue --delegation-id task-a --item-id message-... --action edit \
  --text "Use the revised wording"
dsh-liaison stop --delegation-id task-a --timeout-ms 15000
dsh-liaison rework --delegation-id task-a --request-id review-2 \
  --text "Address the review finding"
```

`stop` clears the pending queue, cancels the active turn, and checks quiet state
inside one budget. `rework` restores the stored Session and prompts the original
`sessionId`; it is verified across an abrupt Host exit and stale-socket
recovery.

## Desktop

A Desktop carrier or display was not available in the verification
environment, so Desktop profile sharing is not claimed or documented here.
Test it only in an environment where the Desktop application is actually
observable.
