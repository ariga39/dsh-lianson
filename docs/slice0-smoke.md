# Slice 0 external-operator smoke

External-operator smoke for the pinned DeepSeek Harness Web profile. It is
keyless, uses a scripted Mock LLM provider, and exercises the real Host,
Gateway Remote packages, and Chromium surface.

## Supported clean entry

Linux x64 with:

- Node `24.22.3`
- pnpm `11.28.5` through Corepack
- Chromium runtime prerequisites already provided by the host (no OS
  provisioning is performed)

From a clean checkout:

```sh
corepack pnpm run smoke:slice0
```

The command:

1. Pins and verifies the toolchain, the DeepSeek Harness commit
   `4b399c39a6d1479cd84ca591b4d7c88e24d38a96`, and a clean upstream tree.
2. Stages a shallow fetch of that exact commit when `deepseek-harness/` is
   absent, without running user Git hooks, then keeps the existing checkout
   intact when present.
3. Installs frozen upstream and project dependencies, verifies every imported
   upstream JavaScript module, its declaration entry, the native addon, and the
   assets referenced by the built Web entry; it emits the upstream artifact
   graph and Web dist only when a requirement is missing, then installs the
   pinned Playwright Chromium browser.
4. Runs `tsc --noEmit`, type-aware Oxlint over `src`, `scripts`, and `tests`,
   and the Node tests.

## Checks

```sh
corepack pnpm run smoke:slice0   # bootstrap, typecheck, lint, and all tests
corepack pnpm run verify         # typecheck, lint, and all tests
corepack pnpm run typecheck      # tsc --noEmit
corepack pnpm run lint           # type-aware Oxlint
corepack pnpm run test           # node:test
```

`tests/slice0.test.ts` owns the end-to-end proof. `tests/harness.test.ts` keeps
focused regressions for launch-line concatenation across chunk and CRLF
boundaries, complete redacted stderr on early close, missing Web/declaration
artifact detection, and isolation from a failing user Git hook.

## What the test proves

- A real `dsh web --no-open` Host prints a complete authenticated launch line;
  full stderr is retained through close and stopped with a bounded kill.
- An external operator exchanges `/connect?interactions=answer`, mounts the
  public Node client, Gateway, and Remote packages, and drives one session.
- A real `read` tool call returns its file contents.
- Duplicate `requestId` submissions before and after Step 1 are accepted and
  persist exactly one user message.
- Chromium can open the externally created session while a question is pending,
  show the original prompt, tool output, and question card, then close without
  cancelling it.
- The external operator answers the question; a second Chromium visit observes
  `data-streaming` progress and closes without aborting the turn.
- The final history is contiguous, the turn completes, the selected model is
  the one recorded in `request/header`, and neither Playwright page errors nor
  `session/writer-held` appear.

Temporary `DSH_HOME`, workspaces, and child processes are removed at the end.
Launch tokens are redacted from failure diagnostics, which are limited to
status and event-type summaries.
