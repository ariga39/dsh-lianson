# Issue 33 live acceptance

Verified: 2026-11-27. Result: **pass**. The workflow in
[requirements.md](requirements.md) completed on a live `opencode-go/deepseek-v4.1-flash`
model through the real `dsh-liaison` CLI, against real `dsh web` Hosts, for both
Web-observed and unattended variants. No product fix was required.

## Environment

- `dsh-liaison` at `66d4421` on `feat/live-acceptance`; Node `24.22.3`.
- Pinned upstream `deepseek-harness/` Host, `--profile web --no-open --patch`
  with a disposable overlay mounting `src/plugin.ts` and an isolated
  `DSH_HOME`/runtime directory per variant.
- Model route declared through the pi-ai OpenAI-compatible adapter with
  `compat.supportsDeveloperRole: false` and `maxTokensField: max_tokens`.
  Credentials were read from the operator's authorized store into the Host
  environment only; no credential, cookie, or launch URL appears in logs or in
  this report.
- Target repository: `compte-rendu` baseline `352adace695e7ec1b184ca60c1ca4334e3865660`.
  Each variant used a separate local repository whose only history was a
  single-commit fetch of that baseline, so later routing history was not
  available to either delegation. Dependencies were installed offline from the
  existing pnpm store. No push, fetch of subsequent history, deployment, or
  edit to the original `compte-rendu` checkouts occurred.

## Delegations and accepted revisions

| Variant | Delegation | Session | Delivered revision | Final accepted revision |
| --- | --- | --- | --- | --- |
| Web | A (`web-a-401`) | `session-7cb9ebe9-eee8-4edb-b9ee-74822b07286a` | `9b51d44c950aad926935dc217a559cc39ba4e787` | |
| Web | B (`web-b-401`) | `session-7f1f8ab6-ff4c-4548-9f5a-28e9bd21bdf7` | `de40deff0e90c92e07e19b87ec0cc4a2ae9744db` | `de40deff0e90c92e07e19b87ec0cc4a2ae9744db` |
| Unattended | A (`unatt-a-401`) | `session-900fb1e7-42a2-4ce3-80c9-4aa2af91add6` | `ff3be43f70e66a258d123962062a3bb172f0120d` | |
| Unattended | B (`unatt-b-401`) | `session-c6a99105-787b-405d-83e3-ed068074f79b` | `54a733677e560367aae9a4433f4a8532e1108bc6` | `54a733677e560367aae9a4433f4a8532e1108bc6` |

Each B branch started from its variant's first A revision, committed the test
migration, then rebased onto the A revision that independent review approved.
The final SHAs therefore contain both the accepted production change and the
accepted test migration. A rework stayed in the original session.

## Model evidence

`dsh-liaison verify-model` reported `proven` for all four `task-1` prompts and
for both A `review-2` rework prompts. Expected and observed route were both
`opencode-go/deepseek-v4.1-flash`, sourced from the persisted request header
and an assistant settlement in the same turn. A header alone was not treated as
execution evidence.

## Web observation

The browser evidence was collected by the independent reviewer, not by this
operator, and is retained outside the repository as
`/tmp/dsh33-lead-web-evidence.json`. On the exact B session
`session-7f1f8ab6-ff4c-4548-9f5a-28e9bd21bdf7` the page showed the assigned
scope, the inspected callers `tests/review-configuration.test.ts` and
`tests/dashboard.test.ts`, a visible `932 passed` result, and a live change.
The page closed at `2026-11-27T13:27:33.748Z`.

CLI evidence taken after the close shows execution continuing: 26 further
events, including 4 tool calls and steps 100–101, with the first post-close
assistant event at `2026-11-27T13:27:36.543Z`. The session remained addressable
until its final accepted revision.

## Unattended execution

The unattended variant completed A, review rework, B, rebase, and B review
without any browser ever opening the Host. Approvals and decisions were settled
through the CLI only.

## Route verification

The historical comparison is 23 paths × 3 methods (GET, POST, DELETE) × 2
authentication states = 138 observations, each recording status, body,
`Cache-Control`, and `Content-Type`.

Observations were captured live from the baseline checkout and again by
building `apps/dashboard/src/lib/server/application.ts` from each final
checkout at its accepted SHA:

- baseline `352adac…`: 138 observations
- Web final `de40def…`: 138/138 identical
- unattended final `54a7336…`: 138/138 identical

All three observation files are byte-identical (SHA-256
`87d87b8d7429e33cdd652fafc01315a507cd22ec0c1e7757e8056b34e5b6a328`); there
are zero differences in status, body, `Cache-Control`, or `Content-Type`.
The observations were regenerated from the identified revisions; no saved
earlier output was substituted.

## Independent review and rework

The independent reviewer recorded decisions outside the repository in
`/tmp/dsh-33-review-decisions.json` and requested changes on both first A
revisions:

- Web A `2916cae`: route classification was duplicated between
  `isApplicationRequest` and the `accepted` allowlist. Returned via `rework`;
  `9b51d44` consolidated dispatch ownership and was approved.
- Unattended A `4f8cf63`: Runner bodies were read and then rebuilt into
  synthetic `Request` objects. Returned via `rework`; `ff3be43` passed the
  bounded bytes directly to typed operations and was approved.

Both B revisions (`de40def`, `54a7336`) were approved after review of the
migrated assertions and the removal of the temporary test seam and legacy
worker path. The review requests were published incrementally with checkout,
baseline, SHA, variant, and session ID so review could proceed while B worked.

## Checks

On the Web final SHA, `corepack pnpm check` exited 0 and
`corepack pnpm test` passed: 932 unit tests passed with 1 skipped, plus 7
runtime and 81 inference tests. On the unattended final SHA, the independent
reviewer reran `corepack pnpm test`: 931 passed with 1 skipped, plus 7 runtime
and 81 inference tests. No internal-URL test path or `CoreServiceBinding`
synthetic forwarding remains in either final tree.

## Caveats

- A linked git worktree keeps its index and refs under the parent repository's
  `.git` directory, outside the session's `workspace-write` root. Each local
  commit or rebase therefore required a one-time `danger-full-access` approval
  through the normal interaction flow. This is the documented behavior for
  writes outside the effective root set, not a defect found by this run.
- The acceptance Hosts used disposable `--patch` overlays. Persistent
  `dsh plugin` installation and `dsh Desktop` were not part of this run; the
  keyless integration suite covers persistent installation.
- Acceptance changes exist only as local commits in the isolated checkouts.

## Reproduce the route check

Build and capture from any checkout, then compare:

```sh
node /home/kagami/work/dsh33/state/build.mjs "$CHECKOUT" "$OUT/bundle"
node /tmp/cr401-capture.mjs "$OUT/bundle/application.mjs" "$OUT/observations.json"
node /home/kagami/work/dsh33/state/compare.mjs "$BASELINE" "$OUT/observations.json"
```

Retained evidence (outside this repository):
`/home/kagami/work/dsh33/scenarios/web/baseline-observations.json`,
`/home/kagami/work/dsh33/scenarios/web/b-de40def/observations.json`, and
`/home/kagami/work/dsh33/scenarios/unattended/b-54a7336/observations.json`.
