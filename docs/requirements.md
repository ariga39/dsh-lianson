# Delegate coding work from Codex to DeepSeek Harness

## Goal and roles

In the existing Herdr workflow, Codex delegates coding tasks to DeepSeek Harness (dsh) in place of OpenCode. The user gives Codex a goal, changes the scope through Codex, and receives the verified result from Codex. At any point the user can open dsh Web to see the work. AGY can contribute design or independent review as agreed for each assignment.

## Required behavior

1. **Identify each assignment.** Codex specifies a working directory, checks the actual executor and model, and can find the corresponding dsh session again. Several assignments can progress independently. Waiting for one executor or reviewer must leave Codex free to guide another.
2. **Guide the running session.** Use the interactions available to a person in the dsh terminal as the capability benchmark. Codex submits work, observes actions and results, and distinguishes running, waiting for input, and finished turns. It can add instructions, correct direction, answer a question, handle an approval within the user's authorization, or stop the assignment. When the user changes scope, Codex delivers the change to the intended session. Routine decisions require no user attendance.
3. **Observe the same session in dsh Web.** The user can find a session by task and see the original instructions, replies, tool calls, and results, including work completed before the page opened. An open page shows subsequent progress. Viewing uses the existing session history and stream, so Codex spends no model tokens relaying or summarizing the work. Opening, viewing, closing, or never opening Web must all allow execution to continue.
4. **Follow through to acceptance.** After dsh reports completion, Codex inspects the actual changes, reruns the agreed checks, and follows the required independent review. The original session stays addressable throughout acceptance. Codex sends omissions or review findings back to that session for correction and verifies the revised result.

## Acceptance scenario

The reference is compte-rendu #401. AGY helped design the change, two OpenCode sessions handled production routing and test migration, and Codex checked 138 route observations and followed online PR review. With dsh as both executors, the following workflow must hold:

1. Codex and the designer agree on the split. Codex assigns the production change to session A. Once A produces a fixed revision, Codex starts session B from that revision for test migration.
2. While A awaits review, Codex observes and guides B. Findings for either assignment go back to its original session.
3. A user opening the test-migration session can read the assigned scope, see which callers dsh inspected, and see the tests and their results. Closing the page leaves the task running.
4. Codex verifies routing behavior and both deliverables, follows the agreed review, and accepts the result. The whole workflow also succeeds when the user never opens Web.

Delivery architecture and build order are documented in [design.md](design.md), the [agent guide](../AGENTS.md), and the [open GitHub issues](https://github.com/ariga39/dsh-lianson/issues?q=is%3Aissue+is%3Aopen).
