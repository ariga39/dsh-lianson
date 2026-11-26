# Design brief

> **Historical note**: This brief records the earlier external-daemon design. The current implementation is an in-process `dsh` plugin with a stateless CLI; see the [agent guide](../agent.md), [requirements](requirements.md), and open GitHub issues for the active direction.

AGY prepared the [development plan](development-plan.md) against the [requirements](requirements.md). Codex independently examined the implementation, discussed findings with AGY, and verified the agreed revision. This brief records the questions and evidence used for that design.

## Source baseline

The design uses DeepSeek Harness commit `4b399c39a6d1479cd84ca591b4d7c88e24d38a96`. Both collaborators had access to the complete source tree. [Upstream source](https://github.com/deepseek-ai/deepseek-harness/tree/4b399c39a6d1479cd84ca591b4d7c88e24d38a96)

- Trace the external control path through session creation, model selection, prompt submission, queue updates, cancellation, state reads, waits, history and live following. [SessionController](https://github.com/deepseek-ai/deepseek-harness/blob/4b399c39a6d1479cd84ca591b4d7c88e24d38a96/packages/api/session-controller/src/index.ts)
- Identify which process owns execution. The SDK client launches a runtime subprocess. A proposed integration must explain how Web reaches the exact active session and which process keeps it running after a viewer disconnects. [SDK client](https://github.com/deepseek-ai/deepseek-harness/blob/4b399c39a6d1479cd84ca591b4d7c88e24d38a96/packages/sdk/client/src/client.ts)
- The Web tests cover reloading a browser-originated session and distinguish queued input from steering the current turn. The design needs an additional test for an external operator and a late or absent viewer. [Web replay test](https://github.com/deepseek-ai/deepseek-harness/blob/4b399c39a6d1479cd84ca591b4d7c88e24d38a96/apps/web/tests/session-replay-reload.e2e.ts) · [Steering test](https://github.com/deepseek-ai/deepseek-harness/blob/4b399c39a6d1479cd84ca591b4d7c88e24d38a96/apps/web/tests/steering.e2e.ts)

## Decisions the plan must support

1. Trace a delegation from the user's request to its working directory, executor, model and stable session address. Identify what Herdr, dsh and this project each own.
2. Compare the required interaction surface with the pinned implementation. Cover authorization, questions, corrections, cancellation and follow-up work in the original session. Identify any unverified behavior and how to test it.
3. Design an early end-to-end test for external input, a mid-turn Web viewer, historical tool output, live progress and continued execution after the page closes. Detect session substitution, repeated submissions and missing events.
4. Sequence work around independently verifiable behaviors, using a failing test, minimal implementation and re-verification. Cover the two dependent delegations, overlapping review and implementation, and rework in place.
5. Explain the recommended integration with source references. For assumptions that could change the architecture, identify the observation that would trigger a different decision.

The historical routing comparison covered 23 paths, three methods (GET, POST, DELETE), and two authentication states. Codex compared both historical captures: all 138 observations matched on HTTP status, response body, Cache-Control and Content-Type. Future acceptance must regenerate both captures from identified revisions.

For a historical replay, the source repository is the one named in the [acceptance scenario](requirements.md#acceptance-scenario). Start at `352adace695e7ec1b184ca60c1ca4334e3865660`. The production-routing revision on the dependency chain was `cbf1c514aa714c042292b12f8f6b356cf2371e80`; the dependent test-migration revision was `b155890dc4d175b5edaaf5336988b27910d9766c`.
