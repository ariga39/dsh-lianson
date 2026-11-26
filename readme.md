# dsh-liaison

The goal of dsh-liaison is to let Codex delegate coding work to DeepSeek Harness (dsh), inspect the results, and send review feedback back to the same session. A user can open dsh Web to see what the agent is doing and close it while the work continues.

The design relies on dsh for session history, prompt deduplication, and workspace isolation. The liaison layer only needs to connect an assignment to its dsh session and expose the controls Codex uses.

[Project requirements](docs/requirements.md) · [Design](docs/design.md) · [Open issues](https://github.com/ariga39/dsh-lianson/issues) · [Contributor guide](AGENTS.md)
