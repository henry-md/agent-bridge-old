# Pre-model setup and live confirmation

A local daemon removes repeated Node/HTTP startup and handles setup messages without model reasoning. Run `bridge daemon start` once after configuring each computer. Creating a channel starts an independent fresh nonce proof, so a short-lived hook or canceled caller cannot abandon setup. The runtime and its credentials remain outside Git. Its status gives a loopback origin; open `ORIGIN/ui?channel=NUMBER` for an immediate verified word and proof timing. The pane reconnects after a restart and exposes only the visual word and timing.

For Codex, install the current complete user skill, then run:

```sh
bridge skill install --user --force
bridge hooks install --pane
```

This adds two narrowly scoped command handlers to `~/.codex/hooks.json`, preserving unrelated hooks. `SessionStart` warms the daemon. `UserPromptSubmit` matches only a standalone `/agent-bridge NUMBER`, `$agent-bridge NUMBER`, or its skill-link invocation, and uses the actual chat UUID. When `--pane` is enabled it opens the read-only pane before waiting for proof, so the word appears immediately when the peer arrives even after the hook's bounded deadline. It passes only freshly verified context to the agent. It never launches an AI session, acknowledges ordinary mail, or blocks unrelated prompts. If setup is slow or the peer absent, the normal skill continues pairing while the resident proof stays active.

Codex requires the human to review and trust each command handler in `/hooks` or Desktop Hooks settings before it runs. Installing the file does not grant trust; never edit trusted hashes or bypass this review. [Official Hooks documentation](https://learn.chatgpt.com/docs/hooks) describes this product requirement. The script's stdout `systemMessage` is not a guarantee of an immediate chat bubble in every Desktop version. Use the live pane for instant display; the agent acknowledges the same verified context afterward.

Claude Code can use the same daemon CLI and pane. Native hook installation here targets Codex; do not silently edit Claude settings.

After pulling code updates, rebuild, stop the daemon, and start it again. Active channels and session IDs restore automatically. A waiting local client follows the new private descriptor without changing account identity or ending the relay pairing. Use `bridge channel leave NUMBER --daemon SESSION` only for an explicit disconnect; `daemon stop` alone preserves membership for upgrades.
