# One-time setup

The relay origin is `https://agent-bridge-production-2405.up.railway.app`, and `/healthz` there is a public health check. The source is https://github.com/henry-md/agent-bridge. Node.js 24 or newer is required. Each computer needs its own registered device token.

## CLI

Look for an existing checkout before cloning. On the CD&R Windows 365 Cloud PC it is `~/OneDrive - Clayton, Dubilier & Rice Holdings, L.P/Developer/agent-bridge`. Otherwise clone the repository into the user's developer directory.

In the checkout, run `npm ci`, `npm run build`, then `npm link`. On Windows, any `npm run` script fails when the checkout path contains `&` (as the OneDrive path does), because cmd.exe splits the command there. Build with `node node_modules/typescript/bin/tsc -p tsconfig.json` instead, and run tests with `node --import tsx --test "test/*.test.ts"`. Two symlink tests fail with `EPERM` unless Windows Developer Mode or admin rights are available; that is expected. If the linked `bridge` command is not on PATH yet, use `node <checkout>/dist/cli.js`. To update later, run `git pull --ff-only` in the checkout and rebuild. The `npm link` stays valid.

## Skills

The source of truth is `.agents/skills/agent-bridge` in the checkout, and the same folder serves Codex and Claude Code. Use `--force` only to update this same skill.

- **Codex:** `bridge skill install --user --force` installs it into `~/.codex/skills/agent-bridge`. The `henry-md/codex-skills` repository mirrors that folder; copy the checkout's folder over it and commit whenever the skill changes.
- **Claude Code:** `bridge skill install --claude --force` installs it into `~/.claude/skills/agent-bridge`. Refresh any project copy in `.claude/skills/agent-bridge` by copying the folder.
- `bridge skill install --project PATH` installs into a project's `.agents/skills`.

## Configuration and registration

Run `bridge config show`, which redacts the token. Set the origin with `bridge config set --url https://agent-bridge-production-2405.up.railway.app`. The configuration stays in `~/.agent-bridge/config.json`, and the CLI refuses to save it inside a Git checkout.

Registration needs the relay's administrator secret, which only Railway holds. Never print it. If the Railway CLI is signed in and linked to the `agent-bridge` project, run this from the checkout:

```sh
npx @railway/cli run --service agent-bridge -- node dist/cli.js register NAME
```

Without a linked checkout, read `ADMIN_TOKEN` from `railway variable list -p PROJECT_ID -s agent-bridge -e production --json` into a shell variable without printing it, expose it as `ADMIN_TOKEN` only for `bridge register NAME`, then remove it. `bridge register` saves the new device token straight into the configuration and prints only `{"device":...,"token_saved":true}`.

Sign-in is the user's step: they run `railway login` themselves (or `railway login --browserless` for a pairing code). Use `laptop` for the Mac and `vm` for the Windows 365 Cloud PC; this Cloud PC is currently registered as `cdr-laptop`. A `device_exists` error means the name is taken. Do not revoke a working device to free it; ask the user whether to use another name or whether the old device is retired.

Without Railway access, have the user put a separately issued device token in an environment variable in their own terminal. Never ask for it in chat. Then run `bridge config set --url URL --device NAME --token-env BRIDGE_TOKEN` and remove the variable. Do not replace a working device token just to join a channel.

After registration, `/agent-bridge 4040` pairs active agents. File sharing is optional and separate: add only user-selected folders with `bridge root add ALIAS PATH`, then keep `bridge connect` running on that computer. A missing connector does not prevent channel messages or the secret-word confirmation.

Start the resident runtime once with `bridge daemon start`. Keep it warm between chats; `--daemon` operations use its one reader per channel. Native pre-model acceleration is optional; see [runtime setup](runtime.md).

## Troubleshooting: HTTP 403 from the relay

On the CD&R Cloud PC, `HTTP_ERROR: Relay returned HTTP 403` usually means the corporate web proxy is blocking the relay domain, not that the device token is bad. Confirm by requesting `https://agent-bridge-production-2405.up.railway.app/healthz`: a "Non-compliant action ... site that has not been evaluated for compliance with CD&R's security standards" HTML page means the proxy blocked it. Registration, reinstalling the skill, or pulling the repo cannot fix this, and do not route around the proxy. The user must request approval for `agent-bridge-production-2405.up.railway.app` from TechSecOps@cdr.com with a business justification. A 403 with a JSON body from the relay itself points to a revoked or wrong device token instead; follow Configuration and registration above.
