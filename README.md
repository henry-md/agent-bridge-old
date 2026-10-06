# Agent Bridge

A small authenticated HTTPS relay for agent messages, selected filesystem context, and file attachments. Run a connector on each computer; your existing agent uses the `bridge` CLI through a project-local skill. No MCP server or hosted model is required.

```text
Laptop agent -> bridge CLI -> Railway relay <- VM connector -> shared VM folder
```

Both clients connect outward. File requests work while the connector is running, even if that computer's AI agent is idle. Agent messages remain in a mailbox until the peer reads them. `bridge watch` lets a chat keep listening: Claude Code receives in the foreground during active exchanges and uses one background watcher when idle; Codex keeps listening inside its turn. The bridge never launches a model or shares chat history automatically.

## Live deployment

Relay origin: `https://agent-bridge-production-2405.up.railway.app`. The [health check](https://agent-bridge-production-2405.up.railway.app/healthz) is public; API calls require a device token. Railway is connected to this repository's `main` branch, with successful GitHub checks required before automatic deployment.

On a laptop with the Railway CLI already signed in, register a device without printing the administrator secret:

```sh
bridge config set --url https://agent-bridge-production-2405.up.railway.app
npx @railway/cli run --service agent-bridge -- node dist/cli.js register laptop
```

Then select a shared root, install the skill into your participating project, and run `bridge connect` as shown below. Other computers need their own registered token and selected folders.

## Pair two chats on a channel

After one-time device registration, install the skill on both computers: `bridge skill install --user` for Codex and `bridge skill install --claude` for Claude Code. In each active chat, run:

```text
/agent-bridge 4040
```

The first participant proposes a random readable word. The CLI joins and exchanges setup messages in one process; each chat prints only `Secret word: WORD` after a fresh peer message confirms delivery. Setup is otherwise silent; a lone participant keeps waiting. Enabled skills appear in Codex Desktop's slash command list; `$agent-bridge 4040` also invokes the skill. Use another number, such as `4041`, for another simultaneous conversation. These are logical channels through the same Railway HTTPS origin.

The underlying CLI is:

```sh
bridge channel pair 4040 --timeout 600
bridge send --channel 4040 --text 'Please inspect the VM export.'
bridge inbox --channel 4040 --wait 25
# Block until the next messages arrive, recovering the session as needed.
bridge watch --channel 4040
# Process the inbox page before acknowledging its returned cursor.
bridge channel ack 4040 CURSOR
# Reply, acknowledge already processed mail, then receive in one CLI process.
# First JSON line is the sent receipt; the next is incoming mail or a timeout.
bridge send --channel 4040 --text 'Here is the answer.' --ack CURSOR --watch --timeout 600
bridge channel status 4040
bridge channel leave 4040
```

Codex thread IDs identify separate sessions; Claude Code passes `--session` with its session ID, and other shells use a saved fallback ID per channel. Supply `--session UUID` consistently when running distinct agents that share an environment. There is no global active-channel setting. The relay allows two active sessions from different registered devices per channel and keeps channel messages separate from ordinary device mailboxes. One computer can participate in many different channel numbers concurrently.

Inbox reads leave channel messages unacknowledged until `channel ack`; explicit `--after 0` replays the current round. Sessions stay joined while idle until they leave (the lease defaults to ten years; set `CHANNEL_LEASE_MS` to shorten it). A newer chat joining from the same device takes the channel over, and the older chat's calls fail with `channel_session_replaced`. A new peer must acknowledge a fresh handshake; an old word alone cannot prove an active peer. `bridge watch` and `bridge send` confirm a reset pairing automatically. The word is a visual connection check, and bearer tokens still control access. Attachments retain the existing trusted-workspace access model.

`bridge watch --channel N` exits as soon as unacknowledged messages arrive, or with `"timed_out": true` after `--timeout SECONDS`. It rejoins an expired session with the same identity and retries through network failures and relay restarts. Claude Code uses foreground receives during an active exchange, then one background watcher after 30 seconds idle; its completion wakes that chat. Codex does not, so the skill keeps a foreground watch loop running inside the turn.

A file connector is needed for remote folder reads, but channel pairing and messages work without it. The skill never launches agents automatically.

`channel pair --watch` prints the pair result immediately and, when verified with no ordinary mail, starts receiving in the same process. Its first line has `watching:true`; the next line is incoming mail. In a command tool that yields a running process, use a short initial yield and short waits until the first result is visible, then keep that same process. `--watch-timeout` controls the receive deadline independently of the setup `--timeout`. Buffered ordinary mail or an unverified result returns `watching:false` without starting a receive. Only complete pages of trailing setup acknowledgments for the verified pairing are consumed silently.

`channel pair` returns `verified`, `connection`, setup timings, and any ordinary `messages` encountered while exchanging the word. Print the word only when `verified` is true. Process ordinary mail before acknowledging the returned cursor; the command never acknowledges past it. Timeout or cancellation keeps the session joined. Use one reader per channel, and replace it with the same session ID after rebuilding. The lower-level `channel join --wait 25` command remains available for clients that manage their own handshake. See [latency measurements](docs/latency.md) for separate transport and agent response metrics.

`send --ack CURSOR --watch` reuses one process and HTTP pool for sending, acknowledging and waiting. Its first JSON line confirms the sent reply; the second contains the next inbox page. Only pass a cursor whose mail you have processed. With a positive `--ack` cursor, retries default to a stable reply key derived from that cursor, scoped by the server to this generation and sending session. A reply bound to processed mail stops if that generation has ended. If a later acknowledgment or watch fails, the first receipt still describes a delivered message; retry the same reply without changing its key or payload. An intentional different reply to the same cursor needs a new `--idempotency-key`.

## Resident runtime

For repeated sessions, keep one local process and its HTTP connections warm:

```sh
bridge daemon start
bridge channel pair 4040 --daemon --watch
bridge send --channel 4040 --daemon --text 'Hello' --ack CURSOR --watch --timeout 600
bridge channel leave 4040 --daemon
bridge daemon stop
```

The runtime owns one relay reader per channel. Agent receives use authenticated loopback RPC; ordinary messages remain in SQLite until explicitly acknowledged. Each pair request exchanges a fresh UUID nonce through the authenticated message mailbox and checks its echo against the current generation, pairing, sessions and word. `proof_round_trip_ms` measures the proof; `runtime_setup_ms` includes the caller's setup work. Neither includes a model response.

Open the URL from `bridge daemon status`, followed by `/ui?channel=4040`, for a live word and proof timing. This read-only pane exposes no messages, files, session identifiers or credentials. It shows a word only after a fresh proof and reconnects to the same local port after a daemon restart. The private descriptor and active channel registry live beside/in the existing local configuration, outside Git. An exclusive loopback listener prevents duplicate daemon ownership. Restart restores the same session IDs and replays unacknowledged ordinary mail; `watch --daemon` follows an updated descriptor within its original deadline. Changing account credentials clears saved runtime channels. An expired generation with pending mail stops rather than acknowledging across generations.

Initial setup starts an independent proof when the runtime creates a channel. A hook timing out or a CLI caller disconnecting does not cancel that proof. With `bridge hooks install --pane`, the pane opens while connecting and shows the verified word as soon as the peer arrives. Probe and echo writes run beside the sole reader, so a delayed HTTP receipt cannot strand incoming mail. An authenticated echo of a fresh nonce proves the durable probe without waiting for its outbound POST receipt. Pairing changes rotate the nonce and cancel stale transport retries; acknowledgments wait for earlier unfinished setup replies.

Long local pair/watch calls use RPC windows of at most 25 seconds and retain the original deadline across windows and reconnections. Intermediate timeouts stay internal; transport timeouts produce a structured CLI error without changing membership or acknowledging mail.

HTTP 403 with `channel_session_mismatch` means the session belongs to another authenticated device: each chat must use its own fixed UUID. Two agents testing on one computer also need distinct registered devices and isolated `BRIDGE_CONFIG` files. A plain HTTP 403 without a bridge error code can come from relay URL or network/proxy access and must be diagnosed on the affected computer. After correcting the cause, retry the same session; the runtime recreates a stopped worker.

Do not run a direct `watch` or `inbox` reader alongside the runtime. Use `--daemon` for pair, watch, send-with-ack/watch, ack and leave. Streamed uploads, authenticated downloads, checksums and the outbound file connector retain their existing interfaces.

## Build and run locally

Requires Node.js 24 or newer on each computer. Clone this repo, then:

```sh
npm ci
npm run check
npm run build
npm link
```

Generate an administrator secret and keep it in your terminal environment:

```sh
export ADMIN_TOKEN="$(node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("hex"))')"
export DATA_DIR="./data"
export HOST="127.0.0.1"
npm start
```

The server does not load `.env` automatically; use environment variables or Node's `--env-file` option with an ignored local file. On Railway, configure service variables in Railway. Never commit administrator secrets or client tokens.

## Set up each computer

In a second terminal on the laptop, with the same administrator secret temporarily available:

```sh
bridge config set --url http://127.0.0.1:3000
bridge register laptop --token-env ADMIN_TOKEN
bridge root add projects /absolute/path/to/shared/project
bridge skill install --project /absolute/path/to/participating/project
bridge connect
```

On the VM, use the Railway HTTPS URL and register a different device name:

```powershell
bridge config set --url https://YOUR-SERVICE.up.railway.app
bridge register vm --token-env ADMIN_TOKEN
bridge root add work 'C:\Work\Project'
bridge skill install --project 'C:\Work\Project'
bridge connect
```

Supply `ADMIN_TOKEN` through that terminal's environment for registration, then remove it after setup. Registration stores only the returned device token. `bridge connect` must remain running for remote folder requests. Each shared root is read-only; use dedicated folders rather than sharing your home directory. A connector refuses traversal, absolute paths, escaping symlinks, binary text reads, and common credential/build directories. These exclusions are safeguards, not a substitute for selecting folders you intend to share.

Local configuration defaults to `~/.agent-bridge/config.json`; set `BRIDGE_CONFIG` to choose another file, useful for two test devices on one machine. `bridge config show` redacts credentials. POSIX configuration permissions are restricted; on Windows keep it in your user profile with its normal user-only access controls. The copied skill is project-scoped, so install it into every participating project on each host. Install the CLI with `npm link` on each host as well.

## Ask with both contexts

```sh
bridge devices
bridge list --device vm --root work --path src
bridge search --device vm --root work --path src --query reconciliation
bridge read --device vm --root work --path src/reconciliation.ts
```

Your agent can combine these results with its local files. Results include source device, shared root, relative path, modification time, and truncation indicators. Text reads cap at 256 KiB; listings and searches cap at 100 results. A missing remote file or offline connector is reported explicitly.

## Transfer files and messages

```sh
# On the laptop. Record the returned file ID.
bridge upload ./report.xlsx
bridge send --to vm --text 'Compare this with your local export.' --attach FILE_ID

# On the VM.
bridge inbox --wait 25
bridge download FILE_ID --output ./context/report.xlsx

# Inspect and clean up attachments.
bridge files
bridge file FILE_ID
bridge delete FILE_ID
```

An ordinary inbox call advances the saved cursor after returning messages. Use `--after 0` to explicitly reread history; explicit cursor reads do not advance the saved cursor. Use the returned cursor for additional pages. A message submission can supply `--idempotency-key` for retries across separate CLI runs. The client retains generated keys across retries within one call.

Uploads and downloads stream file bytes; long polling carries small JSON messages and request metadata. Uploads become visible only after successful completion. Downloads verify the expected byte count and SHA-256 before finalizing the destination and refuse overwrites. File IDs are identifiers, not access credentials. All registered devices belong to one trusted workspace and can read its attachments; only the uploader can delete an attachment.

## Deploy to Railway

The repository includes a Dockerfile and `.railway/railway.ts` using Railway's current infrastructure format. Preview it with `npx @railway/cli config plan` and apply with `npx @railway/cli config apply`. It preserves secrets already configured on Railway. Deploy one service, mount a persistent volume at `/data`, set `ADMIN_TOKEN` to a new random secret of at least 32 characters, and set `DATA_DIR=/data`. Generate a public HTTPS domain. Keep one replica and disable serverless sleeping so connectors remain responsive. Configure volume ownership for the container's `node` user (UID 1000); Railway mounts can require `RAILWAY_RUN_UID=0` if ownership cannot be changed.

The service binds `0.0.0.0` and uses Railway's `PORT`. `/healthz` is the public health endpoint. SQLite metadata, uploads, and online database backups live on the volume. Daily SQLite backups retain seven snapshots; they do not independently back up attachment bytes. Railway full-volume backups, including attachment bytes, require the Pro plan. They are not enabled on the initial deployment's existing plan; the service's daily SQLite backups remain active. Volume deployment requires brief downtime; clients reconnect automatically.

For CLI deployment after `npx @railway/cli login`, use `railway init`, add the service and volume, set variables, apply the infrastructure configuration, then run `railway up` and `railway domain`. The Railway CLI can upload this checkout without GitHub integration. Alternatively connect the service to `henry-md/agent-bridge` on GitHub.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ADMIN_TOKEN` | required | Administrator registration/revocation secret, at least 32 characters |
| `DATA_DIR` | `./data` | Persistent SQLite, uploads, and backup directory |
| `HOST` | `0.0.0.0` | Listener host; use loopback for local development |
| `PORT` | `3000` | Railway supplies this automatically |
| `MAX_UPLOAD_BYTES` | `26214400` | 25 MiB maximum individual file |
| `UPLOAD_QUOTA_BYTES` | `1073741824` | 1 GiB total completed and in-progress upload quota |
| `CHANNEL_LEASE_MS` | `315360000000` | Channel session lease (ten years), renewed by every channel call |

For hundreds-of-megabytes or gigabyte transfers, add direct private object-storage uploads and resumable multipart transfer in a later version. Raising the application cap alone does not remove Railway's five-minute upload deadline.

## API and access model

See [docs/api.md](docs/api.md) for endpoints. Use bearer tokens in the `Authorization` header; credentials in URLs are unsupported. Device tokens are stored as hashes on the relay. Administrator credentials only register and revoke devices; use a device token for everyday operations.

The relay itself can see messages, attachments, and file results: HTTPS protects transit, not end-to-end encryption. Configure only work files that may be shared through the chosen deployment. Uploaded files are served as downloads, never executed by the server. Remote roots expose no shell or filesystem write operation.

The default poll is 25 seconds, heartbeat 15 seconds, offline threshold 60 seconds, and file-request lifetime 60 seconds. Read-only request leases allow a connector to recover after disconnect. Revoking a device cancels its access; delete attachments separately if you no longer need them.

## Validation

```sh
npm run check
npm run build
```

Tests cover real HTTP transfers and connector requests, checksums, unauthorized access, revocation, traversal and symlink escapes, interrupted/oversized uploads, duplicate retries, reconnects, request expiry, and restart persistence. GitHub Actions runs the same suite on macOS and Windows with Node 24. Passing native suites validates behavior on each OS; a live two-computer deployment additionally requires both hosts' connectors and authorized shared folders.
