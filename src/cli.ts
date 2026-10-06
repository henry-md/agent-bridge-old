#!/usr/bin/env node
import { Command } from 'commander';
import { spawn } from 'node:child_process';
import { setTimeout as pause } from 'node:timers/promises';
import { cp, lstat, mkdir, realpath, rename, rm, stat } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { homedir } from 'node:os';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { BridgeError, messageInputSchema, nameSchema, requestInputSchema } from './shared/protocol.js';
import { configPath, readConfig, tokenFromEnv, updateConfig, type BridgeConfig } from './client/config.js';
import { RelayClient, validateRelayUrl } from './client/relay.js';
import { runConnector } from './client/connector.js';
import { channelId, channelResult, channelStatus, joinChannel, leaveChannel, pairChannel, requireChannelSession, sendChannelMessage, validateChannelTimeout, watchChannel } from './client/channel.js';

const program = new Command().name('bridge').description('Exchange messages and file context through your private Railway relay').version('0.1.0');
program.configureOutput({ outputError: () => {} });
program.exitOverride();
const output = (value: unknown) => process.stdout.write(`${JSON.stringify(value)}\n`);
const integer = (raw: string) => { const value = Number(raw); if (!Number.isSafeInteger(value) || value < 0) throw new BridgeError(0, 'INVALID_ARGUMENT', 'Expected a nonnegative integer'); return value; };
const wait = (raw: string) => { const value = integer(raw); if (value > 25) throw new BridgeError(0, 'INVALID_ARGUMENT', 'Wait must be between 0 and 25 seconds'); return value; };
const channelTimeout = (raw: string) => validateChannelTimeout(integer(raw));
async function localClient() { const config = await readConfig(); return { config, client: new RelayClient(config.url, config.token!) }; }
const daemons = program.command('daemon').description('Keep a local authenticated bridge runtime warm');
daemons.command('run').action(async () => { const { runDaemon } = await import('./client/daemon.js'); await runDaemon(info => output({ running: true, ...info })); });
daemons.command('status').action(async () => { const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js'); output(await new DaemonClient(await daemonInfo()).status()); });
daemons.command('stop').action(async () => { const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js'); output(await new DaemonClient(await daemonInfo(false)).stop()); });
daemons.command('start').action(async () => {
  const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js');
  try { const existing = await daemonInfo(); await new DaemonClient(existing).status(); output({ running: true, pid: existing.pid, url: existing.url, already_running: true }); return; } catch (error) {
    if (error instanceof BridgeError && error.code === 'CONFIG_CHANGED') { await new DaemonClient(await daemonInfo(false)).stop().catch(() => {}); await pause(100); }
    else if (error instanceof BridgeError && error.code !== 'DAEMON_REQUIRED') throw error;
  }
  const child = spawn(process.execPath, [fileURLToPath(import.meta.url), 'daemon', 'run'], { detached: true, stdio: 'ignore', windowsHide: true });
  const spawned = new Promise<void>((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject); }); await spawned; child.unref();
  child.on('error', () => {});
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) { try { const info = await daemonInfo(); await new DaemonClient(info).status(); output({ running: true, pid: info.pid, url: info.url }); return; } catch { await pause(50); } }
  throw new BridgeError(0, 'DAEMON_START_TIMEOUT', 'Local runtime did not become ready within thirty seconds; a detached startup may still be running');
});
async function runtimeSession(channel: string, explicit?: string) {
  const config = await readConfig();
  const requested = explicit ?? (process.env.CODEX_THREAD_ID && /^[0-9a-f-]{36}$/i.test(process.env.CODEX_THREAD_ID) ? process.env.CODEX_THREAD_ID : undefined);
  const fallback = config.channel_sessions?.[channel]?.fallback_session_id;
  if (requested ?? fallback) return requested ?? fallback!;
  const created = randomUUID();
  await updateConfig(current => { if (!current || current.url !== config.url || current.token !== config.token || current.device !== config.device) throw new BridgeError(0, 'CONFIG_CHANGED', 'Credentials changed while reserving runtime identity'); const saved = current.channel_sessions?.[channel] ?? { sessions: {} }; return { ...current, channel_sessions: { ...current.channel_sessions, [channel]: { ...saved, fallback_session_id: saved.fallback_session_id ?? created } } }; });
  return (await readConfig()).channel_sessions![channel].fallback_session_id!;
}
program.command('hooks').command('install').option('--pane', 'Open the live pane immediately for bridge prompts').action(async options => { const { installHooks } = await import('./client/hooks.js'); output(await installHooks(fileURLToPath(import.meta.url), !!options.pane)); });
const configuration = program.command('config').description('Configure local credentials outside Git');
configuration.command('set').requiredOption('--url <url>', 'Relay HTTPS origin').option('--token-env <variable>', 'Environment variable holding a device token').option('--device <name>', 'Device name').action(async options => {
  const url = validateRelayUrl(options.url);
  const device = options.device ? nameSchema.parse(options.device) : undefined;
  const token = options.tokenEnv ? tokenFromEnv(options.tokenEnv) : undefined;
  const config = await updateConfig(old => {
    const next: BridgeConfig = old?.url === url ? { ...old } : { url, roots: {} };
    if ((token && token !== next.token) || (device && device !== next.device)) { delete next.channel_sessions; delete next.runtime; }
    if (token) { next.token = token; next.inbox_cursor = 0; }
    if (device) next.device = device;
    return next;
  });
  output({ configured: true, path: configPath(), url: config.url, device: config.device ?? null, token_configured: !!config.token });
});
configuration.command('show').action(async () => { const config = await readConfig(false); output({ ...config, token: config.token ? '[REDACTED]' : undefined }); });
program.command('register').argument('<name>').option('--token-env <variable>', 'Environment variable holding admin token', 'ADMIN_TOKEN').action(async (name, options) => {
  nameSchema.parse(name); const config = await readConfig(false); const client = new RelayClient(config.url, tokenFromEnv(options.tokenEnv)); const registration = await client.register(name);
  await updateConfig(current => { if (!current || current.url !== config.url || current.token !== config.token || current.device !== config.device) throw new BridgeError(0, 'CONFIG_CHANGED', 'Relay configuration changed during registration'); const next = { ...current, device: name, token: registration.token, inbox_cursor: 0 }; delete next.channel_sessions; delete next.runtime; return next; }); output({ device: registration.device, token_saved: true });
});
program.command('revoke').argument('<name>').option('--token-env <variable>', 'Environment variable holding admin token', 'ADMIN_TOKEN').action(async (name, options) => { nameSchema.parse(name); const config = await readConfig(false); output(await new RelayClient(config.url, tokenFromEnv(options.tokenEnv)).revoke(name)); });
const roots = program.command('root').description('Allow read-only access to an explicit folder alias');
roots.command('add').argument('<alias>').argument('<path>').action(async (alias, path) => {
  nameSchema.parse(alias); const canonical = await realpath(resolve(path));
  if (!(await stat(canonical)).isDirectory()) throw new BridgeError(0, 'NOT_A_DIRECTORY', 'Shared roots must be directories');
  await updateConfig(config => { if (!config) throw new BridgeError(0, 'CONFIG_REQUIRED', 'Configure the relay first'); return { ...config, roots: { ...config.roots, [alias]: canonical } }; }); output({ root: alias, path: canonical, restart_connector: true });
});
roots.command('remove').argument('<alias>').action(async alias => { await updateConfig(config => { if (!config) throw new BridgeError(0, 'CONFIG_REQUIRED', 'Configure the relay first'); const roots = { ...config.roots }; delete roots[alias]; return { ...config, roots }; }); output({ removed: alias, restart_connector: true }); });
program.command('devices').action(async () => { const { client } = await localClient(); output({ devices: await client.devices() }); });
program.command('connect').description('Run the outbound file connector; stop with Ctrl+C').action(async () => {
  const { config, client } = await localClient(); if (!config.device) throw new BridgeError(0, 'DEVICE_REQUIRED', 'Set the device name before connecting');
  const controller = new AbortController(); const stop = () => controller.abort(); process.once('SIGINT', stop); process.once('SIGTERM', stop);
  output({ connecting: config.device, roots: Object.keys(config.roots) });
  try { await runConnector(client, config.device, config.roots, { signal: controller.signal, onError: error => process.stderr.write(`${JSON.stringify({ error })}\n`) }); } finally { process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop); }
});
for (const operation of ['read', 'list', 'search'] as const) {
  const command = program.command(operation).requiredOption('--device <name>').requiredOption('--root <alias>').option('--path <relative-path>', 'Relative path within shared folder', '').option('--limit <count>', 'Maximum list/search results', integer, 100).option('--idempotency-key <key>');
  if (operation === 'search') command.requiredOption('--query <text>', 'Literal text to find');
  command.action(async options => {
    const { client } = await localClient(); const input = requestInputSchema.parse({ to: options.device, operation, root: options.root, path: options.path, limit: options.limit, ...(options.query ? { query: options.query } : {}) });
    let request = await client.createRequest(input, options.idempotencyKey); const deadline = Date.now() + 70_000;
    while ((request.status === 'pending' || request.status === 'running') && Date.now() < deadline) request = await client.request(request.id, 25);
    if (request.status === 'completed') output(request.result);
    else throw new BridgeError(0, request.error?.code ?? 'REQUEST_TIMEOUT', request.error?.message ?? `Remote request ${request.id} did not complete`);
  });
}
const channels = program.command('channel').description('Pair two chat sessions on a numeric channel');
channels.command('pair').description('Join and exchange the setup word in one process; ordinary mail remains unacknowledged').argument('<channel>', 'Canonical numeric channel', channelId).option('--session <uuid>', 'Chat session UUID').option('--timeout <seconds>', 'Total setup deadline; 0 waits indefinitely', channelTimeout, 600).option('--watch', 'Listen in this process after verified setup with no ordinary mail; prints a second JSON line').option('--watch-timeout <seconds>', 'Receive deadline after setup; 0 waits indefinitely', channelTimeout, 600).option('--daemon', 'Use the resident runtime and fresh echoed-nonce proof').action(async (channel, options) => {
  if (options.daemon) {
    const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js'); const runtime = new DaemonClient(await daemonInfo()); const session = await runtimeSession(channel, options.session);
    const result = await runtime.pair(channel, session, options.timeout ? options.timeout * 1000 : 2_147_483_000);
    output({ ...result, ...(options.watch ? { watching: result.verified && result.messages.length === 0 } : {}) });
    if (options.watch && result.verified && result.messages.length === 0) output(await runtime.watch(channel, session, options.watchTimeout ? options.watchTimeout * 1000 : 2_147_483_000));
    return;
  }
  const { config, client } = await localClient(); const controller = new AbortController(); const stop = () => controller.abort();
  process.once('SIGINT', stop); process.once('SIGTERM', stop);
  let paired: Awaited<ReturnType<typeof pairChannel>>;
  try {
    paired = await pairChannel(client, config, channel, options.session, options.timeout, controller.signal);
    output({ ...paired, ...(options.watch ? { watching: paired.verified && paired.messages.length === 0 } : {}) });
  } finally { process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop); }
  if (options.watch && paired.verified && paired.messages.length === 0) {
    const current = await readConfig();
    if (current.url !== config.url || current.token !== config.token || current.device !== config.device) throw new BridgeError(0, 'CONFIG_CHANGED', 'Relay credentials changed after pairing');
    const session = requireChannelSession(current, channel, paired.session_id);
    if (session.generation !== paired.connection?.generation) throw new BridgeError(0, 'SESSION_CHANGED', 'Channel generation changed after pairing');
    output(await watchChannel(client, current, channel, paired.session_id, options.watchTimeout, paired.connection));
  }
});
channels.command('join').argument('<channel>', 'Canonical numeric channel', channelId).option('--session <uuid>', 'Chat session UUID; defaults to CODEX_THREAD_ID or a saved per-channel UUID').option('--wait <seconds>', 'Wait for the mutual handshake, 0–25 seconds', wait, 25).action(async (channel, options) => {
  const { config, client } = await localClient(); output(channelResult(await joinChannel(client, config, channel, options.session, options.wait)));
});
channels.command('status').argument('<channel>', 'Canonical numeric channel', channelId).option('--session <uuid>', 'Chat session UUID').option('--wait <seconds>', 'Long poll wait, 0–25 seconds', wait, 0).action(async (channel, options) => {
  const { config, client } = await localClient(); output(channelResult(await channelStatus(client, config, channel, options.session, options.wait)));
});
channels.command('leave').option('--daemon', 'Stop the runtime reader before leaving').argument('<channel>', 'Canonical numeric channel', channelId).option('--session <uuid>', 'Chat session UUID').action(async (channel, options) => {
  if (options.daemon) { const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js'); output(await new DaemonClient(await daemonInfo()).leave(channel, await runtimeSession(channel, options.session))); return; }
  const { config, client } = await localClient(); const session = await leaveChannel(client, config, channel, options.session); output({ left: true, channel: session.channel, session_id: session.session_id, generation: session.generation });
});
channels.command('ack').option('--daemon', 'Acknowledge the runtime inbox').argument('<channel>', 'Canonical numeric channel', channelId).argument('<cursor>', 'Inbox cursor processed successfully', integer).option('--session <uuid>', 'Chat session UUID').action(async (channel, cursor, options) => {
  const { config, client } = await localClient(); const session = requireChannelSession(config, channel, options.session); if (options.daemon) { const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js'); output(await new DaemonClient(await daemonInfo()).acknowledge(session.channel, session.session_id, session.generation, cursor)); } else output(await client.acknowledgeChannel(session.channel, session.session_id, session.generation, cursor));
});
program.command('send').option('--daemon', 'Use the runtime for acknowledgment and receiving').option('--to <device>', 'Recipient for the device mailbox').option('--channel <channel>', 'Paired numeric channel', channelId).option('--session <uuid>', 'Chat session UUID for --channel').option('--text <text>', 'Message text', '').option('--attach <file-id>', 'Attach file ID; repeat for multiple files', (value: string, previous: string[]) => [...previous, value], [] as string[]).option('--idempotency-key <key>').option('--ack <cursor>', 'Acknowledge processed channel mail after sending; retries default to a key derived from this cursor', integer).option('--watch', 'Then wait for incoming channel mail in this same process; prints a second JSON line').option('--timeout <seconds>', 'Watch deadline; 0 waits indefinitely', channelTimeout, 0).action(async options => {
  if (options.daemon && !options.channel) throw new BridgeError(0, 'INVALID_ARGUMENT', '--daemon requires --channel');
  if (options.channel && options.to) throw new BridgeError(0, 'INVALID_ARGUMENT', '--channel and --to are mutually exclusive');
  if (!options.channel && !options.to) throw new BridgeError(0, 'INVALID_ARGUMENT', 'Specify --channel or --to');
  if (options.session && !options.channel) throw new BridgeError(0, 'INVALID_ARGUMENT', '--session requires --channel');
  if ((options.ack !== undefined || options.watch) && !options.channel) throw new BridgeError(0, 'INVALID_ARGUMENT', '--ack and --watch require --channel');
  const { config, client } = await localClient();
  if (options.channel) {
    const key = options.idempotencyKey ?? (options.ack > 0 ? `reply:${options.ack}` : undefined);
    const message = await sendChannelMessage(client, config, options.channel, options.session, options.text, options.attach, key, !(options.ack > 0));
    output({ message });
    const current = options.ack !== undefined || options.watch ? await readConfig() : config;
    if (current.url !== config.url || current.token !== config.token || current.device !== config.device) throw new BridgeError(0, 'CONFIG_CHANGED', 'Relay credentials changed after sending');
    if (options.ack > 0 && requireChannelSession(current, options.channel, message.from_session).generation !== message.generation) throw new BridgeError(0, 'SESSION_CHANGED', 'The processed inbox belongs to an earlier generation');
    if (options.daemon) {
      const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js'); const runtime = new DaemonClient(await daemonInfo());
      if (options.ack !== undefined) await runtime.acknowledge(options.channel, message.from_session, message.generation, options.ack);
      if (options.watch) output(await runtime.watch(options.channel, message.from_session, options.timeout ? options.timeout * 1000 : 2_147_483_000));
    } else {
      if (options.ack !== undefined) await client.acknowledgeChannel(options.channel, message.from_session, message.generation, options.ack);
      if (options.watch) output(await watchChannel(client, current, options.channel, message.from_session, options.timeout));
    }
  }
  else { const input = messageInputSchema.parse({ to: options.to, text: options.text, file_ids: options.attach }); output({ message: await client.send(input, options.idempotencyKey) }); }
});
program.command('inbox').option('--channel <channel>', 'Paired numeric channel', channelId).option('--session <uuid>', 'Chat session UUID for --channel').option('--wait <seconds>', 'Long poll wait, 0–25 seconds', wait, 25).option('--after <cursor>', 'Replay after this cursor for this call', integer).action(async options => {
  if (options.session && !options.channel) throw new BridgeError(0, 'INVALID_ARGUMENT', '--session requires --channel');
  const { config, client } = await localClient();
  if (options.channel) { const session = requireChannelSession(config, options.channel, options.session); output(await client.channelInbox(session.channel, session.session_id, session.generation, options.after, options.wait)); return; }
  const after = options.after ?? config.inbox_cursor ?? 0; const page = await client.inbox(after, options.wait);
  output(page);
  if (options.after === undefined && page.cursor > (config.inbox_cursor ?? 0)) await updateConfig(current => {
    if (!current) throw new BridgeError(0, 'CONFIG_CHANGED', 'Configuration was removed during inbox polling');
    if (current.url !== config.url || current.token !== config.token || current.device !== config.device) return current;
    return { ...current, inbox_cursor: Math.max(current.inbox_cursor ?? 0, page.cursor) };
  });
});
program.command('watch').option('--daemon', 'Receive from the resident runtime; never run a second relay reader').description('Wait for the next channel messages, rejoining and reconfirming as needed; exits once messages arrive').requiredOption('--channel <channel>', 'Paired numeric channel', channelId).option('--session <uuid>', 'Chat session UUID').option('--timeout <seconds>', 'Give up after this many seconds; 0 waits indefinitely', channelTimeout, 0).action(async options => {
  const { config, client } = await localClient();
  if (options.daemon) { const { DaemonClient, daemonInfo } = await import('./client/daemon-client.js'); const session = requireChannelSession(config, options.channel, options.session); output(await new DaemonClient(await daemonInfo()).watch(session.channel, session.session_id, options.timeout ? options.timeout * 1000 : 2_147_483_000)); }
  else output(await watchChannel(client, config, options.channel, options.session, options.timeout));
});
program.command('upload').argument('<path>').action(async path => { const { client } = await localClient(); output({ file: await client.upload(path) }); });
program.command('download').argument('<id>').requiredOption('--output <path>', 'New destination path; existing files are never overwritten').action(async (id, options) => { const { client } = await localClient(); const file = await client.download(id, options.output); output({ file, path: resolve(options.output) }); });
program.command('files').action(async () => { const { client } = await localClient(); output({ files: await client.files() }); });
program.command('file').argument('<id>').action(async id => { const { client } = await localClient(); output({ file: await client.file(id) }); });
program.command('delete').argument('<id>').action(async id => { const { client } = await localClient(); output(await client.deleteFile(id)); });
program.command('skill').command('install').option('--project <path>', 'Install inside a project checkout').option('--user', 'Install for this Codex user (default)').option('--claude', 'Install for this Claude Code user').option('--force', 'Replace an existing skill folder').action(async options => {
  if ([options.project, options.user, options.claude].filter(Boolean).length > 1) throw new BridgeError(0, 'INVALID_ARGUMENT', '--user, --claude and --project are mutually exclusive');
  let destination: string;
  if (options.project) { const project = await realpath(resolve(options.project)); if (!(await stat(project)).isDirectory()) throw new BridgeError(0, 'NOT_A_DIRECTORY', 'Project must be a directory'); destination = resolve(project, '.agents/skills/agent-bridge'); }
  else destination = resolve(homedir(), options.claude ? '.claude/skills/agent-bridge' : '.codex/skills/agent-bridge');
  const source = resolve(dirname(fileURLToPath(import.meta.url)), '../.agents/skills/agent-bridge');
  await mkdir(dirname(destination), { recursive: true });
  const staging = `${destination}.${randomUUID()}.tmp`; const backup = `${destination}.${randomUUID()}.backup`; const lock = `${destination}.install-lock`;
  try { await mkdir(lock, { mode: 0o700 }); } catch (error) { if ((error as NodeJS.ErrnoException).code === 'EEXIST') throw new BridgeError(0, 'SKILL_INSTALL_BUSY', 'Another command is installing this skill; retry after it finishes'); throw error; }
  let installed = false; let backedUp = false;
  try {
    await cp(source, staging, { recursive: true, force: false, errorOnExist: true });
    let exists = false;
    try { await lstat(destination); exists = true; } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    if (exists) {
      if (!options.force) throw new BridgeError(0, 'SKILL_EXISTS', 'Skill already exists; use --force to replace it');
      await rename(destination, backup); backedUp = true;
    }
    // Windows cannot rename over even an empty directory. The sibling lock
    // reserves this installation while publication targets an absent path.
    await rename(staging, destination); installed = true;
  } catch (error) {
    if (backedUp) { await rename(backup, destination); backedUp = false; }
    throw error;
  } finally {
    try { await rm(staging, { recursive: true, force: true }); if (installed && backedUp) await rm(backup, { recursive: true, force: true }); }
    finally { await rm(lock, { recursive: true, force: true }); }
  }
  output({ installed: resolve(destination, 'SKILL.md'), scope: options.project ? 'project' : options.claude ? 'claude' : 'user' });
});
try { await program.parseAsync(); } catch (error) {
  const nativeCode = (error as { code?: unknown })?.code;
  const commandCode = typeof nativeCode === 'string' ? nativeCode : undefined;
  if (commandCode === 'commander.helpDisplayed' || commandCode === 'commander.version') process.exitCode = 0;
  else {
    const timeout = error instanceof Error && error.name === 'TimeoutError';
    const canceled = error instanceof Error && error.name === 'AbortError';
    const code = error instanceof BridgeError ? error.code : timeout ? 'REQUEST_TIMEOUT' : canceled ? 'CANCELED' : commandCode ?? 'CLI_ERROR';
    const message = error instanceof BridgeError ? error.message
      : timeout ? 'Bridge request exceeded its deadline; retry with the same session.'
      : canceled ? 'Bridge request canceled; membership and unacknowledged mail are preserved.'
      : commandCode?.startsWith('commander.') ? (error as Error).message
      : 'Command failed; check configuration, paths, and arguments';
    process.stderr.write(`${JSON.stringify({ error: { code, message } })}\n`); process.exitCode = 1;
  }
}
