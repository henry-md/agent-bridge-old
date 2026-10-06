#!/usr/bin/env node
// Narrow, optional pre-model adapter. It never consumes ordinary agent mail.
import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { setTimeout as pause } from 'node:timers/promises';

const path = resolve(process.env.BRIDGE_CONFIG ?? `${homedir()}/.agent-bridge/config.json`);
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const invocation = /^\s*(?:\/agent-bridge|\$agent-bridge|\[\$agent-bridge\]\([^\r\n)]+\))\s+(0|[1-9][0-9]{0,63})\s*$/;
const emit = value => process.stdout.write(JSON.stringify(value) + '\n');
const launch = (command, args) => { const child = spawn(command, args, { detached: true, stdio: 'ignore', windowsHide: true }); child.on('error', () => {}); child.unref(); };
async function info() {
  const config = JSON.parse(await readFile(path, 'utf8'));
  const local = JSON.parse(await readFile(path + '.daemon.json', 'utf8'));
  const url = new URL(local.url);
  const actor = createHash('sha256').update(JSON.stringify([config.url, config.token, config.device])).digest('hex');
  if (local.version !== 1 || local.actor !== actor || typeof local.token !== 'string' || local.token.length < 32 || url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.port || url.pathname !== '/' || url.username || url.password || url.search || url.hash) throw Error('invalid local runtime');
  return local;
}
async function warm() {
  // Node is the only runtime dependency; the linked CLI starts the daemon once.
  launch(process.execPath, [resolve(process.argv[2]), 'daemon', 'start']);
}
try {
  let raw = '';
  for await (const chunk of process.stdin) { raw += chunk; if (raw.length > 65536) throw Error('oversized hook input'); }
  const input = JSON.parse(raw);
  if (input.hook_event_name === 'SessionStart') { if (process.argv[2]) await warm(); }
  else if (input.hook_event_name === 'UserPromptSubmit') {
    const match = typeof input.prompt === 'string' && input.prompt.match(invocation);
    if (match && uuid.test(input.session_id ?? '')) {
      const started = performance.now(); let local;
      try { local = await info(); } catch {
        if (process.argv[2]) { await warm(); while (performance.now() - started < 700) { await pause(20); try { local = await info(); break; } catch {} } }
      }
      if (!local) throw Error('runtime unavailable');
      const channel = match[1], deadline = Math.max(1, 1200 - (performance.now() - started));
      const pane = `${local.url}/ui?channel=${channel}`;
      // Open while connecting so the independent runtime proof can update the
      // word immediately, even if the peer arrives after this hook's deadline.
      if (process.argv.includes('--pane')) {
        if (process.platform === 'darwin') launch('/usr/bin/open', [pane]);
        else if (process.platform === 'win32') launch('rundll32.exe', ['url.dll,FileProtocolHandler', pane]);
        else launch('xdg-open', [pane]);
      }
      const response = await fetch(`${local.url}/v1/channels/${channel}/pair`, { method: 'POST', headers: { Authorization: `Bearer ${local.token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ session_id: input.session_id, timeout_ms: Math.floor(deadline) }), signal: AbortSignal.timeout(Math.floor(deadline) + 100) });
      const result = await response.json();
      if (response.ok && result.verified === true && result.session_id === input.session_id && result.channel === channel && uuid.test(result.proof_nonce ?? '') && uuid.test(result.proof_message_id ?? '') && /^[a-z0-9-]{3,80}$/.test(result.connection?.secret_word ?? '')) {
        // Recheck local account identity before injecting verified context.
        const current = await info(); if (current.actor !== local.actor) throw Error('identity changed');
        const word = result.connection.secret_word;
        emit({ continue: true, systemMessage: `Connected on channel ${channel}. Secret word is ${word}.`, hookSpecificOutput: { hookEventName: 'UserPromptSubmit', additionalContext: `Agent Bridge runtime already joined this chat (${input.session_id}) on channel ${channel} and verified a fresh echoed nonce through the durable message channel. Secret word: ${word}. Proof took ${result.proof_round_trip_ms.toFixed(1)} ms. Pane: ${pane}. Show the verified word and reuse any existing watcher for this channel/session. If none is running, use bridge watch --daemon --channel ${channel} --session ${input.session_id} --timeout 600. Keep listening; do not pair again or start a second relay reader. Ordinary messages remain unacknowledged and must be processed through the skill.` } });
      }
    }
  }
} catch {
  // Optional acceleration must not block a prompt or leak raw configuration/errors.
}
