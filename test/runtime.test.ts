import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { createServer as httpServer, request as httpRequest } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { type TestContext } from 'node:test';
import { setTimeout as pause } from 'node:timers/promises';
import { DaemonClient, type DaemonInfo } from '../src/client/daemon-client.js';
import { RelayClient } from '../src/client/relay.js';
import { createServer } from '../src/server/server.js';

async function daemon(t: TestContext, config: string) {
  const child = spawn(process.execPath, ['dist/cli.js', 'daemon', 'run'], { cwd: process.cwd(), env: { ...process.env, BRIDGE_CONFIG: config, CODEX_THREAD_ID: '' }, stdio: ['ignore', 'pipe', 'pipe'] });
  const exited = once(child, 'exit'); let stderr = '';
  child.stderr!.on('data', chunk => { stderr += chunk; });
  const ready = new Promise<void>((resolve, reject) => {
    child.once('error', reject); child.once('exit', () => reject(new Error(`Daemon exited before readiness: ${stderr}`)));
    let stdout = '';
    child.stdout!.on('data', chunk => { stdout += chunk; if (!stdout.includes('\n')) return; try { const result = JSON.parse(stdout.split('\n')[0]); assert.equal(result.running, true); assert.equal(result.token, undefined); resolve(); } catch (error) { reject(error); } });
  });
  await ready;
  const info = JSON.parse(await readFile(`${config}.daemon.json`, 'utf8')) as DaemonInfo; const client = new DaemonClient(info, config);
  t.after(async () => {
    await client.stop().catch(() => {});
    await Promise.race([exited, pause(3000).then(() => { if (child.exitCode === null) child.kill(); })]);
    assert.equal(stderr, '');
  });
  return { client, info, child, exited };
}
async function fixture(t: TestContext, beforeListen?: (relay: Awaited<ReturnType<typeof createServer>>) => void) {
  const directory = await mkdtemp(join(tmpdir(), 'bridge-runtime-'));
  const adminToken = 'runtime-test-admin-token-at-least-32-bytes';
  const options = { dataDir: join(directory, 'data'), adminToken, logger: false };
  const relay = await createServer(options); beforeListen?.(relay); const url = await relay.listen({ host: '127.0.0.1', port: 0 });
  const locals: Awaited<ReturnType<typeof daemon>>[] = [];
  t.after(async () => { await Promise.all(locals.map(async local => {
    await local.client.stop().catch(() => {});
    await Promise.race([local.exited, pause(3000).then(async () => { if (local.child.exitCode === null) local.child.kill('SIGKILL'); await local.exited; })]);
  })); await relay.close(); await rm(directory, { recursive: true, force: true }); });
  const admin = new RelayClient(url, adminToken);
  async function device(name: string) {
    const registration = await admin.register(name); const config = join(directory, `${name}.json`);
    await writeFile(config, JSON.stringify({ url, token: registration.token, device: name, roots: {} }), { mode: 0o600 });
    const local = await daemon(t, config); locals.push(local);
    return { ...local, config, relay: new RelayClient(url, registration.token), session: randomUUID() };
  }
  return { directory, relay, admin, device };
}

test('resident processes exchange fresh nonce proofs repeatedly while keeping one channel generation', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const [first, second] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  for (const result of [first, second]) { assert.equal(result.verified, true); assert.equal(result.timed_out, false); assert.ok(result.proof_round_trip_ms! >= 0); assert.ok(result.proof_message_id); }
  assert.equal(first.connection!.secret_word, second.connection!.secret_word); assert.notEqual(first.proof_nonce, second.proof_nonce);
  const next = await a.client.pair('4040', a.session, 5000);
  assert.equal(next.verified, true); assert.notEqual(next.proof_nonce, first.proof_nonce); assert.notEqual(next.proof_message_id, first.proof_message_id); assert.equal(next.connection!.generation, first.connection!.generation);
  const replay = await a.relay.channelInbox('4040', a.session, first.connection!.generation, 0, 0);
  const echo = replay.messages.find(message => message.id === next.proof_message_id)!;
  assert.equal(JSON.parse(echo.text.slice('agent-bridge control v2: '.length)).nonce, next.proof_nonce);
  assert.equal((await a.client.status()).channels[0].transport, 'online');
  if (process.platform !== 'win32') assert.equal((await stat(`${a.config}.daemon.json`)).mode & 0o077, 0);
});

test('runtime proofs do not acknowledge ordinary context, attached controls or mail beyond the delivered page', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  const file = join(h.directory, 'context.txt'); await writeFile(file, 'context attachment'); const uploaded = await a.relay.upload(file);
  const question = await a.relay.sendChannel('4040', { session_id: a.session, generation: paired.connection!.generation, text: 'ordinary context must remain pending', file_ids: [] });
  const attached = await a.relay.sendChannel('4040', { session_id: a.session, generation: paired.connection!.generation, text: `agent-bridge setup ack: ${paired.connection!.secret_word}`, file_ids: [uploaded.id] });
  const incoming = await b.client.watch('4040', b.session, 5000);
  // A reader may return the first record before the second arrives.
  while ((await b.client.status()).channels[0].pending_messages < 2) await pause(5);
  const proof = await b.client.pair('4040', b.session, 5000);
  assert.equal(proof.verified, true); assert.deepEqual(proof.messages.map(message => message.id), [question.id, attached.id]);
  const pending = await b.relay.channelInbox('4040', b.session, paired.connection!.generation, undefined, 0);
  assert.ok(pending.messages.some(message => message.id === question.id)); assert.ok(pending.messages.some(message => message.id === attached.id));
  assert.ok(pending.acknowledged_cursor < question.seq);
  await b.client.acknowledge('4040', b.session, paired.connection!.generation, proof.cursor);
  assert.equal((await b.client.status()).channels[0].pending_messages, 0);
  const remaining = await b.relay.channelInbox('4040', b.session, paired.connection!.generation, undefined, 0);
  assert.ok(!remaining.messages.some(message => message.id === question.id || message.id === attached.id));
  // The agent's cursor ends at its delivered ordinary page. Later setup
  // controls are left to the runtime's coalesced acknowledgment.
  assert.ok(remaining.messages.every(message => message.file_ids.length === 0 && message.text.startsWith('agent-bridge control v2: ')));
  await assert.rejects(b.client.acknowledge('4040', b.session, randomUUID(), incoming.cursor), (error: any) => error.code === 'stale_generation');
});

test('local RPC rejects missing credentials and hostile Host headers; relay revocation stops fresh proof', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  assert.equal((await fetch(`${a.info.url}/v1/status`)).status, 401);
  const hostileHost = await new Promise<number>(resolve => { const request = httpRequest(`${a.info.url}/v1/status`, { headers: { Authorization: `Bearer ${a.info.token}`, Host: 'foreign.example' } }, response => { response.resume(); resolve(response.statusCode!); }); request.end(); });
  assert.equal(hostileHost, 403);
  await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  await h.admin.revoke('vm');
  await assert.rejects(b.client.pair('4040', b.session, 5000), (error: any) => error.code === 'unauthorized');
});

test('runtime setup deadlines return unverified when the relay stalls and shutdown cancels initialization', { timeout: 10_000 }, async t => {
  const directory = await mkdtemp(join(tmpdir(), 'bridge-runtime-stall-'));
  const relay = httpServer(() => {}); relay.listen(0, '127.0.0.1'); await once(relay, 'listening');
  t.after(async () => { relay.closeAllConnections(); await new Promise<void>(resolve => relay.close(() => resolve())); await rm(directory, { recursive: true, force: true }); });
  const config = join(directory, 'config.json'); const url = `http://127.0.0.1:${(relay.address() as {port:number}).port}`;
  await writeFile(config, JSON.stringify({ url, token: 'test-token', device: 'laptop', roots: {} }));
  const local = await daemon(t, config); const started = performance.now();
  const result = await local.client.pair('4040', randomUUID(), 100);
  assert.equal(result.verified, false); assert.equal(result.timed_out, true); assert.ok(performance.now() - started < 1000);
  await local.client.stop();
  await local.exited;
});


test('resident receiver restores the same pairing after restart, preserves paged mail, and follows local RPC replacement', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  const sent = [];
  for (let i = 0; i < 103; i++) sent.push(await a.relay.sendChannel('4040', { session_id: a.session, generation: paired.connection!.generation, text: `ordinary ${i}`, file_ids: [] }));
  const malformed = await a.relay.sendChannel('4040', { session_id: a.session, generation: paired.connection!.generation, text: 'agent-bridge control v2: {invalid}', file_ids: [] });
  while ((await b.client.status()).channels[0].pending_messages < 104) await pause(5);
  await b.client.stop(); await b.exited;
  const restored = await daemon(t, b.config);
  assert.equal(restored.info.url, b.info.url); assert.notEqual(restored.info.token, b.info.token);
  while ((await restored.client.status()).channels[0].pending_messages < 104) await pause(5);
  const page = await b.client.watch('4040', b.session, 5000); // old client follows new private descriptor
  assert.equal(page.messages.length, 100); assert.deepEqual(page.messages.map(message => message.id), sent.slice(0, 100).map(message => message.id));
  await b.client.acknowledge('4040', b.session, paired.connection!.generation, page.cursor);
  const remainder = await b.client.watch('4040', b.session, 5000);
  assert.deepEqual(remainder.messages.map(message => message.id), [...sent.slice(100).map(message => message.id), malformed.id]);
  const proof = await b.client.pair('4040', b.session, 5000);
  assert.equal(proof.verified, true); assert.equal(proof.connection!.generation, paired.connection!.generation); assert.equal(proof.connection!.pairing_id, paired.connection!.pairing_id);
  await b.client.acknowledge('4040', b.session, paired.connection!.generation, proof.cursor);
  await restored.client.stop(); await restored.exited;
});

test('exclusive listener rejects a concurrent owner and survives a crash without a stale lock', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop');
  const duplicate = spawn(process.execPath, ['dist/cli.js', 'daemon', 'run'], { env: { ...process.env, BRIDGE_CONFIG: a.config }, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = ''; duplicate.stderr!.on('data', chunk => { stderr += chunk; });
  assert.equal((await once(duplicate, 'exit'))[0], 1); assert.match(stderr, /DAEMON_BUSY/);
  assert.equal((await a.client.status()).pid, a.child.pid);
  a.child.kill('SIGKILL'); await a.exited;
  const replacement = await daemon(t, a.config); assert.equal(replacement.info.url, a.info.url);
  await replacement.client.stop(); await replacement.exited;
});

test('changed credentials cannot expose verified results or cached mail, even at an expired deadline', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  // An ordinary record must survive a setup timeout and an account change.
  const sent = await a.relay.sendChannel('4040', { session_id: a.session, generation: paired.connection!.generation, text: 'still pending', file_ids: [] });
  const pending = await b.client.watch('4040', b.session, 5000); assert.equal(pending.messages[0].id, sent.id);
  const current = JSON.parse(await readFile(b.config, 'utf8')); current.token = 'changed-token'; await writeFile(b.config, JSON.stringify(current));
  await assert.rejects(b.client.pair('4040', b.session, 1), (error: any) => error.code === 'CONFIG_CHANGED');
  await assert.rejects(b.client.watch('4040', b.session, 1), (error: any) => error.code === 'CONFIG_CHANGED');
  const durable = await b.relay.channelInbox('4040', b.session, paired.connection!.generation, undefined, 0);
  assert.ok(durable.messages.some(message => message.id === sent.id));
});


test('a stalled setup POST is canceled at its deadline and ordinary receiving stays responsive', { timeout: 20_000 }, async t => {
  let stall = false;
  const h = await fixture(t, relay => relay.addHook('preHandler', async (request, reply) => {
    if (stall && request.method === 'POST' && request.url.endsWith('/messages') && String((request.body as any)?.text).includes('"kind":"probe"')) {
      await new Promise<void>(resolve => reply.raw.once('close', resolve));
    }
  }));
  const a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  stall = true;
  const result = await b.client.pair('4040', b.session, 100);
  assert.equal(result.verified, false); assert.equal(result.connection, undefined); assert.equal(result.timed_out, true);
  stall = false;
  const sent = await a.relay.sendChannel('4040', { session_id: a.session, generation: paired.connection!.generation, text: 'reader recovered', file_ids: [] });
  const page = await b.client.watch('4040', b.session, 2000); assert.equal(page.messages[0].id, sent.id);
  assert.equal((await b.client.pair('4040', b.session, 2000)).verified, true);
});

test('leave uses the latest persisted generation and stops the worker before removing membership', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  await b.client.leave('4040', b.session);
  assert.equal((await b.client.status()).channels.length, 0);
  assert.equal(JSON.parse(await readFile(b.config, 'utf8')).runtime.channels['4040'], undefined);
  await assert.rejects(b.client.watch('4040', b.session, 100), (error: any) => error.code === 'channel_session_replaced');
});

test('pane streams only the verified word and timing; local context and credentials stay behind authentication', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  const controller = new AbortController();
  const response = await fetch(a.info.url + '/ui/events?channel=4040', { signal: controller.signal });
  const chunk = await response.body!.getReader().read(); controller.abort();
  const value = JSON.parse(new TextDecoder().decode(chunk.value).split('data: ')[1].split('\n')[0]);
  assert.equal(value.word, paired.connection!.secret_word); assert.ok(value.proof_ms >= 0);
  for (const name of ['token', 'session_id', 'messages', 'connection', 'device']) assert.equal(value[name], undefined);
  assert.equal((await fetch(a.info.url + '/ui', { headers: { Origin: 'https://foreign.example' } })).status, 403);
  assert.equal((await fetch(a.info.url + '/v1/status')).status, 401);
});


test('slow control receipts do not delay the next fresh proof or the sole inbox reader', { timeout: 20_000 }, async t => {
  let receiptStarted = false;
  const h = await fixture(t, relay => relay.addHook('preHandler', async request => {
    if (request.url.endsWith('/ack')) { receiptStarted = true; await pause(400); }
  }));
  const a = await h.device('laptop'), b = await h.device('vm');
  await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  while (!receiptStarted) await pause(5);
  const proof = await a.client.pair('4040', a.session, 250);
  assert.equal(proof.verified, true); assert.ok(proof.proof_round_trip_ms! < 250);
});

test('pre-model adapter supplies only fresh verified context and leaves ordinary mail for the agent', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  const message = await b.relay.sendChannel('4040', { session_id: b.session, generation: paired.connection!.generation, text: 'private ordinary context', file_ids: [] });
  const child = spawn(process.execPath, ['.agents/skills/agent-bridge/scripts/prompt-hook.mjs'], { env: { ...process.env, BRIDGE_CONFIG: a.config }, stdio: ['pipe', 'pipe', 'pipe'] });
  let stdout = '', stderr = ''; child.stdout.on('data', chunk => { stdout += chunk; }); child.stderr.on('data', chunk => { stderr += chunk; });
  child.stdin.end(JSON.stringify({ hook_event_name: 'UserPromptSubmit', session_id: a.session, prompt: '/agent-bridge 4040' }));
  assert.equal((await once(child, 'exit'))[0], 0); assert.equal(stderr, '');
  const output = JSON.parse(stdout); assert.equal(output.continue, true); assert.equal(output.hookSpecificOutput.hookEventName, 'UserPromptSubmit');
  assert.ok(output.systemMessage.includes(paired.connection!.secret_word)); assert.ok(output.hookSpecificOutput.additionalContext.includes('watch --daemon'));
  assert.ok(!stdout.includes(message.text)); assert.ok(!stdout.includes(a.info.token));
  const page = await a.client.watch('4040', a.session, 1000); assert.equal(page.messages[0].id, message.id);
  assert.ok((await a.relay.channelInbox('4040', a.session, paired.connection!.generation, undefined, 0)).messages.some(record => record.id === message.id));
});

function runtimeBarrier() {
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  return { pending, release };
}

test('a probe response delayed after its durable commit does not strand ordinary receiving', { timeout: 20_000 }, async t => {
  const receipt = runtimeBarrier(), committed = runtimeBarrier(), echoed = runtimeBarrier();
  let delayedSession: string | undefined;
  const h = await fixture(t, relay => relay.addHook('onSend', async (request, _reply, payload) => {
    const input = request.body as { session_id?: string; text?: string } | undefined;
    if (delayedSession && request.method === 'POST' && request.url.endsWith('/messages')) {
      if (input?.session_id === delayedSession && input.text?.includes('"kind":"probe"')) {
        committed.release(); await receipt.pending;
      } else if (input?.text?.includes('"kind":"echo"')) echoed.release();
    }
    return payload;
  }));
  const a = await h.device('laptop'), b = await h.device('vm');
  const [paired, previous] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  delayedSession = b.session;
  const proving = b.client.pair('4040', b.session, 3000);
  try {
    await Promise.race([committed.pending, pause(1000).then(() => { throw new Error('Probe was not durably committed'); })]);
    await Promise.race([echoed.pending, pause(1000).then(() => { throw new Error('Peer did not durably echo the committed probe'); })]);
    const proof = await Promise.race([proving, pause(500).then(() => { throw new Error('Durable authenticated echo waited for the delayed probe receipt'); })]);
    assert.equal(proof.verified, true); assert.equal(proof.connection!.generation, paired.connection!.generation);
    assert.notEqual(proof.proof_nonce, previous.proof_nonce); assert.notEqual(proof.proof_message_id, previous.proof_message_id);
    // The same reader must continue receiving after publishing the proof,
    // while the original setup POST response remains held.
    const sent = await a.relay.sendChannel('4040', { session_id: a.session, generation: paired.connection!.generation, text: 'ordinary during a delayed probe receipt', file_ids: [] });
    const incoming = await b.client.watch('4040', b.session, 500);
    assert.equal(incoming.timed_out, false, 'Ordinary receiving waited behind a setup POST response');
    assert.equal(incoming.messages[0]?.id, sent.id);
    const durable = await b.relay.channelInbox('4040', b.session, paired.connection!.generation, undefined, 0);
    assert.ok(durable.messages.some(message => message.id === sent.id)); assert.ok(durable.acknowledged_cursor < sent.seq);
    receipt.release();
  } finally { delayedSession = undefined; receipt.release(); await proving; }
});

test('an echo response delayed after its durable commit does not hold the peer reader', { timeout: 20_000 }, async t => {
  const receipt = runtimeBarrier(), committed = runtimeBarrier();
  let delayedSession: string | undefined;
  const h = await fixture(t, relay => relay.addHook('onSend', async (request, _reply, payload) => {
    const input = request.body as { session_id?: string; text?: string } | undefined;
    if (delayedSession && request.method === 'POST' && request.url.endsWith('/messages') && input?.session_id === delayedSession && input.text?.includes('"kind":"echo"')) {
      committed.release(); await receipt.pending;
    }
    return payload;
  }));
  const a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  delayedSession = a.session;
  const proving = b.client.pair('4040', b.session, 3000);
  try {
    await Promise.race([committed.pending, pause(1000).then(() => { throw new Error('Echo was not durably committed'); })]);
    assert.equal((await proving).verified, true, 'The originator can consume a durable echo before its HTTP response finishes');
    const sent = await b.relay.sendChannel('4040', { session_id: b.session, generation: paired.connection!.generation, text: 'ordinary during a delayed echo receipt', file_ids: [] });
    const incoming = await a.client.watch('4040', a.session, 500);
    assert.equal(incoming.timed_out, false, 'The sole reader waited behind an echo POST response');
    assert.equal(incoming.messages[0]?.id, sent.id);
    receipt.release();
    const proof = await a.client.pair('4040', a.session, 3000);
    assert.equal(proof.verified, true); assert.ok(proof.messages.some(message => message.id === sent.id));
  } finally { delayedSession = undefined; receipt.release(); await proving; }
});

test('CLI pair emits its verified first line promptly and retains the same process for ordinary mail', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const started = performance.now();
  const child = spawn(process.execPath, ['dist/cli.js', 'channel', 'pair', '4051', '--session', a.session, '--daemon', '--watch', '--timeout', '5', '--watch-timeout', '5'], {
    env: { ...process.env, BRIDGE_CONFIG: a.config, CODEX_THREAD_ID: '' }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  const exited = once(child, 'exit'); let stdout = '', stderr = ''; const lines: any[] = [];
  const first = runtimeBarrier(), second = runtimeBarrier();
  child.stderr!.on('data', chunk => { stderr += chunk; });
  child.stdout!.on('data', chunk => {
    stdout += chunk;
    while (stdout.includes('\n')) {
      const end = stdout.indexOf('\n'), line = stdout.slice(0, end); stdout = stdout.slice(end + 1);
      if (line) lines.push(JSON.parse(line));
      if (lines.length === 1) first.release(); else if (lines.length === 2) second.release();
    }
  });
  t.after(async () => { if (child.exitCode === null) child.kill(); await exited; });
  const peer = await b.client.pair('4051', b.session, 5000);
  await Promise.race([first.pending, pause(2000).then(() => { throw new Error('CLI did not publish its first result promptly'); })]);
  assert.equal(peer.verified, true); assert.equal(lines[0].verified, true); assert.equal(lines[0].watching, true);
  assert.equal(lines[0].connection.secret_word, peer.connection!.secret_word);
  assert.ok(performance.now() - started < 3000, 'Local CLI startup and first setup result exceeded the prompt deadline');
  assert.equal(child.exitCode, null, 'The same CLI process must remain listening after publishing the word');
  const sent = await b.relay.sendChannel('4051', { session_id: b.session, generation: peer.connection!.generation, text: 'ordinary after the verified first line', file_ids: [] });
  await Promise.race([second.pending, pause(2000).then(() => { throw new Error('CLI watcher did not receive ordinary mail'); })]);
  assert.equal(lines[1].messages[0]?.id, sent.id); assert.equal(lines[1].timed_out, false);
  assert.equal((await exited)[0], 0); assert.equal(stderr, '');
});

test('initial resident setup verifies the same word with either staggered join order', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  for (const [channel, first, second] of [['4052', a, b], ['4053', b, a]] as const) {
    const waiting = first.client.pair(channel, first.session, 3000);
    const deadline = performance.now() + 1000;
    while (!(await first.client.status()).channels.find(value => value.channel === channel)?.connection) {
      assert.ok(performance.now() < deadline, 'First join did not create its waiting membership'); await pause(5);
    }
    const [left, right] = await Promise.all([waiting, second.client.pair(channel, second.session, 3000)]);
    assert.equal(left.verified, true); assert.equal(right.verified, true);
    assert.equal(left.connection!.secret_word, right.connection!.secret_word);
    assert.equal(left.connection!.generation, right.connection!.generation);
    assert.equal(left.connection!.pairing_id, right.connection!.pairing_id);
    assert.notEqual(left.proof_nonce, right.proof_nonce);
  }
});

test('a corrected session ownership rejection recreates the stopped worker under the same UUID', { timeout: 20_000 }, async t => {
  let rejectedSession: string | undefined, rejectOnce = true;
  const h = await fixture(t, relay => relay.addHook('preHandler', async (request, reply) => {
    if (rejectOnce && request.method === 'POST' && request.url.endsWith('/join') && (request.body as { session_id?: string })?.session_id === rejectedSession) {
      rejectOnce = false;
      return reply.code(403).send({ error: { code: 'channel_session_mismatch', message: 'This session belongs to another device' } });
    }
  }));
  const a = await h.device('laptop'), b = await h.device('vm'); rejectedSession = a.session;
  await assert.rejects(a.client.pair('4054', a.session, 3000), (error: any) => error.code === 'channel_session_mismatch');
  assert.equal((await a.client.status()).channels.find(value => value.channel === '4054')?.transport, 'stopped');
  const [left, right] = await Promise.all([a.client.pair('4054', a.session, 3000), b.client.pair('4054', b.session, 3000)]);
  assert.equal(left.verified, true); assert.equal(right.verified, true);
  assert.equal(left.session_id, a.session); assert.equal(right.session_id, b.session);
  assert.equal(left.connection!.secret_word, right.connection!.secret_word);
  assert.equal((await a.client.status()).channels.find(value => value.channel === '4054')?.transport, 'online');
});

test('a canceled local pair caller does not prevent later bootstrap verification in the pane', { timeout: 20_000 }, async t => {
  const h = await fixture(t), a = await h.device('laptop'), b = await h.device('vm');
  const caller = new AbortController();
  const canceled = a.client.pair('4055', a.session, 3000, caller.signal);
  // Attach rejection handling before deliberately closing the local HTTP caller.
  void canceled.catch(() => {});
  const deadline = performance.now() + 1000;
  while (!(await a.client.status()).channels.find(value => value.channel === '4055')?.connection) {
    assert.ok(performance.now() < deadline, 'Canceled caller did not create its waiting membership'); await pause(5);
  }
  caller.abort();
  await assert.rejects(canceled, (error: any) => error.name === 'AbortError');
  const peer = await b.client.pair('4055', b.session, 3000); assert.equal(peer.verified, true);
  // No second local pair invocation is allowed: the resident bootstrap owns
  // its proof lifetime independently of the now-canceled local caller.
  const verifiedDeadline = performance.now() + 1000;
  let local = (await a.client.status()).channels.find(value => value.channel === '4055')!;
  while (!local.verified_at) {
    assert.ok(performance.now() < verifiedDeadline, 'Independent bootstrap did not publish a verified word after the caller canceled');
    await pause(5); local = (await a.client.status()).channels.find(value => value.channel === '4055')!;
  }
  assert.equal(local.session_id, a.session); assert.equal(local.connection?.secret_word, peer.connection!.secret_word);
  const controller = new AbortController();
  const response = await fetch(a.info.url + '/ui/events?channel=4055', { signal: controller.signal });
  try {
    const chunk = await response.body!.getReader().read();
    const pane = JSON.parse(new TextDecoder().decode(chunk.value).split('data: ')[1].split('\n')[0]);
    assert.equal(pane.word, peer.connection!.secret_word); assert.equal(pane.transport, 'online'); assert.ok(pane.verified_at);
  } finally { controller.abort(); }
});

test('explicit ordinary acknowledgment waits for an earlier unfinished echo receipt', { timeout: 20_000 }, async t => {
  const receipt = runtimeBarrier(), committed = runtimeBarrier();
  let delayedSession: string | undefined, heldNonce: string | undefined;
  const h = await fixture(t, relay => relay.addHook('onSend', async (request, _reply, payload) => {
    const input = request.body as { session_id?: string; text?: string } | undefined;
    if (delayedSession && request.method === 'POST' && request.url.endsWith('/messages') && input?.session_id === delayedSession && input.text?.includes('"kind":"echo"')) {
      heldNonce = JSON.parse(input.text.slice('agent-bridge control v2: '.length)).nonce;
      committed.release(); await receipt.pending;
    }
    return payload;
  }));
  const a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  delayedSession = a.session;
  const proving = b.client.pair('4040', b.session, 3000);
  let acknowledging: Promise<unknown> | undefined;
  try {
    await Promise.race([committed.pending, pause(1000).then(() => { throw new Error('Held echo was not committed'); })]);
    assert.equal((await proving).verified, true);
    const sent = await b.relay.sendChannel('4040', { session_id: b.session, generation: paired.connection!.generation, text: 'processed ordinary mail after a pending setup reply', file_ids: [] });
    const incoming = await a.client.watch('4040', a.session, 500);
    assert.equal(incoming.timed_out, false); assert.equal(incoming.messages[0]?.id, sent.id);
    let settled = false;
    acknowledging = a.client.acknowledge('4040', a.session, paired.connection!.generation, incoming.cursor).then(value => { settled = true; return value; });
    await pause(100);
    assert.equal(settled, false, 'Ordinary acknowledgment crossed an unfinished setup reply');
    const pending = await a.relay.channelInbox('4040', a.session, paired.connection!.generation, undefined, 0);
    assert.ok(pending.messages.some(message => message.id === sent.id));
    assert.ok(pending.messages.some(message => message.text.includes('"kind":"probe"') && message.text.includes(heldNonce!)), 'The original probe must remain durable until its echo receipt completes');
    receipt.release(); await acknowledging;
    const cleared = await a.relay.channelInbox('4040', a.session, paired.connection!.generation, undefined, 0);
    assert.ok(!cleared.messages.some(message => message.id === sent.id));
    assert.ok(!cleared.messages.some(message => message.text.includes('"kind":"probe"') && message.text.includes(heldNonce!)));
  } finally { delayedSession = undefined; receipt.release(); await proving; await acknowledging; }
});

test('pairing replacement rotates the nonce while an old probe receipt and echo are pending', { timeout: 20_000 }, async t => {
  const oldReceipt = runtimeBarrier(), oldCommitted = runtimeBarrier(), oldEcho = runtimeBarrier(), releaseEcho = runtimeBarrier();
  let probingSession: string | undefined, oldPeerSession: string | undefined, oldPairing: string | undefined, oldNonce: string | undefined;
  const probes: { nonce: string; pairing_id: string }[] = [];
  const h = await fixture(t, relay => {
    relay.addHook('preHandler', async request => {
      const input = request.body as { session_id?: string; text?: string } | undefined;
      if (oldPairing && request.method === 'POST' && request.url.endsWith('/messages') && input?.session_id === oldPeerSession && input.text?.includes('"kind":"echo"')) {
        const frame = JSON.parse(input.text.slice('agent-bridge control v2: '.length));
        if (frame.pairing_id === oldPairing) { oldEcho.release(); await releaseEcho.pending; }
      }
    });
    relay.addHook('onSend', async (request, _reply, payload) => {
      const input = request.body as { session_id?: string; text?: string } | undefined;
      if (oldPairing && request.method === 'POST' && request.url.endsWith('/messages') && input?.session_id === probingSession && input.text?.includes('"kind":"probe"')) {
        const frame = JSON.parse(input.text.slice('agent-bridge control v2: '.length)); probes.push(frame);
        if (frame.pairing_id === oldPairing) { oldNonce = frame.nonce; oldCommitted.release(); await oldReceipt.pending; }
      }
      return payload;
    });
  });
  const a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  probingSession = a.session; oldPeerSession = b.session; oldPairing = paired.connection!.pairing_id;
  const proving = a.client.pair('4040', a.session, 4000); void proving.catch(() => {});
  let replacing: ReturnType<DaemonClient['pair']> | undefined;
  try {
    await Promise.race([oldCommitted.pending, pause(1000).then(() => { throw new Error('Original probe was not committed'); })]);
    await Promise.race([oldEcho.pending, pause(1000).then(() => { throw new Error('Original peer did not read the old nonce'); })]);
    const replacementSession = randomUUID();
    replacing = b.client.pair('4040', replacementSession, 4000); void replacing.catch(() => {});
    // Keep both obsolete HTTP operations held while the replacement pairing
    // proves itself. Neither receipt nor stale nonce can complete the new pair.
    const [fresh, peer] = await Promise.race([
      Promise.all([proving, replacing]),
      pause(2000).then(() => { throw new Error('Replacement setup remained blocked behind an obsolete probe or echo'); }),
    ]);
    assert.equal(fresh.verified, true); assert.equal(peer.verified, true);
    assert.equal(fresh.connection!.generation, paired.connection!.generation);
    assert.notEqual(fresh.connection!.pairing_id, oldPairing);
    assert.equal(fresh.connection!.peer!.session_id, replacementSession);
    assert.notEqual(fresh.proof_nonce, oldNonce, 'A replacement pairing must learn a new unpredictable nonce');
    assert.ok(probes.some(value => value.pairing_id === fresh.connection!.pairing_id && value.nonce === fresh.proof_nonce));
    assert.ok(!probes.some(value => value.pairing_id === fresh.connection!.pairing_id && value.nonce === oldNonce));
    assert.equal(fresh.connection!.secret_word, peer.connection!.secret_word);
    oldReceipt.release(); releaseEcho.release();
  } finally {
    probingSession = undefined; oldPeerSession = undefined; oldPairing = undefined;
    oldReceipt.release(); releaseEcho.release(); await Promise.allSettled([proving, ...(replacing ? [replacing] : [])]);
  }
});

test('transport retry of an old echo cannot deliver its captured frame to a replacement peer', { timeout: 20_000 }, async t => {
  const rejected = runtimeBarrier(); let rejectOnce = true;
  let echoingSession: string | undefined, oldPairing: string | undefined, oldNonce: string | undefined;
  const h = await fixture(t, relay => relay.addHook('preHandler', async (request, reply) => {
    const input = request.body as { session_id?: string; text?: string } | undefined;
    if (rejectOnce && oldPairing && request.method === 'POST' && request.url.endsWith('/messages') && input?.session_id === echoingSession && input.text?.includes('"kind":"echo"')) {
      const frame = JSON.parse(input.text.slice('agent-bridge control v2: '.length));
      if (frame.pairing_id === oldPairing) {
        rejectOnce = false; oldNonce = frame.nonce; rejected.release();
        return reply.code(503).send({ error: { code: 'server_closing', message: 'Retry after this controlled relay interruption' } });
      }
    }
  }));
  const a = await h.device('laptop'), b = await h.device('vm');
  const [paired] = await Promise.all([a.client.pair('4040', a.session, 5000), b.client.pair('4040', b.session, 5000)]);
  echoingSession = a.session; oldPairing = paired.connection!.pairing_id;
  const obsolete = b.client.pair('4040', b.session, 3000); void obsolete.catch(() => {});
  try {
    await Promise.race([rejected.pending, pause(1000).then(() => { throw new Error('Old echo did not reach the controlled retry'); })]);
    const replacementSession = randomUUID();
    const fresh = await b.client.pair('4040', replacementSession, 3000); assert.equal(fresh.verified, true);
    assert.notEqual(fresh.connection!.pairing_id, oldPairing);
    // RelayClient's first automatic retry waits250 ms. The runtime has already
    // observed and confirmed the replacement before that retry can execute.
    await pause(350);
    const replay = await b.relay.channelInbox('4040', replacementSession, fresh.connection!.generation, 0, 0);
    assert.ok(!replay.messages.some(message => message.text.includes(oldPairing!) && message.text.includes(oldNonce!)), 'An old transport retry was routed to the new peer despite its captured pairing');
    assert.equal((await b.client.status()).channels.find(value => value.channel === '4040')!.pending_messages, 0);
  } finally { echoingSession = undefined; oldPairing = undefined; await Promise.allSettled([obsolete]); }
});
