import Fastify from 'fastify';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { readFile, rm } from 'node:fs/promises';
import { z } from 'zod';
import { BridgeError, channelIdSchema } from '../shared/protocol.js';
import { configPath, readConfig, updateConfig, writePrivateJson } from './config.js';
import { actorKey, daemonInfoPath, type DaemonInfo } from './daemon-client.js';
import { RelayClient } from './relay.js';
import { leaveChannel } from './channel.js';
import { bridgePane } from './daemon-ui.js';
import type { ServerResponse } from 'node:http';
import { ChannelRuntime } from './runtime.js';

const sessionBody = z.object({ session_id: z.string().uuid(), timeout_ms: z.number().int().min(1).max(2_147_483_000).default(600_000) }).strict();


export async function runDaemon(onReady?: (info: Omit<DaemonInfo, 'token'>) => void) {
  // Exclusive loopback bind is the owner lock. The OS releases it on crash;
  // there is no stale PID file to reclaim or accidentally steal from a winner.
  let config = await readConfig();
  const actor = actorKey(config);
  const token = randomBytes(32).toString('base64url'); const file = daemonInfoPath();
  const app = Fastify({ logger: false, bodyLimit: 64 * 1024 });
  const workers = new Map<string, ChannelRuntime>(); const transitions = new Map<string, Promise<unknown>>();
  let url = '';
  const panes = new Map<ServerResponse, string>();
  const publicStatus = async (channel: string) => {
    if (actorKey(await readConfig()) !== actor) return { channel, transport: 'configuration changed', word: null };
    const status = workers.get(channel)?.status();
    return { channel, transport: status?.transport ?? 'waiting', word: status?.verified_at && status.transport === 'online' ? status.connection?.secret_word : null, verified_at: status?.verified_at ?? null, proof_ms: status?.proof_round_trip_ms ?? null };
  };
  const publish = () => { for (const [response, channel] of panes) void publicStatus(channel).then(status => { if (!response.destroyed) response.write(`data: ${JSON.stringify(status)}\n\n`); }).catch(() => { response.end(); }); };
  app.addHook('onRequest', async request => {
    if (url && request.headers.host !== new URL(url).host) throw new BridgeError(403, 'invalid_host', 'Local bridge requests must use the loopback origin');
    if (request.method === 'GET' && ['/ui', '/ui/events'].includes(request.url.split('?')[0])) {
      if (request.headers.origin && request.headers.origin !== url) throw new BridgeError(403, 'invalid_origin', 'The pane is available only on its loopback origin');
      return;
    }
    const auth = request.headers.authorization;
    const actual = Buffer.from(auth ?? ''), expected = Buffer.from(`Bearer ${token}`);
    if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) throw new BridgeError(401, 'unauthorized', 'A local runtime token is required');
    if (request.url !== '/v1/stop' && actorKey(await readConfig()) !== actor) throw new BridgeError(409, 'CONFIG_CHANGED', 'Restart the daemon after changing credentials');
  });
  app.setErrorHandler((error, _request, reply) => {
    if (error instanceof BridgeError) return reply.code(error.status >= 400 && error.status <= 599 ? error.status : 409).send({ error: { code: error.code, message: error.message } });
    if (error instanceof z.ZodError) return reply.code(400).send({ error: { code: 'invalid_input', message: 'Invalid local bridge request' } });
    return reply.code(500).send({ error: { code: 'DAEMON_ERROR', message: 'Local bridge operation failed' } });
  });
  let stopping = false;
  async function serialize<T>(channel: string, operation: () => Promise<T>): Promise<T> {
    const previous = transitions.get(channel);
    const transition = (async () => {
      if (previous) await previous.catch(() => {});
      if (stopping) throw new BridgeError(409, 'RUNTIME_RESTARTING', 'The runtime is restarting');
      return operation();
    })();
    transitions.set(channel, transition);
    try { return await transition; } finally { if (transitions.get(channel) === transition) transitions.delete(channel); }
  }
  async function worker(channel: string, session: string, create: boolean) {
    return serialize(channel, async () => {
      const current = workers.get(channel);
      if (current?.session === session && current.status().transport !== 'stopped') return current;
      if (!create) throw new BridgeError(409, 'channel_session_replaced', 'This channel belongs to a different chat or has not been paired');
      if (current) await current.close('channel_session_replaced');
      const fresh = await updateConfig(current => {
        if (!current || current.url !== config.url || current.token !== config.token || current.device !== config.device) throw new BridgeError(0, 'CONFIG_CHANGED', 'Restart the runtime after changing credentials');
        return { ...current, runtime: { ...current.runtime!, channels: { ...current.runtime!.channels, [channel]: session } } };
      });
      const next = new ChannelRuntime(new RelayClient(fresh.url, fresh.token!), fresh, channel, session); workers.set(channel, next); next.onChange(publish);
      // Keep initial proof alive after a bounded hook or CLI caller disconnects.
      // The live pane can then show the verified word as soon as the peer joins.
      void next.pair(600_000).catch(() => {});
      return next;
    });
  }
  app.get('/ui', async (_request, reply) => reply.header('Cache-Control', 'no-store').header('X-Content-Type-Options', 'nosniff').header('Content-Security-Policy', "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'").type('text/html').send(bridgePane));
  app.get('/ui/events', async (request, reply) => {
    const { channel } = z.object({ channel: channelIdSchema }).strict().parse(request.query);
    const initial = await publicStatus(channel);
    reply.hijack(); reply.raw.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
    panes.set(reply.raw, channel); reply.raw.write(`data: ${JSON.stringify(initial)}\n\n`);
    reply.raw.once('close', () => panes.delete(reply.raw));
  });
  app.get('/v1/status', async () => ({ running: true, version: 1, actor, pid: process.pid, channels: [...workers.values()].map(runtime => runtime.status()) }));
  for (const operation of ['pair', 'watch'] as const) app.post(`/v1/channels/:channel/${operation}`, async (request, reply) => {
    const { channel } = z.object({ channel: channelIdSchema }).parse(request.params); const input = sessionBody.parse(request.body);
    const runtime = await worker(channel, input.session_id, operation === 'pair'); const controller = new AbortController();
    const stop = () => controller.abort(); reply.raw.once('close', stop);
    try { return await runtime[operation](input.timeout_ms, controller.signal); } finally { reply.raw.off('close', stop); }
  });
  app.post('/v1/channels/:channel/ack', async request => {
    const { channel } = z.object({ channel: channelIdSchema }).parse(request.params);
    const input = z.object({ session_id: z.string().uuid(), generation: z.string().uuid(), cursor: z.number().int().nonnegative() }).strict().parse(request.body);
    const runtime = await worker(channel, input.session_id, false);
    if (runtime.status().connection?.generation !== input.generation) throw new BridgeError(409, 'stale_generation', 'The acknowledgment belongs to an earlier generation');
    return runtime.acknowledge(input.cursor);
  });
  app.post('/v1/channels/:channel/leave', async request => {
    const { channel } = z.object({ channel: channelIdSchema }).parse(request.params);
    const input = z.object({ session_id: z.string().uuid() }).strict().parse(request.body);
    return serialize(channel, async () => {
      const runtime = workers.get(channel);
      if (runtime?.session !== input.session_id) throw new BridgeError(409, 'channel_session_replaced', 'This channel belongs to a different chat');
      await runtime.close('channel_session_replaced');
      if (workers.get(channel) === runtime) workers.delete(channel);
      await updateConfig(current => {
        if (!current || actorKey(current) !== actor) throw new BridgeError(0, 'CONFIG_CHANGED', 'Credentials changed while leaving');
        const channels = { ...current.runtime!.channels }; delete channels[channel]; return { ...current, runtime: { ...current.runtime!, channels } };
      });
      const fresh = await readConfig();
      if (actorKey(fresh) !== actor) throw new BridgeError(0, 'CONFIG_CHANGED', 'Credentials changed while leaving');
      return { left: true, ...await leaveChannel(new RelayClient(config.url, config.token!), fresh, channel, input.session_id) };
    });
  });
  let shutdownWork: Promise<void> | undefined;
  let ownsListener = false;
  const removeInfo = async () => { try { const current = JSON.parse(await readFile(file, 'utf8')); if (current.token === token) await rm(file, { force: true }); } catch {} };
  const shutdown = () => shutdownWork ??= (async () => { stopping = true; await Promise.allSettled([...transitions.values()]); await Promise.allSettled([...workers.values()].map(runtime => runtime.close())); if (ownsListener) await removeInfo(); for (const response of panes.keys()) response.end(); await app.close(); })();
  app.post('/v1/stop', async () => { setTimeout(() => { void shutdown(); }, 10); return { stopped: true }; });
  app.addHook('onClose', async () => {
    await Promise.allSettled([...workers.values()].map(runtime => runtime.close()));
    // Metadata is removed before the listener is released in shutdown().
  });
  const closed = new Promise<void>(resolve => app.addHook('onClose', async () => { resolve(); }));
  try {
    // First startup asks the OS for an available port, avoiding Windows/Hyper-V
    // excluded ranges. Binding and publishing the port share the config lock:
    // a concurrent starter then sees the winning port and cannot bind it.
    config = await updateConfig(async current => {
      if (!current || actorKey(current) !== actor) throw new BridgeError(0, 'CONFIG_CHANGED', 'Credentials changed while starting the runtime');
      try { url = await app.listen({ host: '127.0.0.1', port: current.runtime?.port ?? 0 }); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'EACCES') throw error; url = await app.listen({ host: '127.0.0.1', port: 0 }); }
      ownsListener = true;
      return { ...current, runtime: { port: Number(new URL(url).port), channels: current.runtime?.channels ?? {} } };
    });
    await writePrivateJson(file, { version: 1, url, token, pid: process.pid, actor });
    for (const [channel, session] of Object.entries(config.runtime!.channels)) {
      await worker(channel, session, true);
    }
    onReady?.({ version: 1, url, pid: process.pid, actor });
    const stop = () => { void shutdown(); }; process.once('SIGINT', stop); process.once('SIGTERM', stop);
    try { await closed; } finally { process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop); }
  } catch (error) { if (ownsListener) await removeInfo(); await app.close().catch(() => {}); if ((error as NodeJS.ErrnoException).code === 'EADDRINUSE') throw new BridgeError(0, 'DAEMON_BUSY', 'A runtime already owns this configuration port, or another application uses it'); throw error; }
}
