import { createHash } from 'node:crypto';
import { setTimeout as pause } from 'node:timers/promises';
import { readFile } from 'node:fs/promises';
import { z } from 'zod';
import { performance } from 'node:perf_hooks';
import { BridgeError, channelIdSchema } from '../shared/protocol.js';
import { configPath, readConfig, type BridgeConfig } from './config.js';
import type { ChannelRuntime, RuntimePairResult } from './runtime.js';

export interface DaemonInfo { version: 1; url: string; token: string; pid: number; actor: string }
const daemonInfoSchema = z.object({ version: z.literal(1), url: z.string().url(), token: z.string().min(32).max(128), pid: z.number().int().positive(), actor: z.string().length(64) }).strict();
const sessionBody = z.object({ session_id: z.string().uuid(), timeout_ms: z.number().int().min(1).max(2_147_483_000).default(600_000) }).strict();
export const actorKey = (config: BridgeConfig) => createHash('sha256').update(JSON.stringify([config.url, config.token, config.device])).digest('hex');
export const daemonInfoPath = (path = configPath()) => `${path}.daemon.json`;
export async function daemonInfo(requireActor = true, path = configPath()): Promise<DaemonInfo> {
  let raw; try { raw = JSON.parse(await readFile(daemonInfoPath(path), 'utf8')); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') throw new BridgeError(0, 'DAEMON_REQUIRED', 'Run bridge daemon start first'); throw error; }
  const info = daemonInfoSchema.parse(raw); const url = new URL(info.url);
  if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || url.username || url.password || url.search || url.hash || url.pathname !== '/' || !url.port) throw new BridgeError(0, 'DAEMON_INVALID', 'Local daemon must use a loopback HTTP origin');
  if (requireActor && info.actor !== actorKey(await readConfig(true, path))) throw new BridgeError(0, 'CONFIG_CHANGED', 'Restart the daemon after changing relay credentials');
  return info;
}
export class DaemonClient {
  constructor(private info: DaemonInfo, private readonly configFile = configPath()) {}
  async request<T>(path: string, body?: unknown, timeoutMs = 600_500, signal?: AbortSignal): Promise<T> {
    const deadline = AbortSignal.timeout(timeoutMs);
    try { return await this.attempt<T>(path, body, signal ? AbortSignal.any([signal, deadline]) : deadline); }
    catch (error) { if (deadline.aborted && !signal?.aborted) throw new BridgeError(0, 'DAEMON_TIMEOUT', 'The local runtime did not answer before the deadline'); throw error; }
  }
  private async attempt<T>(path: string, body: unknown, stop: AbortSignal): Promise<T> {
    const reconnect = path.endsWith('/watch') || path.endsWith('/pair');
    const input = reconnect ? sessionBody.parse(body) : undefined;
    const started = performance.now(), deadline = input ? started + input.timeout_ms : undefined;
    while (true) {
      try {
        const remaining = deadline === undefined ? undefined : Math.max(1, Math.ceil(deadline - performance.now()));
        // Bound local long polls below native fetch's headers timeout. Keep the
        // caller's original deadline across chunks and daemon reconnections.
        const requestBody = input ? { ...input, timeout_ms: Math.min(25_000, remaining!) } : body;
        const response = await fetch(this.info.url + path, { method: requestBody === undefined ? 'GET' : 'POST', headers: { Authorization: `Bearer ${this.info.token}`, ...(requestBody === undefined ? {} : { 'Content-Type': 'application/json' }) }, body: requestBody === undefined ? undefined : JSON.stringify(requestBody), signal: stop });
        const result = await response.json() as { error?: { code: string; message: string }; timed_out?: boolean; runtime_setup_ms?: number };
        if (!response.ok) throw new BridgeError(response.status, result.error?.code ?? 'DAEMON_ERROR', result.error?.message ?? 'Local daemon request failed');
        if (input && remaining! > 25_000 && result.timed_out === true) continue;
        if (input && path.endsWith('/pair') && typeof result.runtime_setup_ms === 'number') result.runtime_setup_ms = performance.now() - started;
        return result as T;
      } catch (error) {
        let retryable = error instanceof TypeError || error instanceof BridgeError && error.code === 'RUNTIME_RESTARTING';
        if (reconnect && error instanceof BridgeError && error.code === 'unauthorized') {
          const fresh = await daemonInfo(true, this.configFile);
          if (fresh.actor === this.info.actor && fresh.token !== this.info.token) { this.info = fresh; continue; }
        }
        if (!reconnect || stop.aborted || !retryable) throw error;
        while (!stop.aborted) {
          await pause(50, undefined, { signal: stop });
          let fresh;
          try { fresh = await daemonInfo(true, this.configFile); } catch (error) { if (error instanceof BridgeError && error.code === 'DAEMON_REQUIRED') continue; throw error; }
          if (fresh.actor !== this.info.actor) throw new BridgeError(0, 'CONFIG_CHANGED', 'Local runtime identity changed during reconnection');
          this.info = fresh; break;
        }
      }
    }
  }
  async status() {
    const result = await this.request<{ running: true; version: 1; actor: string; pid: number; channels: ReturnType<ChannelRuntime['status']>[] }>('/v1/status', undefined, 1000);
    if (result.running !== true || result.version !== 1 || result.actor !== this.info.actor || result.pid !== this.info.pid || !Array.isArray(result.channels)) throw new BridgeError(0, 'DAEMON_INVALID', 'The saved port is not owned by this bridge runtime');
    return result;
  }
  private wait<T>(channel: string, operation: 'pair' | 'watch', session: string, timeoutMs: number, signal?: AbortSignal) {
    channelIdSchema.parse(channel);
    return this.request<T>(`/v1/channels/${channel}/${operation}`, { session_id: session, timeout_ms: timeoutMs }, timeoutMs + 500, signal);
  }
  pair(channel: string, session: string, timeoutMs = 600_000, signal?: AbortSignal) { return this.wait<RuntimePairResult>(channel, 'pair', session, timeoutMs, signal); }
  watch(channel: string, session: string, timeoutMs = 600_000, signal?: AbortSignal) { return this.wait<ReturnType<ChannelRuntime['inbox']> & { timed_out: boolean; channel: string; session_id: string }>(channel, 'watch', session, timeoutMs, signal); }
  acknowledge(channel: string, session: string, generation: string, cursor: number) { channelIdSchema.parse(channel); return this.request<{ acknowledged_cursor: number }>(`/v1/channels/${channel}/ack`, { session_id: session, generation, cursor }, 40_500); }
  leave(channel: string, session: string) { channelIdSchema.parse(channel); return this.request<{ left: true }>(`/v1/channels/${channel}/leave`, { session_id: session }, 40_500); }
  stop() { return this.request<{ stopped: true }>('/v1/stop', {}, 5000); }
}
