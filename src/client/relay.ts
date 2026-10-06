import { createWriteStream } from 'node:fs';
import { lstat, mkdir, open, rm } from 'node:fs/promises';
import { basename, dirname, resolve } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { BridgeError, type ChannelInbox, type ChannelMessage, type ChannelMessageInput, type ChannelStatus, type Device, type FileInfo, type Message, type MessageInput, type RemoteError, type RemoteRequest, type RequestInput } from '../shared/protocol.js';

export function validateRelayUrl(raw: string): string {
  let url: URL;
  try { url = new URL(raw); } catch { throw new BridgeError(0, 'INVALID_URL', 'Relay URL must be an HTTPS URL'); }
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
  if (url.protocol !== 'https:' && !(url.protocol === 'http:' && loopback)) throw new BridgeError(0, 'INVALID_URL', 'HTTPS is required except for loopback development');
  if (url.username || url.password || url.search || url.hash) throw new BridgeError(0, 'INVALID_URL', 'Relay URL cannot contain credentials, query parameters, or a fragment');
  if (url.pathname !== '/' && url.pathname !== '') throw new BridgeError(0, 'INVALID_URL', 'Relay URL must identify the server origin');
  return url.origin;
}
const transient = new Set([429, 502, 503, 504]);
const delay = (ms: number, signal?: AbortSignal) => new Promise<void>((resolvePromise, reject) => {
  if (signal?.aborted) return reject(signal.reason);
  const done = () => { clearTimeout(timer); signal?.removeEventListener('abort', aborted); };
  const timer = setTimeout(() => { done(); resolvePromise(); }, ms);
  const aborted = () => { done(); reject(signal?.reason); };
  signal?.addEventListener('abort', aborted, { once: true });
});

export class RelayClient {
  readonly url: string;
  constructor(url: string, private token: string) { this.url = validateRelayUrl(url); if (!token) throw new BridgeError(0, 'TOKEN_REQUIRED', 'A device or admin token is required'); }
  private async fetch(path: string, init: RequestInit = {}, retry = true, timeoutMs = 40_000): Promise<Response> {
    for (let attempt = 0; ; attempt++) {
      try {
        const timeout = AbortSignal.timeout(timeoutMs);
        const signal = init.signal ? AbortSignal.any([init.signal, timeout]) : timeout;
        const response = await fetch(`${this.url}${path}`, { ...init, headers: { Authorization: `Bearer ${this.token}`, ...init.headers }, signal });
        if (!response.ok) {
          // Drain small error responses before reusing keep-alive sockets. Canceling
          // a response body can strand the next POST in some native fetch versions.
          let body: { error?: { code?: string; message?: string } } = {};
          if (response.body) {
            const reader = response.body.getReader(); const chunks: Buffer[] = []; let size = 0;
            try {
              while (true) {
                const next = await reader.read(); if (next.done) break;
                size += next.value.length; if (size > 64 * 1024) { await reader.cancel(); break; }
                chunks.push(Buffer.from(next.value));
              }
              if (size <= 64 * 1024) { try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch {} }
            } finally { reader.releaseLock(); }
          }
          if (retry && transient.has(response.status) && body.error?.code !== 'device_offline' && attempt < 3) {
            await delay(Math.min(3000, 250 * 2 ** attempt), init.signal ?? undefined); continue;
          }
          const code = body.error?.code ?? 'HTTP_ERROR';
          const message = code === 'channel_session_mismatch'
            ? 'This session belongs to another device. Use this chat\'s own session UUID, not the peer\'s session UUID.'
            : response.status === 403 && code === 'HTTP_ERROR'
              ? 'Relay returned HTTP 403 without a bridge error code. Check this computer\'s relay URL and network or proxy access.'
              : body.error?.message ?? `Relay returned HTTP ${response.status}`;
          throw new BridgeError(response.status, code, message);
        }
        return response;
      } catch (error) {
        if (!retry || error instanceof BridgeError || init.signal?.aborted || attempt >= 3) throw error;
        await delay(Math.min(3000, 250 * 2 ** attempt), init.signal ?? undefined);
      }
    }
  }
  private async json<T>(path: string, method = 'GET', body?: unknown, key?: string, signal?: AbortSignal, requestHeaders: Record<string, string> = {}): Promise<T> {
    const headers: Record<string, string> = { ...requestHeaders };
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (key) headers['Idempotency-Key'] = key;
    const response = await this.fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal }, method === 'GET' || !!key);
    if (response.status === 204) return {} as T;
    return response.json() as Promise<T>;
  }
  async register(name: string) { return this.json<{ device: Device; token: string }>('/v1/devices', 'POST', { name }); }
  async revoke(name: string) { return this.json<{ revoked: boolean }>(`/v1/devices/${encodeURIComponent(name)}`, 'DELETE'); }
  async devices() { return (await this.json<{ devices: Device[] }>('/v1/devices')).devices; }
  async heartbeat(roots: string[], signal?: AbortSignal) { return this.json('/v1/heartbeat', 'POST', { roots }, undefined, signal); }
  async send(input: MessageInput, idempotencyKey = randomUUID()) { return (await this.json<{ message: Message }>('/v1/messages', 'POST', input, idempotencyKey)).message; }
  async inbox(after = 0, wait = 25) { return this.json<{ messages: Message[]; cursor: number }>(`/v1/messages?after=${after}&wait=${wait}`); }
  async joinChannel(channel: string, sessionId: string, secretWord: string, idempotencyKey: string = randomUUID(), signal?: AbortSignal) {
    return this.json<ChannelStatus>(`/v1/channels/${encodeURIComponent(channel)}/join`, 'POST', { session_id: sessionId, secret_word: secretWord }, idempotencyKey, signal);
  }
  async channelStatus(channel: string, sessionId: string, generation: string, wait = 0, signal?: AbortSignal) {
    const query = new URLSearchParams({ session_id: sessionId, generation, wait: String(wait) });
    return this.json<ChannelStatus>(`/v1/channels/${encodeURIComponent(channel)}?${query}`, 'GET', undefined, undefined, signal);
  }
  async confirmChannel(channel: string, sessionId: string, generation: string, secretWord: string, pairingId: string, signal?: AbortSignal) {
    // Native fetch can strand a subsequent long poll when it reuses a socket
    // after a confirmation POST. Close these small handshake responses only.
    return this.json<ChannelStatus>(`/v1/channels/${encodeURIComponent(channel)}/confirm`, 'POST', { session_id: sessionId, generation, secret_word: secretWord, pairing_id: pairingId }, randomUUID(), signal, { Connection: 'close' });
  }
  async leaveChannel(channel: string, sessionId: string, generation: string, pairingId: string) {
    return this.json<Record<string, never>>(`/v1/channels/${encodeURIComponent(channel)}/sessions/${encodeURIComponent(sessionId)}?${new URLSearchParams({ generation, pairing_id: pairingId })}`, 'DELETE', undefined, randomUUID());
  }
  async sendChannel(channel: string, input: ChannelMessageInput, idempotencyKey: string = randomUUID(), signal?: AbortSignal) {
    return (await this.json<{ message: ChannelMessage }>(`/v1/channels/${encodeURIComponent(channel)}/messages`, 'POST', input, idempotencyKey, signal)).message;
  }
  async channelInbox(channel: string, sessionId: string, generation: string, after?: number, wait = 25, connection = false, signal?: AbortSignal) {
    const query = new URLSearchParams({ session_id: sessionId, generation, wait: String(wait) });
    if (after !== undefined) query.set('after', String(after));
    if (connection) query.set('state', '1');
    return this.json<ChannelInbox>(`/v1/channels/${encodeURIComponent(channel)}/messages?${query}`, 'GET', undefined, undefined, signal);
  }
  async acknowledgeChannel(channel: string, sessionId: string, generation: string, cursor: number, signal?: AbortSignal) {
    return this.json<{ acknowledged_cursor: number }>(`/v1/channels/${encodeURIComponent(channel)}/ack`, 'POST', { session_id: sessionId, generation, cursor }, randomUUID(), signal);
  }
  async createRequest(input: RequestInput, idempotencyKey = randomUUID()) { return (await this.json<{ request: RemoteRequest }>('/v1/requests', 'POST', input, idempotencyKey)).request; }
  async request(id: string, wait = 25) { return (await this.json<{ request: RemoteRequest }>(`/v1/requests/${encodeURIComponent(id)}?wait=${wait}`)).request; }
  async claim(wait = 25, signal?: AbortSignal) { return (await this.json<{ requests: RemoteRequest[] }>(`/v1/connector/requests?wait=${wait}`, 'GET', undefined, undefined, signal)).requests; }
  async result(id: string, result: { lease_token: string; result?: unknown; error?: RemoteError }, signal?: AbortSignal) { return (await this.json<{ request: RemoteRequest }>(`/v1/requests/${encodeURIComponent(id)}/result`, 'POST', result, randomUUID(), signal)).request; }
  async files() { return (await this.json<{ files: FileInfo[] }>('/v1/files')).files; }
  async file(id: string) { return (await this.json<{ file: FileInfo }>(`/v1/files/${encodeURIComponent(id)}`)).file; }
  async deleteFile(id: string) { return this.json<{ deleted: boolean }>(`/v1/files/${encodeURIComponent(id)}`, 'DELETE'); }
  async upload(path: string): Promise<FileInfo> {
    const absolute = resolve(path);
    const handle = await open(absolute, 'r');
    const stat = await handle.stat();
    if (!stat.isFile()) { await handle.close(); throw new BridgeError(0, 'NOT_A_FILE', 'Uploads require a regular file'); }
    const boundary = `bridge-${randomUUID()}`;
    const name = basename(absolute).replace(/[\r\n"\\]/g, '_');
    const head = Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${name}"\r\nContent-Type: application/octet-stream\r\n\r\n`);
    const tail = Buffer.from(`\r\n--${boundary}--\r\n`);
    const hash = createHash('sha256'); let bytes = 0;
    const source = handle.createReadStream({ autoClose: true });
    const body = Readable.from((async function* () {
      yield head;
      for await (const chunk of source) { const buffer = Buffer.from(chunk); bytes += buffer.length; hash.update(buffer); yield buffer; }
      yield tail;
    })());
    try {
      const response = await this.fetch('/v1/files', { method: 'POST', headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': String(head.length + stat.size + tail.length) }, body: body as unknown as BodyInit, duplex: 'half' } as RequestInit, false, 290_000);
      const file = (await response.json() as { file: FileInfo }).file;
      if (bytes !== stat.size || file.size !== bytes || file.sha256 !== hash.digest('hex')) throw new BridgeError(0, 'CHECKSUM_MISMATCH', 'Uploaded file checksum or size differs from the local file');
      return file;
    } finally { body.destroy(); source.destroy(); await handle.close().catch(() => {}); }
  }
  async download(id: string, path: string): Promise<FileInfo> {
    const file = await this.file(id);
    const target = resolve(path);
    await mkdir(dirname(target), { recursive: true });
    try { await lstat(target); throw new BridgeError(0, 'OUTPUT_EXISTS', 'Download destination already exists'); } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e; }
    const temporary = `${target}.bridge-${randomUUID()}.part`;
    let bytes = 0; const hash = createHash('sha256');
    try {
      const response = await this.fetch(`/v1/files/${encodeURIComponent(id)}/content`, {}, true, 290_000);
      if (!response.body) throw new BridgeError(0, 'EMPTY_DOWNLOAD', 'Relay did not provide a download body');
      const verify = new Transform({ transform(chunk: Buffer, _encoding, callback) { bytes += chunk.length; if (bytes > file.size) return callback(new BridgeError(0, 'CHECKSUM_MISMATCH', 'Download exceeds expected size')); hash.update(chunk); callback(null, chunk); } });
      await pipeline(Readable.fromWeb(response.body as import('node:stream/web').ReadableStream), verify, createWriteStream(temporary, { flags: 'wx', mode: 0o600 }));
      if (bytes !== file.size || hash.digest('hex') !== file.sha256) throw new BridgeError(0, 'CHECKSUM_MISMATCH', 'Downloaded file checksum or size differs from metadata');
      // A hard link publishes without overwriting a concurrently created destination.
      const { link } = await import('node:fs/promises');
      await link(temporary, target);
      await rm(temporary);
      return file;
    } finally { await rm(temporary, { force: true }); }
  }
}
