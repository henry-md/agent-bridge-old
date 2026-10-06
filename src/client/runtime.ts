import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { performance } from 'node:perf_hooks';
import { setTimeout as pause } from 'node:timers/promises';
import { z } from 'zod';
import { BridgeError, type ChannelInbox, type ChannelMessage, type ChannelStatus } from '../shared/protocol.js';
import { readConfig, type BridgeConfig } from './config.js';
import { channelId, joinChannel } from './channel.js';
import { RelayClient } from './relay.js';

const prefix = 'agent-bridge control v2: ';
const frameSchema = z.object({ version: z.literal(2), kind: z.enum(['probe', 'echo']), nonce: z.string().uuid(), generation: z.string().uuid(), pairing_id: z.string().uuid(), from_session: z.string().uuid(), to_session: z.string().uuid(), word: z.string().regex(/^[a-z0-9-]{3,80}$/) }).strict();
type Frame = z.infer<typeof frameSchema>;
const identity = (status: ChannelStatus) => `${status.generation}/${status.pairing_id}/${status.peer?.session_id ?? ''}`;
interface Proof { message: ChannelMessage; connection: ChannelStatus; rtt: number }
interface Probe { nonce: string; signal: AbortSignal; attempt?: AbortController; identity?: string; seq?: number; started?: number; inFlight?: Promise<void>; candidate?: Proof; proof?: Proof }
interface ControlReceipt { seq: number; processed: boolean; work?: Promise<void> }
export interface RuntimePairResult extends ChannelInbox {
  verified: boolean; timed_out: boolean; channel: string; session_id: string; proof_nonce?: string; proof_message_id?: string; proof_round_trip_ms?: number; runtime_setup_ms: number;
}

// One runtime owns one inbox reader. Setup is handled in JavaScript while
// ordinary records stay durable and unacknowledged until the agent processes them.
export class ChannelRuntime {
  private readonly controller = new AbortController();
  private pairing = new AbortController();
  private readonly changes = new EventEmitter();
  private readonly ordinary = new Map<string, ChannelMessage>();
  private readonly probes = new Map<string, Probe>();
  private readonly controls = new Map<string, ControlReceipt>();
  private readonly sends = new Set<Promise<void>>();
  private connection?: ChannelStatus;
  private cursor = 0;
  private acknowledged = 0;
  private failure?: unknown;
  private transport: 'connecting' | 'online' | 'reconnecting' | 'stopped' = 'connecting';
  private verifiedAt?: string;
  private proofMs?: number;
  private closeReason = 'RUNTIME_RESTARTING';
  private readonly ready: Promise<void>;
  private readonly work: Promise<void>;
  private pendingAck?: { generation: string; cursor: number };
  private ackTimer?: NodeJS.Timeout;
  private ackWork?: Promise<void>;
  constructor(readonly client: RelayClient, readonly config: BridgeConfig, readonly channel: string, readonly session: string) {
    channelId(channel); z.string().uuid().parse(session); this.changes.setMaxListeners(0);
    this.ready = this.initialize(); this.work = this.loop(); void this.work.catch(() => {});
  }
  private async initialize() {
    try {
      while (true) {
        try { this.connection = await joinChannel(this.client, this.config, this.channel, this.session, 0, this.controller.signal); return; }
        catch (error) { if (this.controller.signal.aborted || !this.recoverable(error)) throw error; this.transport = 'reconnecting'; await pause(100, undefined, { signal: this.controller.signal }); }
      }
    }
    catch (error) { this.failure = error; this.transport = 'stopped'; this.changes.emit('change'); throw error; }
  }
  private retryable(error: unknown) { return error instanceof BridgeError ? error.status >= 500 || error.status === 429 && error.code !== 'RUNTIME_MAILBOX_FULL' : error instanceof TypeError || error instanceof Error && ['TimeoutError', 'AbortError'].includes(error.name); }
  private recoverable(error: unknown) { return this.retryable(error) || error instanceof BridgeError && ['channel_not_connected', 'channel_pairing_changed', 'channel_session_expired', 'stale_generation'].includes(error.code); }
  private background(work: Promise<void>) {
    this.sends.add(work);
    void work.catch(error => {
      if (!this.controller.signal.aborted && !this.recoverable(error)) { this.failure = error; this.controller.abort(); }
    }).finally(() => { this.sends.delete(work); this.changes.emit('change'); });
  }
  private queueProcessedControls() {
    const firstOrdinary = Math.min(Infinity, ...[...this.ordinary.values()].map(message => message.seq));
    let cursor = this.acknowledged;
    for (const receipt of [...this.controls.values()].sort((a, b) => a.seq - b.seq)) {
      if (receipt.seq <= this.acknowledged) continue;
      if (!receipt.processed || receipt.seq >= firstOrdinary) break;
      cursor = receipt.seq;
    }
    if (cursor > this.acknowledged) this.queueControlAck(this.connection!.generation, cursor);
  }
  private receiveControl(message: ChannelMessage, write?: () => Promise<unknown>) {
    if (this.controls.has(message.id)) return;
    const receipt: ControlReceipt = { seq: message.seq, processed: !write }; this.controls.set(message.id, receipt);
    if (write) {
      receipt.work = (async () => {
      while (!this.controller.signal.aborted) {
        try { await write(); break; }
        catch (error) {
          if (this.controller.signal.aborted) return;
          // A changed pairing invalidates the old setup exchange. A new nonce
          // proves the new peer; never send the old control to that peer.
          if (error instanceof BridgeError && ['channel_not_connected', 'channel_pairing_changed', 'channel_session_expired', 'stale_generation'].includes(error.code)) break;
          if (!this.retryable(error)) throw error;
          await pause(100, undefined, { signal: this.controller.signal });
        }
      }
      if (this.controller.signal.aborted || this.controls.get(message.id) !== receipt) return;
      receipt.processed = true; this.queueProcessedControls();
      })();
      this.background(receipt.work);
    }
  }
  private async credentials() {
    const current = await readConfig();
    if (current.url !== this.config.url || current.token !== this.config.token || current.device !== this.config.device) throw new BridgeError(0, 'CONFIG_CHANGED', 'Relay credentials changed; restart the local runtime with the new configuration');
    const saved = current.channel_sessions?.[this.channel]?.sessions[this.session];
    if (saved?.generation !== this.connection?.generation) throw new BridgeError(0, 'SESSION_CHANGED', 'The local channel generation changed outside this runtime');
  }
  private check() {
    if (this.failure) throw this.failure;
    if (this.controller.signal.aborted) throw new BridgeError(409, this.closeReason, 'This runtime channel has stopped or was replaced');
  }
  private update(status: ChannelStatus) {
    if (status.channel !== this.channel || status.session_id !== this.session) throw new BridgeError(0, 'CHANNEL_IDENTITY_MISMATCH', 'Relay returned a different channel or session');
    if (this.connection && identity(this.connection) !== identity(status)) {
      this.pairing.abort(); this.pairing = new AbortController();
      this.verifiedAt = undefined;
      for (const [nonce, probe] of [...this.probes]) {
        probe.attempt?.abort(); probe.inFlight = undefined;
        this.probes.delete(nonce); probe.nonce = randomUUID(); this.probes.set(probe.nonce, probe);
        probe.identity = undefined; probe.seq = undefined; probe.candidate = undefined; probe.proof = undefined;
      }
    }
    this.connection = status;
  }
  private control(message: ChannelMessage, status: ChannelStatus): Frame | 'legacy-probe' | 'legacy-ack' | undefined {
    if (!status.peer || status.status !== 'connected' || message.file_ids.length || message.channel !== this.channel || message.generation !== status.generation
      || message.from !== status.peer.device || message.from_session !== status.peer.session_id || message.to_session !== this.session) return;
    if (message.text === `agent-bridge setup: ${status.secret_word}`) return 'legacy-probe';
    if (message.text === `agent-bridge setup ack: ${status.secret_word}`) return 'legacy-ack';
    if (!message.text.startsWith(prefix)) return;
    let parsed; try { parsed = frameSchema.safeParse(JSON.parse(message.text.slice(prefix.length))); } catch { return; }
    if (!parsed.success) return;
    const frame = parsed.data;
    if (frame.generation !== status.generation || frame.pairing_id !== status.pairing_id || frame.from_session !== status.peer.session_id || frame.to_session !== this.session || frame.word !== status.secret_word) return;
    return frame;
  }
  private async send(frame: Frame, key: string, signal = this.controller.signal) {
    await this.credentials();
    const current = this.connection!;
    if (frame.generation !== current.generation || frame.pairing_id !== current.pairing_id || frame.from_session !== this.session || frame.to_session !== current.peer?.session_id) throw new BridgeError(409, 'channel_pairing_changed', 'Peer changed before sending the setup frame');
    const sent = await this.client.sendChannel(this.channel, { session_id: this.session, generation: frame.generation, text: prefix + JSON.stringify(frame), file_ids: [] }, key, AbortSignal.any([signal, this.pairing.signal]));
    if (sent.to_session !== frame.to_session) throw new BridgeError(409, 'channel_pairing_changed', 'Peer changed while sending the setup frame');
    return sent;
  }
  private complete(probe: Probe) {
    if (probe.signal.aborted || probe.proof) return;
    const candidate = probe.candidate;
    // The authenticated peer can echo this unpredictable nonce only after it
    // receives our durable probe. Its echo is proof even if our POST receipt
    // is delayed; do not wait for that receipt before showing the word.
    if (candidate && probe.started !== undefined && (probe.seq === undefined || candidate.message.seq > probe.seq) && probe.identity === identity(candidate.connection) && probe.identity === identity(this.connection!)) {
      probe.proof = { ...candidate, rtt: performance.now() - probe.started! }; this.verifiedAt = new Date().toISOString(); this.proofMs = probe.proof.rtt; this.changes.emit('change');
    }
  }
  private async submit(probe: Probe) {
    if (probe.signal.aborted) return;
    if (probe.inFlight) return probe.inFlight;
    const status = this.connection!;
    if (status.status !== 'connected' || !status.peer) return;
    const tuple = identity(status);
    if (probe.identity === tuple && probe.seq !== undefined) return;
    probe.identity = tuple; probe.candidate = undefined; probe.proof = undefined; probe.started = performance.now();
    const attempt = new AbortController(); probe.attempt = attempt;
    const stop = AbortSignal.any([probe.signal, attempt.signal]);
    const work = (async () => {
      const sent = await this.send({ version: 2, kind: 'probe', nonce: probe.nonce, generation: status.generation, pairing_id: status.pairing_id, from_session: this.session, to_session: status.peer!.session_id, word: status.secret_word }, `runtime-probe:${probe.nonce}:${status.pairing_id}`, stop);
      await this.credentials();
      if (probe.identity === tuple) { probe.seq = sent.seq; this.complete(probe); }
    })();
    probe.inFlight = work;
    try { await work; } finally { if (probe.inFlight === work) probe.inFlight = undefined; }
  }
  private async loop() {
    try {
      await this.ready;
      while (!this.controller.signal.aborted) {
        try {
          this.check();
          await this.credentials();
          for (const probe of this.probes.values()) this.background(this.submit(probe));
          const status = this.connection!;
          const page = await this.client.channelInbox(this.channel, this.session, status.generation, this.cursor || undefined, 25, true, this.controller.signal);
          await this.credentials();
          this.update(page.connection ?? await this.client.channelStatus(this.channel, this.session, status.generation, 0, this.controller.signal));
          this.transport = 'online'; this.acknowledged = Math.max(this.acknowledged, page.acknowledged_cursor);
          for (const [id, message] of this.ordinary) if (message.seq <= this.acknowledged) this.ordinary.delete(id);
          for (const [id, receipt] of this.controls) if (receipt.seq <= this.acknowledged) this.controls.delete(id);
          const snapshot = this.connection!;
          for (const message of page.messages) {
            if (message.seq <= this.acknowledged) continue;
            const frame = this.control(message, snapshot);
            if (!frame) {
              if (this.ordinary.size >= 1000 && !this.ordinary.has(message.id)) throw new BridgeError(429, 'RUNTIME_MAILBOX_FULL', 'Process the queued mailbox before restarting the runtime reader');
              this.ordinary.set(message.id, message); continue;
            }
            if (frame === 'legacy-probe') {
              this.receiveControl(message, async () => {
                await this.credentials();
                if (identity(snapshot) !== identity(this.connection!)) throw new BridgeError(409, 'channel_pairing_changed', 'Peer changed before replying to legacy setup');
                const reply = await this.client.sendChannel(this.channel, { session_id: this.session, generation: snapshot.generation, text: `agent-bridge setup ack: ${snapshot.secret_word}`, file_ids: [] }, `runtime-legacy:${message.id}`, AbortSignal.any([this.controller.signal, this.pairing.signal]));
                if (reply.to_session !== snapshot.peer?.session_id) throw new BridgeError(409, 'channel_pairing_changed', 'Peer changed while replying to legacy setup');
              });
            } else if (typeof frame !== 'string' && frame.kind === 'probe') {
              this.receiveControl(message, () => this.send({ ...frame, kind: 'echo', from_session: this.session, to_session: frame.from_session }, `runtime-echo:${message.id}`));
            } else if (typeof frame !== 'string') {
              await this.credentials();
              const probe = this.probes.get(frame.nonce);
              if (probe && probe.identity === identity(snapshot)) { probe.candidate = { message, connection: snapshot, rtt: 0 }; this.complete(probe); }
              this.receiveControl(message);
            } else this.receiveControl(message);
          }
          this.cursor = Math.max(page.cursor, this.acknowledged);
          // Wake proof callers before the optional receipt commit. The nonce
          // frame itself is already in the durable authenticated mailbox.
          this.changes.emit('change');
          this.queueProcessedControls();
          if (snapshot.peer && snapshot.status !== 'connected') this.update(await this.client.confirmChannel(this.channel, this.session, snapshot.generation, snapshot.secret_word, snapshot.pairing_id, this.controller.signal));
          for (const probe of this.probes.values()) this.background(this.submit(probe));
          this.changes.emit('change');
        } catch (error) {
          if (this.controller.signal.aborted) break;
          if (error instanceof BridgeError && ['channel_not_connected', 'channel_pairing_changed'].includes(error.code)) {
            try { this.update(await this.client.channelStatus(this.channel, this.session, this.connection!.generation, 0, this.controller.signal)); }
            catch (refresh) { if (!this.retryable(refresh)) throw refresh; this.transport = 'reconnecting'; await pause(100, undefined, { signal: this.controller.signal }); }
            continue;
          }
          if (error instanceof BridgeError && ['channel_session_expired', 'stale_generation'].includes(error.code)) {
            if (this.ordinary.size) throw new BridgeError(409, 'CHANNEL_GENERATION_CHANGED', 'Pending mail belongs to the expired generation; it remains unacknowledged in the relay');
            let renewed;
            try { renewed = await joinChannel(this.client, this.config, this.channel, this.session, 0, this.controller.signal); }
            catch (refresh) { if (!this.retryable(refresh)) throw refresh; this.transport = 'reconnecting'; await pause(100, undefined, { signal: this.controller.signal }); continue; }
            this.update(renewed);
            this.cursor = 0; this.acknowledged = 0; this.controls.clear(); this.verifiedAt = undefined; continue;
          }
          if (!this.retryable(error)) throw error;
          this.transport = 'reconnecting'; this.verifiedAt = undefined; this.changes.emit('change');
          await pause(100, undefined, { signal: this.controller.signal });
        }
      }
    } catch (error) { if (!this.controller.signal.aborted) this.failure = error; }
    finally { this.transport = 'stopped'; this.verifiedAt = undefined; this.changes.emit('change'); }
  }
  private queueControlAck(generation: string, cursor: number) {
    const previous = this.pendingAck;
    this.pendingAck = { generation, cursor: previous?.generation === generation ? Math.max(previous.cursor, cursor) : cursor };
    if (this.ackWork || this.controller.signal.aborted) return;
    if (this.ackTimer) clearTimeout(this.ackTimer);
    // Setup receipts are coalesced, while probe/echo commits remain durable.
    // Receipt disk/network latency must never hold the sole inbox reader.
    this.ackTimer = setTimeout(() => {
      this.ackTimer = undefined;
      const target = this.pendingAck; this.pendingAck = undefined;
      if (!target || this.controller.signal.aborted || this.connection?.generation !== target.generation) return;
      this.ackWork = (async () => {
        await this.credentials();
        if (this.connection?.generation !== target.generation) return;
        const ack = await this.client.acknowledgeChannel(this.channel, this.session, target.generation, target.cursor, this.controller.signal);
        await this.credentials();
        if (this.connection?.generation === target.generation) this.acknowledged = Math.max(this.acknowledged, ack.acknowledged_cursor);
      })().catch(error => {
        if (this.controller.signal.aborted) return;
        if (error instanceof BridgeError && ['stale_generation', 'channel_session_expired'].includes(error.code)) return;
        if (this.retryable(error)) {
          if (this.connection?.generation === target.generation) this.pendingAck = { generation: target.generation, cursor: Math.max(target.cursor, this.pendingAck?.generation === target.generation ? this.pendingAck.cursor : 0) };
        } else { this.failure = error; this.controller.abort(); this.changes.emit('change'); }
      }).finally(() => {
        this.ackWork = undefined;
        if (this.pendingAck) this.queueControlAck(this.pendingAck.generation, this.pendingAck.cursor);
      });
    }, 1000);
  }
  private async changed(signal: AbortSignal) {
    if (signal.aborted) return;
    await new Promise<void>(resolve => {
      const done = () => { this.changes.off('change', done); signal.removeEventListener('abort', done); resolve(); };
      this.changes.once('change', done); signal.addEventListener('abort', done, { once: true });
    });
  }
  private async until<T>(work: Promise<T>, signal: AbortSignal): Promise<T | undefined> {
    if (signal.aborted) { void work.catch(() => {}); return; }
    return new Promise<T | undefined>((resolve, reject) => {
      const clear = () => signal.removeEventListener('abort', aborted);
      const aborted = () => { clear(); resolve(undefined); };
      signal.addEventListener('abort', aborted, { once: true });
      work.then(value => { clear(); resolve(value); }, error => { clear(); reject(error); });
    });
  }
  async pair(timeoutMs: number, signal?: AbortSignal): Promise<RuntimePairResult> {
    const started = performance.now(), deadline = AbortSignal.timeout(timeoutMs), stop = signal ? AbortSignal.any([deadline, signal, this.controller.signal]) : AbortSignal.any([deadline, this.controller.signal]);
    const timedOut = (): RuntimePairResult => ({ messages: [], cursor: 0, acknowledged_cursor: 0, verified: false, timed_out: true, channel: this.channel, session_id: this.session, runtime_setup_ms: performance.now() - started });
    await this.until(this.ready, stop);
    if (stop.aborted) { this.check(); return timedOut(); }
    this.check();
    this.verifiedAt = undefined; this.changes.emit('change');
    const probe: Probe = { nonce: randomUUID(), signal: stop }; this.probes.set(probe.nonce, probe);
    try {
      this.background(this.submit(probe));
      while (!stop.aborted) {
        this.check();
        await this.credentials();
        this.check(); if (stop.aborted) break;
        if (probe.proof && this.transport === 'online' && identity(this.connection!) === probe.identity) {
          return { ...this.inbox(), verified: true, timed_out: false, channel: this.channel, session_id: this.session, connection: probe.proof.connection, proof_nonce: probe.nonce, proof_message_id: probe.proof.message.id, proof_round_trip_ms: probe.proof.rtt, runtime_setup_ms: performance.now() - started };
        }
        await this.changed(stop);
      }
      this.check(); return timedOut();
    } finally { this.probes.delete(probe.nonce); }
  }
  inbox(): ChannelInbox {
    const messages = [...this.ordinary.values()].sort((a, b) => a.seq - b.seq).slice(0, 100);
    return { messages, cursor: messages.at(-1)?.seq ?? this.acknowledged, acknowledged_cursor: this.acknowledged, ...(this.connection ? { connection: this.connection } : {}) };
  }
  async watch(timeoutMs: number, signal?: AbortSignal) {
    const stop = signal ? AbortSignal.any([signal, AbortSignal.timeout(timeoutMs), this.controller.signal]) : AbortSignal.any([AbortSignal.timeout(timeoutMs), this.controller.signal]);
    await this.until(this.ready, stop);
    while (!stop.aborted) { this.check(); await this.credentials(); this.check(); if (stop.aborted) break; const page = this.inbox(); if (page.messages.length) return { ...page, timed_out: false, channel: this.channel, session_id: this.session }; await this.changed(stop); }
    this.check(); return { messages: [], cursor: 0, acknowledged_cursor: 0, timed_out: true, channel: this.channel, session_id: this.session };
  }
  async acknowledge(cursor: number) {
    await this.ready; await this.credentials();
    if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor > this.inbox().cursor) throw new BridgeError(400, 'INVALID_CURSOR', 'Cursor is beyond this runtime inbox');
    const generation = this.connection!.generation;
    // Ordinary mail can be processed while a setup reply is still sending.
    // Its acknowledgment must not erase that durable reply obligation.
    await Promise.all([...this.controls.values()].filter(receipt => receipt.seq <= cursor && !receipt.processed).map(receipt => receipt.work!));
    this.check(); await this.credentials();
    if (this.connection!.generation !== generation) throw new BridgeError(409, 'stale_generation', 'The acknowledgment belongs to an earlier generation');
    const ack = await this.client.acknowledgeChannel(this.channel, this.session, generation, cursor);
    this.acknowledged = Math.max(this.acknowledged, ack.acknowledged_cursor);
    for (const [id, message] of this.ordinary) if (message.seq <= this.acknowledged) this.ordinary.delete(id);
    for (const [id, receipt] of this.controls) if (receipt.seq <= this.acknowledged) this.controls.delete(id);
    this.queueProcessedControls();
    this.changes.emit('change'); return ack;
  }
  status() { return { channel: this.channel, session_id: this.session, connection: this.connection, transport: this.transport, verified_at: this.verifiedAt ?? null, pending_messages: this.ordinary.size, proof_round_trip_ms: this.proofMs ?? null }; }
  onChange(listener: () => void) { this.changes.on('change', listener); return () => this.changes.off('change', listener); }
  async close(reason = 'RUNTIME_RESTARTING') { this.closeReason = reason; this.controller.abort(); if (this.ackTimer) clearTimeout(this.ackTimer); this.changes.emit('change'); await this.work; await this.ackWork; await Promise.allSettled(this.sends); }
}
