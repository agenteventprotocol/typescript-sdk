// Control sender (AEP-0004, `experimental`): `ControlClient.send()` returns a promise
// correlated to the ack/nack by `cause`, with window timeout synthesized
// locally and retries reusing the command `id` (idempotency, AEP-0004 §2.3).
import type { AepEvent, Capture } from './gen/aep-types.js';
import { TransportError } from './errors.js';
import { ulid } from './ulid.js';

export type NackReason = 'unsupported' | 'unauthorized' | 'busy' | 'refused' | 'invalid' | 'timeout';

export class NackError extends Error {
  constructor(
    readonly reason: NackReason,
    readonly detail?: string,
    /** true for the locally synthesized `timeout` nack (never on the wire). */
    readonly synthesized = false,
  ) {
    super(`command nacked: ${reason}${detail ? ` (${detail})` : ''}`);
    this.name = 'NackError';
  }
}

export interface CommandSpec {
  /** `control.{verb}`, e.g. `control.attention.respond`. */
  type: string;
  /** Target addressing (AEP-0004 §2.2). */
  session: string;
  run?: string;
  /** e.g. the `attention.requested` id for `control.attention.respond`. */
  subject?: string;
  /** SHOULD reference the Event that prompted the command (AEP-0004 §6). */
  cause?: string;
  data?: Record<string, unknown>;
  capture?: Capture;
}

export interface SendOptions {
  /** Ack window; default 10 000 ms (AEP-0004 §3). */
  ackWindowMs?: number;
  /** Extra send attempts after window silence, same `id` each time. */
  retries?: number;
}

/**
 * One live emitter claim from the endpoint's `roster` snapshot
 * (AEP-0003 §4.1): the claiming `hello`'s identity plus its declared
 * capability facts, relayed verbatim. A repeated `session` value across
 * entries reports the multi-claimant state — never merged by the endpoint,
 * never merged here.
 */
export interface RosterEntry {
  session: string;
  agent?: string;
  instance?: string;
  control?: { accepts?: string[]; ack_window_ms?: number };
  emits?: string[];
}

/**
 * The structural duplex-socket surface `ControlClient` drives — exactly
 * what the global `WebSocket` satisfies (event-handler properties, string
 * `send`, numeric `readyState` where 1 = OPEN). An injected socket (see
 * `ControlClientOptions.socketFactory`) needs nothing more: no static
 * constants, no event-target machinery.
 */
export interface ControlSocket {
  readyState: number;
  send(data: string): void;
  close(): void;
  onopen: (() => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: (() => void) | null;
  onerror: ((err: unknown) => void) | null;
}

export interface ControlClientOptions {
  /** Relay base URL, e.g. `http://127.0.0.1:8787`. */
  url: string;
  /** Sender identity (an ordinary emitter identity, AEP-0004 §2.2). */
  agent: string;
  host: string;
  token?: string;
  /** Observe every event arriving on the duplex (acks included). */
  onEvent?: (ev: AepEvent) => void;
  onError?: (err: unknown) => void;
  /** Delay before reconnecting after the socket closes (default 1 000 ms). */
  reconnectDelayMs?: number;
  /**
   * Socket construction seam (default `(u) => new WebSocket(u)`): receives
   * the fully built duplex URL (`ws(s)://…/socket` plus the token query).
   * The testing utilities' `ControlStub.socketFactory` plugs in here for
   * relay-free client-side tests.
   */
  socketFactory?: (url: string) => ControlSocket;
}

interface Pending {
  command: AepEvent;
  attemptsLeft: number;
  windowMs: number;
  timer: unknown;
  resolve: (ack: AepEvent) => void;
  reject: (err: NackError) => void;
}

/**
 * Authenticated duplex control channel (WebSocket binding, AEP-0004 §4.1).
 *
 * The constructor opens the connection immediately and reconnects with
 * backoff — there is no separate connect() step; commands sent while
 * (re)connecting queue and go out once the duplex is ready. (The Python
 * SDK's asyncio counterpart uses an explicit `await connect()` instead —
 * an async open cannot live in a constructor.)
 */
export class ControlClient {
  private ws: ControlSocket | null = null;
  private ready = false;
  private closed = false;
  private readonly outbox: string[] = [];
  private readonly pending = new Map<string, Pending>(); // command id -> waiter
  private readonly pendingRosters = new Map<string, {
    resolve: (entries: RosterEntry[]) => void;
    reject: (err: NackError) => void;
    timer: unknown;
  }>(); // roster request id -> waiter

  constructor(private readonly opts: ControlClientOptions) {
    this.connect();
  }

  private connect(): void {
    if (this.closed) return;
    const base = this.opts.url.replace(/\/$/, '').replace(/^http/, 'ws');
    const qs = this.opts.token ? `?access_token=${encodeURIComponent(this.opts.token)}` : '';
    const factory = this.opts.socketFactory ?? ((u: string): ControlSocket => new WebSocket(u));
    const ws = factory(`${base}/socket${qs}`);
    this.ws = ws;
    ws.onopen = () => {
      ws.send(JSON.stringify({
        type: 'hello',
        versions: ['0.1'],
        role: 'consumer',
        identity: { agent: this.opts.agent, instance: this.opts.host },
        capabilities: { filters: ['attr-match'], experimental: {} },
      }));
    };
    ws.onmessage = (m) => {
      let msg: Record<string, unknown>;
      try { msg = JSON.parse(String(m.data)) as Record<string, unknown>; } catch (e) {
        this.opts.onError?.(new TransportError('parse', 'undecodable duplex frame', { cause: e }));
        return;
      }
      if (msg.type === 'hello') {
        // acks arrive via fan-out: subscribe to ack types before releasing sends
        ws.send(JSON.stringify({
          type: 'subscribe',
          id: 'ctl-acks',
          filter: { type: ['control.accepted', 'control.rejected'] },
        }));
        return;
      }
      if (msg.type === 'subscribed') {
        this.ready = true;
        while (this.outbox.length) ws.send(this.outbox.shift() as string);
        return;
      }
      if (msg.type === 'ping') { ws.send(JSON.stringify({ type: 'pong', t: msg.t })); return; }
      if (msg.type === 'roster') {
        // the §4.1 reply: id-correlated, entries relayed verbatim
        const id = msg.id;
        const r = typeof id === 'string' ? this.pendingRosters.get(id) : undefined;
        if (r) {
          this.pendingRosters.delete(id as string);
          clearTimeout(r.timer as Parameters<typeof clearTimeout>[0]);
          r.resolve(Array.isArray(msg.entries) ? (msg.entries as RosterEntry[]) : []);
        }
        return;
      }
      if (msg.type === 'error') {
        // an id-correlated refusal of one of OUR roster requests (the §6.5
        // in-band pattern) settles that waiter — disjoint from the
        // cause-correlated command refusals below
        const errId = msg.id;
        const rw = typeof errId === 'string' ? this.pendingRosters.get(errId) : undefined;
        if (rw) {
          this.pendingRosters.delete(errId as string);
          clearTimeout(rw.timer as Parameters<typeof clearTimeout>[0]);
          const code = String(msg.code ?? 'invalid');
          const known: NackReason[] = ['unsupported', 'unauthorized', 'busy', 'refused', 'invalid'];
          const reason = (known as string[]).includes(code) ? (code as NackReason) : 'invalid';
          rw.reject(new NackError(reason, typeof msg.detail === 'string' ? msg.detail : undefined));
          return;
        }
        // relay-synthesized frames toward the sender (AEP-0004 §4.2's
        // on-behalf refusals and transport-level rejections): settle the
        // pending command now — silence-until-timeout would misreport a
        // stated refusal as a dead target
        const cause = msg.cause;
        const p = typeof cause === 'string' ? this.pending.get(cause) : undefined;
        if (p) {
          this.pending.delete(cause as string);
          clearTimeout(p.timer as Parameters<typeof clearTimeout>[0]);
          const code = String(msg.code ?? 'invalid');
          const known: NackReason[] = ['unsupported', 'unauthorized', 'busy', 'refused', 'invalid'];
          const reason = (known as string[]).includes(code) ? (code as NackReason) : 'invalid';
          const detail = [code !== reason ? code : null, msg.detail].filter(Boolean).join(': ');
          p.reject(new NackError(reason, detail || undefined));
        }
        return;
      }
      if (typeof msg.type === 'string' && msg.type.includes('.')) {
        const ev = msg as unknown as AepEvent;
        this.onAckCandidate(ev);
        this.opts.onEvent?.(ev);
      }
    };
    ws.onclose = () => {
      this.ready = false;
      if (!this.closed) setTimeout(() => this.connect(), this.opts.reconnectDelayMs ?? 1000);
    };
    ws.onerror = (e) => this.opts.onError?.(new TransportError('network', 'websocket error', { cause: e }));
  }

  private onAckCandidate(ev: AepEvent): void {
    if (ev.type !== 'control.accepted' && ev.type !== 'control.rejected') return;
    const cmdId = ev.cause; // ack.cause = command.id (AEP-0004 §6)
    if (typeof cmdId !== 'string') return;
    const p = this.pending.get(cmdId);
    if (!p) return; // not ours, or already settled
    this.pending.delete(cmdId);
    clearTimeout(p.timer);
    if (ev.type === 'control.accepted') {
      p.resolve(ev);
    } else {
      const d = (ev.data ?? {}) as { reason?: string; detail?: string };
      p.reject(new NackError((d.reason as NackReason) ?? 'invalid', d.detail));
    }
  }

  private transmit(json: string): void {
    // readyState 1 = OPEN, compared as the literal: injected ControlSockets
    // carry no static constants (WebSocket.OPEN is 1 by spec).
    if (this.ready && this.ws && this.ws.readyState === 1) this.ws.send(json);
    else this.outbox.push(json);
  }

  /**
   * Send a command; resolves with the `control.accepted` Event, rejects with a
   * NackError on `control.rejected` or on ack-window exhaustion (`timeout`,
   * synthesized locally — AEP-0004 §3). Retries resend the SAME envelope
   * (same `source` and `id`): targets dedupe on (source, id) — AEP-0001
   * §7.4 — and never re-execute.
   */
  send(spec: CommandSpec, options: SendOptions = {}): Promise<AepEvent> {
    const windowMs = options.ackWindowMs ?? 10_000;
    const command: AepEvent = {
      aep: '0.1',
      id: ulid(),
      type: spec.type,
      time: new Date().toISOString(),
      source: `aep://${this.opts.host}/agent/${this.opts.agent}`,
      agent: this.opts.agent,
      session: spec.session, // addressing only; commands MUST omit seq (AEP-0004 §2.2)
      capture: spec.capture ?? 'metadata',
    };
    if (spec.run !== undefined) command.run = spec.run;
    if (spec.subject !== undefined) command.subject = spec.subject;
    if (spec.cause !== undefined) command.cause = spec.cause;
    if (spec.data !== undefined) command.data = spec.data;

    return new Promise<AepEvent>((resolve, reject) => {
      const p: Pending = {
        command,
        attemptsLeft: options.retries ?? 2,
        windowMs,
        timer: null,
        resolve,
        reject,
      };
      this.pending.set(command.id, p);
      const attempt = () => {
        this.transmit(JSON.stringify(command));
        p.timer = setTimeout(() => {
          if (!this.pending.has(command.id)) return; // settled meanwhile
          if (p.attemptsLeft > 0) {
            p.attemptsLeft--;
            attempt(); // same id: safe by construction
            return;
          }
          this.pending.delete(command.id);
          reject(new NackError('timeout', `no ack within ${windowMs} ms`, true));
        }, windowMs);
      };
      attempt();
    });
  }

  /**
   * Request the endpoint's live-claim snapshot (the `roster` frame,
   * AEP-0003 §4.1): resolves with the entries — declarations relayed
   * verbatim, one entry per live claim — or rejects with a NackError on the
   * in-band refusal or on timeout. Gate on the endpoint advertising
   * `capabilities.roster` (hello / discovery document): an endpoint that
   * does not implement the frame IGNORES it (unknown frames, §2), which
   * this method can only report as the synthesized `timeout`. A roster is a
   * snapshot, never a promise — the target's declared `accepts` at command
   * time stays authoritative (AEP-0004 §4).
   */
  roster(options: { timeoutMs?: number } = {}): Promise<RosterEntry[]> {
    const timeoutMs = options.timeoutMs ?? 10_000;
    const id = `ro-${ulid()}`;
    return new Promise<RosterEntry[]>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (!this.pendingRosters.has(id)) return;
        this.pendingRosters.delete(id);
        reject(new NackError('timeout',
          `no roster reply within ${timeoutMs} ms (did the endpoint advertise capabilities.roster?)`, true));
      }, timeoutMs);
      this.pendingRosters.set(id, { resolve, reject, timer });
      this.transmit(JSON.stringify({ type: 'roster', id }));
    });
  }

  close(): void {
    this.closed = true;
    for (const [id, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new NackError('timeout', 'control client closed', true));
      this.pending.delete(id);
    }
    for (const [id, r] of this.pendingRosters) {
      clearTimeout(r.timer as Parameters<typeof clearTimeout>[0]);
      r.reject(new NackError('timeout', 'control client closed', true));
      this.pendingRosters.delete(id);
    }
    this.ws?.close();
  }
}

/** What a target needs from its emitter: acks and outcomes are ordinary
 * sequenced session Events (AEP-0004 §2.2) — a `SessionEmitter` fits.
 * `redeliver` re-sends an already-emitted Event verbatim (the §3 re-ack a
 * retransmitted command elicits); when absent the target degrades to
 * silent dedupe. */
export interface TargetEmitter {
  emit(type: string, data?: Record<string, unknown>, opts?: { cause?: string }): AepEvent;
  redeliver?(ev: AepEvent): void;
}

/**
 * Target-side conformance helper (AEP-0004 §7), mirroring the Python SDK's
 * surface: dedupe on (source, id) — AEP-0001 §7.4, one ack DECISION per
 * deduplicated command with the recorded ack re-emitted byte-identically on
 * a retransmit (a lost ack is recovered by the retry, §2.3/§3),
 * `unsupported` nack for types outside the declared `accepts`.
 */
export class ControlTarget {
  // (source,id) -> the ack Event AS EMITTED; retention rides this dedupe
  // memory (§3): a forgotten command reads as new.
  private readonly seen = new Map<string, AepEvent | undefined>();

  constructor(
    private readonly emitter: TargetEmitter,
    readonly accepts: readonly string[],
  ) {}

  /**
   * Ack/nack `cmd`; call `execute` at most once per (source, id). Returns
   * true when the command was accepted (first delivery). A redelivered
   * command gets its recorded ack re-emitted byte-identically and returns
   * false — same decision, never a second Event.
   */
  handle(cmd: AepEvent, execute: (cmd: AepEvent) => void): boolean {
    const cmdId = cmd.id;
    // Dedupe scope is (source, id) — AEP-0001 §7.4: two senders may legally
    // mint the same id, and a retried command rides the same source (the
    // sender's identity), so retries still dedupe correctly. Key on
    // `${source}\0${id}`: the \0 escape (never a literal NUL byte) can't
    // appear in either field.
    if (typeof cmdId !== 'string' || typeof cmd.source !== 'string') {
      return false;
    }
    const key = `${cmd.source}\0${cmdId}`;
    if (this.seen.has(key)) {
      // duplicate: MUST NOT re-execute; the recorded ack is re-emitted
      // byte-identically — the §2.3 retry recovers a lost ack (§3)
      const recorded = this.seen.get(key);
      if (recorded) this.emitter.redeliver?.(recorded);
      return false;
    }
    this.seen.set(key, undefined);
    if (this.seen.size > 10_000) {
      const oldest = this.seen.keys().next().value;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    if (!this.accepts.includes(cmd.type)) {
      this.seen.set(key, this.emitter.emit('control.rejected', { reason: 'unsupported' }, { cause: cmdId }));
      return false; // §4.2: accepts is authoritative
    }
    this.seen.set(key, this.emitter.emit('control.accepted', {}, { cause: cmdId }));
    execute(cmd);
    return true;
  }
}
