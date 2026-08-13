/**
 * Testing utilities (the `@agenteventprotocol/sdk/testing` subpath): everything folds
 * in-process — no relay, no network, no fixture server.
 *
 * - {@link MemorySink} records emitted Events; pass `.sink` to an `Emitter`.
 * - {@link ScriptedSource} scripts an emitter: real `SessionEmitter`s
 *   (envelope construction, `(epoch, seq)` ownership per AEP-0001 §7) over
 *   one shared in-memory record.
 * - {@link ControlStub} serves BOTH sides of a control exchange (AEP-0004).
 *   Target side: `command()` mints well-formed command frames (§2.2),
 *   `.emitter` is a real recording `SessionEmitter` for a `ControlTarget`,
 *   `.acks` is what the target emitted. Client side: `.socketFactory` plugs
 *   into `ControlClientOptions.socketFactory`, outbound command envelopes
 *   land in `.sent`, and `accept()` / `reject()` / `rosterReply()` script
 *   the far side.
 *
 * Everything reuses the SDK's own envelope construction (`Emitter`/`ulid`);
 * nothing here forks the wire format.
 */
import type { AepEvent } from './gen/aep-types.js';
import type { Sink, SessionEmitter } from './emit.js';
import type { ControlSocket, NackReason, RosterEntry } from './control.js';
import { Emitter } from './emit.js';
import { COMMAND_TYPE } from './projection.js';
import { ulid } from './ulid.js';

/**
 * An in-memory sink: records every Event delivered through {@link sink} in
 * arrival order. Pass `.sink` anywhere a {@link Sink} is expected.
 */
export class MemorySink {
  private readonly recorded: AepEvent[] = [];

  /** The recording {@link Sink} — hand this to an `Emitter` (or a transport seam). */
  readonly sink: Sink = (ev: AepEvent): void => {
    this.recorded.push(ev);
  };

  /** Recorded Events in arrival order (a live view). */
  get events(): readonly AepEvent[] {
    return this.recorded;
  }

  /** Empty the record (the array identity is kept). */
  clear(): void {
    this.recorded.length = 0;
  }
}

export interface ScriptedSourceOptions {
  /**
   * Full `source` URI override stamped on every recorded envelope — pins a
   * corpus-style identity (e.g. `aep://corpus/adapter-a`). By default the
   * emitter-minted `aep://<host>/agent/<agent>` URI stands.
   */
  source?: string;
  /** Producer identity (envelope `agent`); default `scripted`. */
  agent?: string;
  /** Host segment of the minted `source` URI; default `scripted-host`. */
  host?: string;
}

export interface ScriptedSessionOptions {
  /** Emitter epoch for this session (AEP-0001 §7); default 0. */
  epoch?: number;
}

/**
 * A scripted event source: hands out real `SessionEmitter`s — correct
 * envelope construction, per-session `(epoch, seq)` ownership (AEP-0001
 * §5/§7) — recording everything into one shared in-memory list, then
 * replays the recording with {@link play}. Because an `Emitter`'s epoch is
 * emitter-wide, each `session()` call mints a fresh internal `Emitter`
 * (same identity, same record) so per-session epochs stay independent.
 */
export class ScriptedSource {
  private readonly recorded: AepEvent[] = [];
  private readonly host: string;
  private readonly agent: string;
  private readonly source: string | undefined;

  constructor(opts: ScriptedSourceOptions = {}) {
    this.host = opts.host ?? 'scripted-host';
    this.agent = opts.agent ?? 'scripted';
    this.source = opts.source;
  }

  /**
   * A real `SessionEmitter` for `session`, seq starting at 0 (AEP-0001 §7).
   * Hold on to the returned emitter — a second call for the same session
   * string starts a fresh counter (pass a fresh `epoch` if you script a
   * restart).
   */
  session(session: string, opts: ScriptedSessionOptions = {}): SessionEmitter {
    return new Emitter({
      agent: this.agent,
      host: this.host,
      epoch: opts.epoch ?? 0,
      sink: (ev) => {
        if (this.source !== undefined) ev.source = this.source;
        this.recorded.push(ev);
      },
    }).session(session);
  }

  /** Every Event emitted through this source, across sessions, in emit order (a live view). */
  get events(): readonly AepEvent[] {
    return this.recorded;
  }

  /** Deliver all recorded Events, in order, to `onEvent` (a snapshot: emitting during play is safe). */
  play(onEvent: (ev: AepEvent) => void): void {
    for (const ev of [...this.recorded]) onEvent(ev);
  }
}

export interface ControlStubOptions {
  /**
   * Full `source` URI override stamped on minted command frames and on the
   * target emitter's Events; by default the minted
   * `aep://<host>/agent/<agent>` URI stands.
   */
  source?: string;
  /** Identity stamped on minted command frames and on the target emitter; default `control-stub`. */
  agent?: string;
  /** Host segment of the minted `source` URI; default `stub-host`. */
  host?: string;
  /** The target-side session {@link ControlStub.emitter} emits into; default `stub-target`. */
  session?: string;
}

/** A command to mint: `CommandSpec` minus `capture` (the stub stamps `metadata`). */
export interface StubCommandSpec {
  /** `control.{verb}`, e.g. `control.pause`. */
  type: string;
  /** Target addressing (AEP-0004 §2.2); defaults to the stub's session. */
  session?: string;
  run?: string;
  /** e.g. the `attention.requested` id for `control.attention.respond`. */
  subject?: string;
  /** SHOULD reference the Event that prompted the command (AEP-0004 §6). */
  cause?: string;
  data?: Record<string, unknown>;
}

/**
 * Scripted counterpart for BOTH sides of a control exchange (AEP-0004).
 *
 * Target side: {@link command} mints well-formed command frames — fresh
 * ULID `id`, `session` addressing and **no `seq`** (§2.2), the sender's
 * `source`/`agent` identity, `time` — exactly the envelope
 * `ControlClient.send()` puts on the wire. {@link emitter} is a real
 * recording `SessionEmitter` (it satisfies `TargetEmitter` structurally,
 * `redeliver` included), so a `ControlTarget` driven with minted commands
 * acks into {@link acks}.
 *
 * Client side (Python-SDK parity — same semantics, mechanism per idiom:
 * the Python stub replaces `ControlSender`'s transport callable, while
 * this stub injects a fake socket because `ControlClient` owns its wire):
 * pass {@link socketFactory} as `ControlClientOptions.socketFactory`. The
 * fake socket completes the hello/subscribe handshake by itself (each
 * reply on a microtask, after handlers attach), outbound COMMAND envelopes
 * land in {@link sent}, and {@link accept} / {@link reject} /
 * {@link rosterReply} script the far side's answers.
 */
export class ControlStub {
  private readonly recorded: AepEvent[] = [];
  private readonly host: string;
  private readonly agent: string;
  private readonly session: string;
  private readonly source: string | undefined;
  // One recording identity; per-session SessionEmitters minted on demand
  // (acks land on the COMMAND's target session — Python-twin parity).
  private readonly base: Emitter;
  private readonly emitters = new Map<string, SessionEmitter>();
  // Client side: live fake sockets, captured command frames, unanswered
  // roster requests (FIFO, id-correlated to their asking socket).
  private readonly sockets: ControlSocket[] = [];
  private readonly sentFrames: AepEvent[] = [];
  private readonly rosterQueue: Array<{ socket: ControlSocket; id: string }> = [];

  /**
   * The target's emitter: a real `SessionEmitter` over the internal
   * recorder — acks and outcomes are ordinary sequenced session Events
   * (AEP-0004 §2.2), and `redeliver` carries the §3 re-ack byte-identically.
   */
  readonly emitter: SessionEmitter;

  constructor(opts: ControlStubOptions = {}) {
    this.host = opts.host ?? 'stub-host';
    this.agent = opts.agent ?? 'control-stub';
    this.session = opts.session ?? 'stub-target';
    this.source = opts.source;
    this.base = new Emitter({
      agent: this.agent,
      host: this.host,
      sink: (ev) => {
        if (this.source !== undefined) ev.source = this.source;
        this.recorded.push(ev);
      },
    });
    this.emitter = this.sessionFor(this.session);
  }

  /** The stub's real `SessionEmitter` for `session` (minted once, cached). */
  private sessionFor(session: string): SessionEmitter {
    let se = this.emitters.get(session);
    if (se === undefined) {
      se = this.base.session(session);
      this.emitters.set(session, se);
    }
    return se;
  }

  /**
   * Mint a well-formed command frame (AEP-0004 §2.2): `aep` 0.1, fresh ULID
   * `id`, `session` addressing with no `seq` attribute at all, `source`/
   * `agent`/`time`, `capture` `metadata`. Retransmit by passing the SAME
   * returned Event again — a retry reuses the id (§2.3), never `command()`.
   */
  command(spec: StubCommandSpec): AepEvent {
    if (!COMMAND_TYPE.test(spec.type)) {
      throw new TypeError(
        `${spec.type} is not a command type (AEP-0004 §2.2: the control.* verbs or x.<vendor>.control.*)`,
      );
    }
    const cmd: AepEvent = {
      aep: '0.1',
      id: ulid(),
      type: spec.type,
      time: new Date().toISOString(),
      source: this.source ?? `aep://${this.host}/agent/${this.agent}`,
      agent: this.agent,
      session: spec.session ?? this.session, // addressing only; commands MUST omit seq (AEP-0004 §2.2)
      capture: 'metadata',
    };
    if (spec.run !== undefined) cmd.run = spec.run;
    if (spec.subject !== undefined) cmd.subject = spec.subject;
    if (spec.cause !== undefined) cmd.cause = spec.cause;
    if (spec.data !== undefined) cmd.data = spec.data;
    return cmd;
  }

  /** Everything the target emitted through {@link emitter} — acks, nacks, outcomes — in emit order (a live view). */
  get acks(): readonly AepEvent[] {
    return this.recorded;
  }

  /**
   * Command envelopes a `ControlClient` transmitted over the fake socket,
   * in wire order (a live view) — ONLY parsed frames whose `type` contains
   * a `.`: the hello/subscribe/roster protocol frames are handled inside
   * the stub and never leak in. Python `.sent` parity.
   */
  get sent(): readonly AepEvent[] {
    return this.sentFrames;
  }

  /**
   * The socket seam for `ControlClientOptions.socketFactory`: returns a
   * fake `ControlSocket` that fires `onopen` on a microtask AFTER the
   * factory returns (the client attaches its handlers synchronously
   * first), then serves the handshake itself — `hello` is answered with a
   * hello reply, `subscribe` with `{ type: 'subscribed', id }` (id
   * echoed), each reply on its own microtask. Outbound command envelopes
   * (frames whose `type` contains `.`) are captured into {@link sent};
   * `roster` requests queue FIFO for {@link rosterReply}. Bound — pass the
   * property itself.
   */
  readonly socketFactory = (_url: string): ControlSocket => {
    const stub = this;
    const socket: ControlSocket = {
      readyState: 0, // CONNECTING until the microtask open below
      onopen: null,
      onmessage: null,
      onclose: null,
      onerror: null,
      send(data: string): void {
        let frame: Record<string, unknown>;
        try { frame = JSON.parse(data) as Record<string, unknown>; } catch { return; }
        stub.handleFrame(socket, frame);
      },
      close(): void {
        if (socket.readyState === 3) return;
        socket.readyState = 3; // CLOSED
        const i = stub.sockets.indexOf(socket);
        if (i >= 0) stub.sockets.splice(i, 1);
        socket.onclose?.();
      },
    };
    this.sockets.push(socket);
    // Open on a microtask AFTER the factory returns: handlers attach first.
    void Promise.resolve().then(() => {
      if (socket.readyState !== 0) return; // closed before it opened
      socket.readyState = 1; // OPEN
      socket.onopen?.();
    });
    return socket;
  };

  /** Deliver one protocol frame from the stubbed far side to `socket` (microtask). */
  private replyTo(socket: ControlSocket, frame: Record<string, unknown>): void {
    const json = JSON.stringify(frame);
    void Promise.resolve().then(() => {
      if (socket.readyState === 1) socket.onmessage?.({ data: json });
    });
  }

  /** The stubbed far side of the duplex: handshake, capture, roster queue. */
  private handleFrame(socket: ControlSocket, frame: Record<string, unknown>): void {
    const type = frame.type;
    if (typeof type !== 'string') return;
    if (type === 'hello') {
      this.replyTo(socket, { type: 'hello', versions: ['0.1'] });
      return;
    }
    if (type === 'subscribe') {
      this.replyTo(socket, { type: 'subscribed', id: frame.id });
      return;
    }
    if (type === 'roster') {
      this.rosterQueue.push({ socket, id: String(frame.id) });
      return;
    }
    // Only Event envelopes carry a dotted type (AEP-0001 §6); every other
    // frame (pong, unknown protocol chatter) is not a command and never
    // reaches `.sent`.
    if (type.includes('.')) this.sentFrames.push(frame as unknown as AepEvent);
  }

  /** Fan an ack out through every connected fake socket (the relay leg). */
  private fanOut(ev: AepEvent): void {
    const json = JSON.stringify(ev);
    for (const socket of [...this.sockets]) {
      void Promise.resolve().then(() => {
        if (socket.readyState === 1) socket.onmessage?.({ data: json });
      });
    }
  }

  /**
   * Script the target ACCEPTING `cmd`: mints the `control.accepted` ack via
   * the stub's real session emitter on the COMMAND's target session
   * (`cause` = the command's id — AEP-0004 §6), records it in {@link acks},
   * and delivers it through every connected fake socket — settling the
   * client's `send()` promise. Returns the ack.
   */
  accept(cmd: AepEvent): AepEvent {
    const session = typeof cmd.session === 'string' ? cmd.session : this.session;
    const ack = this.sessionFor(session).emit('control.accepted', {}, { cause: cmd.id });
    this.fanOut(ack);
    return ack;
  }

  /**
   * Script the target REJECTING `cmd`: mints the `control.rejected` nack
   * (`data.reason` — default `refused` — plus optional `data.detail`,
   * `cause` = the command's id, AEP-0004 §3/§4.2) on the command's target
   * session, records it in {@link acks}, and delivers it through every
   * connected fake socket — the client surfaces it as a wire `NackError`
   * (never synthesized). Returns the nack.
   */
  reject(cmd: AepEvent, opts: { reason?: NackReason; detail?: string } = {}): AepEvent {
    const data: Record<string, unknown> = { reason: opts.reason ?? 'refused' };
    if (opts.detail !== undefined) data.detail = opts.detail;
    const session = typeof cmd.session === 'string' ? cmd.session : this.session;
    const nack = this.sessionFor(session).emit('control.rejected', data, { cause: cmd.id });
    this.fanOut(nack);
    return nack;
  }

  /**
   * Answer the OLDEST unanswered `roster()` request with `entries`
   * verbatim (the id-correlated `roster` reply frame, AEP-0003 §4.1).
   * Throws when no roster request is pending. The Python twin has no
   * client-side roster surface — this is TS-only, because only the TS
   * `ControlClient` exposes `roster()`.
   */
  rosterReply(entries: RosterEntry[]): void {
    const pending = this.rosterQueue.shift();
    if (pending === undefined) {
      throw new Error('rosterReply(): no pending roster request to answer');
    }
    this.replyTo(pending.socket, { type: 'roster', id: pending.id, entries });
  }
}
