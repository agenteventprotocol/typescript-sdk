// Emit helpers: envelope construction with per-session (epoch, seq) ownership
// (AEP-0001 §5/§7) and capture-ceiling stamping (§8.2).
import type { AepEvent, AepPayloadMap, Capture, Severity } from './gen/aep-types.js';
import { ulid } from './ulid.js';

export type Sink = (ev: AepEvent) => void;

/**
 * The envelope's reserved attribute names (AEP-0001 §5.1) — the exact
 * property set of the envelope schema. Extension attributes (§5.4) MUST NOT
 * collide with any of them; on the wire a clobbered core attribute is
 * indistinguishable from the attribute itself, so the collision can only be
 * caught here, at the producing boundary. Kept as a frozen literal (the
 * generated types are erased at runtime); mirror of `aep-event.schema.json`.
 *
 * @internal Named export for `validate.ts` (the `COMMAND_TYPE` precedent:
 * one definition, shared inside the SDK, deliberately not in the root API).
 */
export const RESERVED_ATTRS: ReadonlySet<string> = new Set([
  'aep', 'agent', 'capture', 'cause', 'data', 'epoch', 'id', 'run', 'seq',
  'session', 'severity', 'source', 'step', 'subject', 'time', 'traceparent', 'type',
]);
const EXT_NAME = /^[a-z0-9]{1,20}$/;

/**
 * Enforce AEP-0001 §5.4 at the producing boundary: an extension attribute
 * whose name collides with a core attribute, is ill-formed, or carries a
 * non-scalar value is refused (thrown), never silently merged into the
 * envelope. Returns the map unchanged when every entry is conformant.
 */
function checkExtensions(extensions: Record<string, unknown>): Record<string, unknown> {
  for (const [name, value] of Object.entries(extensions)) {
    if (RESERVED_ATTRS.has(name)) {
      throw new TypeError(`extension attribute "${name}" collides with a reserved envelope attribute (AEP-0001 §5.4)`);
    }
    if (!EXT_NAME.test(name)) {
      throw new TypeError(`extension attribute name "${name}" must match [a-z0-9]{1,20} (AEP-0001 §5.4)`);
    }
    const t = typeof value;
    if (t !== 'string' && t !== 'boolean' && !(t === 'number' && Number.isInteger(value))) {
      throw new TypeError(`extension attribute "${name}" must be a string, integer, or boolean (AEP-0001 §5.4)`);
    }
  }
  return extensions;
}

export interface EmitterOptions {
  /** Producer identity (envelope `agent`). */
  agent: string;
  /** Host segment of the `source` URI; a stable machine name. */
  host: string;
  /** Where finished events go: a function, or a Transport from transport helpers. */
  sink: Sink;
  /**
   * Emitter epoch (AEP-0001 §7): MUST increase every time the emitter restarts
   * without durable seq state. The caller owns persistence; default 0 suits
   * single-shot processes and tests.
   */
  epoch?: number;
  /** Capture ceiling stamped on events that don't set their own (default "metadata"). */
  capture?: Capture;
}

export interface EmitOptions {
  subject?: string;
  run?: string;
  step?: string;
  cause?: string;
  severity?: Severity;
  capture?: Capture;
  traceparent?: string;
  /** Extension attributes (AEP-0001 §5.4): lowercase alphanumeric names. */
  extensions?: Record<string, unknown>;
}

/** Builds valid envelopes for one producer; hand out one `session()` per session. */
export class Emitter {
  readonly agent: string;
  readonly host: string;
  readonly epoch: number;
  readonly capture: Capture;
  private readonly sink: Sink;

  constructor(opts: EmitterOptions) {
    this.agent = opts.agent;
    this.host = opts.host;
    this.epoch = opts.epoch ?? 0;
    this.capture = opts.capture ?? 'metadata';
    this.sink = opts.sink;
  }

  /** Agent-scoped event (`agent.*` and other session-less types). */
  emit(type: string, data?: Record<string, unknown>, opts: EmitOptions = {}): AepEvent {
    const ev = this.build(type, data, opts);
    this.sink(ev);
    return ev;
  }

  /** A session emitter owning that session's `(epoch, seq)` counter. */
  session(sessionId: string): SessionEmitter {
    return new SessionEmitter(this, sessionId);
  }

  /** @internal */
  build(type: string, data: Record<string, unknown> | undefined, opts: EmitOptions, sessionId?: string): AepEvent {
    const ev: AepEvent = {
      aep: '0.1',
      id: ulid(),
      type,
      time: new Date().toISOString(),
      source: `aep://${this.host}/agent/${this.agent}${sessionId ? `/session/${sessionId}` : ''}`,
      agent: this.agent,
      ...(opts.extensions ? checkExtensions(opts.extensions) : undefined),
    };
    if (opts.subject !== undefined) ev.subject = opts.subject;
    if (opts.run !== undefined) ev.run = opts.run;
    if (opts.step !== undefined) ev.step = opts.step;
    if (opts.cause !== undefined) ev.cause = opts.cause;
    if (opts.severity !== undefined) ev.severity = opts.severity;
    if (opts.traceparent !== undefined) ev.traceparent = opts.traceparent;
    ev.capture = opts.capture ?? this.capture;
    if (data && Object.keys(data).length > 0) ev.data = data;
    return ev;
  }

  /** @internal */
  deliver(ev: AepEvent): void {
    this.sink(ev);
  }
}

/** Session-scoped emission: stamps `session`, `epoch`, and a monotonic `seq`. */
export class SessionEmitter {
  private seq = 0;

  constructor(private readonly emitter: Emitter, readonly sessionId: string) {}

  /** Typed emission for registered payloads; any `string` type is also accepted. */
  emit<T extends keyof AepPayloadMap & string>(type: T, data?: AepPayloadMap[T] & Record<string, unknown>, opts?: EmitOptions): AepEvent;
  emit(type: string, data?: Record<string, unknown>, opts?: EmitOptions): AepEvent;
  emit(type: string, data?: Record<string, unknown>, opts: EmitOptions = {}): AepEvent {
    const ev = this.emitter.build(type, data, opts, this.sessionId);
    ev.session = this.sessionId;
    ev.epoch = this.emitter.epoch;
    ev.seq = this.seq++;
    this.emitter.deliver(ev);
    return ev;
  }

  /**
   * Re-deliver an already-emitted Event verbatim — same `id`, same
   * `(epoch, seq)` position: legal at-least-once redelivery that consumer
   * dedupe collapses (AEP-0001 §7.4). This is the transport for AEP-0004
   * §3's re-acknowledgement of a retransmitted command; the counter is
   * untouched.
   */
  redeliver(ev: AepEvent): void {
    this.emitter.deliver(ev);
  }
}
