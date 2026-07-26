/**
 * State projection: fold a stream of Events into per-session state plus the
 * protocol violations the fold surfaces.
 *
 * The fold rules carry the spec anchors the reference CLI's folds carry —
 * dedupe on `(source, id)` (AEP-0001 §7.4), emitter-scoped session identity
 * `(source, session)` (AEP-0001 §5.2), `(epoch, seq)` ordering (AEP-0001 §7),
 * run and session terminal exclusivity (AEP-0002 §2 convention 5), and
 * pending-attention hygiene (AEP-0002 §7.2). The semantics mirror the
 * reference CLI's validate/timeline folds; the shared golden corpus under
 * `test/fixtures/projection/` (vendored byte-identically in both SDKs) is the
 * executable agreement.
 */
import type { AepEvent } from './gen/aep-types.js';

/**
 * Command frames (AEP-0004 §2.2): `control.attention.respond`,
 * `control.cancel|pause|resume`, and vendor `x.<vendor>.control.*` commands.
 * They carry no `seq`, and their `(source, session)` names the TARGET's
 * session under the SENDER's source — folding them would manufacture phantom
 * sessions, so they are deduplicated but never fold state.
 */
/** @internal Shared with the testing utilities' producing-boundary guard. */
export const COMMAND_TYPE = /^(control\.(attention\.respond|cancel|pause|resume)|x\.[a-z0-9_-]+\.control\..+)$/;

/** Terminal run verbs → the recorded status (AEP-0002 §2 convention 5). */
const RUN_TERMINAL: ReadonlyMap<string, 'finished' | 'failed' | 'cancelled'> = new Map([
  ['run.finished', 'finished'],
  ['run.failed', 'failed'],
  ['run.cancelled', 'cancelled'],
]);

/** One run within a session (AEP-0002 §2 convention 5). */
export interface ProjectedRun {
  /** The `run` envelope attribute. */
  run: string;
  /** `running` until the first terminal Event closes the run. */
  status: 'running' | 'finished' | 'failed' | 'cancelled';
  /** `time` of `run.started`; null when the run was only seen closing. */
  started: string | null;
  /** `time` of the first terminal Event; null while the run is open. */
  ended: string | null;
}

/** One unresolved `attention.requested` (AEP-0002 §7.2). */
export interface PendingAttention {
  /** The `attention.requested` Event id — the `subject` a resolution names. */
  id: string;
  /** The request's `data.kind` (`permission`, `input`, `form`, …); null when absent. */
  kind: string | null;
  /** `time` of the request. */
  since: string | null;
}

/**
 * One session's projected state. Sessions are keyed `(source, session)` —
 * session identity is emitter-scoped (AEP-0001 §5.2): distinct sources
 * sharing a session string are distinct sessions. Unseen values are null,
 * never undefined (the shape serializes canonically).
 */
export interface ProjectedSession {
  /** The emitter's `source` URI. */
  source: string;
  /** The `session` envelope attribute. */
  session: string;
  /** Envelope `agent` of the first folded Event in this session (descriptive, not policed). */
  agent: string;
  /** `time` of `session.started`; null when unseen. */
  started: string | null;
  /** `time` of the first `session.ended`; null while the session is open. */
  ended: string | null;
  /** Last folded `(epoch ?? 0, seq)` from seq-bearing Events (AEP-0001 §7); null before any. */
  position: { epoch: number; seq: number } | null;
  /** Runs in fold order. */
  runs: ProjectedRun[];
  /** Unresolved attention requests in fold order (AEP-0002 §7.2). */
  pending: PendingAttention[];
}

/**
 * A protocol violation the fold surfaced. Each rule carries exactly the keys
 * that scope it — the keys are the contract, there is no prose detail.
 */
export type ProjectionViolation =
  /** The same `(source, id)` named two DIFFERENT Events (AEP-0001 §7.4); the first stays folded. */
  | { rule: 'id-collision'; source: string; id: string }
  /** A second DISTINCT terminal Event for one run (AEP-0002 §2 convention 5); the first terminal's status stays. */
  | { rule: 'run-terminal-exclusivity'; source: string; session: string; run: string }
  /** A second DISTINCT `session.ended` (AEP-0002 §2 convention 5); the first stays. */
  | { rule: 'session-terminal-exclusivity'; source: string; session: string }
  /** `session.ended` while attention requests were pending (AEP-0002 §7.2); `pending` counts them. */
  | { rule: 'ended-with-pending-attention'; source: string; session: string; pending: number }
  /** `epoch` went backwards within a session (AEP-0001 §7). */
  | { rule: 'epoch-regression'; source: string; session: string; id: string }
  /** `seq` failed to advance within an epoch (AEP-0001 §7). */
  | { rule: 'seq-regression'; source: string; session: string; id: string };

/** The projected state: sessions sorted by `(source, session)`, violations in fold order. */
export interface ProjectedState {
  sessions: ProjectedSession[];
  violations: ProjectionViolation[];
}

/** Mutable per-session accumulator (internal). */
interface SessionAccumulator {
  source: string;
  session: string;
  agent: string;
  started: string | null;
  ended: string | null;
  position: { epoch: number; seq: number } | null;
  runs: Map<string, { run: string; status: 'running' | 'finished' | 'failed' | 'cancelled'; started: string | null; ended: string | null }>;
  pending: Map<string, PendingAttention>;
}

/**
 * Sorted-keys canonical JSON — the byte-identity yardstick that tells legal
 * at-least-once redelivery (identical repeat, AEP-0001 §7.4) from an id
 * collision (same `(source, id)`, different Event). `undefined` values are
 * dropped exactly as `JSON.stringify` drops them.
 */
function canonicalJson(v: unknown): string {
  if (v === undefined) return 'undefined';
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
  const entries = Object.entries(v as Record<string, unknown>)
    .filter(([, val]) => val !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, val]) => `${JSON.stringify(k)}:${canonicalJson(val)}`).join(',')}}`;
}

/**
 * Incremental state projection: `apply()` folds one Event at a time in
 * arrival order (ordering regressions surface as violations, AEP-0001 §7);
 * `state()` snapshots the projected state at any point. For a complete
 * capture, {@link projectState} sorts before folding so arrival order is
 * repaired first.
 *
 * The dedupe map is projection-global and consulted at apply time; it is
 * unbounded — a projection lives exactly as long as the capture it folds
 * (unlike the transport-side dedupe sets, which cap their memory).
 */
export class StateProjection {
  /** `${source}\u0000${id}` -> canonical JSON of the first occurrence (AEP-0001 §7.4). */
  private readonly seen = new Map<string, string>();
  /** `${source}\u0000${session}` -> accumulator (AEP-0001 §5.2). */
  private readonly sessions = new Map<string, SessionAccumulator>();
  private readonly violations: ProjectionViolation[] = [];

  /**
   * Fold one Event. Byte-identical redelivery collapses silently; a reused
   * `(source, id)` naming a different Event records `id-collision` and drops
   * the collider (the first occurrence stays folded). Command frames and
   * `agent.*` liveness Events are deduplicated but never fold session state
   * or position.
   */
  apply(ev: AepEvent): void {
    const source = ev.source;
    const id = ev.id;
    if (typeof source !== 'string' || typeof id !== 'string') return; // undedupable: no identity to fold under
    const idKey = `${source}\u0000${id}`;
    const canon = canonicalJson(ev);
    const prior = this.seen.get(idKey);
    if (prior !== undefined) {
      if (prior !== canon) this.violations.push({ rule: 'id-collision', source, id });
      return; // redelivery collapses silently; a collider is dropped
    }
    this.seen.set(idKey, canon);

    // Scope (mirror of the CLI's replay skip): command frames would
    // manufacture phantom sessions, agent.* Events are sessionless liveness.
    if (COMMAND_TYPE.test(ev.type) || ev.type.startsWith('agent.')) return;
    const session = ev.session;
    if (session === undefined) return;

    const sk = `${source}\u0000${session}`;
    let s = this.sessions.get(sk);
    if (!s) {
      s = {
        source, session, agent: ev.agent,
        started: null, ended: null, position: null,
        runs: new Map(), pending: new Map(),
      };
      this.sessions.set(sk, s);
    }

    // Runs (AEP-0002 §2 convention 5). A second DISTINCT run.started for a
    // known run is ignored (first wins, no violation); a terminal with no
    // prior run.started still records the run (started null, no violation).
    if (ev.type === 'run.started' && typeof ev.run === 'string' && !s.runs.has(ev.run)) {
      s.runs.set(ev.run, { run: ev.run, status: 'running', started: ev.time, ended: null });
    }
    const terminal = RUN_TERMINAL.get(ev.type);
    if (terminal !== undefined && typeof ev.run === 'string') {
      const r = s.runs.get(ev.run);
      if (!r) {
        s.runs.set(ev.run, { run: ev.run, status: terminal, started: null, ended: ev.time });
      } else if (r.status !== 'running') {
        this.violations.push({ rule: 'run-terminal-exclusivity', source, session, run: ev.run });
      } else {
        r.status = terminal;
        r.ended = ev.time;
      }
    }

    // Pending attention (AEP-0002 §7.2): resolution is the emitter's
    // attention.resolved (or timeout) — attention.answered does NOT clear.
    // A resolution whose subject is not pending is a silent no-op.
    if (ev.type === 'attention.requested') {
      const kind = ev.data !== undefined && typeof ev.data.kind === 'string' ? ev.data.kind : null;
      s.pending.set(id, { id, kind, since: ev.time });
    }
    if ((ev.type === 'attention.resolved' || ev.type === 'attention.timeout') && typeof ev.subject === 'string') {
      s.pending.delete(ev.subject);
    }

    // Session lifecycle (AEP-0002 §2 convention 5 / §7.2).
    if (ev.type === 'session.started' && s.started === null) s.started = ev.time; // first wins post-dedupe
    if (ev.type === 'session.ended') {
      if (s.pending.size > 0) {
        this.violations.push({ rule: 'ended-with-pending-attention', source, session, pending: s.pending.size });
      }
      if (s.ended !== null) {
        this.violations.push({ rule: 'session-terminal-exclusivity', source, session });
      } else {
        s.ended = ev.time;
      }
    }

    // Ordering + position (AEP-0001 §7): epoch going backwards, or seq
    // failing to advance within an epoch, is a violation; the position is
    // the last folded (epoch ?? 0, seq) regardless.
    if (typeof ev.seq === 'number') {
      const epoch = ev.epoch ?? 0;
      const last = s.position;
      if (last !== null) {
        if (epoch < last.epoch) this.violations.push({ rule: 'epoch-regression', source, session, id });
        else if (epoch === last.epoch && ev.seq <= last.seq) this.violations.push({ rule: 'seq-regression', source, session, id });
      }
      s.position = { epoch, seq: ev.seq };
    }
  }

  /**
   * Snapshot the projected state: sessions sorted by `(source, session)`,
   * runs/pending in fold order, violations in fold order. Unseen values are
   * null, never undefined. The snapshot is detached — later `apply()` calls
   * do not mutate it.
   */
  state(): ProjectedState {
    const sessions = [...this.sessions.values()]
      .sort((a, b) => (a.source < b.source ? -1 : a.source > b.source ? 1
        : a.session < b.session ? -1 : a.session > b.session ? 1 : 0))
      .map((s): ProjectedSession => ({
        source: s.source,
        session: s.session,
        agent: s.agent,
        started: s.started,
        ended: s.ended,
        position: s.position === null ? null : { epoch: s.position.epoch, seq: s.position.seq },
        runs: [...s.runs.values()].map((r): ProjectedRun => ({ run: r.run, status: r.status, started: r.started, ended: r.ended })),
        pending: [...s.pending.values()].map((p): PendingAttention => ({ id: p.id, kind: p.kind, since: p.since })),
      }));
    return { sessions, violations: this.violations.map((v) => ({ ...v })) };
  }
}

/**
 * Batch projection of a complete capture — the same fold as
 * {@link StateProjection}, with arrival order repaired first: Events group by
 * `(source, session)`, each group sorts by `(epoch ?? 0, seq ?? 0)` with a
 * stable sort (ties keep arrival order), and groups apply in sorted
 * `(source, session)` key order, so the violation order is deterministic.
 * After the sort only violations that survive ordering — duplicated
 * positions — surface as `seq-regression` (AEP-0001 §7).
 */
export function projectState(events: Iterable<AepEvent>): ProjectedState {
  const groups = new Map<string, AepEvent[]>();
  for (const ev of events) {
    const source = typeof ev.source === 'string' ? ev.source : '';
    const session = typeof ev.session === 'string' ? ev.session : '';
    // \u0000 can appear in neither field, so the composite sorts as the tuple.
    const key = `${source}\u0000${session}`;
    const group = groups.get(key);
    if (group) group.push(ev);
    else groups.set(key, [ev]);
  }
  const projection = new StateProjection();
  const ordered = [...groups.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  for (const [, group] of ordered) {
    const sorted = [...group].sort((a, b) => ((a.epoch ?? 0) - (b.epoch ?? 0)) || ((a.seq ?? 0) - (b.seq ?? 0)));
    for (const ev of sorted) projection.apply(ev);
  }
  return projection.state();
}
