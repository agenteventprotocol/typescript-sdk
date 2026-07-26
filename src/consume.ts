// Consume helpers: SSE subscription with attr-match filtering, id-dedupe, and
// `(session, epoch, seq)` resume (AEP-0003 §5–7), delivering through a
// callback or as an async iterable. Consumer conformance — dedupe on
// `(source, id)`, tolerate unknown types — is built in, not left to the
// caller.
import type { AepEvent, Capture } from './gen/aep-types.js';
import { TransportError, asTransportError } from './errors.js';

export interface ResumePosition {
  session: string;
  epoch: number;
  seq: number;
}

export interface SubscribeOptions {
  /** Relay base URL, e.g. `http://127.0.0.1:8787`. */
  url: string;
  /** attr-match filter (AEP-0003 §6); `{}` = everything. */
  filter?: Record<string, unknown>;
  /** Subscription capture ceiling — the relay down-levels before delivery. */
  capture?: Capture;
  token?: string;
  /**
   * Resume positions — superseded by live tracking once events flow — or the
   * literal string `'all'` (replay-all, AEP-0003 §5): the FIRST request asks
   * for every session the endpoint holds, each from its earliest held
   * position, with no known-session enumeration; reconnects use tracked
   * positions as usual. Gate on the endpoint advertising `replay.all` (or
   * accept the deterministic refusal): if the first request is refused with a
   * 4xx, the subscription reports it via `onError` and falls back to a
   * live-only stream.
   */
  from?: ResumePosition[] | 'all';
  /**
   * Event delivery. With a callback, every event lands here; without one,
   * events buffer in an internal queue drained by iterating the returned
   * `Subscription` (`for await (const ev of sub)`). With a callback
   * provided, iteration yields nothing — the events went to the callback.
   */
  onEvent?: (ev: AepEvent) => void;
  /** Relay comment lines, e.g. `replay-exhausted <session> <epoch>:<seq>`. */
  onComment?: (comment: string) => void;
  onError?: (err: unknown) => void;
  /**
   * `false` = history-only: the subscriber-facing request carries `live=0`,
   * the relay serves buffered history and closes at its `replay-complete`
   * marker, and the subscription ends by itself — a single pass, no
   * reconnect loop (`done` resolves, iteration ends, no `close()` needed).
   * The 4xx refusal fallbacks (`from: 'all'` refused, resume-set shrink)
   * still retry within the pass; only a completed pass — or a non-4xx
   * failure of it — ends the subscription. Default `true`.
   */
  live?: boolean;
  /** First reconnect delay after a failure; doubles per failure (default 500 ms). */
  backoffInitialMs?: number;
  /** Reconnect delay ceiling (default 10 000 ms). */
  backoffMaxMs?: number;
  /**
   * Budget (in encoded characters) for the resume positions riding the live
   * request's `from` parameter (default 6 000 — far under server header
   * limits); older positions drain through bounded replay requests.
   */
  fromBudget?: number;
  /** Cap on those bounded replay drain requests per (re)connect (default 20). */
  maxReplayChunks?: number;
  /**
   * Bound on connection establishment — request start until response
   * headers arrive — for the live fetch AND the replay drain fetches
   * (default 30 000 ms). Body reads are unbounded: an idle SSE stream is
   * healthy. On expiry the attempt fails as a `network` `TransportError`
   * into the normal retry/backoff path.
   */
  connectTimeoutMs?: number;
}

// Knob defaults.
const BACKOFF_INITIAL_MS = 500;
const BACKOFF_MAX_MS = 10_000;
const FROM_BUDGET = 6_000;
const MAX_REPLAY_CHUNKS = 20;
const CONNECT_TIMEOUT_MS = 30_000;

export interface Subscription {
  close(): void;
  /** Latest per-session positions observed — persist these to resume later. */
  positions(): ResumePosition[];
  /**
   * The completion join: resolves when the subscription ends — `close()`,
   * or `live: false` history completion. Never rejects; errors flow via
   * `onError` and iteration ends.
   */
  readonly done: Promise<void>;
  /**
   * Iterate events when no `onEvent` callback was given (the queue is
   * unbounded until consumed; iteration ends when the subscription ends).
   * Breaking out of a `for await` loop does NOT close the subscription —
   * `close()` stays explicit, and the queue keeps filling meanwhile.
   */
  [Symbol.asyncIterator](): AsyncIterator<AepEvent>;
}

export function subscribe(opts: SubscribeOptions): Subscription {
  let closed = false;
  const live = opts.live ?? true;
  // History-only completion: set when the relay's `replay-complete` comment
  // arrives on the TERMINAL (subscriber-facing) connection. Internal
  // drainReplay bodies end at their own marker without touching the
  // subscription — readBody is told which kind of body it is reading.
  let historyDone = false;
  // Iteration state (used when `onEvent` is absent): an unbounded FIFO of
  // undelivered events, the parked iterator reads waiting on it, and the
  // end-of-subscription sentinel that lets buffered events drain before
  // iteration reports done.
  const queue: AepEvent[] = [];
  const wakers: Array<() => void> = [];
  let ended = false;
  let resolveDone!: () => void;
  const donePromise = new Promise<void>((r) => { resolveDone = r; });
  const deliver = opts.onEvent ?? ((ev: AepEvent) => {
    queue.push(ev);
    wakers.shift()?.();
  });
  /** End the subscription: resolve `done`, wake every parked iterator read. */
  const finish = (): void => {
    if (ended) return;
    ended = true;
    resolveDone();
    while (wakers.length) wakers.shift()?.();
  };
  // Dedupe scope is (source, id) — AEP-0001 §7.4: two emitters may legally
  // mint the same id, so the key must include the emitter's `source`. Key on
  // `${source}\0${id}`: the \0 escape (never a literal NUL byte) can't
  // appear in either field, so the composite can't collide across the
  // source/id boundary.
  const seen = new Set<string>();
  const positions = new Map<string, ResumePosition>();
  // replay-all (AEP-0003 §5): the first request carries `from=all` instead of
  // an enumerated array; positions build from delivery and govern reconnects.
  let pendingAll = opts.from === 'all';
  if (Array.isArray(opts.from)) for (const p of opts.from) positions.set(p.session, p);

  const dedupeKey = (source: string, id: string): string => `${source}\0${id}`;

  const remember = (source: string, id: string) => {
    seen.add(dedupeKey(source, id));
    if (seen.size > 50_000) {
      const first = seen.values().next().value;
      if (first !== undefined) seen.delete(first);
    }
  };

  // The resume set is BOUNDED on the wire: an unbounded known-session set
  // would grow a single `from` query param past the server's HTTP header
  // limit (Node caps the request line + headers at 16 KB), and every
  // reconnect would be refused before any relay code ran — a permanent,
  // opaque failure. Newest positions ride the live request within this
  // budget of encoded characters; older ones drain through bounded `live=0`
  // replay requests whose overlap the (source, id) dedupe absorbs.
  const fromBudget = opts.fromBudget ?? FROM_BUDGET;
  const maxReplayChunks = opts.maxReplayChunks ?? MAX_REPLAY_CHUNKS;
  const backoffInitialMs = opts.backoffInitialMs ?? BACKOFF_INITIAL_MS;
  const backoffMaxMs = opts.backoffMaxMs ?? BACKOFF_MAX_MS;
  const connectTimeoutMs = opts.connectTimeoutMs ?? CONNECT_TIMEOUT_MS;

  const encodedSize = (p: ResumePosition): number =>
    encodeURIComponent(JSON.stringify(p)).length + 3; // +3: the encoded comma

  /** Newest-within-budget tail of `all` (capped at `cap` entries) + the older rest. */
  function splitFrom(all: ResumePosition[], cap: number): { head: ResumePosition[]; rest: ResumePosition[] } {
    const head: ResumePosition[] = [];
    let size = 2;
    let i = all.length - 1;
    for (; i >= 0 && head.length < cap; i--) {
      const s = encodedSize(all[i] as ResumePosition);
      if (head.length > 0 && size + s > fromBudget) break;
      head.unshift(all[i] as ResumePosition);
      size += s;
    }
    return { head, rest: all.slice(0, i + 1) };
  }

  /**
   * Fetch with the connect bound: the abort timer arms at request start and
   * clears the moment `fetch` resolves — which is when response headers
   * arrive — so body reads stay unbounded (an SSE stream must idle freely).
   * A timer-fired abort surfaces as a `network` TransportError into the
   * normal retry path; every other failure passes through untouched.
   */
  async function connectFetch(url: string): Promise<FetchResponse> {
    const ctrl = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; ctrl.abort(); }, connectTimeoutMs);
    try {
      return await fetch(url, { headers: { accept: 'text/event-stream' }, signal: ctrl.signal });
    } catch (e) {
      if (timedOut) throw new TransportError('network', `connect timeout after ${connectTimeoutMs} ms`, { cause: e });
      throw e;
    } finally {
      clearTimeout(timer);
    }
  }

  function streamUrl(from: ResumePosition[] | 'all', live: boolean): string {
    const params = [
      `filter=${encodeURIComponent(JSON.stringify(opts.filter ?? {}))}`,
      opts.capture ? `capture=${opts.capture}` : '',
      opts.token ? `access_token=${encodeURIComponent(opts.token)}` : '',
      from === 'all' ? 'from=all'
        : from.length ? `from=${encodeURIComponent(JSON.stringify(from))}` : '',
      live ? '' : 'live=0',
    ].filter(Boolean).join('&');
    return `${opts.url.replace(/\/$/, '')}/stream?${params}`;
  }

  /**
   * Parse one SSE body through the shared dedupe/position/event pipeline.
   * `terminal` marks the subscriber-facing history-only pass: only there
   * does the relay's `replay-complete` marker end the subscription.
   */
  async function readBody(res: FetchResponse, terminal: boolean): Promise<void> {
    const reader = res.body!.getReader();
    const dec = new TextDecoder();
    let buf = '';
    let data: string[] = [];
    const flush = () => {
      if (!data.length) return;
      const text = data.join('\n');
      data = [];
      let ev: AepEvent;
      try { ev = JSON.parse(text) as AepEvent; } catch (e) {
        opts.onError?.(new TransportError('parse', 'undecodable SSE payload', { cause: e }));
        return;
      }
      if (typeof ev.id !== 'string' || typeof ev.source !== 'string' || seen.has(dedupeKey(ev.source, ev.id))) return;
      remember(ev.source, ev.id);
      if (ev.session !== undefined && ev.seq !== undefined) {
        positions.set(ev.session, { session: ev.session, epoch: ev.epoch ?? 0, seq: ev.seq });
      }
      deliver(ev);
    };
    for (;;) {
      const { done, value } = await reader.read();
      if (done || closed) break;
      buf += dec.decode(value, { stream: true });
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        if (line === '') { flush(); continue; }
        if (line.startsWith(':')) {
          const comment = line.slice(1).trim();
          opts.onComment?.(comment);
          // Only the terminal (subscriber-facing) history pass ends the
          // subscription — internal drainReplay bodies end at their own
          // marker without touching it. `replay-exhausted <session>
          // <epoch>:<seq>` is a DIFFERENT marker (per-session eviction
          // notice) and must not match.
          if (terminal && comment.startsWith('replay-complete')) historyDone = true;
          continue;
        }
        if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, ''));
        // `event:` and `id:` lines are advisory here; position tracking uses the envelope
      }
    }
  }

  /** Drain positions that did not fit the live request: bounded replay-only fetches. */
  async function drainReplay(rest: ResumePosition[]): Promise<void> {
    let remaining = rest;
    for (let chunk = 0; remaining.length && chunk < maxReplayChunks && !closed; chunk++) {
      const { head, rest: older } = splitFrom(remaining, Infinity);
      remaining = older;
      try {
        const res = await connectFetch(streamUrl(head, false));
        if (!res.ok || !res.body) throw new TransportError('http', `SSE HTTP ${res.status}`, { status: res.status });
        await readBody(res, false);
      } catch (e) {
        // A failed replay chunk costs history, never the live pipe: report and
        // stop draining (the refusing relay would refuse the rest the same way).
        if (!closed) opts.onError?.(asTransportError(e));
        return;
      }
    }
  }

  async function loop(): Promise<void> {
    let backoff = backoffInitialMs;
    let fromCap = Infinity;
    while (!closed && !historyDone) {
      let sentFrom = 0;
      let sentAll = false;
      try {
        // replay-all rides the first request only: once events flow (or the
        // request was refused), tracked positions govern every reconnect.
        const useAll = pendingAll && positions.size === 0;
        sentAll = useAll;
        const { head, rest } = useAll
          ? { head: [] as ResumePosition[], rest: [] as ResumePosition[] }
          : splitFrom([...positions.values()], fromCap);
        sentFrom = head.length;
        const res = await connectFetch(streamUrl(useAll ? 'all' : head, live));
        if (!res.ok || !res.body) throw new TransportError('http', `SSE HTTP ${res.status}`, { status: res.status });
        pendingAll = false;
        backoff = backoffInitialMs;
        if (rest.length) void drainReplay(rest);
        // With `live: false` this IS the terminal history pass: only its
        // `replay-complete` marker may end the subscription.
        await readBody(res, !live);
      } catch (e) {
        if (!closed) {
          const err = asTransportError(e);
          opts.onError?.(err);
          // Self-heal: a relay may cap `from` (e.g. AEP_MAX_FROM) and refuse a
          // resume-carrying request with a 4xx. Resume is an optimization —
          // shrink the set and retry promptly rather than looping dead on an
          // identical refusal. Network errors keep the full set: the relay may
          // be down, and resume must survive its return.
          if (err.kind === 'http' && (err.status ?? 0) >= 400 && (err.status ?? 0) < 500 && sentAll) {
            // The endpoint refused `from=all` (deterministically, AEP-0003
            // §5 — it does not implement the value): fall back to a
            // live-only stream; the caller heard the refusal via onError.
            pendingAll = false;
            await new Promise((r) => setTimeout(() => r(undefined), 150));
            continue;
          }
          if (err.kind === 'http' && (err.status ?? 0) >= 400 && (err.status ?? 0) < 500 && sentFrom > 0) {
            fromCap = Math.floor(sentFrom / 2);
            await new Promise((r) => setTimeout(() => r(undefined), 150));
            continue;
          }
        }
      }
      // The single-pass exit sits AFTER the try/catch — placement matters:
      // the 4xx-refusal `continue` paths above skip it, so a refused
      // `from=all` or resume-carrying request still retries even when
      // `live: false`, while a completed history pass — or a non-4xx
      // failure of it — ends the subscription here.
      if (historyDone || !live || closed) break;
      await new Promise((r) => setTimeout(() => r(undefined), backoff));
      backoff = Math.min(backoff * 2, backoffMaxMs);
    }
    finish();
  }
  void loop();

  return {
    close() {
      closed = true;
      finish();
    },
    positions() { return [...positions.values()]; },
    done: donePromise,
    [Symbol.asyncIterator](): AsyncIterator<AepEvent> {
      // Each call returns a fresh iterator over the ONE shared queue —
      // concurrent iterators compete for events rather than fanning out.
      return {
        next: async (): Promise<IteratorResult<AepEvent>> => {
          for (;;) {
            if (queue.length) return { value: queue.shift() as AepEvent, done: false };
            if (ended) return { value: undefined, done: true };
            await new Promise<void>((r) => { wakers.push(r); });
          }
        },
        // Deliberately no `return` handler: breaking out of a `for await`
        // leaves the subscription open — `close()` stays explicit — and
        // the queue keeps filling until it is called.
      };
    },
  };
}
