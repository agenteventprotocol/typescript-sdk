// End-to-end smoke for @agenteventprotocol/sdk against the reference relay: emit over HTTP,
// consume over SSE (dedupe + positions), control round-trip over WS with a
// second SDK instance acting as the target. Run via test/run-smoke.sh.
import { ControlTarget, Emitter, subscribe, ControlClient, NackError, TransportError, httpTransport, wsTransport, jsonlSink, ulid } from '../src/index.js';
import type { AepEvent } from '../src/index.js';
// node:net (declared in test/env.d.ts) backs the in-process parse-beat fixture.

const RELAY = process.env.AEP_RELAY ?? 'http://127.0.0.1:18787';
const TOKEN = process.env.AEP_TOKEN; // the fixture relay is watch-only without it (AEP-0004 §4.1)
declare const process: { env: Record<string, string | undefined>; exit(code: number): never };

let failures = 0;
const check = (name: string, ok: boolean) => {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}`);
  if (!ok) failures++;
};
const sleep = (ms: number) => new Promise((r) => setTimeout(() => r(undefined), ms));

async function main(): Promise<void> {
  const session = `s_sdk_${ulid().slice(-8).toLowerCase()}`;

  // --- consumer -----------------------------------------------------------------
  const got: AepEvent[] = [];
  const sub = subscribe({
    url: RELAY,
    token: TOKEN,
    filter: { session },
    onEvent: (ev) => { got.push(ev); },
  });

  // --- emitting agent on a duplex, accepting control ------------------------------
  const pendingAttention = new Map<string, string>(); // request id -> session
  let target!: ReturnType<Emitter['session']>;
  const wsT = wsTransport(RELAY, {
    agent: 'sdk-agent',
    host: 'smoke-host',
    token: TOKEN,
    controlAccepts: ['control.attention.respond'],
    onCommand: (cmd) => {
      // target side: dedupe + ack + outcome (AEP-0004 §3/§7)
      if (cmd.type !== 'control.attention.respond' || typeof cmd.subject !== 'string') return;
      if (!pendingAttention.has(cmd.subject)) {
        target.emit('control.rejected', { reason: 'invalid' }, { cause: cmd.id });
        return;
      }
      pendingAttention.delete(cmd.subject);
      target.emit('control.accepted', {}, { cause: cmd.id });
      const d = (cmd.data ?? {}) as { answer?: { option?: string } };
      const ans = target.emit('attention.answered',
        { answer: { option: d.answer?.option ?? 'deny' }, via: 'control' },
        { subject: cmd.subject, cause: cmd.id });
      target.emit('attention.resolved', { resolution: 'answered' },
        { subject: cmd.subject, cause: ans.id });
    },
  });
  const emitter = new Emitter({ agent: 'sdk-agent', host: 'smoke-host', sink: wsT.sink, epoch: 1 });
  target = emitter.session(session);

  await sleep(500);
  target.emit('session.started', { client: { name: 'sdk-smoke' } });
  const run = target.emit('run.started', { trigger: 'user' }, { run: 'r_1' });
  const req = target.emit('attention.requested', {
    kind: 'permission',
    prompt: 'ok to proceed?',
    options: [{ id: 'allow', label: 'Allow' }, { id: 'deny', label: 'Deny' }],
    respond_via: ['control'],
  }, { run: 'r_1', cause: run.id, severity: 'notice' });
  pendingAttention.set(req.id, session);

  // --- HTTP transport emits into the same session view (foreign agent scope) -----
  const httpT = httpTransport(RELAY, { token: TOKEN, onError: (e) => console.error('http:', e) });
  const httpEmitter = new Emitter({ agent: 'sdk-agent-2', host: 'smoke-host', sink: httpT.sink, epoch: 1 });
  httpEmitter.session(`${session}_b`).emit('session.started', { client: { name: 'sdk-smoke-b' } });

  // --- control sender ---------------------------------------------------------------
  const control = new ControlClient({ url: RELAY, agent: 'sdk-phone', host: 'smoke-host', token: TOKEN });
  await sleep(500);
  try {
    const ack = await control.send({
      type: 'control.attention.respond',
      session,
      subject: req.id,
      cause: req.id,
      data: { answer: { option: 'allow' } },
    }, { ackWindowMs: 5000, retries: 1 });
    check('control.send resolved with control.accepted', ack.type === 'control.accepted');
    check('ack is cause-linked to the command', typeof ack.cause === 'string');
  } catch (e) {
    check(`control.send resolved (got ${(e as Error).message})`, false);
  }

  // unknown-subject command must nack `invalid`
  try {
    await control.send({
      type: 'control.attention.respond',
      session,
      subject: ulid(),
      data: { answer: { option: 'allow' } },
    }, { ackWindowMs: 5000, retries: 0 });
    check('unknown-subject command nacked', false);
  } catch (e) {
    check('unknown-subject command nacked invalid',
      e instanceof NackError && e.reason === 'invalid');
  }

  // relay error frames settle the pending command immediately (§4.2-style
  // on-behalf refusals: unsupported/unauthorized/no-route) — never as a
  // silent window timeout
  const t0 = Date.now();
  try {
    await control.send({ type: 'control.cancel', session: `s_nobody_${ulid().slice(-6).toLowerCase()}`,
      data: { scope: 'run' }, run: 'r_none' }, { ackWindowMs: 8000, retries: 0 });
    check('unrouted command rejected via the relay error frame', false);
  } catch (e) {
    const fast = Date.now() - t0 < 4000;
    check('unrouted command rejected via the relay error frame (immediate, not timeout)',
      e instanceof NackError && !e.synthesized && fast && /no-route/.test(e.detail ?? ''));
  }

  await sleep(800);
  const types = got.map((e) => e.type);
  check('consumer saw session.started', types.includes('session.started'));
  check('consumer saw attention round-trip',
    types.includes('attention.requested') && types.includes('attention.answered') && types.includes('attention.resolved'));
  check('consumer saw the command on-stream (audit)', types.includes('control.attention.respond'));
  const positions = sub.positions();
  check('positions tracked for resume', positions.some((p) => p.session === session && p.seq > 0));
  const seqs = got.filter((e) => e.session === session && e.seq !== undefined).map((e) => e.seq as number);
  check('seq strictly monotonic in view', seqs.every((s, i) => i === 0 || s > (seqs[i - 1] as number)));

  // --- roster (AEP-0003 §4.1): declared capability visibility --------------------
  // The wsTransport emitter above is still live and claims `session` with a
  // declared control block; the HTTP-emitted `${session}_b` has no live duplex
  // claim and must be absent (absence = no live route).
  try {
    const entries = await control.roster({ timeoutMs: 5000 });
    const mine = entries.find((e) => e.session === session);
    check('roster: the live emitter claim is listed with its declared accepts verbatim',
      mine !== undefined && mine.agent === 'sdk-agent'
      && (mine.control?.accepts ?? []).includes('control.attention.respond'));
    check('roster: a session emitted only over HTTP has no live claim and is absent',
      !entries.some((e) => e.session === `${session}_b`));
  } catch (e) {
    check(`roster: answered (got ${(e as Error).message})`, false);
    check('roster: a session emitted only over HTTP has no live claim and is absent', false);
  }

  // --- replay-all (AEP-0003 §5): cold start without enumeration ------------------
  const allGot: AepEvent[] = [];
  const allSub = subscribe({
    url: RELAY, token: TOKEN, filter: { session }, from: 'all',
    onEvent: (ev) => { allGot.push(ev); },
  });
  await sleep(800);
  check('replay-all: a from:"all" cold start replays held history without enumerating sessions',
    allGot.some((e) => e.type === 'session.started') && allGot.some((e) => e.type === 'attention.requested'));
  allSub.close();

  // --- consumer dedupe scope is (source, id), not bare id (AEP-0001 §7.4) --------
  // Two different emitters may legally mint the same event id: both must be
  // delivered. A repeat of the exact same (source, id) must be dropped.
  const dedupeSessionA = `s_dedupe_a_${ulid().slice(-6).toLowerCase()}`;
  const dedupeSessionB = `s_dedupe_b_${ulid().slice(-6).toLowerCase()}`;
  const dedupeGot: AepEvent[] = [];
  const dedupeSub = subscribe({
    url: RELAY,
    token: TOKEN,
    filter: { session: [dedupeSessionA, dedupeSessionB] },
    onEvent: (ev) => { dedupeGot.push(ev); },
  });
  await sleep(300);

  const sharedId = ulid();
  const evFromA: AepEvent = {
    aep: '0.1', id: sharedId, type: 'session.started', time: new Date().toISOString(),
    source: 'aep://smoke-host/agent/sdk-dedupe-emitter-a', agent: 'sdk-dedupe-emitter-a',
    session: dedupeSessionA, seq: 0, capture: 'metadata', data: { client: { name: 'dedupe-a' } },
  };
  const evFromB: AepEvent = {
    aep: '0.1', id: sharedId, type: 'session.started', time: new Date().toISOString(),
    source: 'aep://smoke-host/agent/sdk-dedupe-emitter-b', agent: 'sdk-dedupe-emitter-b',
    session: dedupeSessionB, seq: 0, capture: 'metadata', data: { client: { name: 'dedupe-b' } },
  };
  httpT.sink(evFromA);
  httpT.sink(evFromB);
  await sleep(500);
  check('same id, different source: both delivered (AEP-0001 §7.4)',
    dedupeGot.some((e) => e.id === sharedId && e.source === evFromA.source)
    && dedupeGot.some((e) => e.id === sharedId && e.source === evFromB.source));

  // exact repeat of (source, id) evFromA, addressed to the other session so
  // the relay's own per-session id-set (a separate, session-scoped mechanism)
  // doesn't itself swallow it before it reaches the SDK's subscriber dedupe
  const repeatOfA: AepEvent = { ...evFromA, session: dedupeSessionB, seq: 1, time: new Date().toISOString() };
  httpT.sink(repeatOfA);
  await sleep(500);
  check('repeat of the same (source, id) is dropped',
    dedupeGot.filter((e) => e.id === sharedId && e.source === evFromA.source).length === 1);

  dedupeSub.close();

  sub.close();
  control.close();
  wsT.close();

  // --- ControlTarget: target-side conformance over an in-memory wire --------------
  // (AEP-0004 §7: dedupe on (source, id) — AEP-0001 §7.4, exactly one ack per
  // deduplicated command, `unsupported` nack for undeclared types — the same
  // helper surface the Python SDK ships.)
  const wire: AepEvent[] = [];
  const targetEmitter = new Emitter({
    agent: 'sdk-target', host: 'smoke-host', epoch: 1,
    sink: (ev) => { wire.push(ev); },
  }).session('s_target_1');
  const ctlTarget = new ControlTarget(targetEmitter, ['control.attention.respond']);
  let executions = 0;
  const mkCmd = (type: string, id: string): AepEvent => ({
    aep: '0.1', id, type, time: new Date().toISOString(),
    source: 'aep://smoke-host/agent/sdk-console', agent: 'sdk-console',
    session: 's_target_1', capture: 'metadata',
    data: { answer: { option: 'allow' } }, subject: 'req-1',
  });

  const cmd = mkCmd('control.attention.respond', ulid());
  const first = ctlTarget.handle(cmd, () => { executions++; });
  const acksNow = wire.filter((e) => e.type === 'control.accepted' && e.cause === cmd.id);
  check('target: first delivery acked and executed once', first && executions === 1 && acksNow.length === 1);

  const dup = ctlTarget.handle(cmd, () => { executions++; });
  const dupAcks = wire.filter((e) => e.type === 'control.accepted' && e.cause === cmd.id);
  check('target: a retransmit is the same command — no re-execute, the recorded ack re-emitted byte-identically (§3)',
    !dup && executions === 1
    && dupAcks.length === 2 && new Set(dupAcks.map((e) => e.id)).size === 1
    && JSON.stringify(dupAcks[0]) === JSON.stringify(dupAcks[1]));

  // same command id, different source: a different sender legally minting
  // the same id is NOT a duplicate — dedupe scope is (source, id), AEP-0001 §7.4
  const sameIdOtherSource: AepEvent = { ...cmd, source: 'aep://smoke-host/agent/sdk-console-2' };
  const otherSourceFirst = ctlTarget.handle(sameIdOtherSource, () => { executions++; });
  check('target: same id from a different source is executed, not treated as a duplicate',
    otherSourceFirst && executions === 2
    && new Set(wire.filter((e) => e.type === 'control.accepted' && e.cause === cmd.id).map((e) => e.id)).size === 2);

  const alien = mkCmd('control.pause', ulid());
  const handled = ctlTarget.handle(alien, () => { executions++; });
  const nacks = wire.filter((e) => e.type === 'control.rejected' && e.cause === alien.id);
  check('target: an undeclared type is nacked unsupported, never executed',
    !handled && executions === 2 && nacks.length === 1
    && (nacks[0]?.data as { reason?: string } | undefined)?.reason === 'unsupported');
  check('target: acks are ordinary sequenced session events (AEP-0004 §2.2)',
    wire.every((e) => typeof e.seq === 'number' && e.session === 's_target_1'));

  // --- extension attributes are validated at the producing boundary (AEP-0001 §5.4)
  // A collision with a core attribute is wire-invisible (a clobbered `id` is
  // just an `id`), so the emitter must refuse it rather than merge it. Grammar
  // and value-type violations are refused the same way.
  const extEmitter = new Emitter({ agent: 'sdk-ext', host: 'smoke-host', sink: () => {} }).session('s_ext_1');
  const refuses = (extensions: Record<string, unknown>): boolean => {
    try { extEmitter.emit('progress.status', {}, { extensions }); return false; }
    catch (e) { return e instanceof TypeError; }
  };
  check('extensions: a reserved-name collision is refused, not merged (§5.4)', refuses({ id: 'spoofed' }));
  check('extensions: colliding on session/epoch/seq is refused (§5.4)',
    refuses({ session: 'x' }) && refuses({ epoch: 9 }) && refuses({ seq: 0 }));
  check('extensions: an ill-formed name is refused (§5.4 [a-z0-9]{1,20})',
    refuses({ my_ext: 'v' }) && refuses({ Ext: 'v' }) && refuses({ ['toolongextensionname123']: 'v' }));
  check('extensions: a non-scalar value (or a non-integer number) is refused (§5.4)',
    refuses({ meta: { k: 1 } }) && refuses({ ratio: 1.5 }) && refuses({ items: [1] }));
  let legalExt: AepEvent | null = null;
  try { legalExt = extEmitter.emit('progress.status', {}, { extensions: { acme1: 'v', acmecount: 3, acmeok: true } }); }
  catch { legalExt = null; }
  check('extensions: legal scalar extensions still emit and round-trip',
    legalExt !== null && legalExt.acme1 === 'v' && legalExt.acmecount === 3 && legalExt.acmeok === true);

  // --- transport-error taxonomy (network / http / parse; nacks stay NackError) ----
  const te = new TransportError('http', 'probe', { status: 401 });
  check('TransportError is an Error carrying its kind (and status for http)',
    te instanceof Error && te.name === 'TransportError' && te.kind === 'http' && te.status === 401);

  // http: the relay ANSWERS with a refusing status (wrong token on ingest)
  const ingestErrs: unknown[] = [];
  const badTokenT = httpTransport(RELAY, { token: 'not-the-token', onError: (e) => ingestErrs.push(e) });
  badTokenT.sink({
    aep: '0.1', id: ulid(), type: 'session.started', time: new Date().toISOString(),
    source: 'aep://smoke-host/agent/sdk-taxonomy', agent: 'sdk-taxonomy',
    session: 's_taxonomy_http', seq: 0, capture: 'metadata',
  });
  await sleep(400);
  check('taxonomy: a refusing HTTP status surfaces as TransportError http with the status',
    ingestErrs.some((e) => e instanceof TransportError && e.kind === 'http' && e.status === 401));

  // network: nothing answers at all
  const netErrs: unknown[] = [];
  const deadSub = subscribe({
    url: 'http://127.0.0.1:9', filter: {},
    onEvent: () => { /* unreachable */ },
    onError: (e) => netErrs.push(e),
  });
  await sleep(600);
  deadSub.close();
  check('taxonomy: an unreachable relay surfaces as TransportError network',
    netErrs.some((e) => e instanceof TransportError && e.kind === 'network'));

  // parse: an undecodable payload is reported AND the stream keeps working —
  // a fixture SSE endpoint serves one malformed frame, then a valid event
  const parseEv: AepEvent = {
    aep: '0.1', id: ulid(), type: 'session.started', time: new Date().toISOString(),
    source: 'aep://smoke-host/agent/sdk-parse', agent: 'sdk-parse',
    session: 's_taxonomy_parse', seq: 0, capture: 'metadata',
  };
  const { createServer } = await import('node:net');
  const sseFixture = createServer((sock) => {
    sock.write('HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n');
    sock.write('data: {not json\n\n');
    sock.write(`data: ${JSON.stringify(parseEv)}\n\n`);
  });
  await new Promise<void>((r) => sseFixture.listen(0, '127.0.0.1', () => r()));
  const addr = sseFixture.address();
  const ssePort = typeof addr === 'object' && addr !== null ? addr.port : 0;
  const parseErrs: unknown[] = [];
  const parseGot: AepEvent[] = [];
  const parseSub = subscribe({
    url: `http://127.0.0.1:${ssePort}`, filter: {},
    onEvent: (ev) => parseGot.push(ev),
    onError: (e) => parseErrs.push(e),
  });
  await sleep(700);
  parseSub.close();
  sseFixture.close();
  check('taxonomy: an undecodable payload is reported as parse and the stream survives it',
    parseErrs.some((e) => e instanceof TransportError && e.kind === 'parse')
    && parseGot.some((e) => e.id === parseEv.id));

  // --- resume-set bounding: a large known-session set must never kill the pipe --
  // An unbounded known-session list sent as one `from` query param would
  // exceed the server's header limit: past ~16 KB the HTTP layer refuses the
  // request before any relay code runs, permanently. The SDK must bound what
  // rides the live request and drain the rest through bounded replay requests.
  const bigSession = `s_bigfrom_${ulid().slice(-8).toLowerCase()}`;
  const bigT = httpTransport(RELAY, { token: TOKEN });
  const mkBig = (seq: number): AepEvent => ({
    aep: '0.1', id: ulid(), type: 'log.emitted', time: new Date().toISOString(),
    source: `aep://smoke-host/agent/sdk-bigfrom/session/${bigSession}`, agent: 'sdk-bigfrom',
    session: bigSession, seq, epoch: 1, capture: 'metadata',
  } as AepEvent);
  for (const seq of [0, 1, 2]) bigT.sink(mkBig(seq));
  await sleep(400);
  const phantomFrom = Array.from({ length: 240 }, (_, i) => ({
    session: `s_phantom_${String(i).padStart(4, '0')}_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`, epoch: 0, seq: -1,
  }));
  const bigGot: AepEvent[] = [];
  const bigSub = subscribe({
    url: RELAY, token: TOKEN, filter: { session: bigSession },
    from: [...phantomFrom, { session: bigSession, epoch: 0, seq: -1 }],
    onEvent: (ev) => bigGot.push(ev),
  });
  await sleep(1500);
  check('bounded resume: buffered events replay despite a 241-position known-session set',
    [0, 1, 2].every((seq) => bigGot.some((e) => e.seq === seq)));
  bigT.sink(mkBig(3));
  await sleep(600);
  check('bounded resume: the stream is LIVE after the oversized cold start',
    bigGot.some((e) => e.seq === 3));
  bigSub.close();

  // --- resume self-heal: a refused resume set shrinks instead of looping dead --
  // A relay may cap `from` (AEP_MAX_FROM); the refusal is a readable 400. The
  // SDK must treat resume as an optimization: shrink and retry, never die.
  const healEv: AepEvent = {
    aep: '0.1', id: ulid(), type: 'session.started', time: new Date().toISOString(),
    source: 'aep://smoke-host/agent/sdk-heal', agent: 'sdk-heal',
    session: 's_heal', seq: 0, capture: 'metadata',
  };
  const { createServer: createHealServer } = await import('node:net');
  const healFixture = createHealServer((sock) => {
    sock.once('data', (raw) => {
      const line = String(raw).split('\r\n')[0] ?? '';
      const q = line.split(' ')[1] ?? '';
      const fromParam = /[?&]from=([^& ]*)/.exec(q)?.[1] ?? '';
      const entries = fromParam ? (JSON.parse(decodeURIComponent(fromParam)) as unknown[]).length : 0;
      if (entries > 0) {
        sock.write('HTTP/1.1 400 Bad Request\r\naccess-control-allow-origin: *\r\nconnection: close\r\n\r\nfrom too large');
        sock.end();
        return;
      }
      sock.write('HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n');
      sock.write(`data: ${JSON.stringify(healEv)}\n\n`);
    });
  });
  await new Promise<void>((r) => healFixture.listen(0, '127.0.0.1', () => r()));
  const healAddr = healFixture.address();
  const healPort = typeof healAddr === 'object' && healAddr !== null ? healAddr.port : 0;
  const healErrs: unknown[] = [];
  const healGot: AepEvent[] = [];
  const healSub = subscribe({
    url: `http://127.0.0.1:${healPort}`, filter: {},
    from: [{ session: 's_heal_x', epoch: 0, seq: -1 }, { session: 's_heal_y', epoch: 0, seq: -1 }],
    onEvent: (ev) => healGot.push(ev),
    onError: (e) => healErrs.push(e),
  });
  await sleep(2000);
  healSub.close();
  healFixture.close();
  check('resume self-heal: the refusal surfaced as a readable HTTP 400',
    healErrs.some((e) => e instanceof TransportError && e.kind === 'http' && e.status === 400));
  check('resume self-heal: the stream recovered by shrinking the resume set',
    healGot.some((e) => e.id === healEv.id));

  // --- async-iterable consumption: subscribe without a callback -----------------
  // `onEvent` is optional: without it, events land in an internal queue drained
  // via `for await (const ev of sub)` — the iterator blocks until delivery, so
  // no polling. Breaking out of the loop does NOT close the subscription
  // (close() stays explicit), and `done` settles only when the subscription
  // ends.
  const iterSession = `s_iter_${ulid().slice(-8).toLowerCase()}`;
  const iterT = httpTransport(RELAY, { token: TOKEN });
  const iterEmitter = new Emitter({ agent: 'sdk-iter', host: 'smoke-host', sink: iterT.sink, epoch: 1 }).session(iterSession);
  const iterSub = subscribe({ url: RELAY, token: TOKEN, filter: { session: iterSession } });
  await sleep(400);
  iterEmitter.emit('session.started', { client: { name: 'sdk-iter' } });
  iterEmitter.emit('progress.status', {});
  iterEmitter.emit('progress.status', {});
  const iterGot: AepEvent[] = [];
  for await (const ev of iterSub) {
    iterGot.push(ev);
    if (iterGot.length >= 3) break;
  }
  check('iterate: for-await delivers live events without an onEvent callback',
    iterGot.length === 3 && iterGot[0]?.type === 'session.started'
    && iterGot.every((e) => e.session === iterSession));
  // still open after the break: the stream keeps consuming (positions advance)
  // and `done` stays pending until the explicit close()
  iterEmitter.emit('progress.status', {}); // seq 3, emitted after the break
  await sleep(500);
  const openAfterBreak = await Promise.race([
    iterSub.done.then(() => 'resolved' as const),
    sleep(300).then(() => 'open' as const),
  ]);
  check('iterate: breaking out of the loop leaves the subscription open (close stays explicit)',
    openAfterBreak === 'open'
    && iterSub.positions().some((p) => p.session === iterSession && p.seq >= 3));
  iterSub.close();
  const iterDone = await Promise.race([
    iterSub.done.then(() => 'resolved' as const),
    sleep(1000).then(() => 'pending' as const),
  ]);
  check('iterate: done resolves once the subscription is closed', iterDone === 'resolved');

  // --- history-only mode (live: false): a finite read of held history -----------
  // A single pass, no reconnect loop: the relay's replay-complete marker on the
  // subscriber-facing connection ends the subscription, so iteration ends by
  // itself — no close() — and `done` resolves.
  const histSession = `s_hist_${ulid().slice(-8).toLowerCase()}`;
  const histT = httpTransport(RELAY, { token: TOKEN });
  const mkHist = (seq: number): AepEvent => ({
    aep: '0.1', id: ulid(), type: 'log.emitted', time: new Date().toISOString(),
    source: `aep://smoke-host/agent/sdk-hist/session/${histSession}`, agent: 'sdk-hist',
    session: histSession, seq, epoch: 1, capture: 'metadata',
  } as AepEvent);
  for (const seq of [0, 1, 2]) histT.sink(mkHist(seq));
  await sleep(400);
  const histGot: AepEvent[] = [];
  const histSub = subscribe({ url: RELAY, token: TOKEN, filter: { session: histSession }, from: 'all', live: false });
  for await (const ev of histSub) histGot.push(ev);
  check('history-only: live:false + from:"all" replays held history and iteration ends by itself',
    [0, 1, 2].every((seq) => histGot.some((e) => e.session === histSession && e.seq === seq)));
  const histDone = await Promise.race([
    histSub.done.then(() => 'resolved' as const),
    sleep(1000).then(() => 'pending' as const),
  ]);
  check('history-only: done resolves without an explicit close()', histDone === 'resolved');

  // an enumerated mid-stream position replays strictly-after events only
  const midGot: AepEvent[] = [];
  const midSub = subscribe({
    url: RELAY, token: TOKEN, filter: { session: histSession },
    from: [{ session: histSession, epoch: 1, seq: 0 }], live: false,
  });
  for await (const ev of midSub) midGot.push(ev);
  check('history-only: an enumerated mid-stream position replays only the later events',
    !midGot.some((e) => e.seq === 0)
    && [1, 2].every((seq) => midGot.some((e) => e.session === histSession && e.seq === seq)));
  const midDone = await Promise.race([
    midSub.done.then(() => 'resolved' as const),
    sleep(1000).then(() => 'pending' as const),
  ]);
  check('history-only: the mid-stream pass also ends by itself', midDone === 'resolved');

  // --- done: the completion join works for callback subscriptions too -----------
  const doneSession = `s_done_${ulid().slice(-8).toLowerCase()}`;
  const doneSub = subscribe({ url: RELAY, token: TOKEN, filter: { session: doneSession }, onEvent: () => {} });
  await sleep(300);
  const doneBefore = await Promise.race([
    doneSub.done.then(() => true),
    sleep(200).then(() => false),
  ]);
  doneSub.close();
  const doneAfter = await Promise.race([
    doneSub.done.then(() => true),
    sleep(1000).then(() => false),
  ]);
  check('done: pending while the live subscription runs, resolved after close()', !doneBefore && doneAfter);

  // --- jsonlSink: append-only JSONL capture (tail -f friendly) -------------------
  // One compact JSON line per event, appended synchronously; a second sink on
  // the same path extends the file; no directory creation — a missing parent
  // propagates to the emit caller (contrast httpTransport's swallow-and-report).
  const { mkdtempSync, readFileSync } = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const sinkDir = mkdtempSync(`${tmpdir()}/aep-sdk-smoke-`);
  const sinkPath = `${sinkDir}/events.jsonl`;
  const readLines = (): string[] => {
    try { return readFileSync(sinkPath, 'utf8').split('\n').filter((l) => l.length > 0); }
    catch { return []; }
  };
  const fileSession = new Emitter({ agent: 'sdk-file', host: 'smoke-host', sink: jsonlSink(sinkPath), epoch: 1 }).session('s_jsonl_1');
  const f1 = fileSession.emit('session.started', { client: { name: 'sdk-file' } });
  const f2 = fileSession.emit('progress.status', {});
  const twoLines = readLines();
  check('jsonlSink: one JSON line per event, parsing back to the emitted envelope',
    twoLines.length === 2
    && JSON.stringify(JSON.parse(twoLines[0])) === JSON.stringify(f1)
    && JSON.stringify(JSON.parse(twoLines[1])) === JSON.stringify(f2));
  const fileSession2 = new Emitter({ agent: 'sdk-file2', host: 'smoke-host', sink: jsonlSink(sinkPath), epoch: 1 }).session('s_jsonl_1');
  const f3 = fileSession2.emit('progress.status', {});
  const f4 = fileSession2.emit('progress.status', {});
  const fourLines = readLines();
  check('jsonlSink: a second sink on the same path appends, never truncates',
    fourLines.length === 4
    && JSON.stringify(JSON.parse(fourLines[2])) === JSON.stringify(f3)
    && JSON.stringify(JSON.parse(fourLines[3])) === JSON.stringify(f4));
  let sinkThrew = false;
  const orphanSession = new Emitter({
    agent: 'sdk-file', host: 'smoke-host',
    sink: jsonlSink(`${sinkDir}/missing/events.jsonl`), epoch: 1,
  }).session('s_jsonl_2');
  try { orphanSession.emit('session.started', { client: { name: 'sdk-file' } }); }
  catch { sinkThrew = true; }
  check('jsonlSink: a missing parent directory propagates the error to the emit caller', sinkThrew);

  // --- tuning knobs (parity table): the timeouts have observable behavior ---------
  // httpTransport timeoutMs: a server that ACCEPTS the TCP connection and then
  // says nothing is indistinguishable from a slow relay without a bound (the
  // runtime's own header timeout is minutes away). The knob turns the hang
  // into a reported 'network' TransportError on the usual swallow-not-throw
  // contract.
  const { createServer: createHangServer } = await import('node:net');
  const hangFixture = createHangServer(() => { /* accept, say nothing, hold the socket */ });
  await new Promise<void>((r) => hangFixture.listen(0, '127.0.0.1', () => r()));
  const hangAddr = hangFixture.address();
  const hangPort = typeof hangAddr === 'object' && hangAddr !== null ? hangAddr.port : 0;
  const hangErrs: unknown[] = [];
  const hangT = httpTransport(`http://127.0.0.1:${hangPort}`, { timeoutMs: 300, onError: (e) => hangErrs.push(e) });
  hangT.sink({
    aep: '0.1', id: ulid(), type: 'session.started', time: new Date().toISOString(),
    source: 'aep://smoke-host/agent/sdk-knobs', agent: 'sdk-knobs',
    session: 's_knobs_http', seq: 0, capture: 'metadata',
  });
  await sleep(2000);
  check('knobs: httpTransport timeoutMs bounds a never-answering POST as TransportError network naming the timeout',
    hangErrs.some((e) => e instanceof TransportError && e.kind === 'network' && /timeout/i.test(e.message)));

  // subscribe connectTimeoutMs: same accept-and-hang server. The per-fetch
  // bound covers request start through response headers, so the hung connect
  // fails as 'network' into the normal retry path.
  const connectErrs: unknown[] = [];
  const connectSub = subscribe({
    url: `http://127.0.0.1:${hangPort}`, filter: {}, connectTimeoutMs: 300,
    onEvent: () => { /* unreachable */ },
    onError: (e) => connectErrs.push(e),
  });
  await sleep(2000);
  connectSub.close();
  hangFixture.close();
  check('knobs: subscribe connectTimeoutMs turns a hung connect into TransportError network within the bound',
    connectErrs.some((e) => e instanceof TransportError && e.kind === 'network' && /timeout/i.test(e.message)));

  // backoffInitialMs/backoffMaxMs: against a closed port the default 500 ms
  // start yields ~2 failures in 1.2 s; a 50/100 ms schedule yields ~12.
  // `>= 3` splits the schedules with generous slack on both sides (no flake).
  const cadenceErrs: unknown[] = [];
  const cadenceSub = subscribe({
    url: 'http://127.0.0.1:9', filter: {}, backoffInitialMs: 50, backoffMaxMs: 100,
    onEvent: () => { /* unreachable */ },
    onError: (e) => cadenceErrs.push(e),
  });
  await sleep(1200);
  cadenceSub.close();
  check('knobs: backoffInitialMs/backoffMaxMs compress the reconnect cadence (>= 3 failures where the default start gives ~2)',
    cadenceErrs.length >= 3 && cadenceErrs.every((e) => e instanceof TransportError && e.kind === 'network'));

  // Acceptance-level ONLY — honestly: no behavioral assertion here. fromBudget
  // and maxReplayChunks shape the resume-set split (exercised for real by the
  // bounded-resume section above at their defaults) and reconnectDelayMs times
  // a reconnect this smoke should not wait out; the contract asserted is that
  // the options compile, are accepted at runtime, and leave the live path
  // working.
  const knobSession = `s_knobs_${ulid().slice(-8).toLowerCase()}`;
  const knobGot: AepEvent[] = [];
  const knobSub = subscribe({
    url: RELAY, token: TOKEN, filter: { session: knobSession },
    fromBudget: 4_000, maxReplayChunks: 5,
    onEvent: (ev) => knobGot.push(ev),
  });
  const knobT = httpTransport(RELAY, { token: TOKEN });
  await sleep(400);
  new Emitter({ agent: 'sdk-knobs', host: 'smoke-host', sink: knobT.sink, epoch: 1 })
    .session(knobSession).emit('session.started', { client: { name: 'sdk-knobs' } });
  await sleep(600);
  knobSub.close();
  check('knobs (acceptance): fromBudget + maxReplayChunks ride a live subscribe that still delivers',
    knobGot.some((e) => e.type === 'session.started' && e.session === knobSession));

  let delayKnobsOk = true;
  try {
    const knobWs = wsTransport(RELAY, { agent: 'sdk-knobs-ws', host: 'smoke-host', token: TOKEN, reconnectDelayMs: 250 });
    knobWs.close();
    const knobCtl = new ControlClient({ url: RELAY, agent: 'sdk-knobs-ctl', host: 'smoke-host', token: TOKEN, reconnectDelayMs: 250 });
    await sleep(200);
    knobCtl.close();
  } catch { delayKnobsOk = false; }
  check('knobs (acceptance): wsTransport + ControlClient reconnectDelayMs accepted — construct, run, close', delayKnobsOk);

  console.log(failures === 0 ? 'SDK SMOKE OK' : `SDK SMOKE FAILED (${failures})`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
