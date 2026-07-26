// Relay-free smoke for the exported testing utilities (src/testing.ts:
// MemorySink / ScriptedSource / ControlStub — target AND client side), the
// state projection (src/projection.ts) and the structural envelope validation
// (src/validate.ts), both re-exported from the root. Everything folds
// in-process: no relay, no network. Run via test/run-smoke.sh (before the
// relay boot).
import { Emitter, SessionEmitter, ControlTarget, StateProjection, projectState, ControlClient, NackError, validateEvent } from '../src/index.js';
import type { AepEvent, ProjectedSession, ProjectionViolation, PendingAttention, RosterEntry, ValidationIssue, ValidationRule } from '../src/index.js';
import { MemorySink, ScriptedSource, ControlStub } from '../src/testing.js';
import { readFileSync } from 'node:fs';

declare const process: { env: Record<string, string | undefined>; exit(code: number): never };

let failures = 0;
const check = (name: string, ok: boolean) => {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}`);
  if (!ok) failures++;
};
const sleep = (ms: number) => new Promise((r) => setTimeout(() => r(undefined), ms));
// Deterministic short poll for the stubbed control handshake (it fires on
// microtasks/timers, never the network): true once `cond` holds, false only
// after the bound — no arbitrary long sleeps.
const waitFor = async (cond: () => boolean, timeoutMs = 2_000): Promise<boolean> => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return true;
    if (Date.now() >= deadline) return false;
    await sleep(10);
  }
};

// Sorted-keys canonical JSON — the same byte-identity yardstick the projection
// uses to tell redelivery from collision (AEP-0001 §7.4); local tiny copy.
const canonical = (v: unknown): string => {
  if (v === undefined) return 'undefined';
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
  const entries = Object.entries(v as Record<string, unknown>)
    .filter(([, val]) => val !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, val]) => `${JSON.stringify(k)}:${canonical(val)}`).join(',')}}`;
};

async function main(): Promise<void> {
  // run-smoke.sh exports AEP_SDK_ROOT (the repo root); '.' suits a direct run
  // from the repo root.
  const ROOT = process.env.AEP_SDK_ROOT ?? '.';
  const FIXTURES = `${ROOT}/test/fixtures/projection`;
  const corpus = readFileSync(`${FIXTURES}/events.jsonl`, 'utf8')
    .split('\n')
    .filter((l) => l.trim() !== '')
    .map((l) => JSON.parse(l) as AepEvent);
  const expected = (JSON.parse(readFileSync(`${FIXTURES}/expected.json`, 'utf8')) as { $comment?: string; state?: unknown }).state;
  check('fixtures: corpus loaded (28 events) and expected.json wraps the state under .state',
    corpus.length === 28 && expected !== undefined);

  // --- MemorySink -----------------------------------------------------------------
  const mem = new MemorySink();
  const memEmitter = new Emitter({ agent: 'mem-agent', host: 'smoke-host', sink: mem.sink });
  const memSession = memEmitter.session('s-mem');
  const m0 = memSession.emit('session.started', { client: { name: 'testing-smoke' } });
  const m1 = memSession.emit('log.emitted', { level: 'info', message: 'hello' });
  const m2 = memEmitter.emit('agent.presence', { status: 'online' });
  check('MemorySink: events passed through .sink are recorded in order',
    mem.events.length === 3
    && mem.events[0]?.id === m0.id && mem.events[1]?.id === m1.id && mem.events[2]?.id === m2.id);
  mem.clear();
  check('MemorySink: clear() empties the record', mem.events.length === 0);

  // --- ScriptedSource -------------------------------------------------------------
  const scripted = new ScriptedSource({ agent: 'scripted-agent' });
  const sx = scripted.session('s-x');
  check('ScriptedSource: session() returns a real SessionEmitter', sx instanceof SessionEmitter);
  const x0 = sx.emit('session.started', { client: { name: 'testing-smoke' } });
  const x1 = sx.emit('tool.requested', { tool: { name: 'grep' } });
  const sy = scripted.session('s-y', { epoch: 2 });
  const y0 = sy.emit('session.started', { client: { name: 'testing-smoke' } });
  const x2 = sx.emit('tool.completed', { tool: { name: 'grep' } });
  check('ScriptedSource: envelopes carry the stub source (aep:// URI default) and agent',
    x0.source.startsWith('aep://') && x0.agent === 'scripted-agent' && y0.source.startsWith('aep://'));
  check('ScriptedSource: seq contiguous from 0 within a session, session string stamped (AEP-0001 §7)',
    x0.session === 's-x' && x1.session === 's-x' && x2.session === 's-x'
    && x0.seq === 0 && x1.seq === 1 && x2.seq === 2);
  check('ScriptedSource: epoch option honored per session (AEP-0001 §7)',
    y0.session === 's-y' && y0.epoch === 2 && y0.seq === 0 && x0.epoch === 0);
  check('ScriptedSource: .events accumulates across sessions in emit order',
    scripted.events.map((e: AepEvent) => e.id).join(',') === [x0, x1, y0, x2].map((e) => e.id).join(','));
  const played: AepEvent[] = [];
  scripted.play((ev: AepEvent) => played.push(ev));
  check('ScriptedSource: play() delivers all recorded events in order',
    played.length === 4
    && played.map((e) => e.id).join(',') === scripted.events.map((e: AepEvent) => e.id).join(','));

  // --- ControlStub + ControlTarget -----------------------------------------------
  const stub = new ControlStub();
  const cmd = stub.command({ type: 'control.pause', session: 's-x', data: {} });
  const cmd2 = stub.command({ type: 'control.pause', session: 's-x', data: {} });
  check('ControlStub: command() yields a well-formed command frame (aep 0.1, fresh id, session, source/agent, time)',
    cmd.aep === '0.1' && typeof cmd.id === 'string' && cmd.id.length > 0 && cmd2.id !== cmd.id
    && cmd.session === 's-x' && typeof cmd.source === 'string' && cmd.source.startsWith('aep://')
    && typeof cmd.agent === 'string' && cmd.agent.length > 0 && typeof cmd.time === 'string');
  check('ControlStub: command frames omit seq — addressing only (AEP-0004 §2.2)',
    cmd.seq === undefined && !('seq' in cmd));

  const target = new ControlTarget(stub.emitter, ['control.pause']);
  let executions = 0;
  const first = target.handle(cmd, () => { executions++; });
  const acks = stub.acks.filter((e: AepEvent) => e.type === 'control.accepted' && e.cause === cmd.id);
  check('ControlStub + ControlTarget: first delivery acked — control.accepted in .acks with cause = command id (AEP-0004 §6)',
    first && executions === 1 && acks.length === 1);

  const again = target.handle(cmd, () => { executions++; });
  const reAcks = stub.acks.filter((e: AepEvent) => e.cause === cmd.id);
  check('ControlStub + ControlTarget: retransmit re-emits the recorded ack byte-identically, no second distinct ack (AEP-0004 §3)',
    !again && executions === 1 && reAcks.length === 2
    && canonical(reAcks[0]) === canonical(reAcks[1])
    && new Set(reAcks.map((e: AepEvent) => e.id)).size === 1);

  const alien = stub.command({ type: 'control.cancel', session: 's-x', data: { scope: 'run' } });
  const handledAlien = target.handle(alien, () => { executions++; });
  const nacks = stub.acks.filter((e: AepEvent) => e.type === 'control.rejected' && e.cause === alien.id);
  check("ControlStub + ControlTarget: a type outside accepts is nacked 'unsupported', never executed (AEP-0004 §4.2)",
    !handledAlien && executions === 1 && nacks.length === 1
    && (nacks[0]?.data as { reason?: string } | undefined)?.reason === 'unsupported');

  // --- projection: batch fold of the golden corpus --------------------------------
  const state = projectState(corpus);
  check('projection: batch fold of the golden corpus equals expected.state (canonical sorted-keys compare)',
    canonical(state) === canonical(expected));

  // --- projection: batch sort repairs arrival order (AEP-0001 §7) ------------------
  // Only the s-alpha events, both sources: no colliders, no duplicated
  // positions; the one redelivered line is byte-identical, so dedupe makes
  // arrival order irrelevant (AEP-0001 §7.4).
  const alpha = corpus.filter((e) => e.session === 's-alpha');
  const reversed = [...alpha].reverse();
  const alphaState = projectState(alpha);
  check('projection: reversed s-alpha subset folds identically to file order (batch sort repair, AEP-0001 §7)',
    alphaState.sessions.length === 2
    && canonical(projectState(reversed)) === canonical(alphaState));

  // --- projection: incremental apply reports ordering regressions ------------------
  const inc = new StateProjection();
  const mkInc = (id: string, epoch: number, seq: number): AepEvent => ({
    aep: '0.1', id, type: 'log.emitted', time: `2026-07-22T06:01:0${seq}.000Z`,
    source: 'aep://corpus/adapter-x', agent: 'agent-x', session: 's-inc',
    epoch, seq, data: { level: 'info' },
  });
  inc.apply(mkInc('ev-inc-00', 1, 0));
  inc.apply(mkInc('ev-inc-01', 0, 1));
  check("projection: incremental apply flags epoch going backwards as 'epoch-regression' (AEP-0001 §7)",
    inc.state().violations.some((v: ProjectionViolation) => v.rule === 'epoch-regression'));

  // --- projection: attention.answered does NOT clear pending (AEP-0002 §7.2) -------
  // A's prefix through attention.answered (ev-a-00..ev-a-05), before resolved:
  // resolution is the emitter's resolved (or timeout) event, never answered.
  const aPrefix = corpus.slice(0, 6);
  const answered = new StateProjection();
  for (const ev of aPrefix) answered.apply(ev);
  const aAlpha = answered.state().sessions
    .find((s: ProjectedSession) => s.source === 'aep://corpus/adapter-a' && s.session === 's-alpha');
  check('projection: attention.answered does NOT clear pending — only resolved/timeout do (AEP-0002 §7.2)',
    aPrefix[5]?.type === 'attention.answered'
    && aAlpha !== undefined && aAlpha.pending.some((p: PendingAttention) => p.id === 'ev-a-04'));

  // --- projection: scope exclusions -----------------------------------------------
  // Command frames name the TARGET's session under the SENDER's source
  // (AEP-0004 §2.2): folding them would manufacture phantom sessions.
  check('projection: command frames fold no phantom session (four real sessions, none under source aep://corpus/mc)',
    state.sessions.length === 4
    && state.sessions.every((s: ProjectedSession) => s.source !== 'aep://corpus/mc'));

  const presence = corpus.find((e) => e.type === 'agent.presence');
  const presenceProj = new StateProjection();
  if (presence) presenceProj.apply(presence);
  check('projection: agent.presence produces no session state (sessionless liveness)',
    presence !== undefined && presenceProj.state().sessions.length === 0);

  // --- validateEvent: structural envelope validation (AEP-0001 §5) -----------------
  // Rules mirror aep-event.schema.json at the pin. The golden corpus is
  // envelope-clean by construction: its violations are stream-SEMANTIC
  // (projection territory — duplicated positions, epoch bumps, orphan
  // terminals), never structural.
  check('validateEvent: all 28 golden-corpus events are structurally clean (empty issue list)',
    corpus.every((ev) => validateEvent(ev).length === 0));

  const freshEmitted = new Emitter({ agent: 'validate-agent', host: 'smoke-host', sink: () => {} })
    .session('s-validate').emit('session.started', { client: { name: 'testing-smoke' } });
  check('validateEvent: a freshly minted Emitter event is clean', validateEvent(freshEmitted).length === 0);

  // A known-good base; each negative below spreads over it so exactly one
  // defect is in play per case. NOTE the type must be family-valid at the
  // schema grammar (the smoke's wire-level `log.emitted` idiom is relay-grade
  // ingestible but structurally OUTSIDE the §5.3 family set — unknown types
  // are tolerated by consumers, still flagged by a validator).
  const good: AepEvent = {
    aep: '0.1', id: 'ev-val-00', type: 'progress.status', time: '2026-07-22T06:10:00.000Z',
    source: 'aep://corpus/adapter-v', agent: 'validate-agent', session: 's-validate',
    seq: 0, data: { level: 'info' },
  };
  check('validateEvent: the hand-built base envelope is clean', validateEvent(good).length === 0);
  const flags = (v: unknown, rule: ValidationRule, attr?: string): boolean =>
    validateEvent(v).some((i: ValidationIssue) => i.rule === rule && (attr === undefined || i.attr === attr));

  const nullIssues = validateEvent(null);
  const arrayIssues = validateEvent([good]);
  check("validateEvent: a non-object (null, array) is a single 'not-an-object' issue — checking stops there",
    nullIssues.length === 1 && nullIssues[0]?.rule === 'not-an-object'
    && arrayIssues.length === 1 && arrayIssues[0]?.rule === 'not-an-object');

  check("validateEvent: aep other than '0.1' is a 'version' issue",
    flags({ ...good, aep: '0.2' }, 'version'));

  const { id: _droppedId, ...withoutId } = good;
  check("validateEvent: a missing required attribute is 'required' naming the attr (§5.1)",
    flags(withoutId, 'required', 'id'));

  check("validateEvent: wrong-typed attributes are 'attr-type' naming the attr (empty source, fractional seq, negative epoch, array data)",
    flags({ ...good, source: '' }, 'attr-type', 'source')
    && flags({ ...good, seq: 1.5 }, 'attr-type', 'seq')
    && flags({ ...good, epoch: -1 }, 'attr-type', 'epoch')
    && flags({ ...good, data: [1] }, 'attr-type', 'data'));

  check("validateEvent: a type outside the grammar is 'type-syntax' (unknown family + empty segment; single segment) (§5.3)",
    flags({ ...good, type: 'bogus..type' }, 'type-syntax', 'type')
    && flags({ ...good, type: 'session' }, 'type-syntax', 'type'));

  check("validateEvent: format violations are 'format' (out-of-range time, bare-name source, malformed traceparent)",
    flags({ ...good, time: '2026-13-01T00:00:00Z' }, 'format', 'time')
    && flags({ ...good, source: 'adapter-a' }, 'format', 'source')
    && flags({ ...good, traceparent: 'not-a-traceparent' }, 'format', 'traceparent'));

  check("validateEvent: an unknown severity is an 'enum' issue",
    flags({ ...good, severity: 'fatal' }, 'enum', 'severity'));

  // All three scoping branches (AEP-0001 §5.2 + the AEP-0004 §2.2 carve-out).
  const { seq: _droppedSeq, ...withoutSeq } = good;
  check("validateEvent: scoping — agent.* must be sessionless (session/run/step/seq all absent)",
    flags({ ...good, type: 'agent.presence', data: { status: 'online' } }, 'scoping'));
  check("validateEvent: scoping — a command frame addresses a session but never carries seq",
    flags({ ...good, type: 'control.cancel', data: { scope: 'run' } }, 'scoping'));
  check("validateEvent: scoping — an ordinary session event requires session AND seq",
    flags({ ...withoutSeq, type: 'session.started', data: { client: { name: 'testing-smoke' } } }, 'scoping'));

  const longName = 'a'.repeat(21);
  check("validateEvent: an ill-formed property name is 'extension-name' (21 chars; uppercase) (§5.4)",
    flags({ ...good, [longName]: 'v' }, 'extension-name', longName)
    && flags({ ...good, Acme: 'v' }, 'extension-name', 'Acme'));
  check("validateEvent: a non-scalar extension value is 'extension-value' (§5.4)",
    flags({ ...good, acme: { nested: true } }, 'extension-value', 'acme'));

  // --- ControlStub: the CLIENT side over an injected socket ------------------------
  // ControlClient owns its wire, so the stub injects a fake socket via
  // `socketFactory` (the PY twin stubs the builder's transport callable —
  // mechanism per idiom, same semantics): the handshake answers hello ->
  // hello and subscribe -> subscribed on microtasks AFTER handlers attach,
  // outbound command envelopes land in `.sent`, and accept() / reject() /
  // rosterReply() script the far side. All relay-free.
  const clientStub = new ControlStub();
  const client = new ControlClient({
    url: 'http://stub-relay', agent: 'console', host: 'h1',
    socketFactory: clientStub.socketFactory,
  });

  const sendP = client.send({ type: 'control.pause', session: 'stub-target', data: {} });
  void sendP.catch(() => { /* settled and asserted below; guards the race path */ });
  const sawCommand = await waitFor(() => clientStub.sent.length >= 1);
  const wireCmd = clientStub.sent[0];
  check('ControlStub client: send() puts the command envelope in .sent — type, session, NO seq attribute (AEP-0004 §2.2)',
    sawCommand && wireCmd !== undefined && wireCmd.type === 'control.pause'
    && wireCmd.session === 'stub-target' && wireCmd.agent === 'console'
    && !('seq' in wireCmd));

  let ack: AepEvent | null = null;
  if (wireCmd !== undefined) {
    clientStub.accept(wireCmd);
    // await AFTER accept(): the stub delivers the ack through the fake
    // socket's onmessage on a microtask, which settles the send promise.
    ack = await Promise.race([sendP, sleep(2_000).then(() => null)]);
  }
  check('ControlStub client: accept() settles the send with the control.accepted ack, cause = command id, recorded in .acks (AEP-0004 §6)',
    wireCmd !== undefined && ack !== null
    && ack.type === 'control.accepted' && ack.cause === wireCmd.id
    && clientStub.acks.some((e: AepEvent) => e.type === 'control.accepted' && e.cause === wireCmd.id));

  const sendP2 = client.send({ type: 'control.pause', session: 'stub-target', data: {} });
  void sendP2.catch(() => { /* rejection asserted below; guards the race path */ });
  const sawSecond = await waitFor(() => clientStub.sent.length >= 2);
  const wireCmd2 = clientStub.sent[1];
  let settled: 'resolved' | 'timeout' | 'rejected' = 'timeout';
  let nack: unknown = null;
  if (wireCmd2 !== undefined) {
    clientStub.reject(wireCmd2, { reason: 'busy', detail: 'scripted' });
    try {
      const r = await Promise.race([sendP2, sleep(2_000).then(() => 'race-timeout' as const)]);
      settled = r === 'race-timeout' ? 'timeout' : 'resolved';
    } catch (e) { settled = 'rejected'; nack = e; }
  }
  check('ControlStub client: reject() settles the send as a wire NackError with the scripted reason/detail — never a synthesized timeout (AEP-0004 §4.2)',
    sawSecond && settled === 'rejected'
    && nack instanceof NackError && nack.reason === 'busy' && nack.detail === 'scripted' && !nack.synthesized
    && wireCmd2 !== undefined
    && clientStub.acks.some((e: AepEvent) => e.type === 'control.rejected' && e.cause === wireCmd2.id));

  const rosterP = client.roster({ timeoutMs: 5_000 });
  void rosterP.catch(() => { /* answered below; guards the race path */ });
  await sleep(50); // let the roster frame ride the microtask handshake before answering
  const entries: RosterEntry[] = [{
    session: 'stub-target', agent: 'tgt',
    control: { accepts: ['control.pause'], ack_window_ms: 5_000 },
  }];
  clientStub.rosterReply(entries);
  const roster = await Promise.race([rosterP, sleep(2_000).then(() => null)]);
  check('ControlStub client: rosterReply() answers roster() with the entries verbatim (AEP-0003 §4.1)',
    roster !== null && roster.length === 1
    && roster[0]?.session === 'stub-target' && roster[0]?.agent === 'tgt'
    && (roster[0]?.control?.accepts ?? []).includes('control.pause')
    && roster[0]?.control?.ack_window_ms === 5_000);

  check('ControlStub client: .sent holds only command envelopes — handshake frames never leak in',
    clientStub.sent.length === 2 && clientStub.sent.every((e: AepEvent) => e.type === 'control.pause'));

  client.close();

  console.log(failures === 0 ? 'TESTING SMOKE OK' : `TESTING SMOKE FAILED (${failures})`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
