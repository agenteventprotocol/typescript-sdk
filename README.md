# `@agenteventprotocol/sdk`: the official AEP TypeScript SDK

**Typed emit/consume/control helpers for the
[Agent Event Protocol](https://github.com/agenteventprotocol/agent-event-protocol), with
zero runtime dependencies.**

AEP is an open standard for the events AI agents emit while they work —
sessions, runs, tool calls, attention requests — so any consumer can observe
and steer any agent. This SDK wraps the protocol's schema-generated envelope
and payload types with the pieces every TypeScript emitter or consumer needs:
envelope construction with per-session `(epoch, seq)` ownership, SSE
subscription with dedupe and resume, and the control command state machine
with correlated acks. It runs on Node ≥ 22 or any runtime with `fetch`,
`WebSocket`, WebCrypto, and the `node:fs` builtin (Bun, Deno) —
`jsonlSink`'s file append is the SDK's one Node-API touchpoint.

[![CI](https://github.com/agenteventprotocol/typescript-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/agenteventprotocol/typescript-sdk/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

## Install

**Unpublished — build from the repo.** The package is not on npm yet
(`private: true` until the protocol's `v0.1` tag); until then:

```sh
git clone https://github.com/agenteventprotocol/typescript-sdk.git
cd typescript-sdk
npm install          # dev dependency for the test fixture
npm run build        # compiles src/ to dist/ (js + d.ts + maps)
npm pack             # produces the installable tarball
```

The published artifact ships compiled `dist/` (built on `prepack` with
`typescript@6.0.3`, `nodenext` resolution), so a plain strict `tsc` consumer
— no `@types/node`, no `skipLibCheck` — typechecks against it out of the box.

The package is **ESM-only** (`"type": "module"`, a single root export, no
CommonJS build): load it with `import`; `require('@agenteventprotocol/sdk')` is not
supported.

## Quickstart

```ts
import { Emitter, wsTransport, subscribe, ControlClient } from '@agenteventprotocol/sdk';

const t = wsTransport('http://127.0.0.1:8787', { agent: 'my-agent', host: 'host-1' });
const session = new Emitter({ agent: 'my-agent', host: 'host-1', sink: t.sink, epoch: 1 })
  .session('s_001');
session.emit('session.started', { client: { name: 'my-agent' } });   // typed payload

subscribe({ url: 'http://127.0.0.1:8787', filter: { severity: { gte: 'notice' } },
            onEvent: (ev) => console.log(ev.type) });

const ctl = new ControlClient({ url: 'http://127.0.0.1:8787', agent: 'phone', host: 'host-1' });
await ctl.send({ type: 'control.attention.respond', session: 's_001',
                 subject: requestId, data: { answer: { option: 'allow' } } });
```

## Layout

| Path | What |
|---|---|
| `src/gen/aep-types.ts` | **Generated** from the protocol's schema registry — envelope, payload interfaces, `AepPayloadMap`, type registries. Never edit; CI regenerates and diffs it against the spec repo pinned in `SPEC_VERSION` |
| `src/emit.ts` | `Emitter` / `SessionEmitter`: envelope construction, per-session `(epoch, seq)` ownership, capture stamping |
| `src/consume.ts` | `subscribe()`: SSE + attr-match, `(source, id)` dedupe, bounded `(session, epoch, seq)` resume (`positions()`), `from: 'all'` replay-all (AEP-0003 §5). Delivery is a callback (`onEvent`) **or** async iteration; `live: false` is a single history pass; `done` is the completion join; backoff/budget/connect-timeout knobs (see Consuming without a callback and Tuning below) |
| `src/control.ts` | `ControlClient`: `send()` resolves on the correlated ack, rejects `NackError` on nack or synthesized `timeout`, retries reuse the command `id` (AEP-0004); `roster()` returns the live-claim snapshot (AEP-0003 §4.1); `reconnectDelayMs` and `socketFactory` (a `ControlSocket` seam) options. `ControlTarget`: target-side dedupe, one ack per deduplicated command, `unsupported` nack (§7) |
| `src/validate.ts` | `validateEvent()`: dependency-free structural envelope validation mirroring `aep-event.schema.json` at the `SPEC_VERSION` pin — schema patterns verbatim, documented `date-time`/`uri` format approximations, stable rule ids (see Validation below) |
| `src/projection.ts` | `StateProjection` / `projectState()`: fold events into per-session state — lifecycle, runs, pending attention, resume position — plus the protocol violations the fold surfaces (see State projection below) |
| `src/testing.ts` | Relay-free testing utilities behind the `@agenteventprotocol/sdk/testing` subpath: `MemorySink`, `ScriptedSource`, `ControlStub` — both control sides: the target's recording emitter and acks, plus a fake-socket `socketFactory` for `ControlClient` (see Testing utilities below) |
| `src/transports.ts` | `httpTransport` (POST ingest, `timeoutMs`-bounded), `wsTransport` (duplex hello + inbound commands, `reconnectDelayMs`), `jsonlSink` (append-only JSONL file capture, the stdio/file binding of AEP-0003 §3.1 — write errors propagate to the emit caller) |
| `src/errors.ts` | `TransportError`: the typed transport-failure taxonomy (see Errors below) |
| `test/` | End-to-end smoke (`test/run-smoke.sh`) against a vendored snapshot of the reference relay (`test/fixtures/relay/`) |
| `RELEASING.md` | How the package ships: the tag-triggered publish workflow (npm provenance) and the maintainer procedure around it |

## Consuming without a callback

`onEvent` is optional. Without it, the returned `Subscription` is an async
iterable: events queue internally (unbounded until consumed) and `for await`
drains them in delivery order. Breaking out of the loop does **not** close
the subscription — the stream keeps consuming, and the queue keeps filling,
until the explicit `close()`. `sub.done` is the completion join: a promise
that resolves when the subscription ends and never rejects (errors still
flow via `onError`; iteration ends).

```ts
const sub = subscribe({ url: 'http://127.0.0.1:8787', filter: { session: 's_001' } });
for await (const ev of sub) {
  console.log(ev.type, ev.seq);
  if (ev.type === 'session.ended') break;
}
sub.close();      // breaking out never closes — close() stays explicit
await sub.done;   // resolves once the subscription has ended
```

Pass `live: false` for a history-only read: the request carries `live=0`,
the relay serves the history it holds, and the subscription ends by itself
at the relay's `replay-complete` marker — a single pass, no reconnect loop,
no `close()` required (`done` resolves, iteration ends). Combine it with
`from: 'all'` for an enumeration-free snapshot of everything the endpoint
holds:

```ts
const hist = subscribe({ url, filter: { session: 's_001' }, from: 'all', live: false });
for await (const ev of hist) render(ev);   // ends by itself at replay-complete
```

With an `onEvent` callback provided, iteration yields nothing — the events
went to the callback.

### Tuning

Every reconnect/replay constant `subscribe()` uses is an option; the
defaults are given in parentheses. `backoffInitialMs` (500) and
`backoffMaxMs` (10 000) shape the failure backoff. `fromBudget` (6 000
encoded characters) and `maxReplayChunks` (20) bound the resume set on the
wire: the newest positions ride the live request far under server header
limits, older ones drain through bounded replay requests, and a relay
refusing a resume-carrying request (4xx) gets a shrinking retry — resume
is an optimization, never worth a dead stream. `connectTimeoutMs`
(30 000) bounds connection establishment — request start until response
headers arrive — for the live fetch and the replay drains; body reads stay
unbounded (an idle SSE stream is healthy), and expiry fails the attempt as
a `network` `TransportError` into the normal retry path. On the emit side,
`httpTransport`'s `timeoutMs` (10 000) bounds each whole POST (reported
via `onError`, the same swallow contract), and `wsTransport` /
`ControlClient` take `reconnectDelayMs` (1 000) for their reconnect
timers.

## Testing utilities

`@agenteventprotocol/sdk/testing` ships the pieces a test suite needs to exercise emitters,
consumers, and control targets **entirely in-process** — no relay, no
network, no vendored fixture server:

- **`MemorySink`** — records every emitted event in order; pass `.sink`
  straight to an `Emitter`. `clear()` empties the record.
- **`ScriptedSource`** — a scripted emitter: `session()` hands out real
  `SessionEmitter`s (correct `source` URI, per-session `(epoch, seq)`
  ownership) over one shared in-memory record; `play()` replays the
  recording into any consumer-side function.
- **`ControlStub`** — both sides of the control plane, no relay. Target
  side: `command()` mints well-formed command frames (fresh ULID id,
  `session` addressing, **no `seq`** — AEP-0004 §2.2), `.emitter` is a real
  recording `SessionEmitter` to hand a `ControlTarget`, and `.acks` is
  everything the target emitted (acks, nacks, outcomes). Client side: pass
  `.socketFactory` as `ControlClientOptions.socketFactory` — the fake
  socket completes the hello/subscribe handshake by itself (on microtasks,
  after the client's handlers attach), outbound command envelopes land in
  `.sent` (protocol frames never leak in), and `accept()` / `reject()` /
  `rosterReply()` script the far side: acks are minted by the stub's real
  session emitter on the command's target session (so they also land in
  `.acks`) and delivered back through every connected socket. The
  semantics match the Python SDK's client-side stub; the mechanism
  differs per idiom — Python replaces `ControlSender`'s transport
  callable, TypeScript injects the socket because `ControlClient` owns
  its wire. `rosterReply()` is TS-only (only this SDK exposes
  `roster()`).

```ts
import { ScriptedSource, ControlStub } from '@agenteventprotocol/sdk/testing';
import { ControlTarget, projectState } from '@agenteventprotocol/sdk';

const src = new ScriptedSource({ agent: 'my-agent' });
const s = src.session('s_001');
s.emit('session.started', { client: { name: 'my-agent' } });
s.emit('run.started', {}, { run: 'r1' });

const stub = new ControlStub();
const target = new ControlTarget(stub.emitter, ['control.pause']);
target.handle(stub.command({ type: 'control.pause', session: 's_001' }), () => {/* pause */});
// stub.acks[0].type === 'control.accepted'

const state = projectState(src.events);   // assert on sessions/runs/pending
```

Client-side control tests run relay-free the same way:

```ts
import { ControlClient } from '@agenteventprotocol/sdk';
import { ControlStub } from '@agenteventprotocol/sdk/testing';

const stub = new ControlStub();
const ctl = new ControlClient({ url: 'http://stub', agent: 'console', host: 'h1',
                                socketFactory: stub.socketFactory });
const pending = ctl.send({ type: 'control.pause', session: 'stub-target', data: {} });
// once the frame lands in stub.sent (a microtask later):
//   stub.accept(stub.sent[0])  -> `pending` resolves with the control.accepted ack
//   stub.reject(stub.sent[0], { reason: 'busy' })  -> rejects with a wire NackError
```

The subpath is part of the public API; it is not re-exported from the
package root, so production bundles never pull it in by accident.

## State projection

`StateProjection` (incremental `apply()`) and `projectState()` (batch — the
same fold with arrival order repaired first) turn a stream of events into
the state a dashboard or a test assertion needs: **one entry per session**
— keyed `(source, session)`, emitter-scoped identity per AEP-0001 §5.2 —
with lifecycle times, its **runs**, its **pending attention** requests, and
the resume position, plus the protocol **violations** the fold surfaces
(`(source, id)` collisions per AEP-0001 §7.4, `(epoch, seq)` regressions
per AEP-0001 §7, run/session terminal exclusivity per AEP-0002 §2
convention 5, sessions ended with pending attention per AEP-0002 §7.2).

```text
{ sessions: [ { source, session, agent, started, ended,
                position: { epoch, seq },
                runs:    [ { run, status, started, ended } ],
                pending: [ { id, kind, since } ] } ],
  violations: [ { rule, source, ... } ] }
```

Unseen values are `null`, never `undefined` — the shape serializes
canonically. Byte-identical redelivery collapses silently (legal
at-least-once delivery); a reused id naming a *different* event is an
`id-collision`. Command frames and `agent.*` liveness events are
deduplicated but never fold session state — a command names the target's
session under the sender's source, and folding it would manufacture phantom
sessions. The projection's semantics mirror the reference CLI's
`validate`/`timeline` folds, pinned by a golden corpus
(`test/fixtures/projection/`) vendored byte-identically in both SDKs.

## Validation

`validateEvent(value)` (root export) checks one value against the
structural envelope contract (AEP-0001 §5) and returns
`ValidationIssue[]` — an empty array means a structurally valid envelope.
Each issue carries a stable rule id (the `ValidationRule` union:
`not-an-object`, `version`, `required`, `attr-type`, `type-syntax`,
`format`, `enum`, `scoping`, `extension-name`, `extension-value`), the
offending attribute or extension-property name where one applies, and a
message with the spec cite. The rules mirror `aep-event.schema.json` at
the `SPEC_VERSION` pin — schema patterns verbatim, including the scoping
conditional (`agent.*` events are sessionless; command frames and
`attention.routed` carry `session` but never `seq`; everything else
requires `session` and `seq`).

```ts
import { validateEvent } from '@agenteventprotocol/sdk';

const issues = validateEvent(JSON.parse(line));
if (issues.length) console.error(issues.map((i) => `${i.rule}(${i.attr ?? ''}): ${i.message}`));
```

One honesty note: the schema's two JSON-Schema *formats* have no
dependency-free equivalent, so `time` and `source` are documented
approximations of ajv-formats (what the reference validator uses). The
RFC 3339 `date-time` check enforces field ranges (month `13` fails) but
not per-month day counts or leap years; the `uri` check requires a scheme
and refuses whitespace/control characters (a bare name like `adapter-a`
fails) without full RFC 3986 parsing. Validation is structural:
stream-level semantics — ordering regressions, duplicated positions,
terminal exclusivity — are the projection's *violations*, not validation
issues.

## Errors

Two error types cover everything the SDK hands you:

- **`NackError`** — a protocol-level nack: the target (or the relay on its
  behalf) *answered* and refused (`reason`, `detail`, plus
  `synthesized: true` for the locally minted ack-window `timeout`). A nack
  is the protocol working, not a transport failure.
- **`TransportError`** — something failed between this process and the
  relay, discriminated by `kind`:
  - `network` — transport-level failure: connect/read/TLS/WebSocket
    framing, or a handshake that lies;
  - `http` — the relay answered with a refusing HTTP status (non-2xx
    ingest or SSE); carries `status`;
  - `parse` — a payload arrived but its JSON is undecodable; the frame is
    dropped, and the drop is reported.

Transport errors reach you through the optional `onError` callbacks
(`subscribe`, `httpTransport`, `wsTransport`, `ControlClient`); the consume
loop keeps reconnecting with backoff regardless. Events of unknown *type*
are not errors — consumers tolerate them by design. `jsonlSink` is the
deliberate exception to the callback route: a failed file append throws to
the `emit()` caller — a capture file that cannot be written is broken at
the producing boundary, not a transport condition to ride out.

One idiom difference from the Python SDK is deliberate: `ControlClient`'s
constructor opens the connection immediately (commands queue while it
connects), while the Python asyncio counterpart requires an explicit
`await connect()` — an async open cannot live in a constructor.

## Verify

```sh
npm run typecheck        # strict, no emit
npm install && bash test/run-smoke.sh   # emit, consume, control round-trip incl. target-side dedupe/nack, resume
```

`SPEC_VERSION` pins the exact
[agent-event-protocol](https://github.com/agenteventprotocol/agent-event-protocol) commit
the committed `src/gen/` types were generated from; CI regenerates from that
pin and fails on any diff.

## Versioning

This package implements AEP 0.1 — every `AepEvent` it constructs or accepts
carries `aep: "0.1"`. Pre-1.0, a **minor version bump may be breaking** (the
compatibility boundary set by the protocol's
[GOVERNANCE](https://github.com/agenteventprotocol/agent-event-protocol/blob/main/GOVERNANCE.md)).
The SDK versions independently of the protocol; see
[`RELEASING.md`](RELEASING.md) for the full versioning policy.

## Links

- [Specification](https://github.com/agenteventprotocol/agent-event-protocol) — spec,
  schema registry, conformance fixtures, docs
- [Python SDK](https://github.com/agenteventprotocol/python-sdk)
- [Reference stack](https://github.com/agenteventprotocol/reference) — relay, CLI,
  adapters, bridges, demo

## License

Apache-2.0 — see [LICENSE](LICENSE).
