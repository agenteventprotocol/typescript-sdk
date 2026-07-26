// Emitter transports: where built events go. HTTP POST (batch-friendly), a
// WebSocket duplex that can also receive control commands (AEP-0003 §3/§5),
// and an append-only JSONL file sink (the stdio/file binding, AEP-0003 §3.1).
import { appendFileSync } from 'node:fs';
import type { AepEvent } from './gen/aep-types.js';
import type { Sink } from './emit.js';
import { TransportError, asTransportError } from './errors.js';

export interface Transport {
  sink: Sink;
  close(): void;
}

/**
 * POST each event to the relay's HTTP ingest (`AEP-Version` header
 * included). `timeoutMs` (default 10 000) bounds each whole POST with an
 * AbortController; expiry is reported via `onError` as a `network`
 * `TransportError` — the same swallow-not-throw contract as every other
 * ingest failure.
 */
export function httpTransport(url: string, opts: { token?: string; timeoutMs?: number; onError?: (e: unknown) => void } = {}): Transport {
  const endpoint = `${url.replace(/\/$/, '')}/events`;
  const timeoutMs = opts.timeoutMs ?? 10_000;
  let queue: Promise<void> = Promise.resolve();
  const sink: Sink = (ev) => {
    queue = queue.then(async () => {
      const ctrl = new AbortController();
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; ctrl.abort(); }, timeoutMs);
      try {
        const headers: Record<string, string> = {
          'content-type': 'application/json',
          'AEP-Version': '0.1',
        };
        if (opts.token) headers.authorization = `Bearer ${opts.token}`;
        const res = await fetch(endpoint, { method: 'POST', headers, body: JSON.stringify(ev), signal: ctrl.signal });
        if (!res.ok) opts.onError?.(new TransportError('http', `ingest HTTP ${res.status}`, { status: res.status }));
      } catch (e) {
        opts.onError?.(timedOut
          ? new TransportError('network', `ingest timeout after ${timeoutMs} ms`, { cause: e })
          : asTransportError(e));
      } finally {
        clearTimeout(timer);
      }
    });
  };
  return { sink, close() { /* drains naturally */ } };
}

/**
 * Append events to a JSONL file (the stdio/file binding, AEP-0003 §3.1): one
 * compact JSON line per event, appended synchronously per event so `tail -f`
 * — or a second sink on the same path — always sees whole lines. No
 * directory creation, and errors PROPAGATE to the `emit()` caller: unlike
 * `httpTransport`, which reports failures via `onError` and keeps going, a
 * capture file that cannot be written is broken at the producing boundary
 * and deserves a loud failure.
 */
export function jsonlSink(path: string): Sink {
  return (ev) => { appendFileSync(path, JSON.stringify(ev) + '\n'); };
}

export interface WsTransportOptions {
  agent: string;
  host: string;
  token?: string;
  /** Declared at hello; commands not listed are relay-nackable (AEP-0004 §4.2). */
  controlAccepts?: string[];
  /** Inbound control commands (foreign-emitter events addressed to a session). */
  onCommand?: (cmd: AepEvent) => void;
  onError?: (e: unknown) => void;
  /** Delay before reconnecting after the socket closes (default 1 000 ms). */
  reconnectDelayMs?: number;
}

/** Duplex emitter connection: hello handshake, outbox while (re)connecting. */
export function wsTransport(url: string, opts: WsTransportOptions): Transport {
  const base = url.replace(/\/$/, '').replace(/^http/, 'ws');
  const reconnectDelayMs = opts.reconnectDelayMs ?? 1000;
  const qs = opts.token ? `?access_token=${encodeURIComponent(opts.token)}` : '';
  let ws: WebSocket | null = null;
  let ready = false;
  let closed = false;
  const outbox: string[] = [];

  function connect(): void {
    if (closed) return;
    ws = new WebSocket(`${base}/socket${qs}`);
    ws.onopen = () => {
      ws?.send(JSON.stringify({
        type: 'hello',
        versions: ['0.1'],
        role: 'emitter',
        identity: { agent: opts.agent, instance: opts.host },
        capabilities: {
          capture: ['none', 'metadata', 'redacted', 'full'],
          control: opts.controlAccepts ? { accepts: opts.controlAccepts, ack_window_ms: 10_000 } : undefined,
          filters: [],
          experimental: {},
        },
      }));
    };
    ws.onmessage = (m) => {
      let msg: Record<string, unknown>;
      try { msg = JSON.parse(String(m.data)) as Record<string, unknown>; } catch (e) {
        opts.onError?.(new TransportError('parse', 'undecodable duplex frame', { cause: e }));
        return;
      }
      if (msg.type === 'hello') {
        ready = true;
        while (outbox.length) ws?.send(outbox.shift() as string);
        return;
      }
      if (msg.type === 'ping') { ws?.send(JSON.stringify({ type: 'pong', t: msg.t })); return; }
      if (typeof msg.type === 'string' && msg.type.startsWith('control.') && !msg.type.startsWith('control.accepted') && !msg.type.startsWith('control.rejected')) {
        opts.onCommand?.(msg as unknown as AepEvent);
      }
    };
    ws.onclose = () => {
      ready = false;
      if (!closed) setTimeout(connect, reconnectDelayMs);
    };
    ws.onerror = (e) => opts.onError?.(new TransportError('network', 'websocket error', { cause: e }));
  }
  connect();

  return {
    sink(ev: AepEvent) {
      const json = JSON.stringify(ev);
      if (ready && ws && ws.readyState === WebSocket.OPEN) ws.send(json);
      else outbox.push(json);
    },
    close() {
      closed = true;
      ws?.close();
    },
  };
}
