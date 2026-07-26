// Transport-error taxonomy: what failed between this process and the relay.
// Protocol-level nacks are NOT transport errors — they stay `NackError`
// (see control.ts): a target refusing a command is the protocol working.

export type TransportErrorKind = 'network' | 'http' | 'parse';

/**
 * A typed transport failure handed to `onError` callbacks:
 *
 * - `network` — transport-level failure: connect/read/TLS/WebSocket framing,
 *   or a handshake that lies;
 * - `http` — the relay ANSWERED with a refusing HTTP status (non-2xx ingest
 *   or SSE); carries `status`;
 * - `parse` — a payload arrived but its JSON is undecodable; the frame is
 *   still dropped, and the drop is reported.
 *
 * The consume loop also throws these internally before its reconnect
 * backoff; they reach the caller only via `onError`.
 */
export class TransportError extends Error {
  readonly status?: number;

  constructor(
    readonly kind: TransportErrorKind,
    message: string,
    opts: { status?: number; cause?: unknown } = {},
  ) {
    super(message, opts.cause !== undefined ? { cause: opts.cause } : undefined);
    this.name = 'TransportError';
    if (opts.status !== undefined) this.status = opts.status;
  }
}

/** Wrap anything non-TransportError as `network` (the catch-all transport kind). */
export function asTransportError(e: unknown): TransportError {
  if (e instanceof TransportError) return e;
  return new TransportError('network', e instanceof Error ? e.message : String(e), { cause: e });
}
