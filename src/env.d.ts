// Minimal ambient declarations for the Node >= 22 / browser-shared runtime
// surface the SDK uses, so `tsc --strict` needs neither lib.dom nor @types/node.

declare function setTimeout(cb: (...args: unknown[]) => void, ms?: number): unknown;
declare function clearTimeout(handle: unknown): void;

declare const console: { log(...a: unknown[]): void; error(...a: unknown[]): void };

declare const crypto: { getRandomValues<T extends Uint8Array>(array: T): T };

// Timeout plumbing for fetch-based transports: the minimal AbortController
// surface the SDK uses — construct, hand `signal` to fetch, `abort()` from a
// timer. The signal stays opaque (`unknown`): the SDK never introspects it.
declare class AbortController {
  constructor();
  readonly signal: unknown;
  abort(): void;
}

interface FetchResponse {
  ok: boolean;
  status: number;
  body: { getReader(): { read(): Promise<{ done: boolean; value?: Uint8Array }> } } | null;
  text(): Promise<string>;
}
declare function fetch(url: string, init?: {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
  signal?: unknown;
}): Promise<FetchResponse>;

declare class TextDecoder {
  constructor(label?: string);
  decode(input?: Uint8Array, options?: { stream?: boolean }): string;
}

// jsonlSink's file append — the SDK's one Node-specific import, declared by
// hand (zero deps, no @types/node, by design) with exactly the
// surface src/ uses.
declare module 'node:fs' {
  export function appendFileSync(path: string, data: string): void;
}

interface WsMessageEvent { data: unknown }
declare class WebSocket {
  constructor(url: string);
  send(data: string): void;
  close(): void;
  onopen: (() => void) | null;
  onmessage: ((ev: WsMessageEvent) => void) | null;
  onclose: (() => void) | null;
  onerror: ((err: unknown) => void) | null;
  readyState: number;
  static readonly OPEN: number;
}
