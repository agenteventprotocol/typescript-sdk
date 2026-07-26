// Minimal ambient declarations for the smokes' Node built-ins (the smokes run
// under Node >= 22; the SDK's only Node import is `node:fs`, for `jsonlSink`).

// smoke-testing.ts reads the golden projection corpus fixtures; smoke.ts's
// jsonlSink checks read the sink file back from a fresh temp directory.
declare module 'node:fs' {
  export function readFileSync(path: string, encoding: 'utf8'): string;
  export function mkdtempSync(prefix: string): string;
}

// smoke.ts's jsonlSink checks put that temp directory under the OS tmpdir.
declare module 'node:os' {
  export function tmpdir(): string;
}

// smoke.ts's in-process SSE fixture server.
declare module 'node:net' {
  interface MiniSocket {
    write(data: string): void;
    end(): void;
    once(event: 'data', cb: (chunk: unknown) => void): void;
  }
  interface MiniServer {
    listen(port: number, host: string, cb: () => void): void;
    close(): void;
    address(): { port: number } | string | null;
  }
  export function createServer(onConnection: (socket: MiniSocket) => void): MiniServer;
}
