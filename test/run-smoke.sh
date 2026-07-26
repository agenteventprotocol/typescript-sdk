#!/usr/bin/env bash
# @aep/sdk end-to-end smoke: starts the vendored relay fixture, transpiles the
# SDK + smoke test, runs it, and tears down. Requires Node >= 22 and npx, plus
# `npm ci` beforehand (the relay fixture needs the `ws` dev dependency).
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=$(mktemp -d)
RELAY_PID=""
cleanup() {
  [ -n "$RELAY_PID" ] && kill "$RELAY_PID" 2>/dev/null || true
  rm -rf "$OUT"
}
trap cleanup EXIT

npx --yes -p typescript@6.0.3 tsc --ignoreConfig src/env.d.ts test/env.d.ts src/index.ts test/smoke.ts test/smoke-testing.ts \
  --outDir "$OUT" --rootDir . --target es2022 --lib es2022 --strict \
  --module nodenext --moduleResolution nodenext

# Relay-free testing/projection smoke first (needs no relay): AEP_SDK_ROOT
# points the transpiled smoke back at the repo tree for the golden corpus
# fixtures under test/fixtures/projection/.
AEP_SDK_ROOT="$(pwd)" node "$OUT/test/smoke-testing.js"

PORT="${AEP_SMOKE_PORT:-18787}"
# Control flows only on authenticated bindings (AEP-0004 §4.1): the vendored
# relay is watch-only without a token, so the smoke runs fully authenticated.
TOKEN="${AEP_SMOKE_TOKEN:-sdk-smoke-token}"
AEP_PORT="$PORT" AEP_TOKEN="$TOKEN" node test/fixtures/relay/server.cjs > "$OUT/relay.log" 2>&1 & RELAY_PID=$!
sleep 0.7

AEP_RELAY="http://127.0.0.1:$PORT" AEP_TOKEN="$TOKEN" node "$OUT/test/smoke.js"
