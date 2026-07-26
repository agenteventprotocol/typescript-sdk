# Releasing

How `@aep/sdk` ships to npm. The versioning **policy** is the
organization's canonical
[VERSIONING.md](https://github.com/agenteventprotocol/agent-event-protocol/blob/main/VERSIONING.md)
in the protocol repository: the package versions independently under
SemVer, decoupled from the protocol version; the README declares which
protocol version(s) the package implements. The next section applies
that policy to this package.

## Versioning policy

This package follows [SemVer 2.0.0](https://semver.org/). The org-wide rules
for what that means — including how generated code is versioned — are the
canonical policy in
[`VERSIONING.md`](https://github.com/agenteventprotocol/agent-event-protocol/blob/main/VERSIONING.md)
in the protocol repository. This section states how those rules apply here.

**Public API.** Everything importable from the package's two entry points
— the root `@aep/sdk` (`src/index.ts`, published as `dist/index.js` /
`dist/index.d.ts`) and the `@aep/sdk/testing` subpath (`src/testing.ts`,
published as `dist/testing.js` / `dist/testing.d.ts`: `MemorySink`,
`ScriptedSource`, `ControlStub` — target side and client side:
`socketFactory`, `sent`, `accept`, `reject`, `rosterReply` — and their
option types) — is the public API. That includes the generated protocol
types re-exported from `src/gen/aep-types.ts` (`AepEvent`, `Severity`,
`Capture`, the payload interfaces, `AepPayloadMap`, and the other type
registries), not just the hand-written `Emitter`, `SessionEmitter`,
`subscribe` (including the returned `Subscription`'s async iteration and
`done` promise), `ControlClient`, `ControlTarget`, `validateEvent`
(with `ValidationIssue` / `ValidationRule`), `StateProjection`,
`projectState`, `httpTransport`, `wsTransport`, `jsonlSink`, the
`ControlSocket` interface, and their associated option/result types. Deep imports
(anything under `src/` or `dist/` not reachable from an entry point), the
`test/` suite, the vendored relay fixture under `test/fixtures/relay/`,
the golden projection corpus under `test/fixtures/projection/`, and any
scripts are **not** public API and can change without a version bump.

MAJOR bumps are incompatible public-API changes, MINOR bumps are
backward-compatible additions, and PATCH bumps are backward-compatible
fixes — with one carve-out, next.

**Pre-1.0 honesty.** Before `1.0.0`, a minor version bump may still break
the public API. Every breaking change — pre- or post-1.0 — ships with a
`CHANGELOG.md` entry stating what breaks and how to migrate.

**Generated code.** `src/gen/aep-types.ts` is regenerated from the schemas
in the protocol repository at the commit pinned in `SPEC_VERSION`, but it's
versioned exactly like hand-written code: a regeneration that changes the
public surface (a new field, a narrowed type, a renamed export) follows the
same SemVer rules as any other change, and the `CHANGELOG.md` entry names
the schema change that caused it.

## The release is the tag

[`.github/workflows/release.yml`](.github/workflows/release.yml) publishes
to npm — with a [provenance attestation](https://docs.npmjs.com/generating-provenance-statements)
— on any `v*` tag push. No tags exist yet, so the workflow is dormant.
Before `npm publish` it re-verifies the shape, and three guards make a stray
tag safe:

1. strict typecheck, the `dist/` build, and the end-to-end smoke must pass;
2. the tag must equal `package.json`'s `version` exactly (`v0.1.0` ⇔ `0.1.0`);
3. `"private": true` must be gone from `package.json` — the pre-release
   shape can never reach the registry.

## Cutting a release (maintainer-run; tagging is never automated)

1. **Sibling agreement — a local pre-tag step.** The two SDK packages
   version independently, but when they release together they ride the same
   release number. With a checkout of
   [`python-sdk`](https://github.com/agenteventprotocol/python-sdk) next to
   this repository, compare:

   ```sh
   node -p 'require("./package.json").version'
   grep -oE '__version__ = "[^"]+"' ../python-sdk/aep_sdk/__init__.py
   ```

   The release cores (`major.minor.patch`) must agree; only the dev-suffix
   convention differs by ecosystem (npm `0.1.0-dev`, PyPI `0.1.0.dev0`).
   This stays a documented local step: the repositories cannot see each
   other from CI.

2. **Make the shape publishable — one commit.** Drop `"private": true` from
   `package.json`, set `version` to the release (e.g. `0.1.0`), roll
   `CHANGELOG.md` (`[Unreleased]` → `[0.1.0] — <date>`), and confirm the
   README's "implements AEP x.y" line still holds.

3. **Verify locally** on that commit — the same steps the workflow runs:

   ```sh
   npm install
   npx --yes -p typescript@6.0.3 tsc -p . --noEmit
   npm run build
   bash test/run-smoke.sh
   ```

4. **Tag and push the tag.**

   ```sh
   git tag -a v0.1.0 -m "@aep/sdk 0.1.0"
   git push origin v0.1.0
   ```

   The workflow does the rest. If publishing fails after the tag exists,
   never move or delete the tag — fix forward and cut the next patch.

## One-time registry setup (before the first tag)

- Confirm the npm `@aep` scope and this package's publish rights.
- Create the `NPM_TOKEN` repository secret (a granular, publish-only
  automation token); the workflow authenticates with it and requests the
  provenance attestation via the job's OIDC `id-token` permission.
