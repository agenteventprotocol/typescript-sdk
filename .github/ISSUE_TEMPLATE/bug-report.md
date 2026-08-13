---
name: Bug report
about: Report a bug in @agenteventprotocol/sdk
title: "[BUG] "
labels: bug
assignees: ''
---

## Component

Which component contains the bug? Check one:

- [ ] emit (`Emitter` / `SessionEmitter`)
- [ ] consume (`subscribe`, resume, dedupe)
- [ ] control (`ControlClient` / `ControlTarget`)
- [ ] transports (`httpTransport` / `wsTransport`)
- [ ] generated types (`src/gen/aep-types.ts`) — regenerated from the
      protocol repository at the `SPEC_VERSION` pin; if the defect is in
      the content (not the vendoring), report it on
      [agent-event-protocol](https://github.com/agenteventprotocol/agent-event-protocol/issues/new/choose)
- [ ] docs / README
- [ ] other

## Reproduction

Steps to reproduce:

1. ...
2. ...

Minimal reproducing example (event, command, or code):

```ts
...
```

## Expected behavior

What should happen? If a spec section defines it, cite it (e.g. AEP-0003 §5).

## Actual behavior

What actually happens? Include relevant output or logs (redact tokens and paths).

## Version and commit

- `@agenteventprotocol/sdk` version or commit:
- `SPEC_VERSION` (repo root file):
- Node version:
- TypeScript version:

## Additional context

Anything else that helps.
