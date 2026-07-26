// Structural envelope validation (AEP-0001 §5): a dependency-free mirror of
// `aep-event.schema.json` at the `SPEC_VERSION` pin. Every regex the schema
// states is carried verbatim; the two JSON-Schema *format* checks
// (`date-time`, `uri`) have no dependency-free equivalent, so they are
// documented approximations of ajv-formats (the reference validator's
// implementation) — see the rule comments below. An empty issue list means
// the value is a structurally valid envelope; stream-level semantics
// (ordering, dedupe, terminal exclusivity) are `projection.ts` territory.
import { RESERVED_ATTRS } from './emit.js';
import { COMMAND_TYPE } from './projection.js';

/** The stable rule ids a {@link ValidationIssue} is tagged with. */
export type ValidationRule =
  | 'not-an-object'
  | 'version'
  | 'required'
  | 'attr-type'
  | 'type-syntax'
  | 'format'
  | 'enum'
  | 'scoping'
  | 'extension-name'
  | 'extension-value';

/**
 * One structural defect: the {@link ValidationRule} that fired, the envelope
 * attribute (or extension property) it names where one applies, and a
 * human-readable message carrying the spec cite.
 */
export interface ValidationIssue {
  rule: ValidationRule;
  attr?: string;
  message: string;
}

// AEP-0001 §6 type grammar (family set per AEP-0002) — the schema `type`
// pattern, verbatim: a registered family with >= 1 dot-segment, or a vendor
// `x.<vendor>.<family>.<segment>+` type.
const TYPE_PATTERN = /^((agent|session|run|progress|tool|delegation|attention|resource|error|state|assessment|message|control)(\.[a-z0-9_]+)+|x\.[a-z0-9_-]+\.[a-z0-9_]+(\.[a-z0-9_]+)+)$/;

// AEP-0001 §5.1 `time` — RFC 3339 date-time. Documented approximation of
// ajv-formats: field ranges are enforced (month 01–12, day 01–31, hour
// 00–23, minute 00–59, second 00–60 for the leap second, offset `Z`/`z` or
// ±HH:MM), an optional fraction is allowed; per-month day counts and leap
// years are NOT checked (2026-02-31 passes here, as it does in most
// practical validators).
const DATE_TIME = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])[Tt]([01]\d|2[0-3]):[0-5]\d:([0-5]\d|60)(\.\d+)?([Zz]|[+-]([01]\d|2[0-3]):[0-5]\d)$/;

// AEP-0001 §5.1 `source` — URI format. Documented approximation of
// ajv-formats: a scheme anchor (RFC 3986 `scheme ":"`, lowercase as every
// AEP source URI is) plus a no-whitespace/no-control-characters sweep. This
// deliberately catches the bare-name class (`adapter-a` is not a URI) while
// accepting any schemed identifier (`aep://…`, `https://…`, `urn:…`).
const URI_SCHEME = /^[a-z][a-z0-9+.-]*:/;

// AEP-0001 §5.1 `traceparent` — the schema pattern, verbatim (W3C Trace
// Context header value, name kept verbatim for OTel interop).
const TRACEPARENT = /^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/;

// AEP-0001 §5.4 — the schema `propertyNames` pattern, verbatim: EVERY
// property name on the envelope (core and extension alike) is lowercase
// alphanumeric, 1–20 characters.
const PROPERTY_NAME = /^[a-z0-9]{1,20}$/;

// AEP-0001 §8.1 / §8.2 — the schema enums, verbatim.
const SEVERITIES: ReadonlySet<unknown> = new Set(['debug', 'info', 'notice', 'warning', 'error', 'critical']);
const CAPTURES: ReadonlySet<unknown> = new Set(['none', 'metadata', 'redacted', 'full']);

/** True when `s` contains whitespace or a control character (URI rule 6). */
function hasWhitespaceOrControl(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c <= 0x20 || c === 0x7f) return true; // C0 controls, space, DEL
  }
  return /\s/.test(s); // non-ASCII whitespace (NBSP and friends)
}

/**
 * Validate one value as an AEP 0.1 Event envelope (AEP-0001 §5): returns
 * every structural defect found, or an empty array for a valid envelope.
 * Rules mirror `aep-event.schema.json` at the `SPEC_VERSION` pin; the
 * `date-time`/`uri` format checks are documented approximations of
 * ajv-formats (see the pattern comments in this module). A non-object input
 * short-circuits to the single `not-an-object` issue.
 */
export function validateEvent(value: unknown): ValidationIssue[] {
  // Rule 1 — an Event is a JSON object (schema `type: "object"`; AEP-0001
  // §5): arrays and null are not envelopes, and nothing else is checkable.
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return [{ rule: 'not-an-object', message: 'an Event is a JSON object (AEP-0001 §5)' }];
  }
  const ev = value as Record<string, unknown>;
  const issues: ValidationIssue[] = [];

  // Rule 2 — `aep` is the literal "0.1" (schema const; AEP-0001 §5.1 and
  // §11: consumers dispatch on the major.minor carried by every Event).
  if (ev.aep !== '0.1') {
    issues.push({ rule: 'version', message: 'aep must be the literal "0.1" (AEP-0001 §5.1)' });
  }

  // Rule 3 — required non-empty strings (schema `required` + `minLength: 1`;
  // AEP-0001 §5.1): absent is `required`, present-but-wrong is `attr-type`.
  for (const attr of ['id', 'type', 'time', 'source', 'agent'] as const) {
    const v = ev[attr];
    if (v === undefined) {
      issues.push({ rule: 'required', attr, message: `${attr} is required (AEP-0001 §5.1)` });
    } else if (typeof v !== 'string' || v === '') {
      issues.push({ rule: 'attr-type', attr, message: `${attr} must be a non-empty string (AEP-0001 §5.1)` });
    }
  }
  const type = typeof ev.type === 'string' && ev.type !== '' ? ev.type : undefined;

  // Rule 4 — `type` grammar (schema pattern; AEP-0001 §6, family set per
  // AEP-0002): a registered family or an `x.<vendor>.…` vendor type, every
  // segment lowercase `[a-z0-9_]` (vendor names also allow `-`).
  if (type !== undefined && !TYPE_PATTERN.test(type)) {
    issues.push({
      rule: 'type-syntax', attr: 'type',
      message: `type "${type}" is outside the AEP-0001 §6 grammar (registered family or x.<vendor> type, dot-separated lowercase segments)`,
    });
  }

  // Rule 5 — `time` is an RFC 3339 date-time (schema format; AEP-0001
  // §5.1). Approximation of ajv-formats with field ranges — see DATE_TIME.
  if (typeof ev.time === 'string' && ev.time !== '' && !DATE_TIME.test(ev.time)) {
    issues.push({ rule: 'format', attr: 'time', message: `time "${ev.time}" is not an RFC 3339 date-time (AEP-0001 §5.1)` });
  }

  // Rule 6 — `source` is a URI (schema format; AEP-0001 §5.1). Approximation
  // of ajv-formats: scheme-anchored, no whitespace or control characters —
  // bare names (`adapter-a`) fail. See URI_SCHEME.
  if (typeof ev.source === 'string' && ev.source !== ''
      && (!URI_SCHEME.test(ev.source) || hasWhitespaceOrControl(ev.source))) {
    issues.push({ rule: 'format', attr: 'source', message: `source "${ev.source}" is not a URI (AEP-0001 §5.1: a schemed identifier, e.g. aep://host/agent/name)` });
  }

  // Rule 7 — optional strings are non-empty when present (schema
  // `minLength: 1`; AEP-0001 §5.1).
  for (const attr of ['subject', 'session', 'run', 'step', 'cause'] as const) {
    const v = ev[attr];
    if (v !== undefined && (typeof v !== 'string' || v === '')) {
      issues.push({ rule: 'attr-type', attr, message: `${attr} must be a non-empty string when present (AEP-0001 §5.1)` });
    }
  }

  // Rule 8 — `seq`/`epoch` are integers >= 0 (schema `integer`,
  // `minimum: 0`; AEP-0001 §5.1/§7: positions in the ordering contract).
  for (const attr of ['seq', 'epoch'] as const) {
    const v = ev[attr];
    if (v !== undefined && (typeof v !== 'number' || !Number.isInteger(v) || v < 0)) {
      issues.push({ rule: 'attr-type', attr, message: `${attr} must be an integer >= 0 (AEP-0001 §5.1/§7)` });
    }
  }

  // Rule 9 — `traceparent` matches the W3C Trace Context form (schema
  // pattern, verbatim; AEP-0001 §5.1).
  if (ev.traceparent !== undefined
      && (typeof ev.traceparent !== 'string' || !TRACEPARENT.test(ev.traceparent))) {
    issues.push({ rule: 'format', attr: 'traceparent', message: 'traceparent must match the W3C Trace Context header form (AEP-0001 §5.1)' });
  }

  // Rule 10 — `severity`/`capture` enums (schema enums, verbatim; AEP-0001
  // §8.1/§8.2). A wrong-typed value is not a member either: same rule.
  if (ev.severity !== undefined && !SEVERITIES.has(ev.severity)) {
    issues.push({ rule: 'enum', attr: 'severity', message: 'severity must be one of debug|info|notice|warning|error|critical (AEP-0001 §8.1)' });
  }
  if (ev.capture !== undefined && !CAPTURES.has(ev.capture)) {
    issues.push({ rule: 'enum', attr: 'capture', message: 'capture must be one of none|metadata|redacted|full (AEP-0001 §8.2)' });
  }

  // Rule 11 — `data` is a plain object (schema `type: "object"`; AEP-0001
  // §5): the payload is a map, never an array, never null.
  if (ev.data !== undefined
      && (typeof ev.data !== 'object' || ev.data === null || Array.isArray(ev.data))) {
    issues.push({ rule: 'attr-type', attr: 'data', message: 'data must be a plain JSON object (AEP-0001 §5)' });
  }

  // Rules 12/13 — property names and extension values (schema
  // `propertyNames` + `additionalProperties`; AEP-0001 §5.4): EVERY property
  // name is lowercase alphanumeric <= 20 chars, and every non-reserved
  // (extension) property carries a scalar string/integer/boolean. The
  // reserved set is emit.ts's RESERVED_ATTRS — one definition.
  for (const [name, v] of Object.entries(ev)) {
    if (!PROPERTY_NAME.test(name)) {
      issues.push({ rule: 'extension-name', attr: name, message: `property name "${name}" must match [a-z0-9]{1,20} (AEP-0001 §5.4)` });
    }
    if (!RESERVED_ATTRS.has(name)) {
      const t = typeof v;
      const scalar = t === 'string' || t === 'boolean' || (t === 'number' && Number.isInteger(v));
      if (!scalar) {
        issues.push({ rule: 'extension-value', attr: name, message: `extension attribute "${name}" must be a string, integer, or boolean (AEP-0001 §5.4)` });
      }
    }
  }

  // Rule 14 — scoping conditional (schema allOf; AEP-0001 §5.2 with the
  // AEP-0004 §2.2 foreign-emitter carve-out): `agent.*` Events are
  // sessionless; command frames and `attention.routed` address a session
  // WITHOUT entering its ordering (session required, seq forbidden); every
  // other type is session-scoped (session and seq required). Mirrors the
  // schema's conditionals, which apply whatever the type's grammar status.
  if (type !== undefined) {
    const has = (a: string): boolean => ev[a] !== undefined;
    if (type.startsWith('agent.')) {
      const offending = ['session', 'run', 'step', 'seq'].filter(has);
      if (offending.length > 0) {
        issues.push({ rule: 'scoping', message: `agent-scoped events omit session/run/step/seq (AEP-0001 §5.2); found ${offending.join(', ')}` });
      }
    } else if (COMMAND_TYPE.test(type) || type === 'attention.routed') {
      if (!has('session') || has('seq')) {
        issues.push({ rule: 'scoping', message: 'command frames and attention.routed carry session (addressing) and never seq (AEP-0001 §5.2; AEP-0004 §2.2)' });
      }
    } else if (!has('session') || !has('seq')) {
      issues.push({ rule: 'scoping', message: 'session-scoped events require session and seq (AEP-0001 §5.2)' });
    }
  }

  return issues;
}
