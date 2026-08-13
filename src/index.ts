// @agenteventprotocol/sdk — AEP 0.1 helpers over the generated envelope + payload types.
// Types are generated from the protocol's schema registry (AEP-0002);
// everything handwritten lives in the emit/consume/control/transport modules.
export * from './gen/aep-types.js';
export { ulid } from './ulid.js';
export { Emitter, SessionEmitter } from './emit.js';
export type { EmitterOptions, EmitOptions, Sink } from './emit.js';
export { subscribe } from './consume.js';
export type { SubscribeOptions, Subscription, ResumePosition } from './consume.js';
export { ControlClient, ControlTarget, NackError } from './control.js';
export type { CommandSpec, SendOptions, ControlClientOptions, ControlSocket, NackReason, TargetEmitter, RosterEntry } from './control.js';
export { validateEvent } from './validate.js';
export type { ValidationIssue, ValidationRule } from './validate.js';
export { StateProjection, projectState } from './projection.js';
export type { ProjectedState, ProjectedSession, ProjectedRun, PendingAttention, ProjectionViolation } from './projection.js';
export { TransportError } from './errors.js';
export type { TransportErrorKind } from './errors.js';
export { httpTransport, wsTransport, jsonlSink } from './transports.js';
export type { Transport, WsTransportOptions } from './transports.js';
