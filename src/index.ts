/**
 * Ceves - Event Sourcing for Cloudflare Workers, AWS Lambda, and NATS
 *
 * This is the main entry point for the Ceves library.
 * The library provides decorator-based abstractions for event sourcing
 * with automatic state restoration on Cloudflare Workers.
 *
 * @packageDocumentation
 */

export const CEVES_VERSION = '0.4.0';

// Storage interfaces
//
// `ISnapshotStore` is still defined in `./storage/interfaces` and used by the
// S3-based AWS-Lambda adapter (which has no DO storage to fall back on), but
// it is no longer re-exported from the top-level `ceves` package: on the
// Cloudflare/DO path, state is persisted to DurableObjectState.storage, so
// out-of-band snapshots are dead weight. If you really need it (AWS path),
// import it from the `ceves/aws` subpath, which re-exports it.
export type {
  IEventStore,
  StoredEvent,
  StoredSnapshot,
} from './storage/interfaces';

// Storage implementations
//
// Removed: `R2SnapshotStore` / `D1SnapshotStore`. The Cloudflare/DO variant
// of Ceves persists state to DurableObjectState.storage, not R2/D1, so these
// classes were never invoked in production. The S3 variant for the AWS
// Lambda adapter still lives under `./storage/S3SnapshotStore` and is
// re-exported via `ceves/aws`.
export { R2EventStore } from './storage/R2EventStore';

// Storage errors
//
// `SnapshotStoreError` / `SnapshotWriteError` / `SnapshotCorruptedError` are
// still thrown by the S3-based snapshot path (AWS Lambda adapter), so they
// stay exported. On the DO/CF path nothing throws them anymore.
export {
  EventStoreError,
  EventWriteError,
  SnapshotStoreError,
  SnapshotWriteError,
  SnapshotCorruptedError,
} from './storage/errors';

// State types (ADR-009: BaseState is now a class)
export { BaseState } from './schemas/State';

// Domain Events
export { NO_EVENT, type DomainEvent } from './events/DomainEvent';

// Event Metadata
export type { EventMetadata } from './events/EventMetadata';

// Error classes
export {
  CevesError,
  CommandValidationError,
  EventApplicationError,
  StateRestorationError,
  AggregateNotFoundError,
  AggregateAlreadyExistsError,
  VersionConflictError,
  VersionMismatchError,
  BusinessRuleViolationError,
  UnauthorizedError,
  ForbiddenError,
  ZodError,
  // Cross-RPC error coercion helpers
  CROSS_RPC_ERROR_HEADER,
  rehydrateErrorFromResponse,
  serializeErrorToResponse,
  stubFetchWithTypedErrors,
} from './errors';

// Decorators (Event handlers only)
export {
  EventHandler,
  getEventHandlers,
  findEventHandler,
  clearEventHandlers,
  executeSideEffects,
  type IEventHandler,
  type EventHandlerEntry,
} from './decorators';

// Durable-Object aggregate core
export { AggregateObject } from './core/AggregateObject';
export {
  AggregateRouter,
  type AggregateRouterConfig,
  type AuthContext,
} from './core/AggregateRouter';
export { setExceptionCapturer } from './core/exception-capture';

// Ceves Routing - Base classes for commands and queries
export { QueryRoute } from './routing/QueryRoute';
export {
  CommandRoute,
  CreateCommandRoute,
  type CommandBody,
  type BaseEvent as CommandRouteEvent
} from './routing/CommandRoute';
// Helper that coerces CF synthetic uncatchable DO errors into a structured
// 503 response. Use it in any custom `handle()` override that calls
// `stub.fetch()` directly.
export { safeDOFetch } from './routing/safeDOFetch';

// Multitenancy
export type { ITenantResolver } from './tenancy/TenantResolver';
export { ApiKeyTenantResolver } from './tenancy/ApiKeyTenantResolver';
export { HeaderTenantResolver } from './tenancy/HeaderTenantResolver';
export {
  MissingApiKeyError,
  InvalidApiKeyError,
  UnauthorizedAccessError,
} from './tenancy/errors';

// Routing - @Route decorator, registry, and router factory (vendored core)
export {
  Route,
  getRegisteredRoutes,
  clearRoutes,
  findRouteByUrl,
  type RouteOptions,
} from './routing/Route';
export {
  routeRegistry,
  type RouteMetadata,
} from './routing/RouteRegistry';
export { AggregateRoute } from './routing/AggregateRoute';
export {
  createRouter,
  type RouterOptions,
  type OpenAPIMetadata,
  type EnvConfig,
} from './routing/createRouter';

// Re-export Chanfana and Hono for convenience
export { OpenAPIRoute } from 'chanfana';
export type { Context } from 'hono';
export type { Hono } from 'hono';

// Event Projection
export type { IEventProjector, ProjectedEvent } from './projection';
export { registerProjector, getProjectors, clearProjectors } from './projection';
export { ProjectionDispatcher } from './projection';
export type { ProjectorCursor, ProjectionDispatcherDeps, WaitUntilFn } from './projection';
// Dead-letter queue for projection failures
export type { DlqRecord, DlqWriter } from './projection';
