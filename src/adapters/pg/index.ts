/**
 * Ceves PostgreSQL adapter (`ceves/pg`).
 *
 * Runs command/query handlers as PostgreSQL functions (PLV8) with aggregate
 * state in a PostgreSQL table. Emitted events are RETURNED to the HTTP
 * wrapper and appended to the external event log (R2/S3) — events are never
 * stored in PostgreSQL.
 *
 * Pieces:
 * - `createPgDispatcher` / `installPlv8Dispatcher` — the in-database pipeline,
 * - `registerPgAggregateState` — state-class registration (ADR-009 empty state),
 * - `generate*Sql` — SQL emission consumed by the `ceves-generate-pg` CLI,
 * - `registerPgRoutes` — thin Hono wrapper preserving the OpenAPI endpoints,
 * - `EventSink` implementations for the returned events.
 *
 * See `docs/architecture/postgresql.md` for the full architecture.
 *
 * @packageDocumentation
 */

export type {
  PgSql,
  PgQueryClient,
  PgStateRow,
  PgCommandInput,
  PgQueryInput,
  PgDispatchResult,
  PgDispatcher,
  PgDispatcherOptions,
  PgRouteManifestEntry,
} from './types';

export { createPgDispatcher } from './dispatcher';

export {
  registerPgAggregateState,
  getPgAggregateStateClass,
  clearPgAggregateStates,
  classifyRegisteredRoutes,
  collectPgManifest,
} from './registry';
export type { ClassifiedRoute, PgRouteClass, PgRouteInstance } from './registry';

export { installPlv8Dispatcher, PLV8_GLOBAL_KEY } from './plv8';
export type { PgPlv8Handle } from './plv8';

export {
  generateSchemaSql,
  generateModuleUpsertSql,
  generateDispatchFunctionsSql,
  generateRouteWrappersSql,
  generateFullSql,
} from './sqlgen';
export type { PgSqlGenOptions } from './sqlgen';

export { EventStoreSink, InMemoryEventSink } from './eventSink';
export type { EventSink } from './eventSink';

export { registerPgRoutes } from './gateway';
export type { PgGatewayOptions, HonoLike } from './gateway';

// Transactional outbox — in-database enqueue + wrapper-side relay
export { getPgOutbox, PgOutboxWriter, OUTBOX_EVENT_KIND } from './outbox';
export type { PgOutboxHandle, OutboxFetchRequest, OutboxCorrelation } from './outbox';
export { drainPgOutbox } from './outboxRelay';
export type {
  PgOutboxRow,
  OutboxDeliverer,
  PgOutboxRelayOptions,
  PgOutboxDrainSummary,
} from './outboxRelay';
