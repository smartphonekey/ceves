/**
 * PostgreSQL adapter types for Ceves.
 *
 * The PG variant runs command/query processing INSIDE PostgreSQL via PLV8
 * (JavaScript/TypeScript functions in the database). Aggregate state lives in
 * a PostgreSQL table; the emitted domain event is RETURNED to the caller and
 * is intentionally NEVER stored in PostgreSQL — the HTTP wrapper appends it
 * to the external event log (R2/S3) via an {@link EventSink}.
 *
 * @packageDocumentation
 */

import type { AuthContext } from '../../core/AggregateRouter';
import type { StoredEvent } from '../../storage/interfaces';

/**
 * Synchronous SQL access as provided by PLV8's `plv8.execute()`.
 *
 * - `SELECT` / `... RETURNING` statements resolve to an array of row objects.
 * - Plain `INSERT` / `UPDATE` / `DELETE` resolve to the affected-row count.
 *
 * Inside PostgreSQL this is backed by `plv8.execute`; in unit tests a fake
 * implementation over an in-memory map is injected instead.
 */
export interface PgSql {
  execute(query: string, params?: unknown[]): unknown;
}

/**
 * Async SQL client contract used OUTSIDE the database (gateway + outbox
 * relay). node-postgres compatible; adapt other drivers to it.
 */
export interface PgQueryClient {
  query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

/**
 * Current-state row for one aggregate, as stored in `<schema>.aggregate_state`.
 */
export interface PgStateRow {
  state: Record<string, unknown>;
  version: number;
  org_id: string;
}

/**
 * Input for one command dispatch inside PostgreSQL.
 */
export interface PgCommandInput {
  /** Aggregate type, e.g. 'LockAggregate'. Optional — derived from the route when omitted. */
  aggregateType?: string;
  /** Aggregate instance id (addressing identity — the `:id` URL segment). */
  aggregateId: string;
  /**
   * Route registry key, `"<METHOD>:<path>"` with the registered path pattern
   * (e.g. `"POST:/locks/:id/AddKey"`). A concrete URL path also works — it is
   * matched against registered patterns as a fallback.
   */
  routeKey: string;
  /** Validated command body. */
  command: Record<string, unknown>;
  /** Caller identity (worker-derived; equivalent of the trusted auth headers). */
  auth?: AuthContext;
  /**
   * Config values exposed to handlers as `env`. Inside PostgreSQL there are
   * no Cloudflare bindings — only plain JSON config travels here. Handlers
   * that need real I/O bindings must keep that part in the HTTP wrapper.
   */
  env?: Record<string, unknown>;
}

/**
 * Input for one query dispatch inside PostgreSQL.
 */
export interface PgQueryInput {
  aggregateType?: string;
  aggregateId: string;
  routeKey: string;
  /** Merged query parameters (URL params + query string, or POST body). */
  query: Record<string, unknown>;
  auth?: AuthContext;
  env?: Record<string, unknown>;
}

/**
 * Result of a dispatch. Mirrors the wire behaviour of the Durable Object
 * variant: `status` + JSON `body` are what the HTTP layer returns verbatim.
 *
 * `event` is the just-persisted-to-state domain event envelope. It is
 * returned so the caller (HTTP wrapper) can append it to the external event
 * log and run side effects / projections. It is never written to PostgreSQL.
 */
export interface PgDispatchResult {
  status: number;
  /** JSON-serializable response body (object for commands; any JSON for queries). */
  body: unknown;
  event: StoredEvent | null;
  /**
   * Outbox row id when the emitted event was committed to `<schema>.outbox`
   * as part of the state-write transaction (the default). The wrapper must
   * then NOT append the event itself — the relay owns delivery, which is what
   * makes it survive a wrapper crash. `null` means the event was not
   * outboxed (`outbox.events: false`), so the wrapper appends it directly.
   */
  eventOutboxId?: string | null;
}

/**
 * Dispatcher surface installed as `globalThis.__ceves_pg__` inside PLV8.
 */
export interface PgDispatcher {
  executeCommand(input: PgCommandInput): Promise<PgDispatchResult>;
  executeQuery(input: PgQueryInput): Promise<PgDispatchResult>;
}

/**
 * Options shared by the dispatcher and the SQL generator.
 */
export interface PgDispatcherOptions {
  /** PostgreSQL schema holding the ceves tables/functions. Default: 'ceves'. */
  schema?: string;
  /** Fallback orgId when neither state nor auth carries one. */
  defaultOrgId?: string;
  /** Injectable clock (ISO string) for tests. */
  now?: () => string;
  /** Transactional-outbox behaviour during dispatch. */
  outbox?: {
    /**
     * Intercept `globalThis.fetch` during command dispatch and enqueue the
     * request into `<schema>.outbox` instead of performing I/O (PLV8 has no
     * network anyway). The caller receives a stub 202 response whose body
     * reads throw. Default: true. Explicit `getPgOutbox().enqueue(...)`
     * works regardless of this flag.
     */
    interceptFetch?: boolean;
    /**
     * Commit the emitted event to `<schema>.outbox` (kind `'event'`) inside
     * the state-write transaction, so it is delivered to the external event
     * log by the relay instead of by a fire-and-forget append that dies with
     * the process. Default: true — this is what makes event delivery
     * guaranteed (state and event commit together, or neither does).
     *
     * Set false only when the wrapper appends events itself and you accept
     * losing an event if it crashes between commit and the append.
     */
    events?: boolean;
  };
}

/**
 * One route entry in the generated manifest — the bridge between the
 * decorator registries and the generated per-route SQL functions.
 */
export interface PgRouteManifestEntry {
  /** Registry key: `"<METHOD>:<path>"`. */
  key: string;
  method: string;
  path: string;
  className: string;
  aggregateType: string;
  kind: 'command' | 'query';
  isCreateCommand: boolean;
  /** Generated SQL function name (without schema), e.g. 'cmd_add_key'. */
  functionName: string;
}
