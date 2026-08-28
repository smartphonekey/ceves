/**
 * PG dispatcher — the Ceves command/query pipeline, running inside PostgreSQL.
 *
 * This is the PLV8 equivalent of `AggregateObject`'s command execution path.
 * Instead of Durable Object storage it reads/writes the aggregate's current
 * state row in `<schema>.aggregate_state`, using `SELECT ... FOR UPDATE` for
 * the per-aggregate serialization a Durable Object gets from single-threading.
 *
 * Semantics mirrored from the DO variant:
 * - create/update split (`isCreateCommand`), AA-92 idempotent duplicate create,
 * - `NO_EVENT` sentinel, `eventSchema` runtime parse, `customizeResponse` hook,
 * - the standard `{ success, aggregateId, version, event: { type, data } }`
 *   response and the AA-119 error envelope.
 *
 * Deliberate differences:
 * - The emitted event is committed to the transactional OUTBOX (kind
 *   `'event'`) in the same transaction as the state write, and also returned
 *   to the caller. The wrapper-side relay appends it to the external event
 *   log (R2/S3): state and event commit atomically, so a wrapper crash can
 *   no longer lose an event. PostgreSQL still holds no event LOG — outbox
 *   rows are a transient queue, deleted once delivered.
 * - `sideEffects` / projectors still run wrapper-side — they need network
 *   I/O PostgreSQL doesn't have.
 * - Error envelopes are returned as data (status + body); nothing throws
 *   across the PLV8 boundary after state has been written, so an error can
 *   never commit a half-applied mutation.
 *
 * @packageDocumentation
 */

import { findRouteByUrl } from '../../routing/Route';
import { applyEventToState } from '../../decorators/EventHandler';
import { NO_EVENT, type DomainEvent } from '../../events/DomainEvent';
import type { StoredEvent } from '../../storage/interfaces';
import { AggregateNotFoundError } from '../../errors/AggregateNotFoundError';
import { VersionConflictError } from '../../errors/VersionConflictError';
import { CevesError } from '../../errors/CevesError';
import { buildErrorEnvelope } from '../../errors/cross-rpc';
import {
  classifyRegisteredRoutes,
  getPgAggregateStateClass,
  type ClassifiedRoute,
  type PgRouteInstance,
} from './registry';
import { createOutboxCapture, OUTBOX_EVENT_KIND, PgOutboxWriter, type OutboxCapture } from './outbox';
import type {
  PgCommandInput,
  PgDispatcher,
  PgDispatcherOptions,
  PgDispatchResult,
  PgQueryInput,
  PgSql,
  PgStateRow,
} from './types';

/** Guard for SQL identifiers we interpolate (schema name). */
function assertSqlIdentifier(value: string, what: string): void {
  if (!/^[a-z_][a-z0-9_]*$/u.test(value)) {
    throw new Error(`Invalid ${what}: "${value}" (expected lowercase identifier)`);
  }
}

/** Normalize a PgSql.execute result into a rows array (DML counts → []). */
function asRows(result: unknown): Record<string, unknown>[] {
  return Array.isArray(result) ? (result as Record<string, unknown>[]) : [];
}

/** jsonb columns arrive as objects from plv8, as strings from some drivers. */
function parseJsonColumn(value: unknown): Record<string, unknown> {
  if (typeof value === 'string') return JSON.parse(value) as Record<string, unknown>;
  return (value ?? {}) as Record<string, unknown>;
}

/**
 * Resolve a route by registry key ("POST:/locks/:id/AddKey") with a fallback
 * URL match for concrete paths ("POST:/locks/lock-1/AddKey").
 */
function resolveRoute(routeKey: string, kind: 'command' | 'query'): ClassifiedRoute | null {
  const routes = classifyRegisteredRoutes();
  const direct = routes.find((r) => r.key === routeKey && r.kind === kind);
  if (direct) return direct;

  const separator = routeKey.indexOf(':');
  if (separator < 0) return null;
  const method = routeKey.slice(0, separator);
  const pathname = routeKey.slice(separator + 1);
  const match = findRouteByUrl(method, pathname);
  if (!match) return null;
  const matchedClass = match.RouteClass as unknown;
  return routes.find((r) => (r.RouteClass as unknown) === matchedClass && r.kind === kind) ?? null;
}

/** The standard success body for command responses (same wire shape as the DO). */
interface CommandSuccessBody {
  success: true;
  aggregateId: string;
  version: number;
  event: { type: string; data: Record<string, unknown> } | null;
}

function noEventResponse(aggregateId: string, version: number): PgDispatchResult {
  const body: CommandSuccessBody = { success: true, aggregateId, version, event: null };
  return { status: 200, body, event: null };
}

function errorResult(error: unknown): PgDispatchResult {
  const { status, envelope } = buildErrorEnvelope(error);
  return { status, body: envelope, event: null };
}

/**
 * State-row store bound to one schema. Parameterized SQL only; state travels
 * as a JSON string with an explicit `::jsonb` cast so the same statements
 * work under plv8 and every node driver.
 */
class PgStateStore {
  constructor(
    private readonly sql: PgSql,
    private readonly schema: string,
  ) {}

  private get table(): string {
    return `${this.schema}.aggregate_state`;
  }

  load(aggregateType: string, aggregateId: string, forUpdate: boolean): PgStateRow | null {
    const rows = asRows(
      this.sql.execute(
        `SELECT state, version, org_id FROM ${this.table} ` +
          `WHERE aggregate_type = $1 AND aggregate_id = $2${forUpdate ? ' FOR UPDATE' : ''}`,
        [aggregateType, aggregateId],
      ),
    );
    const row = rows[0];
    if (!row) return null;
    return {
      state: parseJsonColumn(row.state),
      version: Number(row.version ?? 0),
      org_id: String(row.org_id ?? ''),
    };
  }

  /** Returns false when a concurrent transaction created the row first. */
  insert(aggregateType: string, aggregateId: string, version: number, orgId: string, state: unknown): boolean {
    const rows = asRows(
      this.sql.execute(
        `INSERT INTO ${this.table} (aggregate_type, aggregate_id, version, org_id, state, updated_at) ` +
          `VALUES ($1, $2, $3, $4, $5::jsonb, now()) ` +
          `ON CONFLICT (aggregate_type, aggregate_id) DO NOTHING RETURNING version`,
        [aggregateType, aggregateId, version, orgId, JSON.stringify(state)],
      ),
    );
    return rows.length > 0;
  }

  /** Returns false when the optimistic version guard failed. */
  update(
    aggregateType: string,
    aggregateId: string,
    newVersion: number,
    orgId: string,
    state: unknown,
    expectedVersion: number,
  ): boolean {
    const rows = asRows(
      this.sql.execute(
        `UPDATE ${this.table} SET version = $3, org_id = $4, state = $5::jsonb, updated_at = now() ` +
          `WHERE aggregate_type = $1 AND aggregate_id = $2 AND version = $6 RETURNING version`,
        [aggregateType, aggregateId, newVersion, orgId, JSON.stringify(state), expectedVersion],
      ),
    );
    return rows.length > 0;
  }
}

/** Execute the route's command handler with DO-identical argument order. */
async function runCommandHandler(
  route: ClassifiedRoute,
  input: PgCommandInput,
  state: Record<string, unknown> | null,
  isCreate: boolean,
): Promise<DomainEvent | typeof NO_EVENT> {
  const instance = route.instance;
  const executeCommand = instance.executeCommand;
  if (!executeCommand) {
    throw new CevesError(`Route ${route.RouteClass.name} has no executeCommand`, 500);
  }
  const env = input.env ?? {};
  const auth = input.auth;
  return isCreate
    ? await executeCommand.call(instance, input.command, env, auth)
    : await executeCommand.call(instance, input.command, state, env, auth);
}

/**
 * Runtime `eventSchema` parse of the stripped event-data payload — same
 * contract as the DO variant, but BEFORE the state write: inside PostgreSQL
 * an error returned after a write would still commit it, so malformed events
 * must be rejected while nothing has been persisted yet.
 */
function assertEventMatchesSchema(
  route: ClassifiedRoute,
  eventType: string,
  eventData: Record<string, unknown>,
): void {
  const schema = route.RouteClass.eventSchema;
  if (!schema) return;
  const parsed = schema.safeParse(eventData);
  if (!parsed.success) {
    throw new CevesError(
      `Command handler ${route.RouteClass.name} returned a malformed "${eventType}" event: ` +
        `payload does not match its declared eventSchema`,
      500,
    );
  }
}

/**
 * Create the PG dispatcher over a synchronous SQL handle (plv8.execute inside
 * PostgreSQL, a fake in tests). All route/handler lookups go through the same
 * decorator registries the Durable Object variant uses.
 */
export function createPgDispatcher(sql: PgSql, options: PgDispatcherOptions = {}): PgDispatcher {
  const schema = options.schema ?? 'ceves';
  assertSqlIdentifier(schema, 'PostgreSQL schema name');
  const store = new PgStateStore(sql, schema);
  const outboxWriter = new PgOutboxWriter(sql, schema);
  const interceptFetch = options.outbox?.interceptFetch !== false;
  const outboxEvents = options.outbox?.events !== false;
  const now = options.now ?? ((): string => new Date().toISOString());
  const defaultOrgId = options.defaultOrgId ?? '';

  async function executeCommand(input: PgCommandInput): Promise<PgDispatchResult> {
    const route = resolveRoute(input.routeKey, 'command');
    if (!route) {
      return errorResult(new CevesError(`Unknown command route: ${input.routeKey}`, 404));
    }
    const aggregateType = route.instance.aggregateType as string;

    // Transactional outbox: fetches (and explicit getPgOutbox() enqueues)
    // made by the handler become rows in <schema>.outbox — same transaction
    // as the state write. Error envelopes below are RETURNED (the
    // transaction commits), so every failure path must run capture.rollback()
    // to compensate; that is why persistAndRespond throws instead of
    // returning error envelopes itself.
    const capture = createOutboxCapture({
      writer: outboxWriter,
      correlation: { aggregateType, aggregateId: input.aggregateId },
      interceptFetch,
      mode: 'command',
    });
    try {
      const isCreate = route.RouteClass.isCreateCommand === true;
      const row = store.load(aggregateType, input.aggregateId, true);

      // AA-92: duplicate create is an idempotent no-op, not a 409.
      if (isCreate && row) return noEventResponse(input.aggregateId, row.version);
      if (!isCreate && !row) {
        throw new AggregateNotFoundError(aggregateType, input.aggregateId);
      }

      const domainEvent = await runCommandHandler(route, input, row?.state ?? null, isCreate);
      if (domainEvent === NO_EVENT) {
        return noEventResponse(input.aggregateId, row?.version ?? 0);
      }

      // `await` matters: without it a rejection from persistAndRespond would
      // escape this try/catch and cross the PLV8 boundary as an exception.
      return await persistAndRespond({
        route, input, row, isCreate, domainEvent, aggregateType, capture,
      });
    } catch (error) {
      capture.rollback();
      return errorResult(error);
    } finally {
      capture.restore();
    }
  }

  /** Apply the event to state, write the row, build the success response. */
  async function persistAndRespond(args: {
    route: ClassifiedRoute;
    input: PgCommandInput;
    row: PgStateRow | null;
    isCreate: boolean;
    domainEvent: DomainEvent;
    aggregateType: string;
    capture: OutboxCapture;
  }): Promise<PgDispatchResult> {
    const { route, input, row, isCreate, domainEvent, aggregateType, capture } = args;
    const version = (row?.version ?? 0) + 1;
    const stateOrgId = typeof row?.state.orgId === 'string' ? row.state.orgId : '';
    const eventOrgId = stateOrgId || input.auth?.orgId || defaultOrgId;

    const storedEvent: StoredEvent = {
      aggregateType,
      aggregateId: input.aggregateId,
      version,
      type: domainEvent.type,
      timestamp: now(),
      orgId: eventOrgId,
      event: domainEvent,
    };

    const StateClass = getPgAggregateStateClass(aggregateType);
    const newState = applyEventToState(
      aggregateType,
      (row?.state ?? null) as InstanceType<typeof StateClass> | null,
      storedEvent,
      StateClass,
    );

    // Strip the `type` discriminator from the data payload (wire shape parity
    // with the DO variant) and run the optional eventSchema runtime parse
    // BEFORE any write — see assertEventMatchesSchema for why the order flips.
    const { type: eventType, ...eventDataWithoutType } = domainEvent as DomainEvent &
      Record<string, unknown>;
    assertEventMatchesSchema(route, eventType, eventDataWithoutType);

    // AA-58 parity: surface the human-readable id from state on the wire; the
    // row itself stays keyed by the addressing id (like DO storage stays with
    // the DO the request addressed).
    const responseAggregateId = newState.id && newState.id !== input.aggregateId
      ? newState.id
      : input.aggregateId;
    storedEvent.aggregateId = responseAggregateId;

    const newOrgId = newState.orgId || eventOrgId;
    if (row) {
      const updated = store.update(
        aggregateType, input.aggregateId, version, newOrgId, newState, row.version,
      );
      if (!updated) {
        // Thrown (not returned) so the caller's catch also compensates any
        // outbox rows this command enqueued before losing the write race.
        throw new VersionConflictError(
          `Concurrent modification of ${aggregateType}/${input.aggregateId}: ` +
            `expected version ${row.version} was gone at write time`,
          row.version,
          version,
          aggregateType,
          input.aggregateId,
        );
      }
    } else {
      const inserted = store.insert(aggregateType, input.aggregateId, version, newOrgId, newState);
      // Lost a concurrent-create race: the aggregate exists now, so the
      // caller's desired end-state holds — idempotent no-op, no event shipped.
      if (!inserted) return noEventResponse(input.aggregateId, version);
    }

    // GUARANTEED DELIVERY: the event row joins the state write in ONE
    // transaction — they commit together or not at all. If the wrapper dies
    // right after commit, the relay still delivers the event to the event
    // log; a fire-and-forget append would simply have lost it.
    const eventOutboxId = outboxEvents ? capture.enqueue(OUTBOX_EVENT_KIND, storedEvent) : null;

    const standardResponse: CommandSuccessBody = {
      success: true,
      aggregateId: responseAggregateId,
      version,
      event: { type: eventType, data: eventDataWithoutType },
    };
    const body = route.instance.customizeResponse
      ? await route.instance.customizeResponse(standardResponse, domainEvent, newState)
      : standardResponse;

    return { status: isCreate ? 201 : 200, body, event: storedEvent, eventOutboxId };
  }

  async function executeQuery(input: PgQueryInput): Promise<PgDispatchResult> {
    const route = resolveRoute(input.routeKey, 'query');
    if (!route) {
      return errorResult(new CevesError(`Unknown query route: ${input.routeKey}`, 404));
    }
    const aggregateType = route.instance.aggregateType as string;

    // Query mode: fetch is intercepted only to fail with a descriptive error
    // (queries are reads — they must not enqueue outbox work).
    const capture = createOutboxCapture({
      writer: outboxWriter,
      correlation: { aggregateType, aggregateId: input.aggregateId },
      interceptFetch,
      mode: 'query',
    });
    try {
      const row = store.load(aggregateType, input.aggregateId, false);
      if (!row) {
        return errorResult(new AggregateNotFoundError(aggregateType, input.aggregateId));
      }

      const result = await runQueryHandler(route.instance, row.state, input);
      return { status: 200, body: result, event: null };
    } catch (error) {
      return errorResult(error);
    } finally {
      capture.restore();
    }
  }

  return { executeCommand, executeQuery };
}

/**
 * Execute a query handler with a minimal context stub in place of the Hono
 * context (there is no HTTP request inside PostgreSQL). The stub carries
 * `env` — the sanctioned way for query handlers to reach configuration.
 * Queries that reach into the real Hono context beyond `env` are not
 * PG-dispatchable — keep those on the wrapper side.
 */
async function runQueryHandler(
  instance: PgRouteInstance,
  state: Record<string, unknown>,
  input: PgQueryInput,
): Promise<unknown> {
  const executeQuery = instance.executeQuery;
  if (!executeQuery) {
    throw new CevesError('Route has no executeQuery', 500);
  }
  const contextStub = { env: input.env ?? {}, get: (): undefined => undefined };
  return await executeQuery.call(instance, state, input.query, contextStub);
}
