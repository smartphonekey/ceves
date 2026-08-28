/**
 * Thin HTTP wrapper for the PG variant.
 *
 * Mounts every registered `@Route` command/query onto a Hono app at the SAME
 * method + path the OpenAPI schema describes — only the execution target
 * changes: instead of forwarding to a Durable Object, each request becomes a
 * single `SELECT <schema>.execute_command(...)` / `execute_query(...)` call.
 * State and processing live in PostgreSQL; the wrapper stays thin:
 *
 * - request-body validation with the route's own Zod schema,
 * - auth-context propagation (same `authContext` middleware contract as
 *   `CommandRoute`/`QueryRoute`),
 * - delivering the emitted event to the external event log ({@link EventSink}).
 *   By default the dispatcher commits that event to the transactional outbox
 *   inside the state-write transaction, so the wrapper drains it via the
 *   relay (guaranteed delivery) rather than appending it directly; either
 *   way PostgreSQL holds no event log,
 * - running handler `sideEffects` — network I/O that cannot run inside the DB.
 *
 * Runs unchanged on Cloudflare Workers and Node (Hono is isomorphic; the SQL
 * client is injected — node-postgres, postgres.js, or a serverless driver).
 *
 * @packageDocumentation
 */

import type { Context } from 'hono';
import { createLogger } from '../../logger';
import { executeSideEffects } from '../../decorators/EventHandler';
import type { StoredEvent } from '../../storage/interfaces';
import type { AuthContext } from '../../core/AggregateRouter';
import { classifyRegisteredRoutes, type ClassifiedRoute } from './registry';
import type { EventSink } from './eventSink';
import type { PgDispatchResult, PgQueryClient } from './types';
import { drainPgOutbox, type PgOutboxRelayOptions } from './outboxRelay';

const logger = createLogger({ component: 'PgGateway' });

export type { PgQueryClient } from './types';

/** Structural Hono surface we need — keeps the wrapper version-agnostic. */
export interface HonoLike {
  on(method: string, path: string, handler: (c: Context) => Promise<Response>): unknown;
}

type MaybePerRequest<T> = T | ((c: Context) => T);

/** Options for {@link registerPgRoutes}. */
export interface PgGatewayOptions {
  /** SQL client, or a per-request factory (e.g. Hyperdrive binding on CF). */
  client: MaybePerRequest<PgQueryClient>;
  /** Where returned events are appended (R2/S3). Omit to drop events (dev only). */
  eventSink?: MaybePerRequest<EventSink>;
  /** PostgreSQL schema of the generated functions. Default 'ceves'. */
  schema?: string;
  /** JSON config passed into PostgreSQL as the handler `env`. Default {}. */
  env?: MaybePerRequest<Record<string, unknown>>;
  /** Run `@EventHandler.sideEffects` wrapper-side after a command. Default true. */
  runSideEffects?: boolean;
  /**
   * Transactional-outbox relay wiring. The gateway drains `<schema>.outbox`
   * in the background after every successful command (`drainAfterCommand`,
   * default true) — the low-latency fast path. This happens even without
   * this option whenever the dispatcher outboxed the event, since the relay
   * is then the durable path to the event log; `eventSink` above is wired
   * into the relay automatically. Also run `drainPgOutbox` on a schedule
   * (cron) as the catch-up sweeper that survives wrapper crashes.
   */
  outbox?: PgOutboxRelayOptions & { drainAfterCommand?: boolean };
}

function resolve<T>(value: MaybePerRequest<T>, c: Context): T {
  return typeof value === 'function' ? (value as (c: Context) => T)(c) : value;
}

function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function missingAggregateIdResponse(): Response {
  return jsonResponse(
    { success: false, error: 'MissingAggregateId', message: 'Aggregate ID not found in URL path' },
    400,
  );
}

/**
 * Auth context stored in Hono context via middleware — the same contract
 * `CommandRoute.buildAuthHeaders` consumes on the DO path.
 */
interface GatewayAuthContext {
  authType: 'api-key' | 'jwt';
  orgId?: string;
  isSuper?: boolean;
  userEmail?: string;
  userId?: string;
}

/** Map the middleware auth context to the dispatcher's AuthContext. */
function buildAuth(c: Context): AuthContext {
  const authContext = c.get('authContext') as GatewayAuthContext | undefined;
  if (!authContext) return {};
  if (authContext.authType === 'api-key') {
    return { orgId: authContext.orgId, isSuper: authContext.isSuper };
  }
  return { userId: authContext.userId, email: authContext.userEmail };
}

/** Extract the route's JSON-body Zod schema, when it declares one. */
function bodySchemaOf(route: ClassifiedRoute): { safeParse: (v: unknown) => { success: boolean; error?: { message?: string } } } | null {
  const schema = route.instance.schema as
    | { request?: { body?: { content?: Record<string, { schema?: unknown }> } } }
    | undefined;
  const candidate = schema?.request?.body?.content?.['application/json']?.schema;
  if (candidate && typeof (candidate as { safeParse?: unknown }).safeParse === 'function') {
    return candidate as { safeParse: (v: unknown) => { success: boolean; error?: { message?: string } } };
  }
  return null;
}

/** Best-effort waitUntil: CF Workers has one; elsewhere the promise floats. */
function scheduleBackground(c: Context, promise: Promise<unknown>): void {
  const guarded = promise.catch((err: unknown) => {
    logger.error('PG gateway background task failed', {
      error: err instanceof Error ? err.message : String(err),
    });
  });
  try {
    c.executionCtx.waitUntil(guarded);
  } catch {
    // Not on Workers (or no execution context) — the guarded promise floats.
  }
}

/** Read + JSON-parse the request body; empty body → {}. */
async function readJsonBody(c: Context): Promise<Record<string, unknown> | null> {
  try {
    const text = await c.req.text();
    if (!text) return {};
    return JSON.parse(text) as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** Call the generated PostgreSQL function and normalize its jsonb result. */
async function callPgFunction(
  client: PgQueryClient,
  schema: string,
  fn: 'execute_command' | 'execute_query',
  args: {
    aggregateType: string;
    aggregateId: string;
    routeKey: string;
    payload: Record<string, unknown>;
    auth: AuthContext;
    env: Record<string, unknown>;
  },
): Promise<PgDispatchResult> {
  const { rows } = await client.query(
    `SELECT ${schema}.${fn}($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb) AS result`,
    [
      args.aggregateType,
      args.aggregateId,
      args.routeKey,
      JSON.stringify(args.payload),
      JSON.stringify(args.auth),
      JSON.stringify(args.env),
    ],
  );
  const raw = rows[0]?.result;
  const parsed = typeof raw === 'string' ? (JSON.parse(raw) as unknown) : raw;
  if (!parsed || typeof parsed !== 'object') {
    throw new Error(`PostgreSQL ${fn} returned no result`);
  }
  return parsed as PgDispatchResult;
}

/** Shared per-request plumbing for command and query handlers. */
interface RequestPlumbing {
  client: PgQueryClient;
  env: Record<string, unknown>;
  auth: AuthContext;
  aggregateId: string;
}

function buildPlumbing(c: Context, options: PgGatewayOptions): RequestPlumbing | null {
  const aggregateId = c.req.param('id');
  if (!aggregateId) return null;
  return {
    client: resolve(options.client, c),
    env: options.env ? resolve(options.env, c) : {},
    auth: buildAuth(c),
    aggregateId,
  };
}

/**
 * Ship the emitted event: append to the event log (fire-and-forget, like the
 * DO's R2 persist) and run `sideEffects` (awaited — a failure surfaces as the
 * same 500 SideEffectError response the DO variant returns).
 */
async function shipEvent(
  c: Context,
  options: PgGatewayOptions,
  route: ClassifiedRoute,
  event: StoredEvent,
  outboxed: boolean,
): Promise<Response | null> {
  if (outboxed) {
    // The event committed to <schema>.outbox with the state write; the relay
    // owns delivery. Appending here too would double-write (the sink is
    // idempotent per version, but the relay is the durable path).
    if (!options.eventSink) {
      logger.warn(
        'PG gateway has no eventSink — outboxed events will accumulate undelivered',
        { aggregateType: event.aggregateType, eventType: event.type },
      );
    }
  } else if (options.eventSink) {
    const sink = resolve(options.eventSink, c);
    scheduleBackground(c, sink.append(event));
  } else {
    logger.warn('PG gateway has no eventSink — emitted event NOT persisted to event log', {
      aggregateType: event.aggregateType,
      aggregateId: event.aggregateId,
      eventType: event.type,
    });
  }

  if (options.runSideEffects === false) return null;
  try {
    await executeSideEffects(
      route.instance.aggregateType as string,
      event,
      c.env as Record<string, unknown>,
    );
    return null;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(
      {
        success: false,
        error: 'SideEffectError',
        message: `Event applied but side effects failed: ${message}`,
        aggregateId: event.aggregateId,
        version: event.version,
      },
      500,
    );
  }
}

function commandHandler(route: ClassifiedRoute, options: PgGatewayOptions, schema: string) {
  return async (c: Context): Promise<Response> => {
    const plumbing = buildPlumbing(c, options);
    if (!plumbing) return missingAggregateIdResponse();

    const body = await readJsonBody(c);
    if (body === null) {
      return jsonResponse(
        { success: false, error: 'InvalidRequestBody', message: 'Request body must be valid JSON' },
        400,
      );
    }

    const bodySchema = bodySchemaOf(route);
    if (bodySchema) {
      const parsed = bodySchema.safeParse(body);
      if (!parsed.success) {
        return jsonResponse(
          {
            success: false,
            errors: [{ code: 400, message: parsed.error?.message ?? 'Invalid request body' }],
          },
          400,
        );
      }
    }

    const result = await callPgFunction(plumbing.client, schema, 'execute_command', {
      aggregateType: route.instance.aggregateType as string,
      aggregateId: plumbing.aggregateId,
      routeKey: route.key,
      payload: body,
      auth: plumbing.auth,
      env: plumbing.env,
    });

    const eventOutboxed = result.eventOutboxId != null;
    if (result.event) {
      const sideEffectFailure = await shipEvent(c, options, route, result.event, eventOutboxed);
      if (sideEffectFailure) return sideEffectFailure;
    }

    // Outbox fast path: the command just committed, so any rows it enqueued
    // are visible — deliver them now instead of waiting for the sweeper.
    // An outboxed event alone is reason enough to drain: with events in the
    // outbox, the relay is the ONLY path to the event log, so a gateway with
    // no explicit `outbox` options must still drain.
    const wantsDrain = (options.outbox !== undefined || eventOutboxed) && result.status < 400;
    if (wantsDrain && options.outbox?.drainAfterCommand !== false) {
      const relayOptions: PgOutboxRelayOptions = {
        ...options.outbox,
        schema: options.outbox?.schema ?? schema,
        // Built-in 'event' deliverer, wired from the gateway's own sink so
        // guaranteed event delivery needs no extra configuration.
        eventSink: options.outbox?.eventSink ?? (options.eventSink ? resolve(options.eventSink, c) : undefined),
      };
      delete (relayOptions as { drainAfterCommand?: boolean }).drainAfterCommand;
      scheduleBackground(c, drainPgOutbox(plumbing.client, relayOptions));
    }
    return jsonResponse(result.body, result.status);
  };
}

function queryHandler(route: ClassifiedRoute, options: PgGatewayOptions, schema: string) {
  return async (c: Context): Promise<Response> => {
    const plumbing = buildPlumbing(c, options);
    if (!plumbing) return missingAggregateIdResponse();

    // Same parameter merge as QueryRoute.extractQueryParams: POST body wins,
    // otherwise URL params + query string.
    let query: Record<string, unknown>;
    if (c.req.method === 'POST') {
      const body = await readJsonBody(c);
      query = body ?? {};
    } else {
      query = { ...c.req.param(), ...c.req.query() };
    }

    const result = await callPgFunction(plumbing.client, schema, 'execute_query', {
      aggregateType: route.instance.aggregateType as string,
      aggregateId: plumbing.aggregateId,
      routeKey: route.key,
      payload: query,
      auth: plumbing.auth,
      env: plumbing.env,
    });
    return jsonResponse(result.body, result.status);
  };
}

/**
 * Mount every registered command/query `@Route` onto `app`, executing them in
 * PostgreSQL. Returns how many of each were mounted. Routes without an
 * aggregate contract (plain worker routes) are left for the host app.
 */
export function registerPgRoutes(
  app: HonoLike,
  options: PgGatewayOptions,
): { commands: number; queries: number } {
  const schema = options.schema ?? 'ceves';
  if (!/^[a-z_][a-z0-9_]*$/u.test(schema)) {
    throw new Error(`Invalid PostgreSQL schema name: "${schema}"`);
  }

  let commands = 0;
  let queries = 0;
  for (const route of classifyRegisteredRoutes()) {
    if (route.kind === 'command') {
      app.on(route.method, route.path, commandHandler(route, options, schema));
      commands += 1;
    } else {
      app.on(route.method, route.path, queryHandler(route, options, schema));
      queries += 1;
    }
  }
  logger.info('Mounted PG-dispatched routes', { commands, queries });
  return { commands, queries };
}
