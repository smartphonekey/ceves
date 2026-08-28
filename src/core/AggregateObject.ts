/**
 * AggregateObject - Base class for Durable Object aggregates
 *
 * This class implements the core pattern for event-sourced aggregates running
 * in Cloudflare Durable Objects. It provides:
 * - In-memory state management with automatic event application
 * - Zero-latency state persistence via DO Storage API (SQLite-backed)
 * - Automatic state restoration from DO storage or R2 migration
 * - Command handler discovery and execution via decorator registry
 * - Query handler discovery and execution via decorator registry
 * - Event persistence to R2/S3 for event sourcing audit log
 *
 * @packageDocumentation
 */

import { DurableObject } from 'cloudflare:workers';
import type { AuthContext } from './AggregateRouter';
import type { DurableObjectState } from '@cloudflare/workers-types';
import { applyEventToState, executeSideEffects } from '../decorators/EventHandler';
import type { IEventStore, StoredEvent } from '../storage/interfaces';
import type { BaseState } from '../schemas/State';
import type { ITenantResolver } from '../tenancy/TenantResolver';
import { HeaderTenantResolver } from '../tenancy/HeaderTenantResolver';
import { R2EventStore } from '../storage/R2EventStore';
import { CevesError } from '../errors/CevesError';
import { AggregateNotFoundError } from '../errors/AggregateNotFoundError';
import { StateRestorationError } from '../errors/StateRestorationError';
import { serializeErrorToResponse } from '../errors/cross-rpc';
import { captureException } from './exception-capture';
import { NO_EVENT, type DomainEvent } from '../events/DomainEvent';
import { createLogger } from '../logger';
import { findRouteByUrl } from '../routing/Route';
import { getProjectors } from '../projection/ProjectorRegistry';
import { ProjectionDispatcher } from '../projection/ProjectionDispatcher';
import type { ProjectedEvent } from '../projection/IEventProjector';
import type { DlqWriter } from '../projection/DlqWriter';

/**
 * AA-117: the bounded batch size for R2 event replay. `restoreFromR2Batched`
 * loads at most this many events per page, applies them, then checkpoints the
 * resulting state (incl. version) to DO storage before loading the next page.
 *
 * It bounds two things at once:
 *  - peak memory + CPU per cold start (only one batch is in memory at a time —
 *    the old `loadAll()` pulled the whole stream in and OOM'd large aggregates
 *    before any progress was saved), and
 *  - how much replay work a mid-replay isolate kill can waste (the next cold
 *    start resumes from the last checkpointed version).
 *
 * With this set to 50, a 375-event stream restores over 8 pages; an isolate
 * killed partway resumes from the last 50-event boundary instead of from 0.
 * Lower = more durable + more `ctx.storage.put`/`list` calls; higher = cheaper
 * but coarser resume granularity and a larger working set. 50 is a pragmatic
 * middle ground.
 */
const REPLAY_CHECKPOINT_INTERVAL = 50;

/**
 * Interface for command handler instances that can be discovered by DO
 */
interface CommandHandlerInstance {
  aggregateType?: string;
  executeCommand?: (command: unknown, state: unknown, env: unknown, auth?: AuthContext) => Promise<DomainEvent | typeof NO_EVENT>;
  /**
   * Optional response-customization hook. When present, it's called by the
   * framework after the event is persisted to enrich the success response
   * body with server-computed fields. See `CommandRoute.customizeResponse`.
   *
   * Note: the `data` payload here EXCLUDES the `type` field — the outer
   * `type` is the only carrier of the event-type discriminator.
   */
  customizeResponse?: (
    response: {
      success: true;
      aggregateId: string;
      version: number;
      event: { type: string; data: Record<string, unknown> } | null;
    },
    event: DomainEvent,
    state: unknown,
  ) => Promise<Record<string, unknown>>;
}

/**
 * Interface for command handler classes with static isCreateCommand property
 * and optional eventSchema for runtime validation of the emitted event.
 */
interface CommandHandlerClass {
  isCreateCommand?: boolean;
  name: string;
  /**
   * Optional Zod schema (or any object with a `safeParse` method) describing
   * the shape of the **data-only payload** carried under `event.data` in the
   * success response — i.e. the event fields WITHOUT the outer `type`
   * discriminator. When present, the framework parses the stripped data
   * through it after `executeCommand()` returns, and a parse failure throws
   * (handler returned malformed data).
   *
   * The schema describes exactly what's on the wire under `event.data`, so
   * routes should declare it without a `type: z.literal(...)` field.
   *
   * Typed loosely as `unknown` here so AggregateObject doesn't pull in a
   * `zod` dependency directly; the actual narrowing happens at runtime.
   */
  eventSchema?: { safeParse: (data: unknown) => { success: boolean; error?: unknown } };
}

/**
 * Interface for route match result from findRouteByUrl
 */
interface RouteMatchResult {
  RouteClass: new () => unknown;
  params: Record<string, string>;
}

/**
 * Extract a human-readable reason string from a Zod parse error (or any
 * object exposing a `message` field). Returns 'unknown' when the parser
 * provides no error payload.
 */
function describeParseError(error: unknown): string {
  if (error == null) return 'unknown';
  if (typeof error === 'object' && 'message' in error) {
    return String((error as { message?: unknown }).message);
  }
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

/**
 * Type-safe wrapper for findRouteByUrl
 * Uses explicit type assertions to handle workspace package type resolution
 * during lint
 */
function findCommandRouteByUrl(method: string, pathname: string): RouteMatchResult | undefined {
  // Route-registry lookup - types may not be available at lint time
  const result = (findRouteByUrl as (m: string, p: string) => {
    RouteClass: new () => unknown;
    params: Record<string, string>;
  } | undefined)(method, pathname);

  if (!result) return undefined;

  return {
    RouteClass: result.RouteClass,
    params: result.params,
  };
}

/**
 * Environment interface for AggregateObject
 */
export interface AggregateObjectEnv {
  EVENTS_BUCKET?: R2Bucket;
  SNAPSHOTS_BUCKET?: R2Bucket;
  ADMIN_API_KEY?: string;
  ALLOWED_EMAIL_DOMAIN?: string;
  DEFAULT_ORG_ID?: string;
  [key: string]: unknown;
}

/**
 * Base class for Durable Object aggregates
 *
 * Subclasses must:
 * 1. Pass the state class to super() constructor (matches generic type parameter)
 * 2. Import all command and event handlers in the worker entry point
 *
 * Everything else is automatic:
 * - aggregateType is derived from class name (e.g., BankAccountAggregate)
 * - Command execution via @CommandHandler registry
 * - Query execution via @QueryHandler registry
 * - Event application via @EventHandler registry
 * - State restoration and persistence
 *
 * @example
 * ```typescript
 * import { AggregateObject } from 'ceves';
 *
 * export class BankAccountAggregate extends AggregateObject<BankAccountState> {
 *   constructor(ctx: DurableObjectState, env: Env) {
 *     super(ctx, env, BankAccountState);  // That's it!
 *   }
 * }
 * ```
 *
 * @template TState - The aggregate state type (must extend BaseState)
 */
export abstract class AggregateObject<TState extends BaseState = BaseState> extends DurableObject {
  /**
   * In-memory aggregate state
   * Null if aggregate doesn't exist yet (no events)
   */
  protected state: TState | null = null;

  /**
   * Flag indicating whether state has been loaded from storage
   * Ensures state is only loaded once per DO lifecycle
   */
  protected stateLoaded = false;

  /**
   * Flag indicating this is a brand new aggregate with no prior state
   * When true, skips R2 snapshot/events loading for performance
   */
  protected isNewAggregate = false;

  /**
   * Event store for persisting events (R2/S3)
   */
  protected eventStore!: IEventStore;

  /**
   * Tenant resolver for multitenancy support
   */
  protected tenantResolver!: ITenantResolver;

  /**
   * Aggregate ID (extracted from DO ID)
   */
  protected aggregateId: string;

  /**
   * Durable Object state context
   * Stored explicitly because the mocked DurableObject class in unit tests doesn't provide it
   */
  protected declare ctx: DurableObjectState<Record<string, unknown>>;

  /**
   * Environment bindings
   */
  protected override env: AggregateObjectEnv;

  /**
   * Logger instance for aggregate operations
   */
  protected logger = createLogger({ component: 'AggregateObject' });

  /**
   * Get the aggregate type from the class name
   * Automatically derived from constructor.name, so no manual configuration needed
   *
   * Uses the class name as-is for event storage and handler registration.
   * Example: BankAccountAggregate → 'BankAccountAggregate'
   *
   * Override this getter if you need a custom aggregate type identifier.
   */
  protected get aggregateType(): string {
    return this.constructor.name;
  }

  /**
   * Projection dispatcher for sending events to registered projectors
   */
  private projectionDispatcher: ProjectionDispatcher | null = null;

  /**
   * State class constructor for creating empty state instances (ADR-009)
   * Set via constructor parameter to avoid duplication with generic type
   */
  private StateClass: new () => TState;

  /**
   * Get the state class constructor (ADR-009)
   * Used internally for state restoration and empty state creation
   */
  protected getStateClass(): new () => TState {
    return this.StateClass;
  }

  /**
   * Constructor
   *
   * @param ctx - Durable Object state
   * @param env - Environment bindings
   * @param StateClass - State class constructor (matches the generic type parameter)
   *
   * @example
   * ```typescript
   * export class BankAccountAggregate extends AggregateObject<AccountState> {
   *   constructor(ctx: DurableObjectState, env: Env) {
   *     super(ctx, env, AccountState);  // Pass state class once
   *   }
   * }
   * ```
   */
  constructor(ctx: DurableObjectState, env: AggregateObjectEnv, StateClass: new () => TState) {
    super(ctx, env);
    // Cast needed: DurableObjectState from platform is untyped, we use Record<string, unknown> internally
    this.ctx = ctx as DurableObjectState<Record<string, unknown>>;
    this.env = env;
    this.StateClass = StateClass;

    // Extract aggregateId from DO ID (initially uses hex hash, updated after state loads)
    // Note: ctx.id.name is not exposed in Miniflare/workerd, so we start with hex hash
    // After state loads, we update this to use state.id (human-readable ID)
    this.aggregateId = ctx.id.toString();

    // Initialize storage (subclasses can override by setting in constructor)
    this.initializeStores(env);

    // Load state from DO storage during initialization
    // blockConcurrencyWhile prevents requests until complete
    void ctx.blockConcurrencyWhile(async () => {
      try {
        // Try DO storage first (new approach)
        const storedState = await ctx.storage.get<TState>('state');

        if (storedState) {
          this.logger.info('Loaded state from DO storage', {
            aggregateType: this.aggregateType,
            aggregateId: this.aggregateId,
            version: storedState.version,
          });
          this.state = storedState;
          this.stateLoaded = true;

          // Use human-readable ID from state if available (instead of hex hash from ctx.id)
          if (this.state.id) {
            this.aggregateId = this.state.id;
            this.logger.info('Using human-readable ID from state', {
              aggregateType: this.aggregateType,
              aggregateId: this.aggregateId,
            });
          }
        } else {
          // No state in DO storage - try to load stored aggregate name for R2 lookups
          // This handles DOs that have no state yet but have had their name persisted
          const storedName = await ctx.storage.get<string>('aggregateName');
          if (storedName) {
            this.aggregateId = storedName;
            this.logger.info('Using stored aggregate name for R2 lookups', {
              aggregateType: this.aggregateType,
              aggregateId: this.aggregateId,
            });
          }

          // No state in DO storage - this is a brand new aggregate
          // Set flag to skip R2 checks for performance (~350ms savings)
          this.isNewAggregate = true;
          this.logger.info('New aggregate detected, will skip R2 migration checks', {
            aggregateType: this.aggregateType,
            aggregateId: this.aggregateId,
          });
        }

        // If no state in DO storage, will load from R2 in ensureStateLoaded()
        // This supports migration from R2 snapshots to DO storage
      } catch (error) {
        this.logger.error('Error loading state from DO storage', {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          error: error instanceof Error ? error.message : String(error),
        });
        // Fall back to R2 snapshot loading in ensureStateLoaded()
      }
    });
  }

  /**
   * Initialize event store and snapshot store
   * Can be overridden by subclasses for custom storage configuration
   *
   * Note: SNAPSHOTS_BUCKET is optional - only needed for R2 snapshot migration.
   * New DO implementations use DO Storage API and don't require snapshots.
   *
   * @param env - Environment bindings
   */
  protected initializeStores(env: AggregateObjectEnv): void {
    // Auto-initialize R2 event store from environment bindings
    if (env.EVENTS_BUCKET) {
      // SNAPSHOTS_BUCKET is optional (only kept for legacy R2 migration paths);
      // state itself lives in DO storage, so its absence is fine.
      this.eventStore = new R2EventStore(
        env.EVENTS_BUCKET,
        env.SNAPSHOTS_BUCKET || env.EVENTS_BUCKET
      );
    }

    // Initialize projection dispatcher if projectors are registered
    this.buildProjectionDispatcher(env);

    // Auto-initialize tenant resolver with default for local development
    // Uses DEFAULT_ORG_ID from env vars or falls back to 'default-org'
    if (!this.tenantResolver) {
      const defaultOrgId = (typeof env.DEFAULT_ORG_ID === 'string' ? env.DEFAULT_ORG_ID : undefined) ?? 'default-org';
      this.tenantResolver = new HeaderTenantResolver('X-Org-Id', defaultOrgId);
    }

    // Subclasses can override this method for custom storage configuration
  }

  /**
   * (Re)build the projection dispatcher against the CURRENT event store.
   *
   * Called from the constructor path (`initializeStores`) and again from
   * `setStores()`: adapters that inject their event store after
   * construction (e.g. the NATS runtime, where no `EVENTS_BUCKET` binding
   * exists) would otherwise never get a dispatcher — registered projectors
   * silently wouldn't fire — and a dispatcher built against a
   * constructor-time store would keep using it after a later swap.
   *
   * AA-93: `waitUntil` from DurableObjectState is threaded in so the
   * fire-and-forget projection chain keeps the isolate alive long enough
   * for projector send + DLQ write + Sentry flush. The optional DLQ writer
   * comes from the `createDlqWriter` hook.
   */
  private buildProjectionDispatcher(env: AggregateObjectEnv): void {
    if (getProjectors().length === 0 || !this.eventStore) {
      this.projectionDispatcher = null;
      return;
    }
    const waitUntil = this.ctx.waitUntil
      ? this.ctx.waitUntil.bind(this.ctx)
      : undefined;
    const dlq = this.createDlqWriter(env);
    this.projectionDispatcher = new ProjectionDispatcher(
      this.ctx.storage,
      this.eventStore,
      this.aggregateType,
      () => this.aggregateId,
      { waitUntil, dlq }
    );
  }

  /**
   * Set storage instances and tenant resolver (dependency injection).
   *
   * This is the adapter-wiring hook: runtime adapters (the NATS runtime, an
   * AWS entry point, tests) call it to swap the default R2 wiring for their
   * own stores. App code on the Cloudflare path normally never calls it —
   * the constructor wires R2 from the env bindings.
   *
   * @param eventStore - Event store implementation
   * @param tenantResolver - Tenant resolver implementation
   */
  setStores(
    eventStore: IEventStore,
    tenantResolver: ITenantResolver
  ): void {
    this.eventStore = eventStore;
    this.tenantResolver = tenantResolver;
    this.buildProjectionDispatcher(this.env);
  }

  /**
   * Main entry point - receives requests forwarded from Worker via CommandRoute/QueryRoute
   *
   * ROUTING PATTERN:
   * 1. Worker receives request (e.g., POST /users/:id/CreateUser)
   * 2. @Route decorated CommandRoute/QueryRoute handles validation and forwards to DO
   * 3. DO.fetch() routes to appropriate handler method
   * 4. Commands execute IN the DO, produce events, persist state
   * 5. Queries return state for QueryRoute to process
   *
   * INTERNAL ENDPOINTS:
   * - GET /__state: Return raw aggregate state (for QueryRoute)
   * - DELETE with X-Admin-Delete: Admin cleanup operation
   *
   * @param request - HTTP request forwarded from Worker
   * @returns HTTP response with execution result
   */
  /**
   * Discard an unread request body before responding.
   *
   * The body reaches this DO as a live stream (`duplex: 'half'` from
   * createAuthenticatedRequest). Sending a response while it is still unread
   * raises workerd's uncaught "TypeError: Can't read from request stream
   * after response has been sent", which destabilizes wrangler dev's
   * ProxyWorker and kills the local worker process — the AA-193 crash class
   * (b2c-api docs/ci-evidence). Safe to call on bodiless or already-consumed
   * requests. Command execution reads the body itself; this covers every
   * OTHER response path (auth rejection, no-handler 404, thrown errors).
   */
  private async drainRequestBody(request: Request): Promise<void> {
    try {
      if (request.body && !request.bodyUsed) {
        await request.body.cancel();
      }
    } catch {
      // Draining is best-effort — never let it mask the real response.
    }
  }

  override async fetch(request: Request): Promise<Response> {
    this.logger.debug('fetch() ENTRY', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
      url: request.url,
      method: request.method,
    });

    this.setProjectorEnv();
    try {
      // Handle admin delete (bypasses normal auth)
      if (request.method === 'DELETE' && request.headers.get('X-Admin-Delete') === 'true') {
        return this.handleAdminDelete(request);
      }

      const url = new URL(request.url);

      // Extract human-readable aggregate ID from URL path early so R2 lookups
      // use the correct key even before state has been loaded.
      // Path format: /{aggregateType}/{aggregateId}/...
      const pathParts = url.pathname.split('/').filter(Boolean);
      const urlAggregateId = pathParts.length >= 2 ? pathParts[1] : undefined;
      if (urlAggregateId && this.aggregateId !== urlAggregateId) {
        this.aggregateId = urlAggregateId;
        // Persist so alarm() and future DO warm-ups can find the correct R2 prefix
        void this.ctx.storage.put('aggregateName', urlAggregateId);
      }

      // Handle projection resync (bypasses normal auth, uses admin key)
      if (url.pathname.endsWith('/ResyncProjections')) {
        return this.handleResyncProjections(request);
      }

      // Load state (only once per DO lifecycle)
      await this.ensureStateLoaded();

      const pathSegments = url.pathname.split('/').filter(Boolean);
      const lastSegment = pathSegments[pathSegments.length - 1];

      // Authorization check
      try {
        this.checkAuthorization(request);
      } catch (error) {
        this.logger.warn('Authorization failed', {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          error: error instanceof Error ? error.message : String(error),
        });
        await this.drainRequestBody(request);
        return this.handleError(error);
      }

      // Handle internal state query (for QueryRoute pattern)
      if (request.method === 'GET' && lastSegment === '__state') {
        return this.handleStateQuery();
      }

      // Look up and execute command handler
      const routeMatchResult = findCommandRouteByUrl(request.method, url.pathname);
      if (routeMatchResult) {
        const response = await this.handleCommandExecution(routeMatchResult, request);
        if (response) {
          return response;
        }
      }

      // No handler found
      this.logger.warn('No handler found', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        method: request.method,
        pathname: url.pathname,
      });
      await this.drainRequestBody(request);
      return new Response(
        JSON.stringify({
          success: false,
          error: 'NotFound',
          message: `No handler found for ${request.method} ${url.pathname}`,
        }),
        { status: 404, headers: { 'Content-Type': 'application/json' } }
      );
    } catch (error) {
      await this.drainRequestBody(request);
      return this.handleError(error);
    }
    // NOTE: Do NOT clear __CEVES_ENV__ here. dispatchNewEvent() is fire-and-forget:
    // its async chain (loadCursor → sendEvent) may still be running after this
    // handler returns. Clearing the env would cause "SpacetimeDB not configured"
    // errors. All DOs share the same worker env, so leaving it set is safe.
  }

  /**
   * Handle admin delete request
   *
   * Validates dual authentication (API key + JWT with allowed domain)
   * and clears all DO storage.
   */
  private async handleAdminDelete(request: Request): Promise<Response> {
    this.logger.info('Admin delete request received', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
    });

    const adminApiKey = request.headers.get('X-Admin-API-Key');
    const authHeader = request.headers.get('Authorization');

    // Check admin API key
    if (!adminApiKey || adminApiKey !== this.env.ADMIN_API_KEY) {
      return new Response(
        JSON.stringify({ success: false, error: 'Unauthorized', message: 'Invalid admin API key' }),
        { status: 401, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Check Authorization header
    if (!authHeader) {
      return new Response(
        JSON.stringify({ success: false, error: 'Unauthorized', message: 'Missing Authorization header' }),
        { status: 401, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Verify JWT with allowed email domain
    try {
      const { decodeJwt } = await import('jose');
      const token = authHeader.replace(/^Bearer\s+/i, '');
      const payload = decodeJwt(token);

      const email = typeof payload.email === 'string' ? payload.email : undefined;
      const allowedDomain = this.env.ALLOWED_EMAIL_DOMAIN ?? 'example.com';

      if (!email || !email.endsWith(`@${allowedDomain}`)) {
        return new Response(
          JSON.stringify({
            success: false,
            error: 'Forbidden',
            message: `Email domain must be @${allowedDomain}`,
          }),
          { status: 403, headers: { 'Content-Type': 'application/json' } }
        );
      }
    } catch {
      return new Response(
        JSON.stringify({ success: false, error: 'Unauthorized', message: 'Invalid JWT token' }),
        { status: 401, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Clear DO storage
    this.logger.info('Clearing DO storage (admin delete)', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
    });
    await this.ctx.storage.deleteAll();
    this.state = null;
    this.stateLoaded = false;

    return new Response(
      JSON.stringify({ success: true, message: 'Durable Object storage cleared' }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Handle projection resync request (POST /ResyncProjections)
   *
   * Resets all projector cursors to 0 and schedules an alarm for full replay.
   * Requires the X-Admin-API-Key header.
   */
  private async handleResyncProjections(request: Request): Promise<Response> {
    this.logger.info('Projection resync request received', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
    });

    const adminApiKey = request.headers.get('X-Admin-API-Key');

    if (!adminApiKey || adminApiKey !== this.env.ADMIN_API_KEY) {
      return new Response(
        JSON.stringify({ success: false, error: 'Unauthorized', message: 'Invalid admin API key' }),
        { status: 401, headers: { 'Content-Type': 'application/json' } }
      );
    }

    if (!this.projectionDispatcher) {
      return new Response(
        JSON.stringify({ success: false, error: 'BadRequest', message: 'No projectors configured' }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }

    await this.projectionDispatcher.resetAndScheduleCatchUp();

    this.logger.info('Projection resync scheduled', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
    });

    return new Response(
      JSON.stringify({
        success: true,
        message: 'Projection resync scheduled',
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Handle internal state query (GET /__state)
   *
   * Returns raw aggregate state for QueryRoute pattern.
   *
   * If `this.state` is null the aggregate has never received an event, i.e.
   * it doesn't exist. Return 404 (via AggregateNotFoundError's chanfana
   * shape) instead of stringifying `null` — that previously crashed query
   * routes with `TypeError: Cannot read properties of null` when they
   * accessed `state.someField`. QueryRoute's `if (!stateResponse.ok) return
   * stateResponse;` propagates this 404 to the client cleanly.
   *
   * ADR-009's "non-null state" contract is about *event handlers during
   * command processing* (first event sees an empty state); it's not a
   * promise that querying a never-created aggregate fabricates a state.
   */
  private handleStateQuery(): Response {
    if (this.state === null) {
      this.logger.debug('Aggregate not found on state query', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
      });
      const error = new AggregateNotFoundError(this.aggregateType, this.aggregateId);
      return new Response(
        JSON.stringify({
          success: false,
          errors: [{ code: error.httpStatusCode, message: error.message }],
        }),
        { status: error.httpStatusCode, headers: { 'Content-Type': 'application/json' } }
      );
    }

    this.logger.debug('Returning raw state', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
    });
    return new Response(JSON.stringify(this.state), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  /**
   * Handle command execution via registered CommandRoute
   *
   * @returns Response if this is a command route, null if should fall through
   */
  private async handleCommandExecution(
    routeMatch: RouteMatchResult,
    request: Request
  ): Promise<Response | null> {
    const { RouteClass, params: pathParams } = routeMatch;

    // Create instance and check if this is a CommandRoute
    const RouteConstructor = RouteClass as unknown as new () => CommandHandlerInstance;
    const handlerInstance: CommandHandlerInstance = new RouteConstructor();

    // Skip if not a command route (let queries fall through)
    if (!handlerInstance.aggregateType || !handlerInstance.executeCommand) {
      return null;
    }

    const HandlerClass = RouteClass as unknown as CommandHandlerClass;
    this.logger.debug('Found command handler', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
      handler: HandlerClass.name,
      pathParams,
    });

    try {
      // Build command from request body (handle empty body for DELETE).
      //
      // Read the body BEFORE any early-return below. The body reaches this DO
      // as a live stream (`duplex: 'half'` from createAuthenticatedRequest);
      // responding while it is still unread raises workerd's uncaught
      // "TypeError: Can't read from request stream after response has been
      // sent", which destabilizes wrangler dev's ProxyWorker and kills the
      // local worker process (the AA-193 crash class; PR #285 fixed the
      // unifiedAuth instance). CI run 30532156969 captured the AA-92
      // idempotent no-op below firing 54ms before exactly that death.
      let requestBody: Record<string, unknown> = {};
      try {
        const text = await request.text();
        if (text) {
          requestBody = JSON.parse(text) as Record<string, unknown>;
        }
      } catch (bodyError) {
        this.logger.error('Failed to parse request body as JSON', {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          handler: HandlerClass.name,
          error: bodyError instanceof Error ? bodyError.message : String(bodyError),
        });
        return new Response(
          JSON.stringify({
            success: false,
            error: 'InvalidRequestBody',
            message: 'Request body must be valid JSON',
          }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Check create/update semantics
      const isCreateCommand = HandlerClass.isCreateCommand ?? false;

      if (isCreateCommand && this.state !== null) {
        // AA-92: An idempotent duplicate create is the documented contract,
        // not an exception. Re-creating an aggregate that already exists is a
        // no-op — return 200 with `event: null` (the same shape as the
        // NO_EVENT path) instead of throwing AggregateAlreadyExistsError (409).
        // The old 409 was caught + error-logged by every idempotent caller
        // (shadow-sync, projectors, post-create flows), flooding CF Workers
        // logs and Sentry with ~19k expected-outcome "errors" per 7 days that
        // buried real bugs. `info` level keeps an audit trail without the noise.
        this.logger.info('Create command on existing aggregate — idempotent no-op', {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          handler: HandlerClass.name,
        });
        return new Response(
          JSON.stringify({
            success: true,
            aggregateId: this.aggregateId,
            version: this.state?.version ?? 0,
            event: null,
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } }
        );
      }

      if (!isCreateCommand && this.state === null) {
        this.logger.warn('Update command on non-existent aggregate', {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          handler: HandlerClass.name,
        });
        throw new AggregateNotFoundError(this.aggregateType, this.aggregateId);
      }

      this.logger.debug('Executing command', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        handler: HandlerClass.name,
        isCreateCommand,
      });

      // Caller identity for the handler, derived from the trusted auth
      // headers the worker injected when forwarding (CommandRoute.
      // buildAuthHeaders / AggregateRouter.injectAuthHeaders). Client-supplied
      // values for these headers never reach the DO — the worker's ingress
      // guard rejects spoofed internal headers and overwrites them from its
      // own auth context.
      const auth: AuthContext = {
        orgId: request.headers.get('X-Org-Id') ?? undefined,
        userId: request.headers.get('X-User-Id') ?? undefined,
        email: request.headers.get('X-User-Email') ?? undefined,
        isAdmin: request.headers.get('X-Is-Admin') === 'true' || undefined,
        isSuper: request.headers.get('X-Super-Access') === 'true' || undefined,
      };

      // Execute command - produces domain event (or NO_EVENT for idempotent skip)
      // CreateCommandRoute expects (command, env, auth?) - state is always null
      // CommandRoute expects (command, state, env, auth?)
      const domainEvent = isCreateCommand
        ? await (handlerInstance.executeCommand as (cmd: unknown, env: unknown, auth?: AuthContext) => Promise<DomainEvent | typeof NO_EVENT>)(
            requestBody,
            this.env,
            auth
          )
        : await handlerInstance.executeCommand(requestBody, this.state, this.env, auth);

      // Check for NO_EVENT sentinel - skip apply/persist, return success
      if (domainEvent === NO_EVENT) {
        this.logger.info('Command returned NO_EVENT, skipping persist', {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          handler: HandlerClass.name,
        });
        return new Response(
          JSON.stringify({
            success: true,
            aggregateId: this.aggregateId,
            version: this.state?.version ?? 0,
            event: null,
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Apply and persist (AA-92: genuine create → 201, update → 200)
      return this.applyAndPersistEvent(domainEvent, request, HandlerClass, handlerInstance, isCreateCommand);
    } catch (error) {
      this.logger.error('Command execution error', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        handler: HandlerClass.name,
        error: error instanceof Error ? error.message : String(error),
      });
      return this.handleError(error);
    }
  }

  /**
   * Apply domain event to state and persist
   *
   * Shared logic for event application, state persistence, R2 storage, and side effects.
   *
   * @param HandlerClass - The route class (used for `name` and the optional
   *                       static `eventSchema` runtime validation hook)
   */
  private async applyAndPersistEvent(
    domainEvent: DomainEvent,
    request: Request,
    HandlerClass?: CommandHandlerClass,
    handlerInstance?: CommandHandlerInstance,
    isCreateCommand = false,
  ): Promise<Response> {
    const handlerName = HandlerClass?.name;
    const version = (this.state?.version ?? 0) + 1;
    const requestOrgId = await this.tenantResolver.resolveOrgId(request);
    const eventOrgId = this.state?.orgId ?? requestOrgId;

    const storedEvent: StoredEvent = {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
      version,
      type: domainEvent.type,
      timestamp: new Date().toISOString(),
      orgId: eventOrgId,
      event: domainEvent,
    };

    this.logger.info('Applying event', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
      eventType: storedEvent.type,
      handler: handlerName,
    });

    // Apply event to in-memory state
    this.applyEvent(storedEvent);

    // Update aggregateId to use human-readable ID from state
    if (this.state?.id && this.aggregateId !== this.state.id) {
      const oldId = this.aggregateId;
      this.aggregateId = this.state.id;
      storedEvent.aggregateId = this.state.id;  // patch storedEvent before persist — fixes AA-58 Bug 1
      this.logger.info('Updated aggregateId to human-readable ID', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        oldId,
      });
    }

    // Persist state to DO storage
    if (this.state) {
      await this.ctx.storage.put('state', this.state);
      this.logger.info('Persisted state to DO storage', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        version: this.state.version,
      });
    }

    // Persist event to R2 (fire-and-forget)
    this.persistEvent(storedEvent).catch((err) => {
      this.logger.error('Failed to persist event to R2', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        error: err instanceof Error ? err.message : String(err),
      });
    });

    // Dispatch to registered projectors (fire-and-forget)
    if (this.projectionDispatcher) {
      const projectedEvent: ProjectedEvent = {
        aggregateType: storedEvent.aggregateType,
        aggregateId: storedEvent.aggregateId,
        version: storedEvent.version,
        type: storedEvent.type,
        timestamp: Date.parse(storedEvent.timestamp),
        data: storedEvent.event,
        orgId: storedEvent.orgId || '',
      };
      this.projectionDispatcher.dispatchNewEvent(projectedEvent);
    }

    // Execute side effects
    try {
      await executeSideEffects(this.aggregateType, storedEvent, this.env);
      this.logger.info('Side effects completed', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
      });
    } catch (sideEffectError) {
      this.logger.error('Side effects failed', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        eventType: storedEvent.type,
        error: sideEffectError instanceof Error ? sideEffectError.message : String(sideEffectError),
      });
      const errorMessage = sideEffectError instanceof Error ? sideEffectError.message : String(sideEffectError);
      return new Response(
        JSON.stringify({
          success: false,
          error: 'SideEffectError',
          message: `Event applied but side effects failed: ${errorMessage}`,
          aggregateId: this.aggregateId,
          version,
        }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Strip the `type` discriminator out of the data payload — the outer
    // `event.type` is the only carrier of the discriminator, so duplicating
    // it inside `event.data` was redundant. The wire shape becomes
    //   event: { type, data: { ...event-fields-minus-type } }
    const { type: eventType, ...eventDataWithoutType } = domainEvent as DomainEvent &
      Record<string, unknown>;

    // Optional runtime correctness check: if the route declared a static
    // `eventSchema`, parse the just-emitted event-data through it. A failure
    // here means the handler returned a malformed event — surface loudly
    // rather than ship bad data to the client. The schema describes the
    // stripped data payload (no `type` field).
    this.assertEventMatchesSchema(eventDataWithoutType, eventType, HandlerClass, handlerName);

    const standardResponse = {
      success: true as const,
      aggregateId: this.aggregateId,
      version,
      event: { type: eventType, data: eventDataWithoutType },
    };

    const responseBody = await this.customizeResponseBody(
      standardResponse,
      domainEvent,
      handlerInstance,
      handlerName,
    );

    // AA-92: a genuine create (a create event was emitted) is RESTfully a
    // 201 Created; update commands stay 200. The idempotent-duplicate-create
    // no-op is handled earlier and returns 200 with `event: null`.
    return new Response(
      JSON.stringify(responseBody),
      { status: isCreateCommand ? 201 : 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  /**
   * Apply the optional `customizeResponse` route hook, letting a route enrich
   * the response body with server-computed fields (e.g. a generated UUID).
   * Defaults to the standard response. A hook failure is logged and falls back
   * to the standard response — the event is already persisted, so a
   * presentation-layer hook error must never surface as a 500.
   */
  private async customizeResponseBody(
    standardResponse: {
      success: true;
      aggregateId: string;
      version: number;
      event: { type: string; data: Record<string, unknown> } | null;
    },
    domainEvent: DomainEvent,
    handlerInstance?: CommandHandlerInstance,
    handlerName?: string,
  ): Promise<Record<string, unknown>> {
    if (!handlerInstance?.customizeResponse) {
      return standardResponse;
    }
    try {
      return await handlerInstance.customizeResponse(standardResponse, domainEvent, this.state);
    } catch (hookError) {
      this.logger.error('customizeResponse hook failed', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        handler: handlerName,
        error: hookError instanceof Error ? hookError.message : String(hookError),
      });
      return standardResponse;
    }
  }

  /**
   * Runtime correctness check: when the route class declares a static
   * `eventSchema`, parse the just-emitted event-data payload through it.
   * A parse failure means the handler returned malformed data — throw
   * clearly so the bug is caught at the framework boundary rather than
   * shipped to the client.
   *
   * `eventSchema` describes the **data-only payload** (i.e. the fields under
   * `event.data` on the wire, with the outer `type` discriminator stripped).
   *
   * No-op when `HandlerClass.eventSchema` is undefined (preserves backward
   * compatibility for routes that haven't migrated to schema-based events).
   */
  private assertEventMatchesSchema(
    eventData: Record<string, unknown>,
    eventType: string,
    HandlerClass?: CommandHandlerClass,
    handlerName?: string,
  ): void {
    const schema = HandlerClass?.eventSchema;
    if (!schema) return;

    const parseResult = schema.safeParse(eventData);
    if (parseResult.success) return;

    const reason = describeParseError(parseResult.error);
    this.logger.error('Emitted event failed eventSchema parse', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
      handler: handlerName,
      eventType,
      error: reason,
    });
    throw new Error(
      `Handler ${handlerName ?? 'unknown'} emitted an event whose data ` +
        `failed its declared eventSchema (event type: ${eventType}). ` +
        `This is a handler bug — it returned data inconsistent with its ` +
        `static eventSchema. Underlying error: ${reason}`
    );
  }

  protected applyEvent(event: StoredEvent): void {
    // Use applyEventToState utility - provides empty state for first event (ADR-009)
    this.state = applyEventToState<TState>(
      this.aggregateType,
      this.state,
      event,
      this.getStateClass()
    );
    if (this.state && event.version != null) {
      this.state.version = event.version;
    }
  }

  /**
   * Apply one bounded batch of events to in-memory state, then checkpoint the
   * resulting state (including its `version`) to DO storage.
   *
   * Each event apply is wrapped with per-event diagnostics
   * (`applyEventOrWrapDiagnostics`) that record position (eventIndex /
   * totalEvents) and event identity (type / version) before rethrowing —
   * turning Cloudflare's opaque `internal error; reference = ...` into a
   * one-line, greppable `StateRestorationError` (see AA-108 follow-up).
   * `CevesError` subclasses pass through unchanged so downstream `instanceof`
   * checks remain valid.
   *
   * AA-117 / batched replay: `restoreFromR2Batched` loads events one bounded
   * page at a time and calls this once per page. Persisting `state` after the
   * batch advances the checkpointed `version` by at most one batch, so a
   * mid-replay isolate kill (CPU / wall-clock — uncatchable from inside the
   * DO) resumes from the last persisted version on the next cold start instead
   * of re-applying from zero.
   */
  private async replayBatch(events: StoredEvent[]): Promise<void> {
    for (let i = 0; i < events.length; i++) {
      const event = events[i];
      if (event === undefined) continue;
      this.applyEventOrWrapDiagnostics(event, i, events.length);
    }

    if (this.state) {
      await this.ctx.storage.put('state', this.state);
      this.logger.info('Replay batch checkpoint persisted', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        version: this.state.version,
        eventsInBatch: events.length,
      });
    }
  }

  /**
   * Apply a single event during replay, converting any non-CevesError
   * failure into a `StateRestorationError` carrying the replay position
   * (eventIndex / totalEvents) and event identity (type / version).
   *
   * Extracted from `replayEventsWithDiagnostics` so the loop itself stays
   * under the sonarjs cognitive-complexity budget — the
   * one-event-at-a-time error wrapping is its own concern.
   */
  private applyEventOrWrapDiagnostics(event: StoredEvent, eventIndex: number, totalEvents: number): void {
    try {
      this.applyEvent(event);
    } catch (cause) {
      this.logger.error('State restoration failed during event replay', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        eventType: event.type,
        eventVersion: event.version,
        eventIndex,
        totalEvents,
        cause: cause instanceof Error ? cause.message : String(cause),
        causeName: cause instanceof Error ? cause.name : 'Unknown',
      });
      if (cause instanceof CevesError) {
        throw cause;
      }
      throw new StateRestorationError(
        {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          eventType: event.type,
          eventVersion: event.version,
          eventIndex,
          totalEvents,
        },
        cause,
      );
    }
  }

  /**
   * Persist event to R2/S3
   *
   * @param event - Event to persist
   */
  protected async persistEvent(event: StoredEvent): Promise<void> {
    if (!this.eventStore) {
      throw new Error('Event store not initialized');
    }

    await this.eventStore.save(event);
  }

  /**
   * Ensure state is loaded from storage (DO storage or R2 fallback)
   *
   * This method is called once per DO lifecycle, on the first request.
   * Subsequent requests use the in-memory state directly.
   *
   * Flow:
   * 1. Check if state already loaded (exit early if true)
   * 2. Mark state as loaded (guard against re-entry)
   * 3. Healthy DO storage state → done
   * 4. Otherwise → restore from R2 events (pre-migration aggregates)
   *
   * AA-117 — resumable replay:
   * If `r2_replay_in_progress` is set in DO storage, a previous replay
   * was killed mid-way by isolate termination (CPU / wall-clock) and the
   * `state.version` in storage represents how far it got. We re-enter the
   * replay path and skip events at or below `state.version` so each cold
   * start makes forward progress and eventually completes the replay
   * even when the full event count blows the CPU budget in a single go.
   */
  protected async ensureStateLoaded(): Promise<void> {
    if (this.stateLoaded) {
      return;
    }
    this.stateLoaded = true;

    const replayInProgress = !!(await this.ctx.storage.get<boolean>('r2_replay_in_progress'));

    // State already loaded from DO storage AND no prior crashed replay
    // → trust DO storage as authoritative.
    if (this.state && this.state.version > 0 && !replayInProgress) {
      this.logger.debug('State already loaded from DO storage, skipping R2 restore', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
      });
      return;
    }

    if (!this.eventStore) {
      this.logger.error('No event store available, cannot restore state', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
      });
      return;
    }

    // Either a fresh aggregate (no DO storage state) or recovering from a
    // crashed replay. Page through R2 in bounded batches rather than loading
    // the whole stream — see `restoreFromR2Batched`.
    await this.restoreFromR2Batched(replayInProgress);
  }

  /**
   * Restore aggregate state by paging R2 events in bounded batches of
   * `REPLAY_CHECKPOINT_INTERVAL`, applying each page and checkpointing the
   * resulting state (incl. version) to DO storage before loading the next.
   *
   * Why batched (AA-117 follow-up): the previous implementation called
   * `eventStore.loadAll()`, pulling the ENTIRE event stream into memory before
   * applying anything. For a large aggregate that OOM-/wall-clock-killed the
   * isolate during the load — before any state was checkpointed — so every
   * subsequent cold start re-loaded the whole stream and died identically, a
   * non-recoverable loop. (This is the failure behind the MatterDevice
   * `Network connection lost` restores: a chatty device piled up thousands of
   * large shadow events and could never finish restoring.)
   *
   * Bounded paging caps peak memory + CPU per cold start to one batch, and the
   * per-batch checkpoint guarantees forward progress: each cold start consumes
   * at least one batch and the persisted `version` lets the next resume where
   * this one left off, so even a stream too large to restore in a single
   * isolate eventually completes across cold starts.
   */
  private async restoreFromR2Batched(replayInProgress: boolean): Promise<void> {
    const eventStore = this.eventStore;
    if (!eventStore) {
      return; // already guarded by ensureStateLoaded; keeps the type narrow
    }

    this.logger.info('Restoring state from R2 events (batched)', {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
      startVersion: this.state?.version ?? 0,
      resuming: replayInProgress,
      batchSize: REPLAY_CHECKPOINT_INTERVAL,
    });

    // `flagPersisted` tracks whether `r2_replay_in_progress` is set in DO
    // storage right now — from a prior crashed replay (resume) or because we
    // set it below before the first batch.
    let flagPersisted = replayInProgress;
    let eventsApplied = 0;

    let batch = await eventStore.load(
      this.aggregateType,
      this.aggregateId,
      this.state?.version ?? 0,
      REPLAY_CHECKPOINT_INTERVAL,
    );

    while (batch.length > 0) {
      // Mark replay in-progress before the FIRST apply. If the isolate is
      // killed mid-batch the flag stays set and the next cold start resumes
      // from the last checkpointed `state.version`.
      if (!flagPersisted) {
        await this.ctx.storage.put('r2_replay_in_progress', true);
        flagPersisted = true;
      }

      await this.replayBatch(batch);
      eventsApplied += batch.length;

      // A short page means we've drained the stream.
      if (batch.length < REPLAY_CHECKPOINT_INTERVAL) {
        break;
      }

      batch = await eventStore.load(
        this.aggregateType,
        this.aggregateId,
        this.state?.version ?? 0,
        REPLAY_CHECKPOINT_INTERVAL,
      );
    }

    // Replay finished (or there was nothing to do): clear the in-progress flag.
    if (flagPersisted) {
      await this.ctx.storage.delete('r2_replay_in_progress');
    }

    if (eventsApplied > 0) {
      this.logger.info('Restored state from R2 events (batched)', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        version: this.state?.version ?? 0,
        eventsApplied,
        resumed: replayInProgress,
      });
    } else {
      this.logger.warn('No events found in R2, nothing to restore', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
      });
    }
  }

  /**
   * Handle errors during command/query execution
   *
   * Distinguishes between:
   * - CevesError instances with specific HTTP status codes (400, 401, 403, 404, 409, etc.)
   * - Unexpected errors (500 Internal Server Error)
   *
   * All error responses use chanfana's standard format:
   * { success: false, errors: [{ code: number, message: string }] }
   *
   * @param error - Error that occurred
   * @returns Error response in chanfana format
   */


  protected handleError(error: unknown): Response {
    const message = error instanceof Error ? error.message : 'Unknown error';
    const stack = error instanceof Error ? error.stack : undefined;

    // Log the actual message (was: literal "Error", which is opaque in
    // Cloudflare Workers Observability — `$metadata.message` shows the first
    // arg, and a constant string string is useless when grepping CI output
    // or alerting on a specific 3rd-party failure).
    this.logger.error(message, {
      aggregateType: this.aggregateType,
      aggregateId: this.aggregateId,
      errorName: error instanceof Error ? error.name : 'Unknown',
      stack,
    });

    // Non-domain (unexpected) errors get captured to whatever sink the
    // host wired (typically Sentry) — keeping diagnostic trail for the
    // 500 path even though serializeErrorToResponse below would let the
    // caller rehydrate the message. Without this capture, the message
    // is structured but not telemetered.
    if (!(error instanceof CevesError)) {
      captureException(error);
    }

    // AA-119: serializeErrorToResponse emits the standard chanfana wire
    // shape (success/errors) AND embeds the typed payload under
    // `__ceves` with the `X-Ceves-Error: 1` header so callers using
    // stubFetchWithTypedErrors() can rebuild the original error class.
    // Both CevesError and bare-Error paths go through one helper now so
    // the wire format never drifts between branches.
    return serializeErrorToResponse(error);
  }

  /**
   * Authorization hook - override to implement custom authorization logic
   *
   * Called automatically before executing commands and queries.
   * Default implementation allows all requests (no-op).
   *
   * Override this method in aggregate subclasses to:
   * - Validate authentication credentials (API keys, JWTs, etc.)
   * - Check resource ownership (e.g., user owns this aggregate)
   * - Enforce role-based access control (RBAC)
   * - Implement multi-tenancy authorization
   * - Allow public access to specific endpoints
   *
   * **Throwing errors:**
   * - Throw `UnauthorizedError` (401) when authentication is missing/invalid
   * - Throw `ForbiddenError` (403) when authenticated but lacks permission
   * - Throw other `CevesError` subclasses for domain-specific authorization failures
   *
   * **Access to request and state:**
   * - `request.headers` - Extract auth tokens, user IDs, org IDs, etc.
   * - `request.url` - Check pathname for public endpoints
   * - `this.state` - Validate ownership/permissions against aggregate state
   *
   * @param request - HTTP request with headers for authorization
   * @throws {UnauthorizedError} If authentication is missing or invalid
   * @throws {ForbiddenError} If authenticated but lacks permission
   * @throws {CevesError} For other authorization failures
   *
   * @example
   * ```typescript
   * // B2C authorization: Check user owns the resource
   * protected override checkAuthorization(request: Request): void {
   *   const userId = request.headers.get('X-User-Id');
   *   if (!userId) {
   *     throw new UnauthorizedError('Missing user ID');
   *   }
   *   if (this.state?.ownerId && this.state.ownerId !== userId) {
   *     throw new ForbiddenError('User does not own this resource');
   *   }
   * }
   * ```
   *
   * @example
   * ```typescript
   * // B2B authorization: Check organization match
   * protected override checkAuthorization(request: Request): void {
   *   const orgId = request.headers.get('X-Org-Id');
   *   if (!orgId) {
   *     throw new UnauthorizedError('Missing organization ID');
   *   }
   *   if (this.state?.orgId && this.state.orgId !== orgId) {
   *     throw new ForbiddenError('Organization mismatch');
   *   }
   * }
   * ```
   *
   * @example
   * ```typescript
   * // Allow public access to specific endpoints
   * protected override checkAuthorization(request: Request): void {
   *   const url = new URL(request.url);
   *
   *   // Public endpoints - no auth required
   *   if (url.pathname.endsWith('/public-info')) {
   *     return;
   *   }
   *
   *   // All other endpoints require authentication
   *   const apiKey = request.headers.get('X-API-Key');
   *   if (!apiKey || apiKey !== this.env.API_SECRET) {
   *     throw new UnauthorizedError('Invalid API key');
   *   }
   * }
   * ```
   */
  protected checkAuthorization(_request: Request): void {
    // Default: allow all requests
    // Override this method in aggregates to implement authorization logic
  }

  /**
   * Hook for providing a projection DLQ writer (AA-93).
   *
   * Default returns `undefined` — projection failures are surfaced only via
   * logger.warn + alarm-driven retry. Override in app code (or in a base
   * aggregate subclass) to bind a real DLQ store, e.g. an R2-backed writer:
   *
   * ```ts
   * protected override createDlqWriter(env: Bindings): DlqWriter | undefined {
   *   return env.PROJECTION_DLQ_BUCKET
   *     ? new R2DlqWriter(env.PROJECTION_DLQ_BUCKET, env.ENVIRONMENT)
   *     : undefined;
   * }
   * ```
   *
   * Returning `undefined` (the default) preserves backward compatibility for
   * apps that don't have a DLQ binding configured.
   */
  protected createDlqWriter(_env: AggregateObjectEnv): DlqWriter | undefined {
    return undefined;
  }

  /**
   * Expose env bindings to projectors via globalThis.__CEVES_ENV__.
   * Projectors run outside the Hono context so they can't access c.env.
   */
  private setProjectorEnv(): void {
    const g = globalThis as Record<string, unknown>;
    g['__CEVES_ENV__'] = this.env;
  }

  /**
   * Alarm handler - called by the Cloudflare runtime when a scheduled alarm fires.
   *
   * Used by the projection system for retry/catch-up after delivery failures.
   * Subclasses that override this method should call super.alarm() to preserve
   * projection functionality.
   */
  override async alarm(): Promise<void> {
    try {
      if (!this.projectionDispatcher) return;
      await this.ensureStateLoaded();
      const version = this.state?.version;
      if (version == null) return;
      this.logger.info('Alarm catchUp starting', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        version,
      });
      // Set the shared projector env and DO NOT clear it afterwards — same rule
      // as fetch() (see the note at the end of that handler). __CEVES_ENV__ is a
      // single global shared by EVERY DO in the isolate; clearing it here races
      // with other DOs' in-flight projection chains (and with the concurrent
      // alarms a resync schedules), surfacing as spurious "<projector> not
      // configured" failures that land in the DLQ. The env is identical for all
      // DOs, so leaving it set is safe.
      this.setProjectorEnv();
      await this.projectionDispatcher.catchUp(version);
    } catch (error) {
      this.logger.error('Alarm handler failed', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      });
      throw error; // Re-throw so Cloudflare can retry the alarm
    }
  }
}
