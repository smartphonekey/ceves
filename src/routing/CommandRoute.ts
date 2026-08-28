/**
 * CommandRoute - Base class for command routes that forward to Durable Objects
 * 
 * Handles:
 * - Extracting aggregate ID from URL path
 * - Getting DO stub from environment
 * - Injecting auth headers from Hono context
 * - Forwarding request to Durable Object
 */

import { OpenAPIRoute } from 'chanfana';
import type { Context } from 'hono';
import type { DurableObjectNamespace, DurableObjectStub } from '@cloudflare/workers-types';
import type { ZodType } from 'zod';
import { createLogger } from '../logger';
import { NO_EVENT } from '../events/DomainEvent';
import { safeDOFetch } from './safeDOFetch';
import { handleCevesError } from './cevesErrorResponse';

const logger = createLogger({ component: 'CommandRoute' });

/** Extract the `type` field from an event-data payload, if present. */
function pickEventType(eventData: unknown): string | undefined {
  if (typeof eventData !== 'object' || eventData === null) return undefined;
  const candidate = (eventData as { type?: unknown }).type;
  return typeof candidate === 'string' ? candidate : undefined;
}

function missingAggregateIdResponse(): Response {
  return new Response(
    JSON.stringify({
      success: false,
      error: 'MissingAggregateId',
      message: 'Aggregate ID not found in URL path',
    }),
    { status: 400, headers: { 'Content-Type': 'application/json' } },
  );
}

/**
 * Auth context stored in Hono context via middleware
 */
interface AuthContext {
  authType: 'api-key' | 'jwt';
  orgId?: string;
  isSuper?: boolean;
  userEmail?: string;
  userId?: string;
}

/**
 * Command body type - commands are just the body fields from the request
 *
 * Using Record<string, unknown> because:
 * - Commands are always objects (request body)
 * - aggregateId comes from URL params, not body
 * - State contains entity identifiers (lockId, uuid, etc.)
 *
 * Define your command type from Zod:
 * ```typescript
 * const MyBodySchema = z.object({ ... });
 * type MyBody = z.infer<typeof MyBodySchema>;  // Use this as TCommand
 * ```
 */
export type CommandBody = Record<string, unknown>;

/**
 * Base event interface - all events must have a type field
 *
 * Note: This is a plain TypeScript interface, not a Zod schema.
 * Events are produced by executeCommand(), not from external input,
 * so runtime validation is not needed at this level.
 */
export interface BaseEvent {
  type: string;
}


/**
 * Abstract base class for UPDATE command routes with full type safety
 *
 * Use this for commands that modify EXISTING aggregates. The state parameter
 * is guaranteed to be non-null because the framework validates the aggregate
 * exists before calling executeCommand().
 *
 * For CREATE commands (new aggregates), use CreateCommandRoute instead.
 *
 * Commands are forwarded to Durable Objects where they execute with:
 * - Ordered execution (single-threaded in DO)
 * - Transactional state updates
 * - No race conditions
 *
 * @template TCommand - Command type (body fields, derived from Zod schema)
 * @template TState - Aggregate state type (NEVER null for update commands)
 * @template TEvent - Domain event type (must extend BaseEvent)
 *
 * Usage:
 * ```typescript
 * const AddKeyBodySchema = z.object({
 *   keyUuid: z.string().uuid(),
 *   keyIndexNumber: z.number().int().optional(),
 * });
 * type AddKeyBody = z.infer<typeof AddKeyBodySchema>;
 *
 * @Route({ method: 'POST', path: '/locks/:id/AddKey' })
 * export class AddKeyRoute extends CommandRoute<AddKeyBody, LockState, KeyAddedEventData> {
 *   aggregateType = 'LockAggregate';
 *   schema = { request: { body: { content: { 'application/json': { schema: AddKeyBodySchema } } } } };
 *
 *   async executeCommand(command: AddKeyBody, state: LockState): Promise<KeyAddedEventData> {
 *     // state is NEVER null - framework guarantees aggregate exists
 *     return { type: EventTypes.KEY_ADDED, lockId: state.lockId, keyUuid: command.keyUuid };
 *   }
 * }
 * ```
 */
export abstract class CommandRoute<
  TCommand extends CommandBody = CommandBody,
  TState = unknown,
  TEvent extends BaseEvent | typeof NO_EVENT = BaseEvent,
  TEnv = unknown
> extends OpenAPIRoute {
  /**
   * Indicates this is an update command (aggregate must exist)
   * Framework uses this to validate semantics before execution
   */
  static readonly isCreateCommand: boolean = false;

  /**
   * Optional Zod schema describing the shape of the event-data payload returned
   * by `executeCommand()`.
   *
   * When set, the framework will:
   * 1. Surface the schema in the OpenAPI response under
   *    `event.data` (so clients see the typed event shape).
   * 2. Run a runtime `safeParse` on the value returned by `executeCommand()`
   *    just before constructing the success response. A parse failure means
   *    the handler returned a malformed event — the framework throws so the
   *    bug is caught loud, not silently shipped to the client.
   *
   * When omitted (the default), the framework falls back to a generic
   * `z.record(z.unknown())` shape for `event.data` and skips the runtime parse.
   * This keeps existing routes that haven't been migrated working unchanged.
   *
   * @example
   * ```typescript
   * const KeyAddedEventSchema = z.object({
   *   type: z.literal('KeyAdded'),
   *   keyUuid: z.string(),
   * });
   *
   * @Route({ method: 'POST', path: '/locks/:id/AddKey' })
   * export class AddKeyRoute extends CommandRoute<...> {
   *   static readonly eventSchema = KeyAddedEventSchema;
   *   // ...
   * }
   * ```
   */
  static readonly eventSchema?: ZodType;

  /**
   * Aggregate type - must match DO binding name
   * Example: 'LockAggregate' maps to LOCK binding
   */
  abstract aggregateType: string;

  /**
   * Build command object from request body
   *
   * Commands are just the body fields - aggregateId comes from URL params
   * and is handled by the base class for DO routing.
   *
   * Uses Chanfana's `getValidatedData()` for full Zod validation including:
   * - Type validation
   * - Zod refinements (.refine(), .superRefine())
   * - Custom validators
   *
   * Override this method if you need custom command building logic.
   *
   * @param _c - Hono context (unused, kept for override compatibility)
   * @returns Command object (body fields only)
   */
  async buildCommand(_c: Context): Promise<TCommand> {
    // Use Chanfana's validation which applies all Zod refinements
    const data = await this.getValidatedData<typeof this.schema>();
    return (data.body ?? {}) as unknown as TCommand;
  }

  /**
   * Execute command business logic and produce domain event
   *
   * This method contains the command's business logic. It validates business rules
   * and produces a domain event describing what happened.
   *
   * For UPDATE commands (this class), state is NEVER null - the framework
   * guarantees the aggregate exists before calling this method.
   *
   * @param command - Command object (from buildCommand)
   * @param state - Current aggregate state (NEVER null for update commands)
   * @param env - Environment bindings (from Durable Object). Type it by
   *              passing your app's validated env type as the TEnv generic
   *              (e.g. `CommandRoute<Body, State, Event, Bindings>`).
   * @param auth - Caller identity, derived inside the DO from the trusted
   *               auth headers the worker forwarded (`buildAuthHeaders`).
   *               Optional so existing 3-param implementations keep working.
   * @returns Domain event to apply
   */
  abstract executeCommand(command: TCommand, state: TState, env: TEnv, auth?: AuthContext): Promise<TEvent>;

  /**
   * Optional hook called after the event is persisted and before the success
   * response is returned to the client.
   *
   * Override this to add server-computed fields (e.g., a server-generated UUID
   * or a derived projection field) to the response body. The default
   * implementation returns the standard response unchanged.
   *
   * The hook is NOT called for the `NO_EVENT` path — if your command returns
   * `NO_EVENT`, the framework returns the standard
   * `{ success, aggregateId, version, event: null }` response directly.
   *
   * Use this hook instead of overriding `handle()` when you only need to add
   * extra fields to the response. Overriding `handle()` requires reimplementing
   * body validation, auth header forwarding, and DO routing.
   *
   * @param response - The standard success response object. Includes the
   *                   emitted `event` (`{ type, data }`) under the `event` key —
   *                   spread `...response` to forward it through.
   * @param event - The domain event that was just persisted
   * @param state - The aggregate state AFTER the event was applied (post-state).
   *                For update commands this is non-null; for create commands
   *                this is the freshly-created state (also non-null).
   * @returns The response body to return to the client. Must be JSON-serializable.
   */
  protected customizeResponse(
    response: {
      success: true;
      aggregateId: string;
      version: number;
      event: { type: string; data: TEvent } | null;
    },
    _event: TEvent,
    _state: TState,
  ): Promise<Record<string, unknown>> {
    return Promise.resolve(response);
  }

  /**
   * Pre-commit hook (optional, worker-side).
   *
   * Runs AFTER request validation but BEFORE the DO subrequest. Use it for
   * pre-flight checks against external read models the DO can't see — e.g.
   * "does this temp key belong to this lock?" against D1, or partner-scope
   * authz checks that depend on D1 state.
   *
   * Throw a `CevesError` (or any error with `httpStatusCode` + `buildResponse`)
   * to short-circuit with a typed wire response. Throw a plain `Error` to
   * abort with 500 via Hono's error handler.
   *
   * NOT for business-rule validation that the DO can self-check — those
   * belong in `executeCommand` so the DO remains the source of truth.
   */
  protected beforeCommit?(c: Context, command: TCommand): Promise<void>;

  /**
   * Post-commit hook (optional, worker-side).
   *
   * Runs ONLY on a successful (2xx) DO response, after the event has been
   * durably persisted to the DO + R2 event log. Use it for synchronous
   * side effects whose absence would make the event meaningless to read-
   * model consumers — typically D1 writes for tables the DO doesn't
   * maintain (e.g. `temp_keys`).
   *
   * `eventData` is the just-persisted event's data payload (the inner
   * `event.data` from the standard CommandRoute response), or `undefined`
   * if the route emitted `NO_EVENT`.
   *
   * Errors thrown here are caught, logged, and surfaced via the framework's
   * exception capturer (Sentry). They do NOT fail the response — the event
   * is already durable, so failing the response would be a lie. The right
   * remedy for a D1 drift here is a reconciliation job, not a 5xx.
   *
   * Optionally returns a record that's MERGED into the response body — use
   * for legacy field shape compat (e.g. injecting `tempKeyUuid` so older
   * clients keep working). Return `void` to leave the response unchanged.
   */
  protected afterCommit?(
    c: Context,
    eventData: unknown,
    command: TCommand,
  ): Promise<Record<string, unknown> | void>;

  /**
   * Main handler - validates request and forwards to Durable Object
   *
   * Request validation happens here (before DO forwarding) to:
   * 1. Catch invalid requests early (before DO instantiation)
   * 2. Apply Zod refinements and custom validators
   * 3. Ensure only valid data reaches the DO
   *
   * The DO is responsible for:
   * 1. Looking up the handler class from registry
   * 2. Loading/ensuring state
   * 3. Validating create/update semantics
   * 4. Executing the command via handler.executeCommand()
   * 5. Applying event and persisting state
   *
   * This ensures ordered execution and transactionality within the DO.
   */
  override async handle(c: Context): Promise<Response> {
    const aggregateId = c.req.param('id');
    if (!aggregateId) return missingAggregateIdResponse();

    const validatedData = await this.getValidatedData<typeof this.schema>();
    const command = (validatedData.body ?? {}) as TCommand;

    // Pre-commit hook (worker-side) — runs BEFORE the DO subrequest.
    // Use for D1-backed pre-flight checks the DO can't perform itself.
    const preflightError = await this.runBeforeCommit(c, command);
    if (preflightError) return preflightError;

    const stub = this.getDurableObjectStub(c, aggregateId);
    const headers = this.buildAuthHeaders(c);
    const forwardRequest = new Request(c.req.raw.url, {
      method: c.req.raw.method,
      headers,
      body: validatedData.body ? JSON.stringify(validatedData.body) : undefined,
    });

    // Wrap so a CF synthetic uncatchable error (DO reset during deploy /
    // storage timeout / "internal error; reference = ...") becomes a
    // structured 503 instead of an unhandled 500 — see safeDOFetch.ts.
    const response = await safeDOFetch(stub.fetch(forwardRequest));

    // Post-commit hook (worker-side) — runs only on 2xx responses, after
    // the event is durable. See `afterCommit` docstring for semantics.
    if (response.ok && this.afterCommit) {
      return await this.runAfterCommit(c, response, command, aggregateId);
    }

    return response;
  }

  /**
   * Run the optional `beforeCommit` pre-flight hook. Returns a typed
   * Response if the hook threw a `CevesError`, or `null` to continue.
   * Plain Errors propagate to the upstream error handler.
   */
  private async runBeforeCommit(c: Context, command: TCommand): Promise<Response | null> {
    if (!this.beforeCommit) return null;
    try {
      await this.beforeCommit(c, command);
      return null;
    } catch (err) {
      const typed = handleCevesError(err);
      if (typed) return typed;
      throw err;
    }
  }

  /**
   * Runs the `afterCommit` hook against a successful DO response.
   * Extracted from `handle` to keep cognitive complexity in check.
   */
  private async runAfterCommit(
    c: Context,
    response: Response,
    command: TCommand,
    aggregateId: string,
  ): Promise<Response> {
    const eventData = await this.extractEventData(response);

    try {
      // Re-check the hook here rather than asserting it. The caller already
      // tests `this.afterCommit` (see `handle`), but that narrowing does not
      // cross the method boundary. Checking immediately before the call keeps
      // the method bound to `this` (no unbound-method reference) and makes the
      // absent-hook case an explicit no-op.
      if (!this.afterCommit) return response;
      const extras = await this.afterCommit(c, eventData, command);
      if (extras && typeof extras === 'object' && Object.keys(extras).length > 0) {
        const original: Record<string, unknown> = await response.json();
        return new Response(JSON.stringify({ ...original, ...extras }), {
          status: response.status,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      return response;
    } catch (err) {
      // The event is already in the DO + R2; failing the response would lie
      // to the client. Surface via Sentry instead. See afterCommit docstring.
      logger.error('afterCommit hook failed', {
        aggregateId,
        eventType: pickEventType(eventData),
        error: err instanceof Error ? err.message : String(err),
      });
      return response;
    }
  }

  /** Best-effort `event.data` lookup from the standard CommandRoute response. */
  private async extractEventData(response: Response): Promise<unknown> {
    try {
      const parsed: { event?: { data?: unknown } | null } = await response.clone().json();
      return parsed.event?.data;
    } catch {
      // NO_EVENT routes or non-standard body shape — pass undefined.
      return undefined;
    }
  }

  /**
   * Build headers with auth context injected for forwarding to Durable Object.
   *
   * @param c - Hono context with auth context
   * @returns Headers with auth fields set
   */
  private buildAuthHeaders(c: Context): Headers {
    const headers = new Headers(c.req.raw.headers);
    const authContext = c.get('authContext') as AuthContext | undefined;

    if (!authContext) {
      return headers;
    }

    if (authContext.authType === 'api-key' && authContext.orgId) {
      headers.set('X-Org-Id', authContext.orgId);
      if (authContext.isSuper) {
        headers.set('X-Super-Access', 'true');
      }
    }

    if (authContext.authType === 'jwt') {
      if (authContext.userEmail) {
        headers.set('X-User-Email', authContext.userEmail);
      }
      if (authContext.userId) {
        headers.set('X-User-Id', authContext.userId);
      }
    }

    return headers;
  }

  /**
   * Get Durable Object stub from environment
   *
   * @param c - Hono context with env bindings
   * @param aggregateId - Aggregate ID for DO routing
   * @returns Durable Object stub
   */
  protected getDurableObjectStub(c: Context, aggregateId: string): DurableObjectStub {
    const binding = this.aggregateTypeToBinding(this.aggregateType);
    const env = c.env as Record<string, unknown>;
    const namespace = env[binding] as DurableObjectNamespace;

    if (!namespace) {
      throw new Error(
        `Durable Object namespace "${binding}" not found in environment. ` +
          `Check wrangler.jsonc has [[durable_objects.bindings]] with binding = "${binding}"`
      );
    }

    const id = namespace.idFromName(aggregateId);
    return namespace.get(id);
  }

  /**
   * Convert aggregate type to DO binding name
   *
   * Examples:
   * - 'UserAggregate' → 'USER'
   * - 'LockAggregate' → 'LOCK'
   * - 'HubAggregate' → 'HUB'
   *
   * @param aggregateType - Aggregate type name
   * @returns Uppercase snake_case binding name
   */
  private aggregateTypeToBinding(aggregateType: string): string {
    const snakeCase = aggregateType.replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase();

    // Remove '_AGGREGATE' suffix if present
    return snakeCase.replace(/_AGGREGATE$/, '');
  }
}

/**
 * Abstract base class for CREATE command routes
 *
 * Use this for commands that CREATE NEW aggregates. The state parameter
 * is always null because create commands run against non-existent aggregates.
 *
 * For UPDATE commands (existing aggregates), use CommandRoute instead.
 *
 * Commands are forwarded to Durable Objects where they execute with:
 * - Ordered execution (single-threaded in DO)
 * - Transactional state updates
 * - No race conditions
 *
 * @template TCommand - Command type (body fields, derived from Zod schema)
 * @template TState - Aggregate state type (for typing the returned event)
 * @template TEvent - Domain event type (must extend BaseEvent)
 *
 * Usage:
 * ```typescript
 * const CreateUserBodySchema = z.object({
 *   email: z.string().email(),
 *   name: z.string().optional(),
 * });
 * type CreateUserBody = z.infer<typeof CreateUserBodySchema>;
 *
 * @Route({ method: 'POST', path: '/users/:id/CreateUser' })
 * export class CreateUserRoute extends CreateCommandRoute<CreateUserBody, UserState, UserCreatedEvent> {
 *   aggregateType = 'UserAggregate';
 *   schema = { request: { body: { content: { 'application/json': { schema: CreateUserBodySchema } } } } };
 *
 *   async executeCommand(command: CreateUserBody): Promise<UserCreatedEvent> {
 *     // state is always null for create commands - no need to check
 *     return { type: 'UserCreated', email: command.email, name: command.name };
 *   }
 * }
 * ```
 */
export abstract class CreateCommandRoute<
  TCommand extends CommandBody = CommandBody,
  TState = unknown,
  TEvent extends BaseEvent | typeof NO_EVENT = BaseEvent,
  TEnv = unknown
> extends CommandRoute<TCommand, TState, TEvent, TEnv> {
  /**
   * Indicates this is a create command (aggregate must NOT exist)
   * Framework uses this to validate semantics before execution
   */
  static override readonly isCreateCommand = true;

  /**
   * Execute create command business logic and produce domain event
   *
   * For CREATE commands, state is always null (guaranteed by framework).
   * No state parameter needed since create commands don't have prior state.
   *
   * @param command - Command object (from buildCommand)
   * @param env - Environment bindings (from Durable Object)
   * @param auth - Caller identity from the forwarded auth headers (optional)
   * @returns Domain event to apply
   *
   * The create variant drops `state` (always null), so its parameters sit in
   * different positions than the base signature's: position 2 is the base's
   * `state` slot and position 3 is the base's `env` slot. The union types
   * exist only to satisfy override variance — concrete routes should
   * annotate the params as `(command, env: <YourEnv>, auth?: AuthContext)`;
   * the framework always dispatches create commands as (command, env, auth).
   */
  abstract override executeCommand(
    command: TCommand,
    env: TEnv | TState,
    auth?: AuthContext | TEnv,
  ): Promise<TEvent>;

  /**
   * Optional hook to customize the success response. Inherited from
   * `CommandRoute` — see its docstring for full details.
   *
   * For CREATE commands, the `state` argument is the freshly-created
   * aggregate state (the framework has already applied the event before
   * calling this hook).
   */
}
