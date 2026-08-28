/**
 * QueryRoute - Base class for Ceves query routes
 *
 * Extends Chanfana's OpenAPIRoute to provide automatic DO forwarding for queries.
 * Works with the @Route decorator for auto-registration.
 *
 * Convention-based routing:
 * - URL pattern: /{aggregateType}/:id/{queryName}
 * - Example: /users/550e8400-e29b-41d4-a716-446655440000/profile
 * - Aggregate ID is automatically extracted from second segment
 */

import { OpenAPIRoute } from 'chanfana';
import type { Context } from 'hono';
import type { BaseState } from '../schemas/State';
import { safeDOFetch } from './safeDOFetch';
import { handleCevesError } from './cevesErrorResponse';

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

/** Create missing aggregate ID error response */
function missingAggregateIdResponse(): Response {
  return new Response(
    JSON.stringify({ success: false, error: 'MissingAggregateId', message: 'Aggregate ID not found in URL path' }),
    { status: 400, headers: { 'Content-Type': 'application/json' } }
  );
}

/** Build auth headers from Hono context */
function buildAuthHeaders(baseHeaders: Headers, authContext: AuthContext | undefined): Headers {
  if (!authContext) return baseHeaders;
  if (authContext.authType === 'api-key' && authContext.orgId) {
    baseHeaders.set('X-Org-Id', authContext.orgId);
    if (authContext.isSuper) {
      baseHeaders.set('X-Super-Access', 'true');
    }
  }
  if (authContext.authType === 'jwt') {
    if (authContext.userEmail) baseHeaders.set('X-User-Email', authContext.userEmail);
    if (authContext.userId) baseHeaders.set('X-User-Id', authContext.userId);
  }
  return baseHeaders;
}

/**
 * Abstract base class for Ceves query routes
 *
 * Combines:
 * - Chanfana's OpenAPIRoute for HTTP routing + validation
 * - Ceves' query execution pattern with state access
 *
 * @template TState - Aggregate state type
 * @template TQuery - Query parameters type
 * @template TResponse - Query response type
 *
 * @example
 * ```typescript
 * interface ProfileResponse {
 *   userId: string;
 *   email: string;
 * }
 *
 * @Route({ method: 'GET', path: '/users/:id/profile' })
 * export class GetUserProfileQuery extends QueryRoute<UserState, {}, ProfileResponse> {
 *   aggregateType = 'UserAggregate';
 *
 *   schema = {
 *     request: {
 *       params: z.object({ id: z.string().uuid() }),
 *     },
 *     responses: {
 *       200: {
 *         description: 'User profile',
 *         content: {
 *           'application/json': {
 *             schema: z.object({ userId: z.string(), email: z.string() }),
 *           },
 *         },
 *       },
 *     },
 *   };
 *
 *   async execute(state: UserState, _query: {}): Promise<ProfileResponse> {
 *     return {
 *       userId: state.userId,
 *       email: state.email,
 *     };
 *   }
 * }
 * ```
 */
export abstract class QueryRoute<
  TState extends BaseState = BaseState,
  TQuery = unknown,
  TResponse = unknown
> extends OpenAPIRoute {
  /**
   * Aggregate type - must match DO binding name
   * Example: 'UserAggregate' maps to USER binding
   */
  abstract aggregateType: string;

  /**
   * Execute query logic with loaded state
   * 
   * @param state - Current aggregate state
   * @param query - Validated query parameters
   * @param c - Hono context (for accessing env, headers, etc.)
   * @returns Query response
   */
  abstract executeQuery(state: TState, query: TQuery, c: Context): Promise<TResponse>;

  /**
   * Main handler - loads state from DO and executes query
   *
   * Automatically:
   * - Extracts aggregate ID from URL path (second segment)
   * - Gets DO stub from environment
   * - Loads current state
   * - Calls execute() with state and query
   */
  override async handle(c: Context): Promise<Response> {
    const aggregateId = c.req.param('id');
    if (!aggregateId) return missingAggregateIdResponse();

    // Get DO stub and build state URL
    const stub = this.getDurableObjectStub(c, aggregateId);
    const stateUrl = new URL(c.req.url);
    stateUrl.pathname = stateUrl.pathname + '/__state';

    // Forward auth headers from Hono context to DO
    const authContext = c.get('authContext') as AuthContext | undefined;
    const headers = buildAuthHeaders(new Headers(c.req.raw.headers), authContext);

    // Wrap so a CF synthetic uncatchable error (DO reset during deploy /
    // storage timeout / "internal error; reference = ...") becomes a
    // structured 503 instead of an unhandled 500 — see safeDOFetch.ts.
    const stateResponse = await safeDOFetch(
      stub.fetch(new Request(stateUrl.toString(), { method: 'GET', headers })),
    );
    if (!stateResponse.ok) return stateResponse;

    const state: TState = await stateResponse.json();
    const query = await this.extractQueryParams(c);

    // Execute query logic with loaded state
    try {
      const result = await this.executeQuery(state, query, c);
      return new Response(JSON.stringify(result), { status: 200, headers: { 'Content-Type': 'application/json' } });
    } catch (error) {
      const cevesResponse = handleCevesError(error);
      if (cevesResponse) return cevesResponse;
      throw error;
    }
  }

  /** Extract query parameters from request */
  private async extractQueryParams(c: Context): Promise<TQuery> {
    if (c.req.method === 'POST') {
      return await c.req.json() as TQuery;
    }
    const data = await this.getValidatedData<typeof this.schema>();
    return {
      ...(typeof data.params === 'object' ? data.params : {}),
      ...(typeof data.query === 'object' ? data.query : {}),
    } as TQuery;
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
    const namespace = (c.env as Record<string, unknown>)[binding] as DurableObjectNamespace;

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
