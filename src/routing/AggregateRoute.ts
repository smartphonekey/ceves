/**
 * Abstract base class for routes that forward requests to Durable Objects
 *
 * This class provides the common pattern for HTTP routes that need to:
 * 1. Extract an aggregate ID from the request
 * 2. Get a Durable Object stub from the environment
 * 3. Forward the request to the DO
 * 4. Return the DO's response
 *
 * Use this when building Cloudflare Workers that route to event-sourced
 * Durable Objects (e.g., with Ceves).
 *
 * @example
 * ```typescript
 * @Route({ method: 'POST', path: '/users/:id/assign-key' })
 * export class AssignKeyRoute extends AggregateRoute {
 *   aggregateType = 'UserAggregate';
 *
 *   schema = {
 *     request: {
 *       params: z.object({ id: z.string().uuid() }),
 *       body: {
 *         content: {
 *           'application/json': {
 *             schema: z.object({ keyId: z.string() }),
 *           },
 *         },
 *       },
 *     },
 *     responses: {
 *       200: {
 *         description: 'Key assigned successfully',
 *         content: {
 *           'application/json': {
 *             schema: z.object({ success: z.boolean() }),
 *           },
 *         },
 *       },
 *     },
 *   };
 *
 *   protected extractAggregateId(c: Context): string {
 *     return c.req.param('id');
 *   }
 * }
 * ```
 */

import { OpenAPIRoute } from 'chanfana';
import type { Context } from 'hono';

/**
 * Abstract base class for routes that forward to Durable Objects
 *
 * Subclasses must:
 * - Set `aggregateType` property (e.g., 'UserAggregate')
 * - Implement `extractAggregateId()` to get ID from request
 * - Define `schema` for OpenAPI documentation
 */
export abstract class AggregateRoute extends OpenAPIRoute {
  /**
   * Aggregate type - must match the DO binding name in wrangler.jsonc
   *
   * Example: 'UserAggregate' maps to USER_AGGREGATE binding
   */
  abstract aggregateType: string;

  /**
   * Extract aggregate ID from the request
   *
   * Common patterns:
   * - Path params: `c.req.param('id')`
   * - Query params: `c.req.query('userId')`
   * - Request body: `(await c.req.json()).userId`
   * - Generate new: `crypto.randomUUID()` (for create commands)
   *
   * @param c - Hono context with request data
   * @returns Aggregate ID to route to
   */
  protected abstract extractAggregateId(c: Context): string | Promise<string>;

  /**
   * Main handler - forwards request to Durable Object
   *
   * Override this if you need custom logic before/after DO forwarding.
   * Most of the time, you only need to implement `extractAggregateId()`.
   */
  override async handle(c: Context): Promise<Response> {
    // Get aggregate ID from request
    const aggregateId = await this.extractAggregateId(c);

    // Get DO stub
    const stub = this.getDurableObjectStub(c, aggregateId);

    // Reconstruct request to forward to DO
    // We need to rebuild the request because Chanfana may have consumed the body stream
    const url = new URL(c.req.url);
    const headers = new Headers(c.req.raw.headers);
    let body: string | null = null;

    // Get body for POST/PUT/PATCH requests
    if (['POST', 'PUT', 'PATCH'].includes(c.req.method)) {
      try {
        const json: unknown = await c.req.json();
        body = JSON.stringify(json);
        headers.set('Content-Type', 'application/json');
      } catch {
        // If JSON parsing fails, try to get text
        try {
          body = await c.req.text();
        } catch {
          // If all else fails, leave body as null
        }
      }
    }

    // Create new request for DO
    const doRequest = new Request(url.toString(), {
      method: c.req.method,
      headers,
      body,
    });

    // Forward to DO
    const doResponse = await stub.fetch(doRequest);

    return doResponse;
  }

  /**
   * Get Durable Object stub from environment
   *
   * Converts aggregateType to binding name:
   * - 'UserAggregate' → 'USER_AGGREGATE'
   * - 'BankAccount' → 'BANK_ACCOUNT'
   *
   * Override this if you have custom DO binding logic.
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
   * - 'BankAccount' → 'BANK_ACCOUNT'
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
