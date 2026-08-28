/**
 * Router factory for the Ceves framework
 *
 * Creates a Chanfana OpenAPIRouter with auto-discovered routes
 */

import { fromHono, ApiException } from 'chanfana';
import { Hono, type Context } from 'hono';
import { HTTPException } from 'hono/http-exception';
import type { z } from 'zod';
import { getRegisteredRoutes } from './Route.js';
import type { OpenAPIRoute } from 'chanfana';
import { createEnvValidationMiddleware } from './envValidation.js';
import { createLogger } from '../logger.js';

const logger = createLogger({ component: 'createRouter' });

/**
 * Best-effort, never-throws stringification for non-`Error` throws.
 * Used by the Hono `onError` fallback so a `throw 42` /
 * `throw { reason: 'x' }` still produces a non-empty, greppable error
 * message rather than `"[object Object]"` or `"undefined"`.
 */
function safeStringify(value: unknown): string {
  if (value == null) return String(value);
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return Object.prototype.toString.call(value);
  }
}

/**
 * OpenAPI server entry
 */
interface OpenAPIServer {
  /** Server URL */
  url: string;
  /** Human-readable description of the server */
  description?: string;
}

/**
 * OpenAPI metadata configuration
 */
export interface OpenAPIMetadata {
  /** API title */
  title?: string;
  /** API version */
  version?: string;
  /** API description */
  description?: string;
  /** Server list for the OpenAPI spec */
  servers?: OpenAPIServer[];
}

/**
 * Environment validation configuration
 */
export interface EnvConfig {
  /** Zod schema for environment validation */
  schema: z.ZodType;
  /** Skip validation in development (default: false) */
  skipInDev?: boolean;
  /** Custom error handler for validation failures */
  onError?: (error: z.ZodError, c: Context) => Response | Promise<Response>;
}

/**
 * Router configuration options
 */
export interface RouterOptions {
  /** Base path for all routes (e.g., '/api/v1') */
  basePath?: string;
  /** OpenAPI metadata */
  openapi?: OpenAPIMetadata;
  /** Swagger UI path (default: '/docs') */
  docsPath?: string;
  /** OpenAPI spec path (default: '/openapi.json') */
  schemaPath?: string;
  /** Whether to enable Swagger UI (default: true) */
  enableDocs?: boolean;
  /** Environment variable validation configuration */
  env?: EnvConfig;
  /** Middleware to apply before routes are registered */
  middleware?: Array<(c: Context, next: () => Promise<void>) => Promise<void | Response>>;
  /** Plain Hono routes registered before chanfana (bypass OpenAPI, no auth) */
  plainRoutes?: (app: Hono) => void;
  /**
   * Explicit list of `@Route`-decorated classes to expose.
   *
   * When provided, only routes whose `RouteClass` is in this array are
   * registered (intersected with the global decorator registry, which still
   * owns each class's `method` + `path` metadata).
   *
   * The intended source is the `REGISTERED_ROUTES` array emitted by
   * `ceves-generate-imports` (`src/_decoratorImports.generated.ts`). Passing
   * it explicitly means the worker no longer depends on whatever happens to
   * have been imported into the global registry — a route stays off the API
   * surface unless it's in this array.
   *
   * When omitted, falls back to `getRegisteredRoutes()` (the legacy implicit
   * "everything the decorator processor collected" behaviour).
   */
  routes?: ReadonlyArray<typeof OpenAPIRoute>;
}

/**
 * Create Chanfana router with all registered routes.
 *
 * This factory function:
 * 1. Auto-discovers all @Route decorated classes from the global registry
 * 2. Creates a Chanfana OpenAPIRouter with the provided configuration
 * 3. Registers all routes with Chanfana for automatic OpenAPI generation
 * 4. Returns a configured Hono app ready to handle requests
 *
 * **Usage:**
 * ```typescript
 * import { createRouter } from 'ceves';
 * import './routes'; // Import files with @Route decorators
 *
 * // With typed bindings
 * type Bindings = {
 *   DB: D1Database;
 *   API_KEY: string;
 * };
 *
 * const app = createRouter<{ Bindings: Bindings }>({
 *   basePath: '/api/v1',
 *   openapi: {
 *     title: 'My API',
 *     version: '1.0.0',
 *     description: 'API built with Ceves',
 *   },
 * });
 *
 * export default app;
 * ```
 *
 * @param options - Router configuration options
 * @returns Configured Hono app with Chanfana OpenAPI support
 */
export function createRouter<Env extends Record<string, unknown> = Record<string, never>>(options: RouterOptions = {}) {
  const {
    openapi = {},
    docsPath = '/docs',
    schemaPath = '/openapi.json',
    enableDocs = true,
    env,
    middleware = [],
    plainRoutes,
    routes: explicitRoutes,
  } = options;

  // Create base Hono app with typed bindings
  const app = new Hono<Env>();

  // AA-193: drain the incoming request body before ANY response leaves the
  // worker with the body unread. Sending a response while the request stream
  // is live is the documented local-workerd crash trigger: workerd neuters
  // the stream on response-send, wrangler's ProxyWorker (still holding the
  // client's live body) hits "Can't read from request stream after response
  // has been sent" → Broken pipe → "Network connection lost" → the dev
  // worker dies mid-run (reproduced 5/5; see
  // docs/ci-evidence/AA-193-local-workerd-failure.md). Site fixes exist
  // (unifiedAuth PR #285, ceves AggregateObject.drainRequestBody), but any
  // NEW early-return path would reintroduce the crash — this outermost
  // middleware makes the guarantee structural for every router-built worker:
  // auth rejections, validation 400s, unmatched-route 404s, and onError
  // responses all pass through here before Hono hands the Response to the
  // runtime. `finally` covers the throw path; `bodyUsed` skips bodies a
  // downstream consumer (route handler, DO forward) already claimed;
  // `cancel()` discards without buffering (same choice as ceves).
  app.use(async (c, next) => {
    try {
      await next();
    } finally {
      const raw = c.req.raw;
      if (raw.body && !raw.bodyUsed) {
        try {
          await raw.body.cancel();
        } catch {
          // Best-effort: draining must never mask the real response.
        }
      }
    }
  });

  // AA-119: coerce non-Error throws BEFORE Hono's runtime sees them.
  //
  // Hono's `onError` only fires for thrown `Error` instances. Domain
  // code that does `throw "uh oh"` or `throw { reason: '...' }`
  // bypasses onError entirely — the throw escapes into Cloudflare's
  // runtime, which serialises it as a generic 500 with no message
  // (the symptom behind AA-111: bare unhandled error with empty
  // message). This middleware catches the throw, wraps it in a real
  // Error preserving the original payload in `.message`, and rethrows
  // so onError can produce a structured response.
  app.use(async (_c, next) => {
    try {
      await next();
    } catch (err) {
      if (err instanceof Error) {
        throw err;
      }
      throw new Error(`Unknown error: ${safeStringify(err)}`);
    }
  });

  // Add global error handler.
  //
  // chanfana 3 changed the error flow: validation failures and ApiException
  // subclasses are now caught inside chanfana, converted to Hono
  // HTTPException, and re-thrown so they reach this onError. Calling
  // err.getResponse() returns chanfana's pre-built JSON error response with
  // the correct status code. See chanfana migration guide v2 → v3:
  // https://chanfana.pages.dev/migration-to-chanfana-3
  //
  // We still keep the direct ApiException branch as a defensive fallback for
  // any handler that throws an ApiException outside chanfana's wrap path
  // (e.g. middleware or DO entrypoints proxying back into Hono).
  app.onError((err, c) => {
    if (err instanceof HTTPException) {
      return err.getResponse();
    }

    if (err instanceof ApiException) {
      const status = err.status ?? 500;
      const response = err.buildResponse();
      return c.json(
        {
          success: false,
          errors: response,
        },
        status as 400 | 401 | 403 | 404 | 500
      );
    }

    // AA-119: never re-throw from onError.
    //
    // Re-throwing here used to surface the unhandled error to Hono's
    // default handler, which serialises it as `Internal Server Error`
    // with the message dropped — exactly the symptom behind AA-111
    // (`Bare unhandled "Error" on /locks/.../delete-temp-key — empty
    // message stripped`) and AA-112 (`Unhandled "internal error" on
    // DELETE /tenants/.../webhooks/...`). The thrown value can also be
    // a non-Error (string, number, plain object) when domain code does
    // `throw "something"` — Hono's default handler stringifies those
    // to `"[object Object]"` or similar, also useless for triage.
    //
    // Coerce any non-Error throw to a real Error with a non-empty,
    // greppable message and return a chanfana-shape 500 envelope so
    // every error path produces the same wire format.
    const message = err instanceof Error && err.message
      ? err.message
      : `Unknown error: ${safeStringify(err)}`;
    const errorName = err instanceof Error ? err.name : 'Unknown';
    logger.error('Unhandled error', {
      message,
      errorName,
      stack: err instanceof Error ? err.stack : undefined,
    });
    return c.json(
      {
        success: false,
        errors: [{ code: 500, message }],
      },
      500,
    );
  });

  // Add environment validation middleware if configured
  if (env?.schema) {
    app.use(
      '*',
      createEnvValidationMiddleware(env.schema, {
        skipInDev: env.skipInDev,
        onError: env.onError,
      })
    );
  }

  // Add custom middleware before routes are registered
  for (const mw of middleware) {
    app.use('*', mw);
  }

  // Create Chanfana OpenAPIRouter
  const openapi_instance = fromHono(app, {
    docs_url: enableDocs ? docsPath : undefined,
    openapi_url: schemaPath,
    schema: {
      info: {
        title: openapi.title ?? 'API',
        version: openapi.version ?? '1.0.0',
        description: openapi.description,
      },
      servers: (openapi.servers ?? []) as unknown as [],
      security: [{ BearerAuth: [] }, { ApiKeyAuth: [] }],
    },
  });

  // Register security schemes via the registry (components is excluded from schema type)
  openapi_instance.registry.registerComponent('securitySchemes', 'BearerAuth', {
    type: 'http',
    scheme: 'bearer',
    bearerFormat: 'JWT',
    description: 'JWT Bearer token for authentication.',
  });
  openapi_instance.registry.registerComponent('securitySchemes', 'ApiKeyAuth', {
    type: 'apiKey',
    in: 'header',
    name: 'X-API-Key',
    description: 'API key for organization access.',
  });

  // Register `@Route`-decorated classes.
  //
  // The decorator registry (`getRegisteredRoutes()`) is still the source of
  // truth for each class's `method` + `path` metadata — that's where the
  // `@Route({ method, path })` call writes it. When the caller passed an
  // explicit `routes` array (typically `REGISTERED_ROUTES` from the
  // generated barrel), we filter the registry result to only those classes.
  // That makes the worker's route surface declarative rather than "whatever
  // got imported by side effect into the singleton".
  const allRoutes = getRegisteredRoutes();
  const routes = explicitRoutes
    ? (() => {
        const allowed = new Set<typeof OpenAPIRoute>(explicitRoutes);
        const matched = allRoutes.filter((r) => allowed.has(r.RouteClass));
        // Surface a clear error if the caller listed a class that never made
        // it into the decorator registry — almost always a missing `@Route`
        // decorator or a stale `REGISTERED_ROUTES` barrel.
        if (matched.length !== explicitRoutes.length) {
          const found = new Set(matched.map((r) => r.RouteClass));
          const missing = explicitRoutes
            .filter((c) => !found.has(c))
            .map((c) => c.name || '<anonymous>');
          throw new Error(
            `createRouter: ${missing.length} class(es) in options.routes have no @Route metadata in the decorator registry — ${missing.join(', ')}. Regenerate the barrel (\`npx ceves-generate-imports\`) or add the missing @Route(...) decorator.`,
          );
        }
        return matched;
      })()
    : allRoutes;
  logger.debug('Registering routes', { count: routes.length, routes: routes.map(r => `${r.method} ${r.path}`) });

  for (const { RouteClass, method, path } of routes) {
    // Register route with Chanfana using the appropriate HTTP method
    const lowerMethod = method.toLowerCase();

    switch (lowerMethod) {
      case 'get':
        openapi_instance.get(path, RouteClass);
        break;
      case 'post':
        openapi_instance.post(path, RouteClass);
        break;
      case 'put':
        openapi_instance.put(path, RouteClass);
        break;
      case 'delete':
        openapi_instance.delete(path, RouteClass);
        break;
      case 'patch':
        openapi_instance.patch(path, RouteClass);
        break;
      case 'all':
        openapi_instance.all(path, RouteClass);
        break;
      default:
        // Use .on() for custom HTTP methods
        openapi_instance.on(method, path, RouteClass);
        break;
    }
  }

  // Register plain Hono routes AFTER chanfana route registration.
  // These bypass OpenAPI/chanfana handling and use Hono's native routing directly.
  if (plainRoutes) {
    plainRoutes(app as unknown as Hono);
  }

  // Return `app`, NOT `openapi_instance`. This looks like a missed cleanup; it
  // is not, and swapping it breaks every endpoint in production.
  //
  // chanfana's fromHono() returns a *Proxy* (HonoOpenAPIRouterType) that writes
  // route registrations through to the underlying `app`. Sentry's withSentry()
  // cannot wrap that Proxy — a worker whose entry is
  // `Sentry.withSentry(..., openapi_instance)` returns 500 on EVERY route, not
  // just on error paths. Because the Proxy registers onto `app` anyway, `app`
  // already carries every chanfana route, so returning it loses nothing.
  //
  // Rule: whatever reaches withSentry() must be the original Hono instance.
  return app;
}
