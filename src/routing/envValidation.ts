/**
 * Environment validation middleware for createRouter
 */

import type { Context } from 'hono';
import type { z } from 'zod';
import { setGlobalLoggerEnv, type LoggerEnv } from '../logger.js';

/**
 * Cache for validated environments (per-worker instance)
 * Prevents re-validation on every request
 */
const validatedEnvCache = new WeakMap<object, boolean>();

/**
 * Create middleware that validates environment variables using a Zod schema
 *
 * Validation happens once per worker instance and is cached.
 * If validation fails, returns a 500 error with detailed validation errors.
 *
 * @param schema - Zod schema to validate against
 * @param options - Validation options
 * @returns Hono middleware function
 *
 * @example
 * ```typescript
 * import { createRouter } from 'ceves';
 * import { z } from 'zod';
 *
 * const app = createRouter({
 *   env: {
 *     schema: z.object({ DATABASE_URL: z.string().url() }),
 *   },
 * });
 * ```
 */
export function createEnvValidationMiddleware<T extends z.ZodType>(
  schema: T,
  options: {
    /** Skip validation in development (default: false) */
    skipInDev?: boolean;
    /** Custom error handler */
    onError?: (error: z.ZodError, c: Context) => Response | Promise<Response>;
  } = {}
) {
  return async (c: Context, next: () => Promise<void>) => {
    // `c.env` is `any` on an untyped Context — pin it to a record once so
    // every use below is type-safe.
    const env = c.env as Record<string, unknown>;

    // Skip if already validated for this env object
    if (validatedEnvCache.has(env)) {
      return next();
    }

    // Skip in development if configured
    if (options.skipInDev && env.ENVIRONMENT === 'development') {
      return next();
    }

    // Validate environment
    const result = schema.safeParse(env);

    if (!result.success) {
      // Mark as validated to prevent infinite loops
      validatedEnvCache.set(env, false);

      // Use custom error handler if provided
      if (options.onError) {
        return options.onError(result.error, c);
      }

      // Default error response
      return c.json(
        {
          success: false,
          errors: [
            {
              code: 500,
              message: 'Environment configuration is invalid',
              details: result.error.issues.map((err) => ({
                field: err.path.join('.'),
                message: err.message,
                code: err.code,
              })),
            },
          ],
        },
        500
      );
    }

    // Mark as validated
    validatedEnvCache.set(env, true);

    // Store validated env back (with transforms applied)
    // This allows handlers to access the transformed values
    Object.assign(env, result.data);

    // Propagate validated env to module-level loggers
    setGlobalLoggerEnv(result.data as LoggerEnv);

    return next();
  };
}
