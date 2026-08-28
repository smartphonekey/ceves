/**
 * Global route registry for Chanfana OpenAPIRoute classes
 */

import { OpenAPIRoute } from 'chanfana';
import { createLogger } from '../logger.js';

const logger = createLogger({ component: 'RouteRegistry' });

/** Result of matching a route segment against a path segment */
interface SegmentMatchResult {
  matches: boolean;
  /**
   * Present only for `:param` segments. Name and value always travel together,
   * so they live in one object — as two independent optionals the compiler
   * could not know that a present name implies a present value, which is why
   * the caller used to need a non-null assertion.
   */
  param?: { name: string; value: string };
}

/** Match a route segment against a path segment */
function matchSegment(routeSegment: string, pathSegment: string): SegmentMatchResult {
  if (routeSegment.startsWith(':')) {
    return { matches: true, param: { name: routeSegment.slice(1), value: pathSegment } };
  }
  return { matches: routeSegment === pathSegment };
}

/** Try to match all segments and extract params */
function matchAllSegments(
  routeSegments: string[],
  pathSegments: string[]
): Record<string, string> | null {
  if (routeSegments.length !== pathSegments.length) return null;

  const params: Record<string, string> = {};
  for (let i = 0; i < routeSegments.length; i++) {
    const routeSegment = routeSegments[i];
    const pathSegment = pathSegments[i];
    if (!routeSegment || !pathSegment) return null;

    const result = matchSegment(routeSegment, pathSegment);
    if (!result.matches) return null;
    if (result.param) params[result.param.name] = result.param.value;
  }
  return params;
}

/**
 * Route metadata stored in registry
 */
export interface RouteMetadata {
  /** Route class constructor */
  RouteClass: typeof OpenAPIRoute;
  /** HTTP method (GET, POST, PUT, DELETE, PATCH, etc.) */
  method: string;
  /** Route path (e.g., '/users/:id') */
  path: string;
  /** Unique key for the route */
  key: string;
}

/**
 * Global registry of all @Route decorated classes
 */
class RouteRegistry {
  private routes = new Map<string, RouteMetadata>();

  /**
   * Register a route class with its path and method
   *
   * @throws Error if a different class is already registered for the same route
   */
  register(RouteClass: typeof OpenAPIRoute, method: string, path: string): void {
    // Generate unique key from method + path
    const key = `${method}:${path}`;

    if (this.routes.has(key)) {
      const existing = this.routes.get(key);

      // Allow re-registration of SAME class (HMR reloads the same file)
      // Class name comparison handles HMR where class reference changes but name stays same
      if (existing?.RouteClass.name === RouteClass.name) {
        logger.debug('Re-registering route (HMR)', { key, class: RouteClass.name });
        // Update the reference (HMR creates new class instance)
        this.routes.set(key, { RouteClass, method, path, key });
        return;
      }

      // Different class trying to register same route = error
      throw new Error(
        `Route already registered: ${key} (existing: ${existing?.RouteClass.name}, new: ${RouteClass.name})`
      );
    }

    this.routes.set(key, {
      RouteClass,
      method,
      path,
      key,
    });
  }

  /**
   * Get all registered routes sorted by specificity
   *
   * Routes are sorted by path segment count (descending) to ensure
   * more specific routes are registered before general ones in Hono.
   *
   * Example order:
   * 1. POST /locks/:id/SetOrganization (3 segments)
   * 2. POST /locks/:id (2 segments)
   *
   * This ensures Hono matches the most specific route first.
   */
  getAll(): RouteMetadata[] {
    const routes = Array.from(this.routes.values());

    // Sort by path segment count (descending) - more segments = more specific
    return routes.sort((a, b) => {
      const aSegments = a.path.split('/').filter(Boolean).length;
      const bSegments = b.path.split('/').filter(Boolean).length;
      return bSegments - aSegments; // Descending order
    });
  }

  /**
   * Get route by key
   */
  get(key: string): RouteMetadata | undefined {
    return this.routes.get(key);
  }

  /**
   * Check if route is registered
   */
  has(key: string): boolean {
    return this.routes.has(key);
  }

  /**
   * Clear all routes (for testing)
   */
  clear(): void {
    this.routes.clear();
  }

  /**
   * Get count of registered routes
   */
  get count(): number {
    return this.routes.size;
  }

  /**
   * Find route by matching actual URL path against registered patterns
   *
   * This is used by Durable Objects to find the handler class for a forwarded request.
   * Path parameters like :id are matched against actual values.
   *
   * @param method - HTTP method (GET, POST, etc.)
   * @param pathname - Actual URL pathname (e.g., '/users/abc123/CreateUser')
   * @returns Matching route metadata with extracted path params, or undefined
   *
   * @example
   * // Given registered route: POST /users/:id/CreateUser
   * findByUrlAndMethod('POST', '/users/abc123/CreateUser')
   * // Returns: { route: {...}, params: { id: 'abc123' } }
   */
  findByUrlAndMethod(
    method: string,
    pathname: string
  ): { route: RouteMetadata; params: Record<string, string> } | undefined {
    const pathSegments = pathname.split('/').filter(Boolean);

    // Search through routes sorted by specificity (most specific first)
    for (const route of this.getAll()) {
      if (route.method !== method) continue;

      const routeSegments = route.path.split('/').filter(Boolean);
      const params = matchAllSegments(routeSegments, pathSegments);
      if (params) return { route, params };
    }

    return undefined;
  }
}

/**
 * Singleton registry instance
 */
export const routeRegistry = new RouteRegistry();
