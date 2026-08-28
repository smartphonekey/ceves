/**
 * Header-based Tenant Resolver Implementation
 *
 * This module provides a simple header-based tenant resolver that reads the organization ID
 * directly from an HTTP header. Useful when an upstream gateway or middleware
 * sets the orgId header after validating credentials.
 *
 * Key Design Decisions:
 * - Reads orgId from HTTP header (no database lookup in hot path)
 * - Configurable header name (default: X-Org-Id)
 * - Optional header support with default fallback for local development
 * - Works with any gateway, middleware, or custom auth layer
 *
 * @packageDocumentation
 */

import type { ITenantResolver } from './TenantResolver';
import { MissingApiKeyError } from './errors';

/**
 * Header-based implementation of the ITenantResolver interface.
 *
 * Resolves tenant from an HTTP header (typically set by an upstream gateway or middleware).
 * This decouples authentication from application logic - the auth layer validates credentials
 * and sets the orgId header, and this resolver simply reads it.
 *
 * @example
 * ```typescript
 * // Basic usage (required header)
 * const resolver = new HeaderTenantResolver('X-Org-Id');
 * const orgId = await resolver.resolveOrgId(request);
 *
 * // With default fallback for local development
 * const resolver = new HeaderTenantResolver('X-Org-Id', 'default-org');
 * const orgId = await resolver.resolveOrgId(request);
 * // Returns 'default-org' if X-Org-Id header is missing
 * ```
 *
 * @example
 * ```typescript
 * // With upstream auth gateway
 * // Gateway validates credentials and sets X-Org-Id header
 * const resolver = new HeaderTenantResolver('X-Org-Id');
 * // Inside your AggregateObject subclass (or wherever stores are wired):
 * this.setStores(new R2EventStore(env.EVENTS_BUCKET), resolver);
 * ```
 */
export class HeaderTenantResolver implements ITenantResolver {
  /**
   * The name of the HTTP header containing the organization ID.
   */
  private readonly headerName: string;

  /**
   * Optional default organization ID to use when header is missing.
   * Useful for local development without authentication.
   */
  private readonly defaultOrgId?: string;

  /**
   * Create a new HeaderTenantResolver.
   *
   * @param headerName - Name of HTTP header containing orgId (e.g., 'X-Org-Id')
   * @param defaultOrgId - Optional default orgId for local dev when header is missing
   *
   * @example
   * ```typescript
   * // Production: require X-Org-Id header
   * const resolver = new HeaderTenantResolver('X-Org-Id');
   *
   * // Development: fallback to default org
   * const resolver = new HeaderTenantResolver('X-Org-Id', 'default-org');
   * ```
   */
  constructor(headerName: string, defaultOrgId?: string) {
    this.headerName = headerName;
    this.defaultOrgId = defaultOrgId;
  }

  /**
   * Resolve organization ID from HTTP header.
   *
   * Reads the configured header from the request. If the header is missing:
   * - Returns defaultOrgId if configured (for local development)
   * - Throws MissingApiKeyError if no default configured (production)
   *
   * @param request - Incoming HTTP request
   * @returns Promise resolving to the organization ID
   * @throws {MissingApiKeyError} If header is missing and no default configured
   *
   * @example
   * ```typescript
   * // Request with X-Org-Id header
   * const request = new Request('https://api.example.com/accounts/1', {
   *   headers: { 'X-Org-Id': 'org-123' }
   * });
   *
   * const resolver = new HeaderTenantResolver('X-Org-Id');
   * const orgId = await resolver.resolveOrgId(request);
   * console.log(orgId); // 'org-123'
   * ```
   *
   * @example
   * ```typescript
   * // Request without header, with default fallback
   * const request = new Request('https://api.example.com/accounts/1');
   *
   * const resolver = new HeaderTenantResolver('X-Org-Id', 'dev-org');
   * const orgId = await resolver.resolveOrgId(request);
   * console.log(orgId); // 'dev-org'
   * ```
   */
  async resolveOrgId(request: Request): Promise<string> {
    const orgId = request.headers.get(this.headerName);

    if (!orgId) {
      // If a default is configured, use it (local development support)
      if (this.defaultOrgId) {
        return Promise.resolve(this.defaultOrgId);
      }

      // No header and no default = authentication failure
      throw new MissingApiKeyError(
        `Missing required header: ${this.headerName}. ` +
          `Ensure your authentication layer sets this header.`
      );
    }

    return Promise.resolve(orgId);
  }
}
