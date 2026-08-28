/**
 * API Key Tenant Resolver for Ceves Event Sourcing Library
 *
 * This module implements tenant resolution via API keys stored in D1 database.
 * It provides B2B authentication by mapping API keys to organization IDs.
 *
 * Key Features:
 * - API key lookup via D1 database
 * - Support for key revocation
 * - Optional default org for development/testing
 * - Clear error messages for auth failures
 *
 * @packageDocumentation
 */

import { ITenantResolver } from './TenantResolver';
import { MissingApiKeyError, InvalidApiKeyError } from './errors';
import { createLogger } from '../logger';

const logger = createLogger({ component: 'ApiKeyTenantResolver' });

/**
 * Resolves organization ID from X-API-Key header using D1 database lookup.
 *
 * This implementation provides B2B authentication by:
 * 1. Extracting API key from X-API-Key request header
 * 2. Querying D1 database for matching non-revoked API key
 * 3. Returning the associated organization ID
 * 4. Supporting optional default org for development without API keys
 *
 * @example
 * ```typescript
 * // Production usage with API key requirement
 * const resolver = new ApiKeyTenantResolver(env.TENANT_DB);
 * const orgId = await resolver.resolveOrgId(request);
 *
 * // Development usage with default org fallback
 * const resolver = new ApiKeyTenantResolver(env.TENANT_DB, 'default-org');
 * const orgId = await resolver.resolveOrgId(request); // Returns 'default-org' if no API key
 * ```
 */
export class ApiKeyTenantResolver implements ITenantResolver {
  /**
   * Creates an API key tenant resolver.
   *
   * @param db - D1 database containing tenants and api_keys tables
   * @param defaultOrgId - Optional default organization ID for development/testing.
   *                       If provided and no X-API-Key header is present, this org ID is returned.
   *                       If not provided, missing API key throws MissingApiKeyError.
   */
  constructor(
    private db: D1Database,
    private defaultOrgId?: string
  ) {}

  /**
   * Resolve organization ID from X-API-Key header.
   *
   * Authentication flow:
   * 1. Check for X-API-Key header
   * 2. If missing and defaultOrgId set → return defaultOrgId (with warning log)
   * 3. If missing and no defaultOrgId → throw MissingApiKeyError
   * 4. Query D1: SELECT org_id FROM api_keys WHERE api_key = ? AND revoked = 0
   * 5. If found → return org_id
   * 6. If not found or revoked → throw InvalidApiKeyError
   *
   * @param request - Incoming HTTP request
   * @returns Promise resolving to organization ID
   * @throws {MissingApiKeyError} If X-API-Key header missing and no defaultOrgId
   * @throws {InvalidApiKeyError} If API key not found or revoked
   *
   * @example
   * ```typescript
   * // Request with valid API key
   * const request = new Request('https://example.com/api/accounts/123', {
   *   headers: { 'X-API-Key': 'sk_live_abc123' }
   * });
   * const orgId = await resolver.resolveOrgId(request); // Returns 'org-456'
   * ```
   */
  async resolveOrgId(request: Request): Promise<string> {
    const apiKey = request.headers.get('X-API-Key');

    // No API key provided
    if (!apiKey) {
      if (this.defaultOrgId) {
        // Development mode: use default org
        logger.warn('No X-API-Key header, using default org', {
          defaultOrgId: this.defaultOrgId,
        });
        return this.defaultOrgId;
      }

      // Production mode: require API key
      throw new MissingApiKeyError('X-API-Key header is required for authentication');
    }

    // Query database for API key
    try {
      const result = await this.db
        .prepare('SELECT org_id FROM api_keys WHERE api_key = ? AND revoked = 0')
        .bind(apiKey)
        .first<{ org_id: string }>();

      if (!result) {
        throw new InvalidApiKeyError('Invalid or revoked API key');
      }

      return result.org_id;
    } catch (error) {
      // Re-throw our custom errors
      if (error instanceof InvalidApiKeyError) {
        throw error;
      }

      // Wrap database errors
      throw new InvalidApiKeyError(
        `Failed to validate API key: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }
}
