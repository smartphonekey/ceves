/**
 * Tenant-Specific Errors for Ceves Event Sourcing Library
 *
 * This module defines error classes for tenant authentication and authorization failures.
 * These errors provide clear, specific feedback when tenant resolution or access control fails.
 *
 * @packageDocumentation
 */

import { CevesError } from '../errors/CevesError';

/**
 * Error thrown when API key header is missing from request.
 *
 * This error indicates that the request did not include required authentication credentials.
 * Returns HTTP 401 Unauthorized responses.
 *
 * @example
 * ```typescript
 * if (!request.headers.get('X-API-Key')) {
 *   throw new MissingApiKeyError('X-API-Key header required');
 * }
 * ```
 */
export class MissingApiKeyError extends CevesError {
  constructor(message: string = 'X-API-Key header is required for authentication') {
    super(message, 401);
  }
}

/**
 * Error thrown when API key is invalid or revoked.
 *
 * This error indicates that the provided API key was not found in the tenant database,
 * or has been revoked, or has expired.
 * Returns HTTP 401 Unauthorized responses.
 *
 * @example
 * ```typescript
 * const result = await db.query('SELECT org_id FROM api_keys WHERE api_key = ?', [apiKey]);
 * if (!result || result.revoked) {
 *   throw new InvalidApiKeyError('Invalid or revoked API key');
 * }
 * ```
 */
export class InvalidApiKeyError extends CevesError {
  constructor(message: string = 'Invalid or revoked API key') {
    super(message, 401);
  }
}

/**
 * Error thrown when attempting to access aggregate belonging to different organization.
 *
 * This error indicates a tenant isolation violation - the request attempted to access
 * an aggregate that belongs to a different organization than the authenticated tenant.
 * Returns HTTP 403 Forbidden responses.
 *
 * @example
 * ```typescript
 * if (state.orgId !== requestOrgId) {
 *   throw new UnauthorizedAccessError(
 *     `Access denied: aggregate belongs to org '${state.orgId}'`
 *   );
 * }
 * ```
 */
export class UnauthorizedAccessError extends CevesError {
  constructor(message: string) {
    super(message, 403);
  }
}
