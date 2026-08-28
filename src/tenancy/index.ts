/**
 * Tenancy module exports for Ceves Event Sourcing Library
 *
 * This module provides tenant resolution interfaces and implementations
 * for multi-tenant event sourcing applications.
 *
 * @packageDocumentation
 */

export type { ITenantResolver } from './TenantResolver';
export { ApiKeyTenantResolver } from './ApiKeyTenantResolver';
export { HeaderTenantResolver } from './HeaderTenantResolver';
export {
  MissingApiKeyError,
  InvalidApiKeyError,
  UnauthorizedAccessError,
} from './errors';
