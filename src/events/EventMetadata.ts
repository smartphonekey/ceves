/**
 * EventMetadata - Infrastructure data for event handlers
 *
 * EventMetadata provides infrastructure fields that are passed separately to event handlers
 * alongside domain events. This maintains clean separation of concerns:
 * - Domain events contain ONLY business data
 * - EventMetadata contains infrastructure fields (aggregateId, version, timestamp, orgId)
 *
 * This separation enables:
 * - Cleaner domain event definitions focused on business intent
 * - Clear architectural boundaries between business logic and infrastructure
 * - Better type safety and maintainability
 *
 * @packageDocumentation
 */

/**
 * Infrastructure metadata passed to event handlers
 *
 * Event handlers receive both a domain event (pure business data) and EventMetadata
 * (infrastructure fields) as separate parameters. This keeps business logic clean
 * while providing access to necessary infrastructure information.
 *
 * All fields in EventMetadata are automatically extracted from the StoredEvent envelope
 * by the base framework classes. Event handlers never need to construct or manage this
 * metadata directly.
 *
 * @example
 * ```typescript
 * // Event handler signature with EventMetadata
 * class AccountOpenedHandler implements IEventHandler<AccountState, AccountOpenedEvent> {
 *   apply(
 *     state: AccountState | null,
 *     event: AccountOpenedEvent,         // Pure business data
 *     metadata: EventMetadata            // Infrastructure data
 *   ): Omit<AccountState, 'version' | 'orgId'> {
 *     // Access infrastructure fields from metadata
 *     const { aggregateId, version, timestamp, orgId } = metadata;
 *
 *     // Use business data from event
 *     return {
 *       id: aggregateId,
 *       owner: event.owner,
 *       balance: event.initialDeposit,
 *       createdAt: timestamp
 *       // version and orgId auto-set by framework
 *     };
 *   }
 * }
 * ```
 *
 * @see {@link StoredEvent} - Source of metadata fields (infrastructure envelope)
 * @see {@link IEventHandler} - Event handler interface that receives metadata
 * @see {@link DomainEvent} - Pure business events without infrastructure fields
 */
export interface EventMetadata {
  /**
   * Unique identifier of the aggregate instance this event belongs to
   *
   * Combined with aggregateType, uniquely identifies an event stream.
   * Extracted from StoredEvent envelope.
   *
   * @example "account-123", "user-456"
   */
  aggregateId: string;

  /**
   * Sequential version number of this event within the aggregate's event stream
   *
   * Versions start at 1 and increment sequentially (1, 2, 3...).
   * Used for ordering events and tracking aggregate state progression.
   * Extracted from StoredEvent envelope.
   *
   * @example 1, 2, 3, 42
   */
  version: number;

  /**
   * ISO 8601 timestamp when the event was created
   *
   * Format: YYYY-MM-DDTHH:mm:ss.sssZ (e.g., "2025-11-22T10:30:00.000Z")
   * Auto-set by the base command handler when events are persisted.
   * Extracted from StoredEvent envelope.
   *
   * @example "2025-11-22T10:30:00.000Z"
   */
  timestamp: string;

  /**
   * Organization/tenant identifier for multi-tenancy isolation
   *
   * Identifies which organization this event belongs to, enabling tenant isolation
   * in multi-tenant systems. This field is at the envelope level (not in domain event)
   * because it's an infrastructure concern, not business logic.
   *
   * Auto-extracted from state or request by the base command handler.
   * Extracted from StoredEvent envelope.
   *
   * @example "org-456", "tenant-abc"
   * @see Epic 8 (Multitenancy) and ADR-008 in architecture.md
   */
  orgId: string;
}
