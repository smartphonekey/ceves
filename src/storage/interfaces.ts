/**
 * Storage Interface Definitions for Ceves Event Sourcing Library
 *
 * This module defines the core storage abstractions for event and snapshot persistence.
 * The interface-based design enables multiple storage backend implementations (R2, D1, in-memory)
 * while maintaining a consistent API for event sourcing operations.
 *
 * Key Design Decisions:
 * - Generic type parameters enable type-safe usage with domain events
 * - Separate interfaces for events and snapshots (Single Responsibility Principle)
 * - afterVersion parameter enables incremental event loading for performance
 * - orgId at envelope level for multi-tenancy (Epic 9: ADR-008)
 *
 * @packageDocumentation
 */

import type { DomainEvent } from '../events/DomainEvent';

/**
 * Represents a stored event in the event store.
 *
 * StoredEvent is an infrastructure envelope that wraps domain events with metadata
 * required for event sourcing (version, timestamp, orgId). The domain event contains
 * pure business data, while the envelope manages infrastructure concerns.
 *
 * Events are immutable records of state changes that have occurred in the system.
 * Each event is uniquely identified by its aggregate type, aggregate ID, and version number.
 *
 * Generic Type Parameter:
 * - TEvent: The domain event type (must extend DomainEvent)
 * - Defaults to DomainEvent for backwards compatibility
 *
 * Architecture:
 * - Domain events (TEvent) contain only business data
 * - StoredEvent envelope adds infrastructure fields (version, timestamp, orgId)
 * - See ADR-008 in architecture.md for full design rationale
 *
 * @example
 * ```typescript
 * // Using with a specific domain event type
 * const event: StoredEvent<AccountOpenedEvent> = {
 *   aggregateType: 'account',
 *   aggregateId: 'acc-123',
 *   version: 1,
 *   type: 'AccountOpened',
 *   timestamp: '2025-11-14T10:30:00Z',
 *   orgId: 'org-456',
 *   event: new AccountOpenedEvent('john@example.com', 100)
 * };
 *
 * // Type-safe access to domain event
 * const owner = event.event.owner; // TypeScript knows this is AccountOpenedEvent
 * ```
 */
export interface StoredEvent<TEvent extends DomainEvent = DomainEvent> {
  /**
   * Type of aggregate this event belongs to (e.g., "account", "user").
   * Used to organize events into logical groups and storage paths.
   */
  aggregateType: string;

  /**
   * Unique identifier of the aggregate instance this event belongs to.
   * Combined with aggregateType, uniquely identifies an event stream.
   */
  aggregateId: string;

  /**
   * Sequential version number of this event within the aggregate's event stream.
   * Versions start at 1 and increment sequentially (1, 2, 3...).
   * Used for ordering events and enabling incremental loading.
   */
  version: number;

  /**
   * Event type name describing the state change (e.g., "AccountOpened", "MoneyDeposited").
   * Used to route events to appropriate event handlers during state restoration.
   *
   * Type is inferred from the domain event's type field (TEvent['type']).
   * This enables type-safe discriminated unions when working with multiple event types.
   */
  type: TEvent['type'];

  /**
   * ISO 8601 timestamp when the event was created.
   * Format: YYYY-MM-DDTHH:mm:ss.sssZ (e.g., "2025-11-14T10:30:00.000Z")
   *
   * Auto-set by the base command handler - domain event handlers don't set this.
   */
  timestamp: string;

  /**
   * Organization/tenant identifier for multi-tenancy isolation.
   *
   * Identifies which organization this event belongs to, enabling tenant isolation
   * in multi-tenant systems. This field is at the envelope level (not in domain event data)
   * because it's an infrastructure concern, not business logic.
   *
   * Auto-extracted from state/request by the base command handler.
   *
   * @see Epic 8 (Multitenancy) and ADR-008 in architecture.md
   */
  orgId: string;

  /**
   * The domain event containing pure business data.
   *
   * Domain events implement the DomainEvent interface and contain only business-relevant
   * fields (no version, timestamp, aggregateId, orgId). Infrastructure fields are managed
   * by this StoredEvent envelope.
   *
   * The type parameter TEvent enables type-safe access to event-specific fields:
   * - event.event.owner (for AccountOpenedEvent)
   * - event.event.amount (for MoneyDepositedEvent)
   *
   * @see Story 9.1 and ADR-008 for domain event architecture
   */
  event: TEvent;
}

/**
 * Represents a stored snapshot of aggregate state.
 *
 * Snapshots are point-in-time captures of aggregate state used to optimize state restoration.
 * Instead of replaying all events from the beginning, state restoration can load a snapshot
 * and only replay events that occurred after the snapshot was created.
 *
 * @example
 * ```typescript
 * const snapshot: StoredSnapshot = {
 *   aggregateType: 'account',
 *   aggregateId: 'acc-123',
 *   version: 42,
 *   timestamp: '2025-11-14T11:00:00Z',
 *   state: { id: 'acc-123', balance: 1500, transactions: 42 }
 * };
 * ```
 */
export interface StoredSnapshot {
  /**
   * Type of aggregate this snapshot belongs to (e.g., "account", "user").
   * Used to organize snapshots and match them with their event streams.
   */
  aggregateType: string;

  /**
   * Unique identifier of the aggregate instance this snapshot belongs to.
   * Combined with aggregateType, uniquely identifies a snapshot.
   */
  aggregateId: string;

  /**
   * Version number of the last event that was applied to create this snapshot.
   * When restoring state, only events with version > snapshot.version need to be replayed.
   */
  version: number;

  /**
   * ISO 8601 timestamp when the snapshot was created.
   * Format: YYYY-MM-DDTHH:mm:ss.sssZ (e.g., "2025-11-14T11:00:00.000Z")
   */
  timestamp: string;

  /**
   * Aggregate state at the time of snapshot creation.
   * Type is `unknown` to enable type-safe usage with generics (e.g., `StoredSnapshot<AccountState>`).
   * Consumers should validate/cast this state using Zod schemas or type guards.
   */
  state: unknown;
}

/**
 * Interface for event storage backend implementations.
 *
 * Implementations must provide persistence and retrieval of events with support for:
 * - Saving individual events to the event stream
 * - Loading all events for an aggregate
 * - Loading events after a specific version (for incremental loading)
 *
 * Events must always be returned in ascending version order (1, 2, 3...) to ensure
 * deterministic state restoration.
 *
 * @example
 * ```typescript
 * // Save an event with domain event
 * const domainEvent = new AccountOpenedEvent('john@example.com', 100);
 * await eventStore.save({
 *   aggregateType: 'account',
 *   aggregateId: 'acc-123',
 *   version: 1,
 *   type: domainEvent.type,
 *   timestamp: new Date().toISOString(),
 *   orgId: 'org-456',
 *   event: domainEvent
 * });
 *
 * // Load all events
 * const events = await eventStore.loadAll('account', 'acc-123');
 *
 * // Load events after version 10 (incremental)
 * const recentEvents = await eventStore.load('account', 'acc-123', 10);
 * ```
 */
export interface IEventStore {
  /**
   * Persist an event to the event store.
   *
   * Events are immutable and append-only. Once saved, events should never be modified or deleted.
   * The implementation must ensure the event is durably persisted before the Promise resolves.
   *
   * The event parameter accepts StoredEvent with any domain event type (defaults to DomainEvent).
   * In practice, the base command handler creates StoredEvent instances automatically.
   *
   * @param event - The event to persist (StoredEvent envelope containing domain event)
   * @returns Promise that resolves when the event is durably stored
   * @throws {EventWriteError} If the event cannot be persisted
   */
  save(event: StoredEvent): Promise<void>;

  /**
   * Load events for an aggregate with optional filtering by version.
   *
   * Returns events in ascending version order (1, 2, 3...).
   * If afterVersion is provided, only events with version > afterVersion are returned.
   * This enables incremental loading when combined with snapshots.
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @param afterVersion - Optional. Only return events with version > afterVersion
   * @param limit - Optional. Return at most this many events (the next page,
   *   in ascending version order). Combined with `afterVersion` this enables
   *   bounded, paged replay so a cold-start restore never has to pull an
   *   entire (possibly huge) event stream into memory at once.
   * @returns Promise resolving to ordered array of events, or empty array if none found
   * @throws {EventStoreError} If events cannot be retrieved
   *
   * @example
   * ```typescript
   * // Load all events
   * const allEvents = await store.load('account', 'acc-123');
   *
   * // Load only events after version 10 (e.g., after a snapshot)
   * const recentEvents = await store.load('account', 'acc-123', 10);
   *
   * // Load the next page of up to 100 events after version 10
   * const page = await store.load('account', 'acc-123', 10, 100);
   * ```
   */
  load(
    aggregateType: string,
    aggregateId: string,
    afterVersion?: number,
    limit?: number
  ): Promise<StoredEvent[]>;

  /**
   * Load all events for an aggregate.
   *
   * Convenience method equivalent to calling `load(aggregateType, aggregateId)` without afterVersion.
   * Returns events in ascending version order (1, 2, 3...).
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @returns Promise resolving to ordered array of all events, or empty array if none found
   * @throws {EventStoreError} If events cannot be retrieved
   */
  loadAll(aggregateType: string, aggregateId: string): Promise<StoredEvent[]>;
}

/**
 * Interface for snapshot storage backend implementations.
 *
 * Implementations must provide persistence and retrieval of aggregate state snapshots.
 * Snapshots are idempotent - saving a new snapshot for an aggregate overwrites any previous snapshot.
 *
 * Only the latest snapshot for each aggregate is retained. Older snapshots are discarded.
 *
 * @example
 * ```typescript
 * // Save a snapshot
 * await snapshotStore.save({
 *   aggregateType: 'account',
 *   aggregateId: 'acc-123',
 *   version: 42,
 *   timestamp: new Date().toISOString(),
 *   state: { id: 'acc-123', balance: 1500, transactions: 42 }
 * });
 *
 * // Load the latest snapshot
 * const snapshot = await snapshotStore.load('account', 'acc-123');
 * if (snapshot) {
 *   console.log(`Snapshot at version ${snapshot.version}`);
 * }
 * ```
 */
export interface ISnapshotStore {
  /**
   * Persist a snapshot, overwriting any previous snapshot for this aggregate.
   *
   * Snapshots are idempotent - calling save multiple times with different versions
   * will keep only the latest snapshot. Previous snapshots are discarded.
   *
   * The implementation must ensure the snapshot is durably persisted before the Promise resolves.
   *
   * @param snapshot - The snapshot to persist
   * @returns Promise that resolves when the snapshot is durably stored
   * @throws {SnapshotWriteError} If the snapshot cannot be persisted
   */
  save(snapshot: StoredSnapshot): Promise<void>;

  /**
   * Load the latest snapshot for an aggregate.
   *
   * Returns the most recent snapshot, or null if no snapshot exists.
   * The absence of a snapshot is not an error - it simply means state must be
   * restored from all events starting from the beginning.
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @returns Promise resolving to the latest snapshot, or null if no snapshot exists
   * @throws {SnapshotCorruptedError} If the snapshot data is invalid or corrupted
   * @throws {SnapshotStoreError} If the snapshot cannot be retrieved for other reasons
   */
  load(
    aggregateType: string,
    aggregateId: string
  ): Promise<StoredSnapshot | null>;
}
