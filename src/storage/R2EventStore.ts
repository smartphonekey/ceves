/**
 * R2-based Event Store Implementation
 *
 * This module provides a Cloudflare R2 object storage implementation of the IEventStore interface.
 * Events are stored as individual JSON files in R2 with a structured path convention that enables
 * efficient querying and maintains event ordering through zero-padded version numbers.
 *
 * Storage Path Convention:
 * - Pattern: {aggregateType}/{aggregateId}/{paddedVersion}.json
 * - Example: account/acc-123/000000001.json, account/acc-123/000000042.json
 * - Version numbers are zero-padded to 9 digits for lexicographic sorting
 *
 * Key Design Decisions:
 * - One file per event (append-only, immutable)
 * - Zero-padded version numbers enable correct ordering via R2's list operation
 * - Dependency injection pattern (R2Bucket passed to constructor) for testability
 * - R2 errors wrapped in domain-specific error types with context
 *
 * @packageDocumentation
 */

import type { R2Bucket } from '@cloudflare/workers-types';
import type { IEventStore, StoredEvent, StoredSnapshot } from './interfaces';
import { EventStoreError, EventWriteError, SnapshotStoreError } from './errors';

/**
 * R2-based implementation of IEventStore and ISnapshotStore interfaces.
 *
 * Persists events to Cloudflare R2 object storage using a structured file path convention.
 * Each event is stored as a separate JSON file, enabling append-only semantics and
 * efficient incremental loading.
 *
 * Snapshots are stored as single JSON files per aggregate, overwriting previous snapshots.
 * This enables fast state restoration by loading a snapshot and replaying only incremental events.
 *
 * @example
 * ```typescript
 * // In a Cloudflare Worker with R2 binding
 * const eventStore = new R2EventStore(env.EVENT_BUCKET, env.SNAPSHOTS_BUCKET);
 *
 * // Save an event
 * await eventStore.save({
 *   aggregateType: 'account',
 *   aggregateId: 'acc-123',
 *   version: 1,
 *   type: 'AccountCreated',
 *   timestamp: new Date().toISOString(),
 *   data: { initialBalance: 0 }
 * });
 *
 * // Load all events for an aggregate
 * const events = await eventStore.loadAll('account', 'acc-123');
 *
 * // Load events after version 10 (incremental loading)
 * const recentEvents = await eventStore.load('account', 'acc-123', 10);
 *
 * // Save a snapshot
 * await eventStore.save({
 *   aggregateType: 'account',
 *   aggregateId: 'acc-123',
 *   version: 100,
 *   timestamp: new Date().toISOString(),
 *   state: { id: 'acc-123', balance: 1500 }
 * });
 * ```
 */
export class R2EventStore implements IEventStore {
  /**
   * The R2 bucket used for event storage.
   */
  private readonly eventsBucket: R2Bucket;

  /**
   * The R2 bucket used for snapshot storage.
   * If not provided, snapshots are stored in the events bucket.
   */
  private readonly snapshotsBucket: R2Bucket;

  /**
   * Create a new R2EventStore.
   *
   * @param eventsBucket - The R2 bucket binding to use for event storage
   * @param snapshotsBucket - Optional separate R2 bucket for snapshot storage.
   *                          If not provided, snapshots are stored in eventsBucket.
   *
   * @example
   * ```typescript
   * // In a Cloudflare Worker with separate buckets
   * export default {
   *   async fetch(request, env) {
   *     const eventStore = new R2EventStore(env.EVENTS_BUCKET, env.SNAPSHOTS_BUCKET);
   *     // Use eventStore...
   *   }
   * }
   * ```
   *
   * @example
   * ```typescript
   * // Using single bucket for both events and snapshots
   * const eventStore = new R2EventStore(env.EVENTS_BUCKET);
   * ```
   */
  constructor(eventsBucket: R2Bucket, snapshotsBucket?: R2Bucket) {
    this.eventsBucket = eventsBucket;
    this.snapshotsBucket = snapshotsBucket || eventsBucket;
  }

  /**
   * Persist an event or snapshot to R2 storage.
   *
   * For events:
   * - Serialized to JSON and written to R2 using path: `{aggregateType}/{aggregateId}/{paddedVersion}.json`
   * - Version numbers are zero-padded to 9 digits for lexicographic sorting
   *
   * For snapshots:
   * - Serialized to JSON and written to R2 using path: `{aggregateType}/{aggregateId}/snapshot.json`
   * - Overwrites any previous snapshot
   *
   * @param item - The event or snapshot to persist
   * @returns Promise that resolves when the item is durably stored in R2
   * @throws {EventWriteError} If an event cannot be written to R2
   * @throws {SnapshotStoreError} If a snapshot cannot be written to R2
   */
  async save(item: StoredEvent): Promise<void>;
  async save(item: StoredSnapshot): Promise<void>;
  async save(item: StoredEvent | StoredSnapshot): Promise<void> {
    // Discriminate between StoredEvent and StoredSnapshot
    // StoredEvent has 'type' field, StoredSnapshot has 'state' field
    if ('type' in item) {
      // It's a StoredEvent
      try {
        const key = this.buildEventKey(item.aggregateType, item.aggregateId, item.version);
        const jsonContent = JSON.stringify(item);

        await this.eventsBucket.put(key, jsonContent);

        return Promise.resolve();
      } catch (error) {
        throw new EventWriteError(
          `Failed to save event for ${item.aggregateType}/${item.aggregateId} v${item.version}`,
          {
            aggregateType: item.aggregateType,
            aggregateId: item.aggregateId,
            version: item.version,
            cause: error instanceof Error ? error : new Error(String(error)),
          }
        );
      }
    } else {
      // It's a StoredSnapshot
      try {
        const key = this.buildSnapshotKey(item.aggregateType, item.aggregateId);
        const jsonContent = JSON.stringify(item);

        await this.snapshotsBucket.put(key, jsonContent, {
          httpMetadata: {
            contentType: 'application/json',
          },
        });

        return Promise.resolve();
      } catch (error) {
        throw new SnapshotStoreError(
          `Failed to save snapshot for ${item.aggregateType}/${item.aggregateId} v${item.version}`,
          {
            aggregateType: item.aggregateType,
            aggregateId: item.aggregateId,
            cause: error instanceof Error ? error : new Error(String(error)),
          }
        );
      }
    }
  }

  /**
   * Load events for an aggregate with optional filtering by version.
   *
   * Lists all event files for the aggregate from R2 using a prefix-based query,
   * optionally filters to events after a specific version, fetches each event file,
   * and returns events in ascending version order.
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @param afterVersion - Optional. Only return events with version > afterVersion
   * @returns Promise resolving to ordered array of events, or empty array if none found
   * @throws {EventStoreError} If events cannot be retrieved from R2
   *
   * @example
   * ```typescript
   * // Load all events
   * const allEvents = await eventStore.load('account', 'acc-123');
   *
   * // Load only events after version 10 (e.g., after a snapshot)
   * const recentEvents = await eventStore.load('account', 'acc-123', 10);
   * // Returns events with version 11, 12, 13, ...
   * ```
   */
  async load(
    aggregateType: string,
    aggregateId: string,
    afterVersion?: number,
    limit?: number
  ): Promise<StoredEvent[]> {
    try {
      const prefix = `${aggregateType}/${aggregateId}/`;

      // When a `limit` is given, page natively in R2: `startAfter` (derived
      // from `afterVersion`, exploiting the zero-padded keys' lexicographic =
      // version ordering) + `limit` fetch only the next page's keys. This never
      // lists or loads the whole stream into memory, and walks past R2's
      // 1000-object list cap one page at a time. Without `limit` we keep the
      // original "list everything" behaviour for backward compatibility.
      const listOptions: { prefix: string; limit?: number; startAfter?: string } = { prefix };
      if (limit !== undefined) {
        listOptions.limit = limit;
        if (afterVersion !== undefined && afterVersion > 0) {
          listOptions.startAfter = this.buildEventKey(aggregateType, aggregateId, afterVersion);
        }
      }

      const listed = await this.eventsBucket.list(listOptions);

      // Keep only real event objects after `afterVersion`. The version is
      // parsed from the key ("account/acc-123/000000042.json" → 42); non-event
      // keys under the same prefix (e.g. a co-located "snapshot.json", which
      // sorts after the numeric keys) parse to NaN and are skipped.
      const relevantObjects = listed.objects.filter((obj) => {
        const versionStr = obj.key.split('/')[2]?.replace('.json', '');
        if (!versionStr) {
          return false;
        }

        const version = parseInt(versionStr, 10);
        if (Number.isNaN(version)) {
          return false;
        }

        if (afterVersion === undefined) {
          return true;
        }

        return version > afterVersion;
      });

      // Fetch and parse each event file
      const events: StoredEvent[] = [];
      for (const obj of relevantObjects) {
        const eventObj = await this.eventsBucket.get(obj.key);
        if (!eventObj) {
          // Object was deleted between list and get - skip it
          continue;
        }

        const eventJson = await eventObj.text();
        const event = JSON.parse(eventJson) as StoredEvent;
        events.push(event);
      }

      // Sort by version ascending to ensure deterministic ordering
      events.sort((a, b) => a.version - b.version);

      return Promise.resolve(events);
    } catch (error) {
      throw new EventStoreError(
        `Failed to load events for ${aggregateType}/${aggregateId}`,
        {
          aggregateType,
          aggregateId,
          cause: error instanceof Error ? error : new Error(String(error)),
        }
      );
    }
  }

  /**
   * Load all events for an aggregate.
   *
   * Convenience method equivalent to calling `load(aggregateType, aggregateId)` without afterVersion.
   * Returns events in ascending version order (1, 2, 3...).
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @returns Promise resolving to ordered array of all events, or empty array if none found
   * @throws {EventStoreError} If events cannot be retrieved from R2
   *
   * @example
   * ```typescript
   * const events = await eventStore.loadAll('account', 'acc-123');
   * console.log(`Loaded ${events.length} events`);
   * ```
   */
  async loadAll(
    aggregateType: string,
    aggregateId: string
  ): Promise<StoredEvent[]> {
    return this.load(aggregateType, aggregateId);
  }

  /**
   * Build the R2 object key for an event.
   *
   * Constructs a structured path with zero-padded version number:
   * `{aggregateType}/{aggregateId}/{paddedVersion}.json`
   *
   * @param aggregateType - Type of aggregate
   * @param aggregateId - ID of aggregate instance
   * @param version - Event version number
   * @returns R2 object key
   *
   * @example
   * ```typescript
   * buildEventKey('account', 'acc-123', 42)
   * // Returns: "account/acc-123/000000042.json"
   * ```
   */
  private buildEventKey(
    aggregateType: string,
    aggregateId: string,
    version: number
  ): string {
    const paddedVersion = this.padVersion(version);
    return `${aggregateType}/${aggregateId}/${paddedVersion}.json`;
  }

  /**
   * Zero-pad a version number to 9 digits for lexicographic sorting.
   *
   * Converts a version number to a zero-padded string that sorts correctly
   * lexicographically. This enables R2's list operation to return events in
   * the correct order without additional sorting.
   *
   * @param version - Version number to pad
   * @returns Zero-padded version string (9 digits)
   *
   * @example
   * ```typescript
   * padVersion(1)    // Returns: "000000001"
   * padVersion(42)   // Returns: "000000042"
   * padVersion(1337) // Returns: "000001337"
   * ```
   */
  private padVersion(version: number): string {
    return version.toString().padStart(9, '0');
  }

  /**
   * Save a snapshot to R2 storage (explicit method for clarity).
   *
   * Alias for save(snapshot) that makes intent clearer.
   *
   * @param snapshot - The snapshot to persist
   * @returns Promise that resolves when the snapshot is durably stored
   */
  async saveSnapshot(snapshot: StoredSnapshot): Promise<void> {
    return this.save(snapshot);
  }

  /**
   * Load the latest snapshot for an aggregate from R2 storage (explicit method).
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @returns Promise resolving to the latest snapshot, or null if no snapshot exists
   */
  async loadSnapshot(
    aggregateType: string,
    aggregateId: string
  ): Promise<StoredSnapshot | null> {
    try {
      const key = this.buildSnapshotKey(aggregateType, aggregateId);
      const snapshotObj = await this.snapshotsBucket.get(key);

      if (!snapshotObj) {
        // No snapshot exists - this is not an error
        return null;
      }

      const snapshotJson = await snapshotObj.text();
      const snapshot = JSON.parse(snapshotJson) as StoredSnapshot;

      return snapshot;
    } catch (error) {
      throw new SnapshotStoreError(
        `Failed to load snapshot for ${aggregateType}/${aggregateId}`,
        {
          aggregateType,
          aggregateId,
          cause: error instanceof Error ? error : new Error(String(error)),
        }
      );
    }
  }

  /**
   * Build the R2 object key for a snapshot.
   *
   * Constructs a simple path for snapshots:
   * `{aggregateType}/{aggregateId}/snapshot.json`
   *
   * @param aggregateType - Type of aggregate
   * @param aggregateId - ID of aggregate instance
   * @returns R2 object key
   *
   * @internal
   *
   * @example
   * ```typescript
   * buildSnapshotKey('account', 'acc-123')
   * // Returns: "account/acc-123/snapshot.json"
   * ```
   */
  private buildSnapshotKey(aggregateType: string, aggregateId: string): string {
    return `${aggregateType}/${aggregateId}/snapshot.json`;
  }
}
