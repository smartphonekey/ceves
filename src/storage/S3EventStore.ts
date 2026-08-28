/**
 * S3-based Event Store Implementation
 *
 * This module provides an AWS S3 implementation of the IEventStore interface.
 * Events are stored as individual JSON files in S3 with a structured path convention that enables
 * efficient querying and maintains event ordering through zero-padded version numbers.
 *
 * Storage Path Convention:
 * - Pattern: {aggregateType}/{aggregateId}/{paddedVersion}.json
 * - Example: account/acc-123/000000001.json, account/acc-123/000000042.json
 * - Version numbers are zero-padded to 9 digits for lexicographic sorting
 *
 * Key Design Decisions:
 * - One file per event (append-only, immutable)
 * - Zero-padded version numbers enable correct ordering via S3's list operation
 * - Dependency injection pattern (S3Client passed to constructor) for testability
 * - S3 errors wrapped in domain-specific error types with context
 * - Mirrors R2EventStore implementation for multi-cloud consistency
 *
 * @packageDocumentation
 */

import {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  GetObjectCommand,
} from '@aws-sdk/client-s3';
import type { IEventStore, StoredEvent, StoredSnapshot } from './interfaces';
import { EventStoreError, EventWriteError, SnapshotStoreError } from './errors';

/**
 * S3-based implementation of IEventStore and ISnapshotStore interfaces.
 *
 * Persists events to AWS S3 object storage using a structured file path convention.
 * Each event is stored as a separate JSON file, enabling append-only semantics and
 * efficient incremental loading.
 *
 * Snapshots are stored as single JSON files per aggregate, overwriting previous snapshots.
 *
 * @example
 * ```typescript
 * // In an AWS Lambda function
 * import { S3Client } from '@aws-sdk/client-s3';
 *
 * const s3 = new S3Client({ region: 'us-east-1' });
 * const eventStore = new S3EventStore(s3, 'my-events-bucket', 'my-snapshots-bucket');
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
 * await eventStore.saveSnapshot({
 *   aggregateType: 'account',
 *   aggregateId: 'acc-123',
 *   version: 100,
 *   timestamp: new Date().toISOString(),
 *   state: { id: 'acc-123', balance: 1500 }
 * });
 * ```
 */
export class S3EventStore implements IEventStore {
  /**
   * The S3 client used for S3 operations.
   */
  private readonly client: S3Client;

  /**
   * The S3 bucket name used for event storage.
   */
  private readonly eventsBucket: string;

  /**
   * The S3 bucket name used for snapshot storage.
   * If not provided, snapshots are stored in events bucket.
   */
  private readonly snapshotsBucket: string;

  /**
   * Create a new S3EventStore.
   *
   * @param client - The S3 client to use for S3 operations
   * @param eventsBucket - The S3 bucket name to use for event storage
   * @param snapshotsBucket - Optional separate bucket for snapshot storage
   *
   * @example
   * ```typescript
   * // In an AWS Lambda with separate buckets
   * import { S3Client } from '@aws-sdk/client-s3';
   *
   * const s3 = new S3Client({ region: process.env.AWS_REGION });
   * const eventStore = new S3EventStore(s3, process.env.EVENTS_BUCKET!, process.env.SNAPSHOTS_BUCKET);
   * ```
   *
   * @example
   * ```typescript
   * // Using single bucket for both events and snapshots
   * const eventStore = new S3EventStore(s3, process.env.EVENTS_BUCKET!);
   * ```
   */
  constructor(client: S3Client, eventsBucket: string, snapshotsBucket?: string) {
    this.client = client;
    this.eventsBucket = eventsBucket;
    this.snapshotsBucket = snapshotsBucket || eventsBucket;
  }

  /**
   * Persist an event or snapshot to S3 storage.
   *
   * For events:
   * - Serialized to JSON and written to S3 using path: `{aggregateType}/{aggregateId}/{paddedVersion}.json`
   * - Version numbers are zero-padded to 9 digits for lexicographic sorting
   *
   * For snapshots:
   * - Serialized to JSON and written to S3 using path: `{aggregateType}/{aggregateId}/snapshot.json`
   * - Overwrites any previous snapshot
   *
   * @param item - The event or snapshot to persist
   * @returns Promise that resolves when the item is durably stored in S3
   * @throws {EventWriteError} If an event cannot be written to S3
   * @throws {SnapshotStoreError} If a snapshot cannot be written to S3
   */
  async save(item: StoredEvent): Promise<void>;
  async save(item: StoredSnapshot): Promise<void>;
  async save(item: StoredEvent | StoredSnapshot): Promise<void> {
    if ('type' in item) {
      // It's a StoredEvent
      try {
        const key = this.buildEventKey(item.aggregateType, item.aggregateId, item.version);
        const jsonContent = JSON.stringify(item);

        const command = new PutObjectCommand({
          Bucket: this.eventsBucket,
          Key: key,
          Body: jsonContent,
          ContentType: 'application/json',
        });

        await this.client.send(command);
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

        const command = new PutObjectCommand({
          Bucket: this.snapshotsBucket,
          Key: key,
          Body: jsonContent,
          ContentType: 'application/json',
        });

        await this.client.send(command);
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
   * Lists all event files for the aggregate from S3 using a prefix-based query,
   * optionally filters to events after a specific version, fetches each event file,
   * and returns events in ascending version order.
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @param afterVersion - Optional. Only return events with version > afterVersion
   * @returns Promise resolving to ordered array of events, or empty array if none found
   * @throws {EventStoreError} If events cannot be retrieved from S3
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

      // List all event files for this aggregate (with pagination support)
      const allKeys: string[] = [];
      let continuationToken: string | undefined;

      do {
        const listCommand = new ListObjectsV2Command({
          Bucket: this.eventsBucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        });

        const response = await this.client.send(listCommand);

        if (response.Contents) {
          allKeys.push(
            ...response.Contents.map((obj) => obj.Key).filter(
              (key): key is string => Boolean(key)
            )
          );
        }

        continuationToken = response.NextContinuationToken;
      } while (continuationToken);

      // Filter by afterVersion if specified
      const relevantKeys = allKeys.filter((key) => {
        if (afterVersion === undefined) {
          return true;
        }

        // Extract version from key: "account/acc-123/000000042.json" → 42
        const versionStr = key.split('/')[2]?.replace('.json', '');
        if (!versionStr) {
          return false;
        }

        const version = parseInt(versionStr, 10);
        return version > afterVersion;
      });

      // Bounded paging: when a `limit` is given, sort by version and keep only
      // the next page so a cold-start restore fetches at most `limit` events
      // instead of the whole stream. (S3 lacks R2's `startAfter`, so we bound
      // after listing rather than during; the win is the bounded GET fan-out.)
      const pagedKeys = limit === undefined
        ? relevantKeys
        : [...relevantKeys]
            .sort((a, b) => {
              const va = parseInt(a.split('/')[2]?.replace('.json', '') ?? '', 10);
              const vb = parseInt(b.split('/')[2]?.replace('.json', '') ?? '', 10);
              return va - vb;
            })
            .slice(0, limit);

      // Fetch and parse each event file in parallel for performance
      const eventPromises = pagedKeys.map(async (key: string) => {
        const getCommand = new GetObjectCommand({
          Bucket: this.eventsBucket,
          Key: key,
        });

        const response = await this.client.send(getCommand);

        if (!response.Body) {
          return null;
        }

        const eventJson = await response.Body.transformToString();
        return JSON.parse(eventJson) as StoredEvent;
      });

      const events = (await Promise.all(eventPromises)).filter(
        (event): event is StoredEvent => event !== null
      );

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
   * @throws {EventStoreError} If events cannot be retrieved from S3
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
   * Build the S3 object key for an event.
   *
   * Constructs a structured path with zero-padded version number:
   * `{aggregateType}/{aggregateId}/{paddedVersion}.json`
   *
   * @param aggregateType - Type of aggregate
   * @param aggregateId - ID of aggregate instance
   * @param version - Event version number
   * @returns S3 object key
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
   * lexicographically. This enables S3's list operation to return events in
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
   * Save a snapshot to S3 storage (explicit method).
   *
   * @param snapshot - The snapshot to persist
   * @returns Promise that resolves when the snapshot is durably stored
   */
  async saveSnapshot(snapshot: StoredSnapshot): Promise<void> {
    return this.save(snapshot);
  }

  /**
   * Load the latest snapshot for an aggregate from S3 storage.
   *
   * @param aggregateType - Type of aggregate
   * @param aggregateId - Unique identifier of the aggregate instance
   * @returns Promise resolving to the latest snapshot, or null if no snapshot exists
   */
  async loadSnapshot(
    aggregateType: string,
    aggregateId: string
  ): Promise<StoredSnapshot | null> {
    try {
      const key = this.buildSnapshotKey(aggregateType, aggregateId);

      const getCommand = new GetObjectCommand({
        Bucket: this.snapshotsBucket,
        Key: key,
      });

      const response = await this.client.send(getCommand);

      if (!response.Body) {
        return null;
      }

      const snapshotJson = await response.Body.transformToString();
      const snapshot = JSON.parse(snapshotJson) as StoredSnapshot;

      return snapshot;
    } catch (error: unknown) {
      // NoSuchKey error means snapshot doesn't exist - this is not an error
      const err = error as { name?: string; $metadata?: { httpStatusCode?: number } };
      if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
        return null;
      }

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
   * Build the S3 object key for a snapshot.
   *
   * @param aggregateType - Type of aggregate
   * @param aggregateId - ID of aggregate instance
   * @returns S3 object key
   *
   * @internal
   */
  private buildSnapshotKey(aggregateType: string, aggregateId: string): string {
    return `${aggregateType}/${aggregateId}/snapshot.json`;
  }
}
