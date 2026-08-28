/**
 * S3-based Snapshot Store Implementation
 *
 * This module provides an AWS S3 implementation of the ISnapshotStore interface.
 * Snapshots are stored as individual JSON files in S3 with a simple single-file-per-aggregate pattern.
 *
 * Storage Path Convention:
 * - Pattern: {aggregateType}/{aggregateId}/snapshot.json
 * - Example: account/acc-123/snapshot.json
 * - Single file per aggregate (overwrites on each save)
 *
 * Key Design Decisions:
 * - Latest snapshot wins (idempotent overwrites)
 * - Missing snapshots return null (expected case, not an error)
 * - Dependency injection pattern (S3Client passed to constructor) for testability
 * - S3 errors wrapped in domain-specific error types with context
 *
 * NOTE: This is the AWS-Lambda-side snapshot store. The original CF-side
 * counterparts (R2SnapshotStore / D1SnapshotStore) were removed because the
 * DO variant of Ceves stores state in DurableObjectState.storage and never
 * needed an out-of-band snapshot store. The Lambda path *does* need one
 * because there is no equivalent built-in per-aggregate K/V store there.
 *
 * @packageDocumentation
 */

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from '@aws-sdk/client-s3';
import type { ISnapshotStore, StoredSnapshot } from './interfaces';
import {
  SnapshotStoreError,
  SnapshotWriteError,
  SnapshotCorruptedError,
} from './errors';

/**
 * S3-based implementation of the ISnapshotStore interface.
 *
 * Persists snapshots to AWS S3 object storage using a simple single-file pattern.
 * Each aggregate has one snapshot file that is overwritten on each save (latest wins).
 *
 * @example
 * ```typescript
 * // In an AWS Lambda
 * import { S3Client } from '@aws-sdk/client-s3';
 *
 * const s3 = new S3Client({ region: 'us-east-1' });
 * const snapshotStore = new S3SnapshotStore(s3, 'my-snapshots-bucket');
 *
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
 *   console.log(`Loaded snapshot at version ${snapshot.version}`);
 * } else {
 *   console.log('No snapshot exists yet');
 * }
 * ```
 */
export class S3SnapshotStore implements ISnapshotStore {
  /**
   * The S3 client used for S3 operations.
   */
  private readonly client: S3Client;

  /**
   * The S3 bucket name used for snapshot storage.
   */
  private readonly bucketName: string;

  /**
   * Create a new S3SnapshotStore.
   *
   * @param client - The S3 client to use for S3 operations
   * @param bucketName - The S3 bucket name to use for snapshot storage
   *
   * @example
   * ```typescript
   * // In an AWS Lambda
   * import { S3Client } from '@aws-sdk/client-s3';
   *
   * const s3 = new S3Client({ region: process.env.AWS_REGION });
   * const snapshotStore = new S3SnapshotStore(s3, process.env.SNAPSHOTS_BUCKET!);
   * ```
   */
  constructor(client: S3Client, bucketName: string) {
    this.client = client;
    this.bucketName = bucketName;
  }

  /**
   * Persist a snapshot to S3 storage.
   *
   * The snapshot is serialized to JSON and written to S3 using a single-file pattern:
   * `{aggregateType}/{aggregateId}/snapshot.json`
   *
   * This operation is idempotent - saving a new snapshot overwrites any existing snapshot
   * for the same aggregate (latest version wins).
   *
   * @param snapshot - The snapshot to persist
   * @returns Promise that resolves when the snapshot is durably stored in S3
   * @throws {SnapshotWriteError} If the snapshot cannot be written to S3
   *
   * @example
   * ```typescript
   * await snapshotStore.save({
   *   aggregateType: 'account',
   *   aggregateId: 'acc-123',
   *   version: 42,
   *   timestamp: new Date().toISOString(),
   *   state: { id: 'acc-123', balance: 1500 }
   * });
   * // Snapshot saved to: account/acc-123/snapshot.json
   * ```
   */
  async save(snapshot: StoredSnapshot): Promise<void> {
    try {
      const key = this.buildSnapshotKey(
        snapshot.aggregateType,
        snapshot.aggregateId
      );
      const jsonContent = JSON.stringify(snapshot);

      const command = new PutObjectCommand({
        Bucket: this.bucketName,
        Key: key,
        Body: jsonContent,
        ContentType: 'application/json',
      });

      await this.client.send(command);

      return Promise.resolve();
    } catch (error) {
      throw new SnapshotWriteError(
        `Failed to save snapshot for ${snapshot.aggregateType}/${snapshot.aggregateId} v${snapshot.version}`,
        {
          aggregateType: snapshot.aggregateType,
          aggregateId: snapshot.aggregateId,
          version: snapshot.version,
          cause: error instanceof Error ? error : new Error(String(error)),
        }
      );
    }
  }

  /**
   * Load the latest snapshot for an aggregate.
   *
   * Fetches the snapshot file from S3 and parses it. If no snapshot exists,
   * returns null (this is an expected case, not an error).
   *
   * @param aggregateType - Type of aggregate (e.g., "account")
   * @param aggregateId - Unique identifier of the aggregate instance
   * @returns Promise resolving to the latest snapshot, or null if no snapshot exists
   * @throws {SnapshotCorruptedError} If the snapshot data is invalid or corrupted
   * @throws {SnapshotStoreError} If the snapshot cannot be retrieved for other reasons
   *
   * @example
   * ```typescript
   * const snapshot = await snapshotStore.load('account', 'acc-123');
   *
   * if (snapshot) {
   *   console.log(`Loaded snapshot at version ${snapshot.version}`);
   *   // Use snapshot.state to restore aggregate state
   * } else {
   *   console.log('No snapshot exists - will restore from all events');
   * }
   * ```
   */
  async load(
    aggregateType: string,
    aggregateId: string
  ): Promise<StoredSnapshot | null> {
    try {
      const key = this.buildSnapshotKey(aggregateType, aggregateId);

      const command = new GetObjectCommand({
        Bucket: this.bucketName,
        Key: key,
      });

      const response = await this.client.send(command);

      // No body means the object doesn't exist
      if (!response.Body) {
        return Promise.resolve(null);
      }

      // Parse the snapshot JSON
      const snapshotJson = await response.Body.transformToString();

      try {
        const snapshot = JSON.parse(snapshotJson) as StoredSnapshot;
        return Promise.resolve(snapshot);
      } catch (parseError) {
        // JSON parse failure indicates corrupted data
        throw new SnapshotCorruptedError(
          `Snapshot data is corrupted for ${aggregateType}/${aggregateId}`,
          {
            aggregateType,
            aggregateId,
            cause:
              parseError instanceof Error
                ? parseError
                : new Error(String(parseError)),
          }
        );
      }
    } catch (error) {
      // Re-throw SnapshotCorruptedError as-is
      if (error instanceof SnapshotCorruptedError) {
        throw error;
      }

      // S3 NoSuchKey error means no snapshot exists (expected case)
      if (
        error &&
        typeof error === 'object' &&
        'name' in error &&
        error.name === 'NoSuchKey'
      ) {
        return Promise.resolve(null);
      }

      // Wrap other S3 errors in SnapshotStoreError
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
   * Constructs a simple single-file path:
   * `{aggregateType}/{aggregateId}/snapshot.json`
   *
   * @param aggregateType - Type of aggregate
   * @param aggregateId - ID of aggregate instance
   * @returns S3 object key
   *
   * @example
   * ```typescript
   * buildSnapshotKey('account', 'acc-123')
   * // Returns: "account/acc-123/snapshot.json"
   * ```
   */
  private buildSnapshotKey(
    aggregateType: string,
    aggregateId: string
  ): string {
    return `${aggregateType}/${aggregateId}/snapshot.json`;
  }
}
