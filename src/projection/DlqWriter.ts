/**
 * Dead-letter queue writer for projection failures (AA-93).
 *
 * When a projector throws (e.g., SpacetimeDB reducer rejected an event), the
 * dispatcher writes a record describing the failed delivery to a DLQ so that
 * (a) failures are durably observable even when Sentry capture cannot run
 * synchronously, and (b) operators can replay or audit failed events.
 *
 * Implementations are app-owned (e.g., R2-backed in b2c-api). Ceves only
 * declares the interface and the record shape so the dispatcher can call
 * `dlq.write(record)` without knowing the storage backend.
 *
 * Errors thrown by `write()` MUST be caught by the caller — a DLQ write
 * failure should never cascade into the projector pipeline. The dispatcher
 * already catches and logs.
 */

/**
 * One DLQ record — the failed projection event plus the rejection details.
 *
 * Field naming matches the ProjectedEvent shape so a record round-trips
 * cleanly: `aggregateType`, `aggregateId`, `version`, `eventType`, `payload`.
 */
export interface DlqRecord {
  /** ISO-8601 timestamp the failure was recorded (writer fills this in if absent). */
  timestamp: string;
  /** Projector id that rejected the event (e.g., 'spacetimedb'). */
  projector: string;
  /** Domain event type (e.g., 'KeyAdded'). */
  eventType: string;
  /** Aggregate type (e.g., 'LockAggregate'). */
  aggregateType: string;
  /** Aggregate id (the human-readable id, not the DO hex hash). */
  aggregateId: string;
  /** Aggregate version of this event. */
  version: number;
  /** The full event-data payload (whatever the handler emitted). */
  payload: unknown;
  /** Rejection details — message is required, status/stack optional. */
  error: { message: string; status?: number; stack?: string };
}

/**
 * Writer interface implemented by DLQ backends (e.g., R2, D1).
 *
 * Idempotency is the implementation's responsibility. The dispatcher calls
 * `write()` once per failure; if the implementation chooses a key per
 * (projector, aggregate, version), the same record overwriting on retry is
 * harmless.
 */
export interface DlqWriter {
  write(record: DlqRecord): Promise<void>;

  /**
   * Delete the DLQ copy of a previously-failed event once it has been
   * successfully (re)delivered. Best-effort and idempotent: deleting a key
   * that doesn't exist (no DLQ record was ever written, or it was already
   * deleted) is a no-op. This relies on a deterministic, date-free key per
   * (projector, aggregateType, aggregateId, version) so a later success can
   * find and remove the record a first failure wrote — keeping the DLQ from
   * overstating failures that eventually went through.
   */
  delete(record: Pick<DlqRecord, 'projector' | 'aggregateType' | 'aggregateId' | 'version'>): Promise<void>;
}
