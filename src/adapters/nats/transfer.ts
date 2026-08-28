/**
 * Org transfer (aggregate "sale") — the seal event and its handler.
 *
 * Selling an aggregate to another tenant (the lock-sale use case) is a
 * FRESH-START transfer: the previous org's history does not follow the
 * aggregate, but its CREATION-TIME IDENTITY does — the id/number/uuid the
 * aggregate was created with must survive the sale.
 * `NatsAggregateNamespace.transferOut()` runs the operation:
 *
 * 1. **Seal** — append a final {@link AGGREGATE_TRANSFERRED_OUT_EVENT}
 *    audit event to the old home-org stream. `NatsEventStore` refuses any
 *    append after it, so the old stream is permanently closed (and the
 *    routed copy tells the seller's feed the aggregate left the org).
 * 2. **Purge** — delete the aggregate's KV state (snapshot, replay markers,
 *    projection cursors, alarm). Storage keys are org-free, so a stale
 *    snapshot would otherwise leak the seller's state to the buyer.
 * 3. **Seed** — re-append the aggregate's ORIGINAL creation event (same
 *    event type, same domain data, same original timestamp) as version 1
 *    of a brand-new stream under `ceves.events.<toOrg>.…`. The existing
 *    event handlers apply it, so the aggregate exists for the buyer with
 *    exactly the identity values it was created with (lock number, uuid,
 *    …) and nothing else — no keys, no accumulated state. On a re-sale the
 *    seed of the previous sale is carried forward, so the original
 *    creation data survives any number of transfers.
 * 4. **Repoint** — CAS-update the org-directory entry to the buyer's org.
 *    From here, commands and loads route to the new, seeded stream.
 * 5. **Evict** — drop the in-process actor so nothing serves stale state.
 *
 * Every step is idempotent (the seed reuses the original timestamp, so a
 * retried seed is byte-identical and absorbed), so a crashed transfer can
 * simply be retried — the seal event marks how far it got, and the repoint
 * only happens after the new stream is ready.
 */

import { EventHandler, findEventHandler, type IEventHandler } from '../../decorators/EventHandler';
import type { BaseState } from '../../schemas/State';
import type { DomainEvent } from '../../events/DomainEvent';

/**
 * Event type of the seal appended as the FINAL event of the old org's
 * stream when an aggregate is transferred out. `NatsEventStore` treats a
 * stream whose last event has this type as sealed and rejects appends.
 */
export const AGGREGATE_TRANSFERRED_OUT_EVENT = 'CevesAggregateTransferredOut';

/** Domain-event payload of the seal event (pure audit data). */
export interface AggregateTransferredOutEvent extends DomainEvent {
  type: typeof AGGREGATE_TRANSFERRED_OUT_EVENT;
  /** The org the aggregate is leaving (its home org until the transfer). */
  fromOrg: string;
  /** The org the aggregate was sold/transferred to. */
  toOrg: string;
}

/** What `transferOut` / `transferAggregate` report back. */
export interface AggregateTransferSummary {
  aggregateType: string;
  aggregateId: string;
  /** The org the aggregate belonged to when the transfer ran. */
  fromOrg: string;
  toOrg: string;
  /**
   * Version of the seal event closing the old stream, or `null` when there
   * was nothing to seal (the aggregate had a directory entry but no events).
   */
  sealedVersion: number | null;
  /**
   * True when the new org's stream was seeded with the aggregate's original
   * creation event (preserving its creation-time identity — id/number/uuid).
   * False only when the old stream had no events to carry forward.
   */
  seeded: boolean;
  /** True when the directory already pointed at `toOrg` (idempotent retry). */
  alreadyTransferred: boolean;
}

/**
 * Register the (state-unchanged) event handler for the seal event, scoped
 * to one aggregate type. Replaying a sealed stream — possible in the short
 * window between the seal append and the directory repoint, or via audit
 * tooling — must not fail on an unknown event type, so the NATS actor host
 * registers this for every aggregate type it hosts. A handler the app
 * itself registered for the event type wins (nothing is overwritten).
 */
export function registerTransferredOutHandler(aggregateType: string): void {
  if (findEventHandler(AGGREGATE_TRANSFERRED_OUT_EVENT, aggregateType)) return;

  class AggregateTransferredOutHandler
    implements IEventHandler<BaseState, AggregateTransferredOutEvent>
  {
    eventType = AGGREGATE_TRANSFERRED_OUT_EVENT;
    aggregateType = aggregateType;

    /** Audit-only: the seal changes no aggregate state. */
    apply(state: BaseState): BaseState {
      return state;
    }
  }

  EventHandler(AggregateTransferredOutHandler);
}

/**
 * Property stamped on the `VersionConflictError` the event store throws
 * when a stream is SEALED (the aggregate was transferred to another org).
 * Duck-typed rather than a subclass, matching how the adapter detects
 * JetStream errors — it survives bundling and cross-package boundaries.
 */
const SEALED_STREAM_FLAG = 'cevesSealedStream';

/** Stamp {@link isSealedStreamConflict}'s marker on a sealed-stream rejection. */
export function markSealedStreamConflict<E extends object>(error: E): E {
  Object.defineProperty(error, SEALED_STREAM_FLAG, { value: true, enumerable: false });
  return error;
}

/**
 * True when a rejection means "this stream is sealed" — the aggregate
 * lives under another org now. The actor host treats it as the signal to
 * finish an interrupted transfer (the seal event in the log carries the
 * target org), turning what would otherwise be a permanently stuck
 * aggregate into a one-time conflict that self-heals.
 */
export function isSealedStreamConflict(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as Record<string, unknown>)[SEALED_STREAM_FLAG] === true
  );
}
