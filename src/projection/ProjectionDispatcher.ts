import type { IEventProjector, ProjectedEvent } from './IEventProjector';
import type { StoredEvent } from '../storage/interfaces';
import type { DlqRecord, DlqWriter } from './DlqWriter';
import { getProjectors } from './ProjectorRegistry';
import { loadCursor, saveCursor } from './ProjectionCursorStore';
import { createLogger } from '../logger';

// Raised 5 → 15 (cold-start hardening). A SpacetimeDB Maincloud DB auto-suspends
// when idle, and a cold-start resume can outlast several short retries. With the
// capped exponential backoff below, 15 attempts span many hours — long enough to
// outlast any realistic cold start — while still eventually giving up on a
// genuinely poison event (e.g. an EventBridge `MalformedDetail` that will never
// succeed) so it can't retry forever.
const MAX_FAILURES = 15;
// BASE retry delay. Each subsequent failure doubles the delay (see
// computeBackoffMs), capped at MAX_BACKOFF_MS.
const RETRY_DELAY_MS = 30_000;
// Upper bound on the exponential backoff so a long-stranded event still gets
// re-checked at least every 30 minutes.
const MAX_BACKOFF_MS = 1_800_000;

const logger = createLogger({ component: 'ProjectionDispatcher' });

/**
 * Exponential backoff for alarm-driven projection retries, capped at
 * MAX_BACKOFF_MS. `failureCount` is the number of consecutive failures recorded
 * for the event (1 on the first failure), so the first retry waits the base
 * RETRY_DELAY_MS and each further failure doubles it: 30s, 60s, 120s, … 30min.
 */
export function computeBackoffMs(failureCount: number): number {
  return Math.min(RETRY_DELAY_MS * 2 ** Math.max(0, failureCount - 1), MAX_BACKOFF_MS);
}

/** Function passed by the DO so the dispatcher can extend the isolate's lifetime
 * across the fire-and-forget projection chain. Matches the
 * `DurableObjectState.waitUntil` signature.
 */
export type WaitUntilFn = (promise: Promise<unknown>) => void;

/**
 * Optional dependencies the DO can plumb in to make projection failures
 * reliable (AA-93):
 *   - `waitUntil`: keeps the isolate alive across the async dispatch chain.
 *     Without this, `dispatchNewEvent` falls back to a `.catch()` log — the
 *     isolate may be terminated before Sentry flushes or before the projector
 *     even runs.
 *   - `dlq`: dead-letter queue writer. When provided, every failed
 *     `sendEvent()` results in a DLQ record (in addition to the existing
 *     logger.warn + alarm-driven retry).
 */
export interface ProjectionDispatcherDeps {
  waitUntil?: WaitUntilFn;
  dlq?: DlqWriter;
}

export class ProjectionDispatcher {
  private readonly waitUntilFn?: WaitUntilFn;
  private readonly dlq?: DlqWriter;

  constructor(
    private readonly storage: DurableObjectStorage,
    private readonly eventStore: { load(aggregateType: string, aggregateId: string, afterVersion?: number): Promise<StoredEvent[]> },
    private readonly aggregateType: string,
    private readonly getAggregateId: () => string,
    deps: ProjectionDispatcherDeps = {}
  ) {
    this.waitUntilFn = deps.waitUntil;
    this.dlq = deps.dlq;
  }

  private get aggregateId(): string {
    return this.getAggregateId();
  }

  /**
   * Fire-and-forget from applyAndPersistEvent. Never throws.
   *
   * AA-93: when constructed with a `waitUntil` (from DurableObjectState), the
   * dispatch chain is wrapped in `waitUntil(...)` so the isolate is kept
   * alive long enough for projector + DLQ + Sentry to run. When no
   * `waitUntil` is provided we fall back to a bare `.catch()` log so behavior
   * stays compatible with existing callers (and unit tests).
   */
  dispatchNewEvent(event: ProjectedEvent): void {
    const promise = this.sendToAll(event).catch((err: unknown) => {
      logger.error('[ProjectionDispatcher] dispatch failed', {
        aggregateType: this.aggregateType,
        aggregateId: this.aggregateId,
        error: err instanceof Error ? err.message : String(err),
      });
    });

    if (this.waitUntilFn) {
      try {
        this.waitUntilFn(promise);
      } catch (err) {
        // waitUntil() throws if called outside an active request scope —
        // shouldn't happen from a DO fetch handler, but guard against it so a
        // misconfiguration doesn't drop the projection chain entirely.
        logger.warn('[ProjectionDispatcher] waitUntil threw, falling back to bare promise', {
          aggregateType: this.aggregateType,
          aggregateId: this.aggregateId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }

  /** Called from alarm() for retry/catch-up */
  async catchUp(currentVersion: number): Promise<void> {
    const projectors = getProjectors();
    for (const projector of projectors) {
      // Each still-behind projector schedules its OWN next retry inside
      // catchUpProjector — at its own exponential backoff, or the base delay
      // when its remote-version check was unreachable. scheduleAlarm keeps the
      // soonest pending alarm, so there's no blanket reschedule here (a flat
      // one would flatten a long backoff down to the base delay).
      await this.catchUpProjector(projector, currentVersion);
    }
  }

  private async sendToAll(event: ProjectedEvent): Promise<void> {
    // Dispatch projectors CONCURRENTLY, not serially. Each is independent — its
    // own cursor, target, and DLQ-on-failure — and `sendToProjector` never
    // throws, so the worst-case latency is the SLOWEST single projector, not
    // the SUM of their timeouts. Serial dispatch summed the per-projector 5s
    // bounds (AA-93) past the request budget and stalled the create path under
    // the single-threaded local wrangler-dev worker.
    const projectors = getProjectors();
    await Promise.all(projectors.map((projector) => this.sendToProjector(projector, event)));
  }

  private async sendToProjector(projector: IEventProjector, event: ProjectedEvent): Promise<void> {
    const cursor = await loadCursor(this.storage, projector.id);

    if (event.version <= cursor.lastSentVersion) return; // already sent

    try {
      await projector.sendEvent(event);
      await saveCursor(this.storage, projector.id, {
        lastSentVersion: event.version,
        failureCount: 0,
        lastFailureAt: null,
        failedAtVersion: null,
      });
      // Cold-start hardening: a successful (re)delivery clears any DLQ copy a
      // prior failure of this same event wrote, so the DLQ doesn't overstate
      // failures that eventually went through. Best-effort; never throws.
      await this.deleteFromDlq(projector.id, event);
    } catch (err) {
      logger.warn('[ProjectionDispatcher] sendEvent failed', {
        projectorId: projector.id,
        eventVersion: event.version,
        error: err instanceof Error ? err.message : String(err),
      });
      // AA-93: best-effort DLQ write so failures stay observable even if
      // Sentry capture (in the projector itself) couldn't flush.
      await this.writeToDlq(projector.id, event, err);
      const isRepeat = cursor.failedAtVersion === event.version;
      const updatedCursor = {
        ...cursor,
        failureCount: isRepeat ? cursor.failureCount + 1 : 1,
        lastFailureAt: new Date().toISOString(),
        failedAtVersion: event.version,
      };
      await saveCursor(this.storage, projector.id, updatedCursor);
      await this.scheduleAlarm(computeBackoffMs(updatedCursor.failureCount));
    }
  }

  private async catchUpProjector(projector: IEventProjector, currentVersion: number): Promise<boolean> {
    const cursor = await loadCursor(this.storage, projector.id);

    if (cursor.failureCount >= MAX_FAILURES) {
      // Give-up guard. With the capped exponential backoff, MAX_FAILURES (15)
      // attempts span many hours — long enough to outlast any SpacetimeDB
      // cold-start resume — so reaching this point means the event is genuinely
      // poison (e.g. EventBridge MalformedDetail that will never succeed). Stop
      // retrying it so it can't block the projector forever.
      logger.error('[ProjectionDispatcher] projector exceeded max failures, skipping', {
        projectorId: projector.id,
        failureCount: cursor.failureCount,
        maxFailures: MAX_FAILURES,
      });
      return false;
    }

    let remoteVersion: number;
    try {
      remoteVersion = await projector.getLastProjectedVersion(this.aggregateType, this.aggregateId);
    } catch (err) {
      logger.warn('[ProjectionDispatcher] getLastProjectedVersion failed', {
        projectorId: projector.id,
        error: err instanceof Error ? err.message : String(err),
      });
      // Couldn't check the remote version (transient). Assume behind and
      // schedule a base-delay retry; scheduleAlarm keeps the soonest pending
      // alarm so this never delays another projector's sooner backoff.
      await this.scheduleAlarm(RETRY_DELAY_MS);
      return true;
    }

    const replayFrom = Math.min(remoteVersion, cursor.lastSentVersion);
    if (replayFrom >= currentVersion) return false; // caught up

    // Load events from R2 after replayFrom version
    const events = await this.eventStore.load(this.aggregateType, this.aggregateId, replayFrom);

    for (const storedEvent of events) {
      const projected = this.toProjectedEvent(storedEvent);
      try {
        await projector.sendEvent(projected);
        await saveCursor(this.storage, projector.id, {
          lastSentVersion: projected.version,
          failureCount: 0,
          lastFailureAt: null,
          failedAtVersion: null,
        });
        // Cold-start hardening: clear any DLQ copy now that the replay landed.
        await this.deleteFromDlq(projector.id, projected);
      } catch (err) {
        logger.warn('[ProjectionDispatcher] catchUp sendEvent failed', {
          projectorId: projector.id,
          eventVersion: projected.version,
          error: err instanceof Error ? err.message : String(err),
        });
        await this.writeToDlq(projector.id, projected, err);
        const cur = await loadCursor(this.storage, projector.id);
        const isRepeat = cur.failedAtVersion === projected.version;
        const updatedCursor = {
          ...cur,
          failureCount: isRepeat ? cur.failureCount + 1 : 1,
          lastFailureAt: new Date().toISOString(),
          failedAtVersion: projected.version,
        };
        await saveCursor(this.storage, projector.id, updatedCursor);
        await this.scheduleAlarm(computeBackoffMs(updatedCursor.failureCount));
        return true; // still behind
      }
    }

    return false; // caught up
  }

  private toProjectedEvent(storedEvent: StoredEvent): ProjectedEvent {
    return {
      aggregateType: storedEvent.aggregateType,
      aggregateId: storedEvent.aggregateId,
      version: storedEvent.version,
      type: storedEvent.type,
      // StoredEvent.timestamp is an ISO string; convert to epoch ms for ProjectedEvent
      timestamp: Date.parse(storedEvent.timestamp),
      data: storedEvent.event,
      orgId: storedEvent.orgId || '',
    };
  }

  /** Reset all projector cursors to 0 and schedule alarm for full replay */
  async resetAndScheduleCatchUp(): Promise<void> {
    const projectors = getProjectors();
    for (const projector of projectors) {
      await saveCursor(this.storage, projector.id, {
        lastSentVersion: 0,
        failureCount: 0,
        lastFailureAt: null,
        failedAtVersion: null,
      });
    }
    // Force schedule alarm even if one already exists
    await this.storage.setAlarm(Date.now() + 1000);
  }

  /**
   * Schedule a retry/catch-up alarm `delayMs` in the future, keeping the
   * SOONEST pending alarm. The DO has a single alarm shared by all projectors,
   * and a fired alarm re-checks every projector — so when projectors back off
   * by different amounts, the earliest one must win. (Previously this kept
   * whichever alarm was set FIRST, which could pin one projector's 30s
   * first-failure retry behind another projector's 30-minute capped backoff —
   * regressing cold-start recovery for multi-projector aggregates.) Failure
   * call sites pass the capped exponential backoff for the current failureCount.
   */
  private async scheduleAlarm(delayMs: number): Promise<void> {
    try {
      const target = Date.now() + delayMs;
      const existing = await this.storage.getAlarm();
      if (!existing || target < existing) {
        await this.storage.setAlarm(target);
      }
    } catch (err: unknown) {
      logger.error('[ProjectionDispatcher] failed to schedule alarm', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * Write a failed projection event to the DLQ. Best-effort: a DLQ write
   * failure must not cascade — the dispatcher already retries via alarm.
   */
  private async writeToDlq(projectorId: string, event: ProjectedEvent, err: unknown): Promise<void> {
    if (!this.dlq) return;
    try {
      const errMsg = err instanceof Error ? err.message : String(err);
      const stack = err instanceof Error ? err.stack : undefined;
      const status = (err && typeof err === 'object' && 'status' in err && typeof (err as { status: unknown }).status === 'number')
        ? (err as { status: number }).status
        : undefined;
      const record: DlqRecord = {
        timestamp: new Date().toISOString(),
        projector: projectorId,
        eventType: event.type,
        aggregateType: event.aggregateType,
        aggregateId: event.aggregateId,
        version: event.version,
        payload: event.data,
        error: { message: errMsg, status, stack },
      };
      await this.dlq.write(record);
    } catch (dlqErr) {
      logger.error('[ProjectionDispatcher] DLQ write failed', {
        projectorId,
        eventVersion: event.version,
        error: dlqErr instanceof Error ? dlqErr.message : String(dlqErr),
      });
    }
  }

  /**
   * Delete the DLQ copy of an event after it was successfully (re)delivered.
   * Best-effort and idempotent (mirrors writeToDlq): if no DLQ writer is wired
   * in, or the record never existed, this is a no-op; a delete failure is logged
   * but never thrown — a stale DLQ entry is harmless, a thrown error would break
   * the success path.
   */
  private async deleteFromDlq(projectorId: string, event: ProjectedEvent): Promise<void> {
    if (!this.dlq) return;
    try {
      await this.dlq.delete({
        projector: projectorId,
        aggregateType: event.aggregateType,
        aggregateId: event.aggregateId,
        version: event.version,
      });
    } catch (dlqErr) {
      logger.error('[ProjectionDispatcher] DLQ delete failed', {
        projectorId,
        eventVersion: event.version,
        error: dlqErr instanceof Error ? dlqErr.message : String(dlqErr),
      });
    }
  }
}
