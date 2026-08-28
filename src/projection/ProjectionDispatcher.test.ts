/**
 * ProjectionDispatcher unit tests — cold-start retry hardening.
 *
 * Covers (AA cold-start hardening):
 *  - computeBackoffMs: exponential, capped at 30 minutes.
 *  - delete-on-success: the DLQ copy is removed when a live send OR a catch-up
 *    replay succeeds; write happens on failure.
 *  - the raised MAX_FAILURES threshold still eventually skips a poison event.
 *  - failure alarms use the capped exponential backoff (not a fixed 30s).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  ProjectionDispatcher,
  computeBackoffMs,
} from './ProjectionDispatcher';
import { registerProjector, clearProjectors } from './ProjectorRegistry';
import type { IEventProjector, ProjectedEvent } from './IEventProjector';
import type { DlqRecord, DlqWriter } from './DlqWriter';
import type { StoredEvent } from '../storage/interfaces';

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/** Minimal in-memory DurableObjectStorage covering what the dispatcher uses. */
class FakeStorage {
  private map = new Map<string, unknown>();
  private alarm: number | null = null;

  get<T>(key: string): Promise<T | undefined> {
    return Promise.resolve(this.map.get(key) as T | undefined);
  }
  put(key: string, value: unknown): Promise<void> {
    this.map.set(key, value);
    return Promise.resolve();
  }
  getAlarm(): Promise<number | null> {
    return Promise.resolve(this.alarm);
  }
  setAlarm(time: number): Promise<void> {
    this.alarm = time;
    return Promise.resolve();
  }
}

interface RecordedDlqCall {
  op: 'write' | 'delete';
  projector: string;
  aggregateType: string;
  aggregateId: string;
  version: number;
}

/** Fake DlqWriter recording write/delete calls. */
class FakeDlqWriter implements DlqWriter {
  calls: RecordedDlqCall[] = [];

  write(record: DlqRecord): Promise<void> {
    this.calls.push({
      op: 'write',
      projector: record.projector,
      aggregateType: record.aggregateType,
      aggregateId: record.aggregateId,
      version: record.version,
    });
    return Promise.resolve();
  }

  delete(
    record: Pick<DlqRecord, 'projector' | 'aggregateType' | 'aggregateId' | 'version'>,
  ): Promise<void> {
    this.calls.push({
      op: 'delete',
      projector: record.projector,
      aggregateType: record.aggregateType,
      aggregateId: record.aggregateId,
      version: record.version,
    });
    return Promise.resolve();
  }

  ops(op: 'write' | 'delete'): RecordedDlqCall[] {
    return this.calls.filter((c) => c.op === op);
  }
}

/** Configurable fake projector. */
class FakeProjector implements IEventProjector {
  sent: ProjectedEvent[] = [];
  constructor(
    readonly id: string,
    private behavior: { fail?: boolean; lastVersion?: number } = {},
  ) {}

  setFail(fail: boolean): void {
    this.behavior.fail = fail;
  }

  sendEvent(event: ProjectedEvent): Promise<void> {
    if (this.behavior.fail) {
      return Promise.reject(new Error(`projector ${this.id} rejected v${event.version}`));
    }
    this.sent.push(event);
    return Promise.resolve();
  }

  getLastProjectedVersion(): Promise<number> {
    return Promise.resolve(this.behavior.lastVersion ?? 0);
  }
}

const AGG_TYPE = 'UserAggregate';
const AGG_ID = 'user-1';

function makeEvent(version: number): ProjectedEvent {
  return {
    aggregateType: AGG_TYPE,
    aggregateId: AGG_ID,
    version,
    type: 'UserCreated',
    timestamp: Date.now(),
    data: { userId: AGG_ID },
    orgId: '',
  };
}

function makeStoredEvent(version: number): StoredEvent {
  return {
    aggregateType: AGG_TYPE,
    aggregateId: AGG_ID,
    version,
    type: 'UserCreated',
    timestamp: new Date().toISOString(),
    event: { userId: AGG_ID },
    orgId: '',
  } as StoredEvent;
}

function makeDispatcher(
  storage: FakeStorage,
  dlq: DlqWriter,
  events: StoredEvent[] = [],
): ProjectionDispatcher {
  const eventStore = {
    load(): Promise<StoredEvent[]> {
      return Promise.resolve(events);
    },
  };
  return new ProjectionDispatcher(
    storage as unknown as DurableObjectStorage,
    eventStore,
    AGG_TYPE,
    () => AGG_ID,
    { dlq },
  );
}

beforeEach(() => {
  clearProjectors();
});

// ---------------------------------------------------------------------------
// computeBackoffMs
// ---------------------------------------------------------------------------

describe('computeBackoffMs', () => {
  it('starts at the 30s base for the first failure', () => {
    expect(computeBackoffMs(1)).toBe(30_000);
  });

  it('doubles per consecutive failure', () => {
    expect(computeBackoffMs(2)).toBe(60_000);
    expect(computeBackoffMs(3)).toBe(120_000);
    expect(computeBackoffMs(4)).toBe(240_000);
  });

  it('caps at 30 minutes', () => {
    // 30s * 2^6 = 1_920_000 > 1_800_000 → capped
    expect(computeBackoffMs(7)).toBe(1_800_000);
    expect(computeBackoffMs(50)).toBe(1_800_000);
  });

  it('treats failureCount 0 as the base (no negative exponent)', () => {
    expect(computeBackoffMs(0)).toBe(30_000);
  });
});

// ---------------------------------------------------------------------------
// delete-on-success
// ---------------------------------------------------------------------------

describe('DLQ delete on successful (re)delivery', () => {
  it('calls delete when a live sendEvent succeeds', async () => {
    const storage = new FakeStorage();
    const dlq = new FakeDlqWriter();
    const projector = new FakeProjector('spacetimedb');
    registerProjector(projector);

    const dispatcher = makeDispatcher(storage, dlq);
    await (dispatcher as unknown as { sendToAll(e: ProjectedEvent): Promise<void> }).sendToAll(
      makeEvent(1),
    );

    expect(dlq.ops('write')).toHaveLength(0);
    expect(dlq.ops('delete')).toHaveLength(1);
    expect(dlq.ops('delete')[0]).toMatchObject({
      projector: 'spacetimedb',
      aggregateType: AGG_TYPE,
      aggregateId: AGG_ID,
      version: 1,
    });
  });

  it('calls write on failure, then delete when a catch-up replay succeeds', async () => {
    const storage = new FakeStorage();
    const dlq = new FakeDlqWriter();
    const projector = new FakeProjector('spacetimedb', { fail: true });
    registerProjector(projector);

    // 1) live send fails → DLQ write
    const dispatcher = makeDispatcher(storage, dlq, [makeStoredEvent(1)]);
    await (dispatcher as unknown as { sendToAll(e: ProjectedEvent): Promise<void> }).sendToAll(
      makeEvent(1),
    );
    expect(dlq.ops('write')).toHaveLength(1);
    expect(dlq.ops('delete')).toHaveLength(0);

    // 2) projector recovers; catch-up replays the event → DLQ delete
    projector.setFail(false);
    await dispatcher.catchUp(1);

    expect(dlq.ops('delete')).toHaveLength(1);
    expect(dlq.ops('delete')[0]).toMatchObject({ projector: 'spacetimedb', version: 1 });
  });
});

// ---------------------------------------------------------------------------
// failure alarm scheduling (exponential backoff, not fixed 30s)
// ---------------------------------------------------------------------------

describe('failure alarm scheduling', () => {
  it('schedules the base backoff on the first failure', async () => {
    const storage = new FakeStorage();
    const dlq = new FakeDlqWriter();
    const projector = new FakeProjector('spacetimedb', { fail: true });
    registerProjector(projector);

    const now = 1_000_000;
    vi.spyOn(Date, 'now').mockReturnValue(now);

    const dispatcher = makeDispatcher(storage, dlq);
    await (dispatcher as unknown as { sendToAll(e: ProjectedEvent): Promise<void> }).sendToAll(
      makeEvent(1),
    );

    // first failure → failureCount 1 → base 30s
    expect(await storage.getAlarm()).toBe(now + computeBackoffMs(1));

    vi.restoreAllMocks();
  });

  it('grows the backoff on a repeated failure of the same event', async () => {
    const dlq = new FakeDlqWriter();
    const projector = new FakeProjector('spacetimedb', { fail: true });
    registerProjector(projector);

    const now = 2_000_000;
    vi.spyOn(Date, 'now').mockReturnValue(now);

    // Fresh storage each attempt would reset the alarm-exists guard; reuse one
    // storage but clear the alarm between attempts so we observe the new delay.
    const storage = new FakeStorage();
    const dispatcher = makeDispatcher(storage, dlq);

    await (dispatcher as unknown as { sendToAll(e: ProjectedEvent): Promise<void> }).sendToAll(
      makeEvent(1),
    );
    expect(await storage.getAlarm()).toBe(now + computeBackoffMs(1)); // 30s

    // Clear the alarm so the next failure's scheduleAlarm isn't blocked by the guard.
    await storage.setAlarm(null as unknown as number);
    await (dispatcher as unknown as { sendToAll(e: ProjectedEvent): Promise<void> }).sendToAll(
      makeEvent(1),
    );
    // same event failed again → failureCount 2 → 60s
    expect(await storage.getAlarm()).toBe(now + computeBackoffMs(2));

    vi.restoreAllMocks();
  });

  it('pulls the alarm earlier when a shorter backoff is scheduled (soonest wins)', async () => {
    const storage = new FakeStorage();
    const dlq = new FakeDlqWriter();
    const projector = new FakeProjector('spacetimedb', { fail: true });
    registerProjector(projector);

    const now = 3_000_000;
    vi.spyOn(Date, 'now').mockReturnValue(now);

    // Simulate another projector having already set a far-future (30-min cap) alarm.
    await storage.setAlarm(now + 30 * 60 * 1000);

    const dispatcher = makeDispatcher(storage, dlq);
    // This projector's FIRST failure wants a 30s retry — it must move the shared
    // alarm earlier, not get stuck behind the 30-min one.
    await (dispatcher as unknown as { sendToAll(e: ProjectedEvent): Promise<void> }).sendToAll(
      makeEvent(1),
    );

    expect(await storage.getAlarm()).toBe(now + computeBackoffMs(1)); // 30s

    vi.restoreAllMocks();
  });

  it('leaves a sooner alarm in place when a longer backoff is scheduled', async () => {
    const storage = new FakeStorage();
    const dlq = new FakeDlqWriter();
    const projector = new FakeProjector('spacetimedb', { fail: true });
    registerProjector(projector);

    const now = 4_000_000;
    vi.spyOn(Date, 'now').mockReturnValue(now);

    // An existing alarm sooner than the 30s base backoff must NOT be pushed out.
    const soonAlarm = now + 5_000;
    await storage.setAlarm(soonAlarm);

    const dispatcher = makeDispatcher(storage, dlq);
    await (dispatcher as unknown as { sendToAll(e: ProjectedEvent): Promise<void> }).sendToAll(
      makeEvent(1),
    );

    expect(await storage.getAlarm()).toBe(soonAlarm);

    vi.restoreAllMocks();
  });
});

// ---------------------------------------------------------------------------
// MAX_FAILURES threshold
// ---------------------------------------------------------------------------

describe('MAX_FAILURES give-up threshold', () => {
  it('keeps retrying past the OLD cap of 5 and only skips once 15 is reached', async () => {
    const storage = new FakeStorage();
    const dlq = new FakeDlqWriter();
    const projector = new FakeProjector('spacetimedb', { fail: true });
    registerProjector(projector);

    const dispatcher = makeDispatcher(storage, dlq, [makeStoredEvent(1)]);

    // Each catchUp run, while below the threshold, reaches sendEvent (which
    // throws) → one DLQ write + failureCount += 1. Once failureCount hits
    // MAX_FAILURES (15) the projector is skipped before sendEvent, so no
    // further DLQ writes happen.
    //
    // Run it 25× — comfortably past both the old cap (5) and the new cap (15).
    for (let i = 0; i < 25; i++) {
      await dispatcher.catchUp(1);
    }

    // It must have attempted MORE than the old cap of 5 (proving the raise),
    // and EXACTLY 15 (the new cap) — attempt #16 onward is skipped.
    const writes = dlq.ops('write').length;
    expect(writes).toBeGreaterThan(5);
    expect(writes).toBe(15);
  });
});
