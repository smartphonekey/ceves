/**
 * AA-117 regression: resumable R2 event replay with periodic checkpointing.
 *
 * Before this fix, a cold-start state restoration that ran more events
 * than the Worker CPU/wall-clock budget allowed would be killed by the
 * Cloudflare runtime. The DO never persisted any partial state, so every
 * subsequent cold start crashed identically — looping forever for that
 * specific aggregate. The Sentry symptom was a recurring
 * `internal error; reference = ...` from worker-side `stub.fetch` because
 * isolate termination is uncatchable from inside the DO.
 *
 * The fix (now batched — AA-117 follow-up):
 *
 *   1. `restoreFromR2Batched` loads events from R2 in bounded pages of
 *      `REPLAY_CHECKPOINT_INTERVAL` (never `loadAll()`), and writes
 *      `r2_replay_in_progress: true` before applying the first page. If
 *      the isolate is killed mid-replay the flag stays set.
 *   2. `replayBatch` persists `state` to DO storage after each page, so
 *      the checkpointed `version` advances one batch at a time. The next
 *      cold start reads the checkpointed state.
 *   3. On the next cold start `ensureStateLoaded` sees both
 *      `state.version > 0` AND the in-progress flag, re-enters the
 *      replay path paging from `state.version`, applies only the
 *      remaining events, then clears the flag.
 *
 * This test exercises both legs of the contract:
 *   a) During a full, successful replay we see checkpoints emitted at
 *      the expected positions and the flag cleared at the end.
 *   b) After a simulated isolate kill mid-replay, a brand-new aggregate
 *      instance reading the same DO storage resumes from the last
 *      checkpoint without re-applying earlier events.
 */

import { describe, it, expect } from 'vitest';
import { AggregateObject, type AggregateObjectEnv } from './AggregateObject';
import { BaseState } from '../schemas/State';
import type { IEventStore, StoredEvent } from '../storage/interfaces';

class CounterState extends BaseState {
  count = 0;
}

function makeEvents(n: number): StoredEvent[] {
  const events: StoredEvent[] = [];
  for (let i = 1; i <= n; i++) {
    events.push({
      aggregateType: 'CounterAggregate',
      aggregateId: 'agg-1',
      version: i,
      type: 'CounterIncremented',
      data: { by: 1 },
      timestamp: new Date(2026, 4, 11, 0, 0, i).toISOString(),
    } as unknown as StoredEvent);
  }
  return events;
}

class FakeStorage {
  private readonly data = new Map<string, unknown>();
  public readonly puts: Array<{ key: string; value: unknown }> = [];
  public readonly deletes: string[] = [];

  get<T>(key: string): Promise<T | undefined> {
    return Promise.resolve(this.data.get(key) as T | undefined);
  }
  put<T>(key: string, value: T): Promise<void> {
    // Deep-clone on write — DO storage in production uses structured
    // clone, and we'd be lying about checkpoint timing otherwise: the
    // production runtime captures the value AT put() time, not by
    // reference. Without this, the in-memory state mutation after the
    // checkpoint would retroactively rewrite the stored snapshot.
    const cloned = typeof structuredClone === 'function'
      ? structuredClone(value)
      : (JSON.parse(JSON.stringify(value)) as T);
    this.data.set(key, cloned);
    this.puts.push({ key, value: cloned });
    return Promise.resolve();
  }
  delete(key: string): Promise<boolean> {
    this.deletes.push(key);
    return Promise.resolve(this.data.delete(key));
  }
  // Misc methods we don't use but ctx.storage typing wants
  deleteAll(): Promise<void> {
    this.data.clear();
    return Promise.resolve();
  }
}

class FakeEventStore implements IEventStore {
  constructor(private readonly events: StoredEvent[]) {}
  save(): Promise<void> {
    return Promise.resolve();
  }
  load(_t: string, _id: string, afterVersion?: number, limit?: number): Promise<StoredEvent[]> {
    const filtered = afterVersion == null
      ? [...this.events]
      : this.events.filter((e) => e.version > afterVersion);
    // Honour the bounded-paging `limit` so the test exercises the same
    // batched restore loop production uses (one page per `load`).
    return Promise.resolve(limit === undefined ? filtered : filtered.slice(0, limit));
  }
  loadAll(): Promise<StoredEvent[]> {
    return Promise.resolve([...this.events]);
  }
}

/**
 * Minimal DurableObjectState shim — only the bits AggregateObject reads
 * during construction and `ensureStateLoaded`.
 */
function makeCtx(storage: FakeStorage, id: string): unknown {
  return {
    id: { toString: () => id, name: id, toJSON: () => id, equals: () => false },
    storage,
    waitUntil: undefined,
    // unused but typed on DurableObjectState
    blockConcurrencyWhile: <T>(cb: () => Promise<T>) => cb(),
  };
}

/**
 * Test-only subclass that:
 *   - Bypasses the decorator registry: `applyEvent` does the increment inline.
 *   - Exposes `ensureStateLoaded` so we can drive it directly.
 *   - Skips R2 event store binding from env (we wire `eventStore` manually).
 */
class CounterAggregate extends AggregateObject<CounterState> {
  protected override get aggregateType(): string {
    return 'CounterAggregate';
  }

  constructor(ctx: unknown, env: AggregateObjectEnv, eventStore: IEventStore) {
    super(ctx as never, env, CounterState);
    // Wire fake event store directly — bypasses initializeStores' R2 path.
    (this as unknown as { eventStore: IEventStore }).eventStore = eventStore;
  }

  protected override initializeStores(): void {
    /* skip — we inject eventStore in the ctor above */
  }

  protected override applyEvent(event: StoredEvent): void {
    if (!this.state) {
      this.state = new CounterState();
    }
    this.state.count += 1;
    this.state.version = event.version;
  }

  public async runEnsureStateLoaded(): Promise<void> {
    return this.ensureStateLoaded();
  }
}

describe('AA-117: ensureStateLoaded checkpoint + resume', () => {
  it('checkpoints state to DO storage every REPLAY_CHECKPOINT_INTERVAL events during a full replay', async () => {
    const events = makeEvents(125); // 125 / 50 = two full checkpoints at 50 and 100, plus a final write at the end
    const storage = new FakeStorage();
    const eventStore = new FakeEventStore(events);
    const agg = new CounterAggregate(makeCtx(storage, 'agg-1'), {} as AggregateObjectEnv, eventStore);

    await agg.runEnsureStateLoaded();

    // Final state has every event applied.
    const finalState = (agg as unknown as { state: CounterState }).state;
    expect(finalState.count).toBe(125);
    expect(finalState.version).toBe(125);

    // Checkpoints: we expect (a) one before the loop ("r2_replay_in_progress"),
    // (b) two mid-loop state puts at index 50 and 100, (c) the final state put,
    // (d) the in-progress flag deletion.
    const statePuts = storage.puts.filter((p) => p.key === 'state');
    expect(statePuts.length).toBe(3); // checkpoints at 50, 100, plus final

    const inProgressPuts = storage.puts.filter((p) => p.key === 'r2_replay_in_progress');
    expect(inProgressPuts.length).toBe(1);
    expect(inProgressPuts[0]!.value).toBe(true);

    expect(storage.deletes).toContain('r2_replay_in_progress');

    // The two mid-loop checkpoints capture progress at version 50 and 100.
    expect((statePuts[0]!.value as CounterState).version).toBe(50);
    expect((statePuts[1]!.value as CounterState).version).toBe(100);
    expect((statePuts[2]!.value as CounterState).version).toBe(125);
  });

  it('resumes from state.version after a simulated isolate kill mid-replay', async () => {
    // Phase 1: pre-seed DO storage as if a previous isolate crashed at
    // event 60 — state.version = 50 (last checkpoint), in-progress flag
    // still set because the second checkpoint was never reached.
    const partialState = new CounterState();
    partialState.count = 50;
    partialState.version = 50;
    const storage = new FakeStorage();
    await storage.put('state', partialState);
    await storage.put('r2_replay_in_progress', true);

    // Phase 2: fresh aggregate instance (= fresh isolate) reading the
    // same DO storage.
    const events = makeEvents(125);
    const eventStore = new FakeEventStore(events);
    const agg = new CounterAggregate(makeCtx(storage, 'agg-1'), {} as AggregateObjectEnv, eventStore);
    // Simulate the constructor blockConcurrencyWhile having loaded state.
    (agg as unknown as { state: CounterState }).state = partialState;

    await agg.runEnsureStateLoaded();

    // Final state has every event applied, but only the 75 remaining
    // events were processed in this run — the first 50 were skipped.
    const finalState = (agg as unknown as { state: CounterState }).state;
    expect(finalState.count).toBe(125); // 50 (preserved) + 75 (applied)
    expect(finalState.version).toBe(125);

    // We should see one resume-phase checkpoint at version 100 (50 + 50),
    // and a final state put at version 125, plus the in-progress flag
    // deletion.
    const statePuts = storage.puts.filter((p) => p.key === 'state');
    // 1 initial seed + 1 mid-resume checkpoint + 1 final = 3
    expect(statePuts.length).toBe(3);
    expect((statePuts[1]!.value as CounterState).version).toBe(100);
    expect((statePuts[2]!.value as CounterState).version).toBe(125);

    expect(storage.deletes).toContain('r2_replay_in_progress');
  });

  it('returns early without R2 list when state is healthy and no replay flag', async () => {
    // Fully migrated aggregate: state.version > 0, no in-progress flag.
    // Should skip R2 entirely.
    const healthyState = new CounterState();
    healthyState.count = 42;
    healthyState.version = 42;
    const storage = new FakeStorage();
    await storage.put('state', healthyState);

    let loadAllCalls = 0;
    const eventStore: IEventStore = {
      save: () => Promise.resolve(),
      load: () => Promise.resolve([]),
      loadAll: () => {
        loadAllCalls++;
        return Promise.resolve([]);
      },
    };
    const agg = new CounterAggregate(makeCtx(storage, 'agg-1'), {} as AggregateObjectEnv, eventStore);
    (agg as unknown as { state: CounterState }).state = healthyState;

    await agg.runEnsureStateLoaded();

    // No R2 read, no state write, no flag write.
    expect(loadAllCalls).toBe(0);
    expect(storage.puts.filter((p) => p.key === 'state')).toHaveLength(1); // only the pre-seed
    expect(storage.puts.filter((p) => p.key === 'r2_replay_in_progress')).toHaveLength(0);
  });

  it('clears a stale in-progress flag when there are no events to apply', async () => {
    // Edge case: state.version = 100, but R2 only has 100 events too.
    // The previous replay was actually complete; only the flag deletion
    // was missed. We should clear the flag without doing extra work.
    const completeState = new CounterState();
    completeState.count = 100;
    completeState.version = 100;
    const storage = new FakeStorage();
    await storage.put('state', completeState);
    await storage.put('r2_replay_in_progress', true);

    const events = makeEvents(100);
    const eventStore = new FakeEventStore(events);
    const agg = new CounterAggregate(makeCtx(storage, 'agg-1'), {} as AggregateObjectEnv, eventStore);
    (agg as unknown as { state: CounterState }).state = completeState;

    await agg.runEnsureStateLoaded();

    expect((agg as unknown as { state: CounterState }).state.version).toBe(100);
    expect(storage.deletes).toContain('r2_replay_in_progress');
    // No fresh state writes — nothing to apply.
    expect(storage.puts.filter((p) => p.key === 'state')).toHaveLength(1); // pre-seed only
  });
});
