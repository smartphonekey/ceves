/**
 * Regression: alarm()-driven projection catch-up must NOT clear the shared
 * `globalThis.__CEVES_ENV__`.
 *
 * `__CEVES_ENV__` is a SINGLE global shared by every Durable Object in the
 * isolate. The fetch() path sets it and deliberately never clears it, because
 * its fire-and-forget projection chain may still be running after the handler
 * returns. The alarm()/catch-up path used to clear it in a `finally` — which
 * races with other DOs' in-flight projection chains (and with the many
 * concurrent alarms a resync schedules), wiping the env out from under a
 * projector mid-`getEnv()`. The symptom was intermittent, spurious
 * "<projector> not configured" failures landing in the DLQ even though the
 * bindings were present. alarm() must follow the same leave-it-set rule.
 */
import { describe, it, expect, afterEach } from 'vitest';
import { AggregateObject, type AggregateObjectEnv } from './AggregateObject';
import { BaseState } from '../schemas/State';

class CounterState extends BaseState {}

function makeCtx(id: string): unknown {
  const data = new Map<string, unknown>();
  return {
    id: { toString: () => id, name: id, toJSON: () => id, equals: () => false },
    storage: {
      get: <T>(k: string) => Promise.resolve(data.get(k) as T | undefined),
      put: <T>(k: string, v: T) => {
        data.set(k, v);
        return Promise.resolve();
      },
      delete: (k: string) => Promise.resolve(data.delete(k)),
      deleteAll: () => {
        data.clear();
        return Promise.resolve();
      },
    },
    waitUntil: undefined,
    blockConcurrencyWhile: <T>(cb: () => Promise<T>) => cb(),
  };
}

/**
 * Test-only subclass: pre-loads a versioned state and injects a fake
 * projection dispatcher so alarm() reaches the setProjectorEnv + catchUp path
 * without any R2 wiring.
 */
class TestAggregate extends AggregateObject<CounterState> {
  public catchUpCalled = false;

  protected override get aggregateType(): string {
    return 'CounterAggregate';
  }

  constructor(ctx: unknown, env: AggregateObjectEnv, catchUp: () => Promise<void>) {
    super(ctx as never, env, CounterState);
    (this as unknown as { state: CounterState }).state = Object.assign(new CounterState(), {
      version: 5,
    });
    (this as unknown as { projectionDispatcher: { catchUp: (v: number) => Promise<void> } }).projectionDispatcher = {
      catchUp: async () => {
        this.catchUpCalled = true;
        await catchUp();
      },
    };
  }

  protected override initializeStores(): void {
    /* skip R2 wiring */
  }

  protected override ensureStateLoaded(): Promise<void> {
    // State is pre-seeded in the ctor; alarm() only needs a non-null version.
    return Promise.resolve();
  }
}

describe('AggregateObject — projector env lifetime on the alarm path', () => {
  afterEach(() => {
    delete (globalThis as Record<string, unknown>)['__CEVES_ENV__'];
  });

  it('leaves __CEVES_ENV__ set after a successful catch-up (no clear)', async () => {
    const env = { DB: {} } as unknown as AggregateObjectEnv;
    const agg = new TestAggregate(makeCtx('agg-1'), env, async () => {});

    await agg.alarm();

    expect(agg.catchUpCalled).toBe(true);
    expect((globalThis as Record<string, unknown>)['__CEVES_ENV__']).toBe(env);
  });

  it('leaves __CEVES_ENV__ set even when catch-up throws (no clear in a finally)', async () => {
    const env = { DB: {} } as unknown as AggregateObjectEnv;
    const agg = new TestAggregate(makeCtx('agg-1'), env, () =>
      Promise.reject(new Error('catchUp boom')),
    );

    await expect(agg.alarm()).rejects.toThrow('catchUp boom');
    expect((globalThis as Record<string, unknown>)['__CEVES_ENV__']).toBe(env);
  });
});
