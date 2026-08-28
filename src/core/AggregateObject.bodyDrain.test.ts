/**
 * AA-193 follow-up: every early-return response from the DO must DRAIN the
 * request body first.
 *
 * The crash class documented in docs/ci-evidence/AA-193 (b2c-api repo):
 * a request body that is never read, handed to the Durable Object as a live
 * stream (`duplex: 'half'`), where the response carries a body, raises
 *   "TypeError: Can't read from request stream after response has been sent"
 * inside workerd, destabilizes the ProxyWorker, and kills the local worker
 * process ("Broken pipe", every remaining test ECONNREFUSED). PR #285 fixed
 * the unifiedAuth instance of this shape; AA-193 predicted the remaining
 * instances live behind other early returns.
 *
 * CI run 30532156969 (PR #309) captured the next instance in the wild, 54ms
 * before the worker died:
 *   10:00:22.975  "Create command on existing aggregate — idempotent no-op"
 *   10:00:23.029  uncaught TypeError: Can't read from request stream after
 *                 response has been sent   (x2)
 *   10:00:23.049  kj/async-io-unix.c++:186 ... Broken pipe
 *
 * The AA-92 idempotent no-op (and its siblings below) returned before
 * `await request.text()`. These tests pin `request.bodyUsed === true` for
 * every early-return response — the node-side proxy for "workerd will not
 * throw the uncaught stream error".
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Route, clearRoutes } from '../routing/Route';
import { AggregateObject, type AggregateObjectEnv } from './AggregateObject';
import { BaseState } from '../schemas/State';
import { UnauthorizedError } from '../errors';
import type { IEventStore, StoredEvent } from '../storage/interfaces';

class CounterState extends BaseState {
  count = 0;
}

class FakeStorage {
  private readonly data = new Map<string, unknown>();
  get<T>(key: string): Promise<T | undefined> {
    return Promise.resolve(this.data.get(key) as T | undefined);
  }
  put<T>(key: string, value: T): Promise<void> {
    this.data.set(key, structuredClone(value));
    return Promise.resolve();
  }
  delete(key: string): Promise<boolean> {
    return Promise.resolve(this.data.delete(key));
  }
  deleteAll(): Promise<void> {
    this.data.clear();
    return Promise.resolve();
  }
  seedState(state: Record<string, unknown>): void {
    this.data.set('state', state);
  }
}

class FakeEventStore implements IEventStore {
  save(): Promise<void> {
    return Promise.resolve();
  }
  load(): Promise<StoredEvent[]> {
    return Promise.resolve([]);
  }
  loadAll(): Promise<StoredEvent[]> {
    return Promise.resolve([]);
  }
}

function makeCtx(storage: FakeStorage, id: string): unknown {
  return {
    id: { toString: () => id, name: id, toJSON: () => id, equals: () => false },
    storage,
    waitUntil: undefined,
    blockConcurrencyWhile: <T>(cb: () => Promise<T>) => cb(),
  };
}

class DrainCounterAggregate extends AggregateObject<CounterState> {
  public denyAuth = false;

  protected override get aggregateType(): string {
    return 'CounterAggregate';
  }

  constructor(ctx: unknown, env: AggregateObjectEnv, eventStore: IEventStore) {
    super(ctx as never, env, CounterState);
    (this as unknown as { eventStore: IEventStore }).eventStore = eventStore;
  }

  protected override initializeStores(): void {
    /* skip R2 wiring — fake event store injected in ctor */
  }

  protected override applyEvent(event: StoredEvent): void {
    if (!this.state) this.state = new CounterState();
    this.state.count += 1;
    this.state.version = event.version;
  }

  protected override checkAuthorization(): void {
    if (this.denyAuth) throw new UnauthorizedError('denied for test');
  }
}

function makeAggregate(opts: { seededState?: boolean; denyAuth?: boolean }) {
  const storage = new FakeStorage();
  if (opts.seededState) {
    storage.seedState({ id: 'agg-1', version: 3, count: 3 });
  }
  const agg = new DrainCounterAggregate(
    makeCtx(storage, 'agg-1'),
    {} as AggregateObjectEnv,
    new FakeEventStore(),
  );
  agg.denyAuth = opts.denyAuth ?? false;
  return agg;
}

function postWithBody(path: string): Request {
  return new Request(`http://internal${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ some: 'payload' }),
  });
}

describe('AggregateObject drains the request body on every early-return response', () => {
  beforeEach(() => {
    clearRoutes();
    Route({ method: 'POST', path: '/counters/:id/CreateCounter' })(
      class CreateCounterRoute {
        static readonly isCreateCommand = true;
        aggregateType = 'CounterAggregate';
        executeCommand(): Promise<{ type: string }> {
          return Promise.resolve({ type: 'CounterCreated' });
        }
      } as never,
    );
    Route({ method: 'POST', path: '/counters/:id/Increment' })(
      class IncrementRoute {
        aggregateType = 'CounterAggregate';
        executeCommand(): Promise<{ type: string }> {
          return Promise.resolve({ type: 'CounterIncremented' });
        }
      } as never,
    );
  });

  it('AA-92 idempotent no-op (create on existing aggregate) — the captured crash trigger', async () => {
    const agg = makeAggregate({ seededState: true });
    const request = postWithBody('/counters/agg-1/CreateCounter');

    const response = await agg.fetch(request);
    const body: { success: boolean; event: null } = await response.json();

    expect(response.status).toBe(200);
    expect(body.event).toBeNull();
    expect(request.bodyUsed).toBe(true);
  });

  it('update command on a non-existent aggregate (404 path)', async () => {
    const agg = makeAggregate({ seededState: false });
    const request = postWithBody('/counters/agg-1/Increment');

    const response = await agg.fetch(request);

    expect(response.status).toBeGreaterThanOrEqual(400);
    expect(request.bodyUsed).toBe(true);
  });

  it('"No handler found" 404 — unmatched POST with body, the shape the dead admin routes produced', async () => {
    const agg = makeAggregate({ seededState: true });
    const request = postWithBody('/counters/agg-1/NoSuchCommand');

    const response = await agg.fetch(request);

    expect(response.status).toBe(404);
    expect(request.bodyUsed).toBe(true);
  });

  it('authorization rejection (401 path)', async () => {
    const agg = makeAggregate({ seededState: true, denyAuth: true });
    const request = postWithBody('/counters/agg-1/Increment');

    const response = await agg.fetch(request);

    expect(response.status).toBe(401);
    expect(request.bodyUsed).toBe(true);
  });
});
