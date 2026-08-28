/**
 * Unit tests for the PG dispatcher — the PLV8-side command/query pipeline.
 *
 * Uses a synchronous in-memory fake of `plv8.execute` that understands the
 * three statements the state store issues (SELECT / INSERT ON CONFLICT /
 * guarded UPDATE), so the full dispatch semantics run without PostgreSQL.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { z } from 'zod';
import { Route, clearRoutes } from '../../routing/Route';
import { EventHandler, clearEventHandlers, type IEventHandler } from '../../decorators';
import type { EventMetadata } from '../../events/EventMetadata';
import { NO_EVENT } from '../../events/DomainEvent';
import { BaseState } from '../../schemas/State';
import { CevesError } from '../../errors/CevesError';
import { createPgDispatcher } from './dispatcher';
import { registerPgAggregateState, clearPgAggregateStates } from './registry';
import type { PgSql } from './types';

/** Aggregate state used across the tests. */
class CounterState extends BaseState {
  owner = '';
  count = 0;
}

interface CounterCreatedEvent {
  type: 'CounterCreated';
  owner: string;
}
interface CounterIncrementedEvent {
  type: 'CounterIncremented';
  amount: number;
}

/** In-memory stand-in for plv8.execute over the aggregate_state table. */
class FakePgSql implements PgSql {
  rows = new Map<string, { state: Record<string, unknown>; version: number; org_id: string }>();
  /** Outbox rows written in the same "transaction" (see outbox.test.ts). */
  outbox: { id: number; kind: string; request: unknown }[] = [];
  failNextUpdate = false;
  failNextInsert = false;
  private nextOutboxId = 1;

  private key(params: unknown[]): string {
    return `${String(params[0])}:${String(params[1])}`;
  }

  execute(query: string, params: unknown[] = []): unknown {
    if (query.includes('.outbox')) {
      if (query.startsWith('INSERT INTO')) {
        const row = {
          id: this.nextOutboxId++,
          kind: String(params[0]),
          request: JSON.parse(String(params[3])) as unknown,
        };
        this.outbox.push(row);
        return [{ id: row.id }];
      }
      const id = Number(params[0]);
      this.outbox = this.outbox.filter((row) => row.id !== id);
      return 1;
    }
    if (query.startsWith('SELECT state')) {
      const row = this.rows.get(this.key(params));
      if (!row) return [];
      return [{ state: structuredClone(row.state), version: row.version, org_id: row.org_id }];
    }
    if (query.startsWith('INSERT INTO')) {
      if (this.failNextInsert || this.rows.has(this.key(params))) {
        this.failNextInsert = false;
        return [];
      }
      const [, , version, orgId, stateJson] = params;
      this.rows.set(this.key(params), {
        state: JSON.parse(String(stateJson)) as Record<string, unknown>,
        version: Number(version),
        org_id: String(orgId),
      });
      return [{ version }];
    }
    if (query.startsWith('UPDATE')) {
      const row = this.rows.get(this.key(params));
      const [, , newVersion, orgId, stateJson, expectedVersion] = params;
      if (this.failNextUpdate || !row || row.version !== Number(expectedVersion)) {
        this.failNextUpdate = false;
        return [];
      }
      this.rows.set(this.key(params), {
        state: JSON.parse(String(stateJson)) as Record<string, unknown>,
        version: Number(newVersion),
        org_id: String(orgId),
      });
      return [{ version: newVersion }];
    }
    throw new Error(`FakePgSql: unexpected query: ${query}`);
  }
}

/** Captures what the command handlers were called with. */
const captured: { env?: unknown; auth?: unknown } = {};

class CreateCounterRoute {
  static readonly isCreateCommand = true;
  aggregateType = 'CounterAggregate';

  executeCommand(
    command: { owner: string },
    env: unknown,
    auth?: unknown,
  ): Promise<CounterCreatedEvent> {
    captured.env = env;
    captured.auth = auth;
    return Promise.resolve({ type: 'CounterCreated', owner: command.owner });
  }
}

class IncrementCounterRoute {
  aggregateType = 'CounterAggregate';

  executeCommand(
    command: { amount: number },
    state: CounterState,
  ): Promise<CounterIncrementedEvent | typeof NO_EVENT> {
    if (command.amount === 0) return Promise.resolve(NO_EVENT);
    if (command.amount < 0) {
      throw new CevesError(`Cannot increment by ${command.amount}`, 422);
    }
    if (state.owner === '') {
      throw new Error('state was not loaded');
    }
    return Promise.resolve({ type: 'CounterIncremented', amount: command.amount });
  }
}

class BadEventRoute {
  static readonly eventSchema = z.object({ amount: z.number() });
  aggregateType = 'CounterAggregate';

  executeCommand(): Promise<{ type: string; amount: string }> {
    return Promise.resolve({ type: 'CounterIncremented', amount: 'not-a-number' });
  }
}

class CustomizedIncrementRoute {
  aggregateType = 'CounterAggregate';

  executeCommand(command: { amount: number }): Promise<CounterIncrementedEvent> {
    return Promise.resolve({ type: 'CounterIncremented', amount: command.amount });
  }

  customizeResponse(
    response: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    return Promise.resolve({ ...response, serverComputed: 'yes' });
  }
}

class GetCounterQuery {
  aggregateType = 'CounterAggregate';

  executeQuery(
    state: CounterState,
    query: { echo?: string },
  ): Promise<{ count: number; echoed?: string }> {
    return Promise.resolve({ count: state.count, echoed: query.echo });
  }
}

@EventHandler
class CounterCreatedHandler implements IEventHandler<CounterState, CounterCreatedEvent> {
  eventType = 'CounterCreated';
  aggregateType = 'CounterAggregate';

  apply(state: CounterState, event: CounterCreatedEvent, metadata: EventMetadata): CounterState {
    return { ...state, id: metadata.aggregateId, orgId: metadata.orgId, owner: event.owner };
  }
}

@EventHandler
class CounterIncrementedHandler
  implements IEventHandler<CounterState, CounterIncrementedEvent>
{
  eventType = 'CounterIncremented';
  aggregateType = 'CounterAggregate';

  apply(state: CounterState, event: CounterIncrementedEvent): CounterState {
    return { ...state, count: state.count + event.amount };
  }
}

function registerTestRoutes(): void {
  const route = Route as unknown as (opts: {
    method: string;
    path: string;
  }) => (cls: unknown) => unknown;
  route({ method: 'POST', path: '/counters/:id/CreateCounter' })(CreateCounterRoute);
  route({ method: 'POST', path: '/counters/:id/Increment' })(IncrementCounterRoute);
  route({ method: 'POST', path: '/counters/:id/BadEvent' })(BadEventRoute);
  route({ method: 'POST', path: '/counters/:id/CustomIncrement' })(CustomizedIncrementRoute);
  route({ method: 'GET', path: '/counters/:id/value' })(GetCounterQuery);
  // Event handlers were registered by the @EventHandler decorators at module
  // load; clearEventHandlers() in afterEach wipes them, so re-register.
  EventHandler(CounterCreatedHandler);
  EventHandler(CounterIncrementedHandler);
}

const FIXED_NOW = '2026-01-02T03:04:05.000Z';

describe('createPgDispatcher', () => {
  let sql: FakePgSql;
  let dispatcher: ReturnType<typeof createPgDispatcher>;

  beforeEach(() => {
    clearRoutes();
    clearEventHandlers();
    clearPgAggregateStates();
    registerTestRoutes();
    registerPgAggregateState('CounterAggregate', CounterState);
    sql = new FakePgSql();
    dispatcher = createPgDispatcher(sql, { now: () => FIXED_NOW });
  });

  afterEach(() => {
    clearRoutes();
    clearEventHandlers();
    clearPgAggregateStates();
  });

  async function createCounter(id = 'c1', owner = 'alice'): Promise<void> {
    const result = await dispatcher.executeCommand({
      aggregateId: id,
      routeKey: 'POST:/counters/:id/CreateCounter',
      command: { owner },
      auth: { orgId: 'org-1' },
    });
    expect(result.status).toBe(201);
  }

  describe('create commands', () => {
    it('creates the aggregate: 201, DO-shaped body, event returned, state row written', async () => {
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/CreateCounter',
        command: { owner: 'alice' },
        auth: { orgId: 'org-1' },
        env: { REGION: 'eu' },
      });

      expect(result.status).toBe(201);
      expect(result.body).toMatchObject({
        success: true,
        aggregateId: 'c1',
        version: 1,
        event: { type: 'CounterCreated', data: { owner: 'alice' } },
      });

      // The emitted event is RETURNED (for the external event log), with the
      // full StoredEvent envelope.
      expect(result.event).toEqual({
        aggregateType: 'CounterAggregate',
        aggregateId: 'c1',
        version: 1,
        type: 'CounterCreated',
        timestamp: FIXED_NOW,
        orgId: 'org-1',
        event: { type: 'CounterCreated', owner: 'alice' },
      });

      // State row persisted with handler-produced state + framework stamps.
      const row = sql.rows.get('CounterAggregate:c1');
      expect(row).toBeDefined();
      expect(row?.version).toBe(1);
      expect(row?.org_id).toBe('org-1');
      expect(row?.state).toMatchObject({ id: 'c1', owner: 'alice', count: 0, version: 1 });

      // env + auth reach the handler with DO-identical argument order.
      expect(captured.env).toEqual({ REGION: 'eu' });
      expect(captured.auth).toEqual({ orgId: 'org-1' });
    });

    it('treats a duplicate create as an idempotent no-op (AA-92): 200, event null', async () => {
      await createCounter();
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/CreateCounter',
        command: { owner: 'bob' },
      });

      expect(result.status).toBe(200);
      expect(result.body).toEqual({ success: true, aggregateId: 'c1', version: 1, event: null });
      expect(result.event).toBeNull();
      expect(sql.rows.get('CounterAggregate:c1')?.state.owner).toBe('alice');
    });

    it('returns an idempotent no-op when a concurrent create wins the insert race', async () => {
      sql.failNextInsert = true;
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/CreateCounter',
        command: { owner: 'alice' },
      });

      expect(result.status).toBe(200);
      expect((result.body as { event: unknown }).event).toBeNull();
      // The event must NOT be shipped — this transaction did not create the row.
      expect(result.event).toBeNull();
    });
  });

  describe('update commands', () => {
    it('applies the event and bumps the version', async () => {
      await createCounter();
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/Increment',
        command: { amount: 5 },
      });

      expect(result.status).toBe(200);
      expect(result.body).toMatchObject({
        success: true,
        aggregateId: 'c1',
        version: 2,
        event: { type: 'CounterIncremented', data: { amount: 5 } },
      });
      expect(result.event?.version).toBe(2);
      // orgId sticks from state, not from (absent) auth.
      expect(result.event?.orgId).toBe('org-1');
      expect(sql.rows.get('CounterAggregate:c1')?.state.count).toBe(5);
    });

    it('404s an update on a missing aggregate with the AA-119 envelope', async () => {
      const result = await dispatcher.executeCommand({
        aggregateId: 'nope',
        routeKey: 'POST:/counters/:id/Increment',
        command: { amount: 1 },
      });

      expect(result.status).toBe(404);
      expect(result.body).toMatchObject({
        success: false,
        errors: [{ code: 404 }],
        __ceves: { name: 'AggregateNotFoundError', httpStatusCode: 404 },
      });
      expect(result.event).toBeNull();
    });

    it('handles NO_EVENT: 200, no event, no state change', async () => {
      await createCounter();
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/Increment',
        command: { amount: 0 },
      });

      expect(result.status).toBe(200);
      expect(result.body).toEqual({ success: true, aggregateId: 'c1', version: 1, event: null });
      expect(result.event).toBeNull();
      expect(sql.rows.get('CounterAggregate:c1')?.version).toBe(1);
    });

    it('maps a thrown CevesError to its httpStatusCode envelope', async () => {
      await createCounter();
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/Increment',
        command: { amount: -3 },
      });

      expect(result.status).toBe(422);
      expect(result.body).toMatchObject({
        success: false,
        errors: [{ code: 422, message: 'Cannot increment by -3' }],
      });
      expect(sql.rows.get('CounterAggregate:c1')?.version).toBe(1);
    });

    it('rejects a malformed event via eventSchema BEFORE writing state', async () => {
      await createCounter();
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/BadEvent',
        command: {},
      });

      expect(result.status).toBe(500);
      expect(JSON.stringify(result.body)).toContain('malformed');
      // Nothing was persisted.
      expect(sql.rows.get('CounterAggregate:c1')?.version).toBe(1);
      expect(result.event).toBeNull();
    });

    it('returns a 409 VersionConflictError when the guarded update misses', async () => {
      await createCounter();
      sql.failNextUpdate = true;
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/Increment',
        command: { amount: 2 },
      });

      expect(result.status).toBe(409);
      expect(result.body).toMatchObject({
        __ceves: { name: 'VersionConflictError', httpStatusCode: 409 },
      });
      expect(result.event).toBeNull();
    });

    it('runs the customizeResponse hook', async () => {
      await createCounter();
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/CustomIncrement',
        command: { amount: 1 },
      });

      expect(result.status).toBe(200);
      expect((result.body as { serverComputed: string }).serverComputed).toBe('yes');
    });

    it('404s an unknown route key', async () => {
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/DoesNotExist',
        command: {},
      });
      expect(result.status).toBe(404);
    });

    it('resolves a concrete URL path against the registered pattern', async () => {
      await createCounter();
      const result = await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/c1/Increment',
        command: { amount: 2 },
      });
      expect(result.status).toBe(200);
      expect(sql.rows.get('CounterAggregate:c1')?.state.count).toBe(2);
    });
  });

  describe('queries', () => {
    it('executes a query against the state row', async () => {
      await createCounter();
      await dispatcher.executeCommand({
        aggregateId: 'c1',
        routeKey: 'POST:/counters/:id/Increment',
        command: { amount: 7 },
      });

      const result = await dispatcher.executeQuery({
        aggregateId: 'c1',
        routeKey: 'GET:/counters/:id/value',
        query: { echo: 'hello' },
      });

      expect(result.status).toBe(200);
      expect(result.body).toEqual({ count: 7, echoed: 'hello' });
      expect(result.event).toBeNull();
    });

    it('404s a query on a missing aggregate', async () => {
      const result = await dispatcher.executeQuery({
        aggregateId: 'ghost',
        routeKey: 'GET:/counters/:id/value',
        query: {},
      });
      expect(result.status).toBe(404);
      expect(result.body).toMatchObject({ __ceves: { name: 'AggregateNotFoundError' } });
    });
  });

  it('rejects an unsafe schema identifier at construction', () => {
    expect(() => createPgDispatcher(sql, { schema: 'bad;DROP TABLE x' })).toThrow(
      /Invalid PostgreSQL schema name/u,
    );
  });
});
