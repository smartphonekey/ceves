/**
 * Unit tests for the transactional outbox:
 * - in-database side: fetch interception during command dispatch, explicit
 *   getPgOutbox() enqueue, compensation when a command fails, query-mode
 *   fetch rejection (dispatcher integration), and
 * - wrapper side: the drainPgOutbox relay (claim → deliver → delete,
 *   retry/backoff, dead-letter, custom deliverer kinds).
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import { Route, clearRoutes } from '../../routing/Route';
import { EventHandler, clearEventHandlers, type IEventHandler } from '../../decorators';
import type { EventMetadata } from '../../events/EventMetadata';
import { BaseState } from '../../schemas/State';
import { CevesError } from '../../errors/CevesError';
import { createPgDispatcher } from './dispatcher';
import { registerPgAggregateState, clearPgAggregateStates } from './registry';
import { getPgOutbox } from './outbox';
import { drainPgOutbox, type PgOutboxRow } from './outboxRelay';
import { InMemoryEventSink } from './eventSink';
import { registerPgRoutes } from './gateway';
import type { PgQueryClient, PgSql } from './types';

/* ------------------------------------------------------------------ */
/* In-database side: dispatcher + FakePgSql with outbox support        */
/* ------------------------------------------------------------------ */

interface FakeOutboxRow {
  id: number;
  kind: string;
  aggregate_type: string;
  aggregate_id: string;
  request: unknown;
}

/** Extends the aggregate_state fake with the outbox statements. */
class FakePgSql implements PgSql {
  rows = new Map<string, { state: Record<string, unknown>; version: number; org_id: string }>();
  outbox: FakeOutboxRow[] = [];
  private nextOutboxId = 1;

  private key(params: unknown[]): string {
    return `${String(params[0])}:${String(params[1])}`;
  }

  execute(query: string, params: unknown[] = []): unknown {
    if (query.includes('.outbox')) return this.executeOutbox(query, params);
    if (query.startsWith('SELECT state')) {
      const row = this.rows.get(this.key(params));
      if (!row) return [];
      return [{ state: structuredClone(row.state), version: row.version, org_id: row.org_id }];
    }
    if (query.startsWith('INSERT INTO')) {
      if (this.rows.has(this.key(params))) return [];
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
      if (!row || row.version !== Number(expectedVersion)) return [];
      this.rows.set(this.key(params), {
        state: JSON.parse(String(stateJson)) as Record<string, unknown>,
        version: Number(newVersion),
        org_id: String(orgId),
      });
      return [{ version: newVersion }];
    }
    throw new Error(`FakePgSql: unexpected query: ${query}`);
  }

  private executeOutbox(query: string, params: unknown[]): unknown {
    if (query.startsWith('INSERT INTO')) {
      const [kind, aggregateType, aggregateId, requestJson] = params;
      const row: FakeOutboxRow = {
        id: this.nextOutboxId,
        kind: String(kind),
        aggregate_type: String(aggregateType),
        aggregate_id: String(aggregateId),
        request: JSON.parse(String(requestJson)) as unknown,
      };
      this.nextOutboxId += 1;
      this.outbox.push(row);
      return [{ id: row.id }];
    }
    if (query.startsWith('DELETE FROM')) {
      const id = Number(params[0]);
      this.outbox = this.outbox.filter((row) => row.id !== id);
      return 1;
    }
    throw new Error(`FakePgSql: unexpected outbox query: ${query}`);
  }
}

class PingState extends BaseState {
  pings = 0;
}

interface PingedEvent {
  type: 'Pinged';
}

class CreatePingRoute {
  static readonly isCreateCommand = true;
  aggregateType = 'PingAggregate';

  async executeCommand(): Promise<PingedEvent> {
    // Fire-and-forget webhook: response not needed → gets outboxed.
    const response = await fetch('https://hooks.example.com/ping', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ hello: 'world' }),
    });
    if (!response.ok) throw new Error('unexpected: outboxed fetch reported failure');
    // Explicit non-fetch enqueue (e.g. MQTT) via the ambient handle.
    getPgOutbox()?.enqueue('mqtt', { topic: 'pings', payload: 'ping' });
    return { type: 'Pinged' };
  }
}

class FailAfterFetchRoute {
  aggregateType = 'PingAggregate';

  async executeCommand(): Promise<PingedEvent> {
    await fetch('https://hooks.example.com/doomed', { method: 'POST' });
    throw new CevesError('business rule says no', 422);
  }
}

class ReadResponseRoute {
  aggregateType = 'PingAggregate';

  async executeCommand(): Promise<PingedEvent> {
    const response = await fetch('https://api.example.com/needs-response');
    // Illegal for an outboxed call — must fail the command loudly.
    await response.json();
    return { type: 'Pinged' };
  }
}

class FetchingQuery {
  aggregateType = 'PingAggregate';

  async executeQuery(): Promise<unknown> {
    return await fetch('https://api.example.com/lookup');
  }
}

@EventHandler
class PingedHandler implements IEventHandler<PingState, PingedEvent> {
  eventType = 'Pinged';
  aggregateType = 'PingAggregate';

  apply(state: PingState, _event: PingedEvent, metadata: EventMetadata): PingState {
    return { ...state, id: metadata.aggregateId, orgId: metadata.orgId, pings: state.pings + 1 };
  }
}

function registerTestRoutes(): void {
  const route = Route as unknown as (opts: {
    method: string;
    path: string;
  }) => (cls: unknown) => unknown;
  route({ method: 'POST', path: '/pings/:id/CreatePing' })(CreatePingRoute);
  route({ method: 'POST', path: '/pings/:id/FailAfterFetch' })(FailAfterFetchRoute);
  route({ method: 'POST', path: '/pings/:id/ReadResponse' })(ReadResponseRoute);
  route({ method: 'GET', path: '/pings/:id/lookup' })(FetchingQuery);
  EventHandler(PingedHandler);
}

describe('outbox capture during PG dispatch', () => {
  let sql: FakePgSql;
  let dispatcher: ReturnType<typeof createPgDispatcher>;
  const realFetch = globalThis.fetch;

  beforeEach(() => {
    clearRoutes();
    clearEventHandlers();
    clearPgAggregateStates();
    registerTestRoutes();
    registerPgAggregateState('PingAggregate', PingState);
    sql = new FakePgSql();
    dispatcher = createPgDispatcher(sql, { now: () => '2026-01-02T03:04:05.000Z' });
  });

  afterEach(() => {
    clearRoutes();
    clearEventHandlers();
    clearPgAggregateStates();
    expect(globalThis.fetch).toBe(realFetch); // interception always restored
  });

  it('intercepts fetch during a command and enqueues it with the state write', async () => {
    const result = await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/CreatePing',
      command: {},
      auth: { orgId: 'org-1' },
    });

    expect(result.status).toBe(201);
    // The intercepted fetch, the explicit mqtt enqueue, AND the emitted event
    // (committed with the state write) are all rows.
    expect(sql.outbox).toHaveLength(3);
    const [fetchRow, mqttRow, eventRow] = sql.outbox;
    expect(fetchRow).toMatchObject({
      kind: 'fetch',
      aggregate_type: 'PingAggregate',
      aggregate_id: 'p1',
      request: {
        url: 'https://hooks.example.com/ping',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{"hello":"world"}',
      },
    });
    expect(mqttRow).toMatchObject({
      kind: 'mqtt',
      request: { topic: 'pings', payload: 'ping' },
    });
    // The event is the LAST row — enqueued only after the state write
    // succeeded — and carries the full StoredEvent envelope.
    expect(eventRow).toMatchObject({
      kind: 'event',
      aggregate_type: 'PingAggregate',
      aggregate_id: 'p1',
      request: { aggregateType: 'PingAggregate', aggregateId: 'p1', version: 1, type: 'Pinged' },
    });
    // The dispatcher reports the row id so the wrapper skips its own append.
    expect(result.eventOutboxId).toBe(String(eventRow?.id));
    // State row written alongside.
    expect(sql.rows.get('PingAggregate:p1')?.version).toBe(1);
  });

  it('compensates enqueued rows when the command fails after fetching', async () => {
    await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/CreatePing',
      command: {},
    });
    const before = sql.outbox.length;

    const result = await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/FailAfterFetch',
      command: {},
    });

    expect(result.status).toBe(422);
    // The doomed command's row was deleted; earlier rows are untouched.
    expect(sql.outbox).toHaveLength(before);
    expect(sql.outbox.every((row) => !JSON.stringify(row.request).includes('doomed'))).toBe(true);
  });

  it('fails the command loudly when handler code reads an outboxed response', async () => {
    await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/CreatePing',
      command: {},
    });
    const before = sql.outbox.length;

    const result = await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/ReadResponse',
      command: {},
    });

    expect(result.status).toBe(500);
    expect(JSON.stringify(result.body)).toContain('cannot run inside');
    expect(sql.outbox).toHaveLength(before); // compensated
  });

  it('rejects fetch during a query with a descriptive error', async () => {
    await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/CreatePing',
      command: {},
    });

    const result = await dispatcher.executeQuery({
      aggregateId: 'p1',
      routeKey: 'GET:/pings/:id/lookup',
      query: {},
    });

    expect(result.status).toBe(500);
    expect(JSON.stringify(result.body)).toContain('read-only');
  });

  it('commits the event to the outbox in the same transaction as the state write', async () => {
    const result = await dispatcher.executeCommand({
      aggregateId: 'p2',
      routeKey: 'POST:/pings/:id/CreatePing',
      command: {},
      auth: { orgId: 'org-5' },
    });

    const eventRows = sql.outbox.filter((row) => row.kind === 'event');
    expect(eventRows).toHaveLength(1);
    // Same payload the wrapper would have appended — nothing is lost by
    // routing it through the outbox instead.
    expect(eventRows[0]?.request).toEqual(result.event);
    expect(result.eventOutboxId).not.toBeNull();
  });

  it('compensates the event row when the command fails after the handler ran', async () => {
    await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/CreatePing',
      command: {},
    });
    const eventsBefore = sql.outbox.filter((row) => row.kind === 'event').length;

    const result = await dispatcher.executeCommand({
      aggregateId: 'p1',
      routeKey: 'POST:/pings/:id/FailAfterFetch',
      command: {},
    });

    expect(result.status).toBe(422);
    // The failed command contributed no event row — state and event stay
    // consistent: neither was committed.
    expect(sql.outbox.filter((row) => row.kind === 'event')).toHaveLength(eventsBefore);
  });

  it('does not enqueue an event when outbox.events is disabled', async () => {
    // Interception stays on (the handler fetches); only event outboxing is off.
    const legacy = createPgDispatcher(sql, { outbox: { events: false } });
    const result = await legacy.executeCommand({
      aggregateId: 'p3',
      routeKey: 'POST:/pings/:id/CreatePing',
      command: {},
    });

    expect(result.status).toBe(201);
    // The event still comes back for the wrapper to append itself...
    expect(result.event).not.toBeNull();
    // ...but nothing was committed to the outbox for it.
    expect(result.eventOutboxId).toBeNull();
    expect(sql.outbox.filter((row) => row.kind === 'event')).toHaveLength(0);
    expect(sql.outbox.filter((row) => row.kind === 'fetch')).toHaveLength(1);
  });

  it('leaves fetch untouched when interception is disabled', async () => {
    const noIntercept = createPgDispatcher(sql, { outbox: { interceptFetch: false } });
    let fetchDuringDispatch: unknown;

    class ProbeRoute {
      static readonly isCreateCommand = true;
      aggregateType = 'PingAggregate';

      executeCommand(): Promise<PingedEvent> {
        fetchDuringDispatch = globalThis.fetch;
        return Promise.resolve({ type: 'Pinged' });
      }
    }
    (Route as unknown as (o: { method: string; path: string }) => (c: unknown) => unknown)({
      method: 'POST',
      path: '/pings/:id/Probe',
    })(ProbeRoute);

    await noIntercept.executeCommand({
      aggregateId: 'p9',
      routeKey: 'POST:/pings/:id/Probe',
      command: {},
    });
    expect(fetchDuringDispatch).toBe(realFetch);
  });
});

/* ------------------------------------------------------------------ */
/* Wrapper side: drainPgOutbox relay over a fake PgQueryClient         */
/* ------------------------------------------------------------------ */

interface RelayRow {
  id: number;
  kind: string;
  aggregate_type: string;
  aggregate_id: string;
  request: unknown;
  status: string;
  attempts: number;
}

/** Fake relay store honoring the claim / delete / fail statements. */
class FakeRelayClient implements PgQueryClient {
  rows: RelayRow[] = [];

  query(text: string, params: unknown[] = []): Promise<{ rows: Record<string, unknown>[] }> {
    if (text.includes("SET status = 'inflight'")) {
      const limit = Number(params[0]);
      const claimed = this.rows.filter((r) => r.status === 'pending').slice(0, limit);
      claimed.forEach((r) => {
        r.status = 'inflight';
      });
      return Promise.resolve({
        rows: claimed.map((r) => ({
          id: r.id,
          kind: r.kind,
          aggregate_type: r.aggregate_type,
          aggregate_id: r.aggregate_id,
          request: r.request,
          attempts: r.attempts,
        })),
      });
    }
    if (text.startsWith('DELETE FROM')) {
      const id = Number(params[0]);
      this.rows = this.rows.filter((r) => r.id !== id);
      return Promise.resolve({ rows: [] });
    }
    if (text.includes("SET status = 'dead'")) {
      const row = this.rows.find((r) => r.id === Number(params[0]));
      if (row) {
        row.status = 'dead';
        row.attempts = Number(params[1]);
      }
      return Promise.resolve({ rows: [] });
    }
    if (text.includes("SET status = 'pending'")) {
      const row = this.rows.find((r) => r.id === Number(params[0]));
      if (row) {
        row.status = 'pending-retry'; // parked for a later drain (backoff)
        row.attempts = Number(params[1]);
      }
      return Promise.resolve({ rows: [] });
    }
    throw new Error(`FakeRelayClient: unexpected query: ${text}`);
  }
}

function relayRow(id: number, kind: string, request: unknown, attempts = 0): RelayRow {
  return {
    id,
    kind,
    aggregate_type: 'PingAggregate',
    aggregate_id: 'p1',
    request,
    status: 'pending',
    attempts,
  };
}

describe('drainPgOutbox', () => {
  it('delivers fetch rows via the fetch impl and deletes them', async () => {
    const client = new FakeRelayClient();
    client.rows.push(
      relayRow(1, 'fetch', { url: 'https://hooks.example.com/a', method: 'POST', body: 'x' }),
    );
    const sent: unknown[] = [];

    const summary = await drainPgOutbox(client, {
      fetchImpl: (url, init) => {
        sent.push({ url, init });
        return Promise.resolve({ ok: true, status: 200 });
      },
    });

    expect(summary).toEqual({ claimed: 1, delivered: 1, retried: 0, dead: 0 });
    expect(client.rows).toHaveLength(0);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({ url: 'https://hooks.example.com/a' });
  });

  it('routes custom kinds to registered deliverers', async () => {
    const client = new FakeRelayClient();
    client.rows.push(relayRow(1, 'mqtt', { topic: 'pings' }));
    const published: PgOutboxRow[] = [];

    const summary = await drainPgOutbox(client, {
      deliverers: {
        mqtt: (row): Promise<void> => {
          published.push(row);
          return Promise.resolve();
        },
      },
    });

    expect(summary.delivered).toBe(1);
    expect(published[0]?.request).toEqual({ topic: 'pings' });
  });

  it('reschedules failed deliveries and dead-letters after maxAttempts', async () => {
    const client = new FakeRelayClient();
    client.rows.push(
      relayRow(1, 'fetch', { url: 'https://down.example.com' }), // attempts 0 → retry
      relayRow(2, 'fetch', { url: 'https://down.example.com' }, 2), // attempts 2 → dead at max 3
      relayRow(3, 'unknown-kind', {}), // no deliverer → retry path
    );

    const summary = await drainPgOutbox(client, {
      maxAttempts: 3,
      fetchImpl: () => Promise.resolve({ ok: false, status: 503 }),
    });

    expect(summary).toEqual({ claimed: 3, delivered: 0, retried: 2, dead: 1 });
    expect(client.rows.find((r) => r.id === 1)?.status).toBe('pending-retry');
    expect(client.rows.find((r) => r.id === 2)?.status).toBe('dead');
    expect(client.rows.find((r) => r.id === 3)?.status).toBe('pending-retry');
  });

  it('delivers event rows to the EventSink and deletes them', async () => {
    const client = new FakeRelayClient();
    const storedEvent = {
      aggregateType: 'PingAggregate',
      aggregateId: 'p1',
      version: 4,
      type: 'Pinged',
      timestamp: '2026-01-02T03:04:05.000Z',
      orgId: 'org-1',
      event: { type: 'Pinged' },
    };
    client.rows.push(relayRow(1, 'event', storedEvent));
    const sink = new InMemoryEventSink();

    const summary = await drainPgOutbox(client, { eventSink: sink });

    expect(summary.delivered).toBe(1);
    expect(sink.events).toEqual([storedEvent]);
    expect(client.rows).toHaveLength(0);
  });

  it('never dead-letters an event — a dropped event would hole the log', async () => {
    const client = new FakeRelayClient();
    // attempts already far beyond maxAttempts: a 'fetch' row would be parked
    // as dead, an 'event' row must keep retrying.
    client.rows.push(relayRow(1, 'event', { type: 'Pinged', version: 1 }, 99));
    client.rows.push(relayRow(2, 'fetch', { url: 'https://down.example.com' }, 99));

    const failingSink = {
      append: (): Promise<void> => Promise.reject(new Error('R2 unavailable')),
    };
    const summary = await drainPgOutbox(client, {
      eventSink: failingSink,
      maxAttempts: 3,
      fetchImpl: () => Promise.resolve({ ok: false, status: 500 }),
    });

    expect(summary).toMatchObject({ retried: 1, dead: 1 });
    expect(client.rows.find((r) => r.id === 1)?.status).toBe('pending-retry');
    expect(client.rows.find((r) => r.id === 2)?.status).toBe('dead');
  });

  it('rejects an unsafe schema name', async () => {
    await expect(drainPgOutbox(new FakeRelayClient(), { schema: 'x; DROP' })).rejects.toThrow(
      /Invalid PostgreSQL schema name/u,
    );
  });
});

/* ------------------------------------------------------------------ */
/* Gateway fast path: drain after a successful command                 */
/* ------------------------------------------------------------------ */

/** Answers execute_command AND relay statements, tracking claims. */
class GatewayOutboxClient implements PgQueryClient {
  claims = 0;
  deleted: number[] = [];
  /** The dispatch result the fake execute_command returns. */
  commandResult: Record<string, unknown> = {
    status: 200,
    body: { success: true, aggregateId: 'p1', version: 2, event: null },
    event: null,
  };
  /** The row the first claim hands back. */
  claimRow: Record<string, unknown> = {
    id: 7,
    kind: 'mqtt',
    aggregate_type: 'PingAggregate',
    aggregate_id: 'p1',
    request: { topic: 'pings' },
    attempts: 0,
  };

  query(text: string, params: unknown[] = []): Promise<{ rows: Record<string, unknown>[] }> {
    if (text.includes('execute_command')) {
      return Promise.resolve({ rows: [{ result: this.commandResult }] });
    }
    if (text.includes("SET status = 'inflight'")) {
      this.claims += 1;
      if (this.claims > 1) return Promise.resolve({ rows: [] });
      return Promise.resolve({ rows: [this.claimRow] });
    }
    if (text.startsWith('DELETE FROM')) {
      this.deleted.push(Number(params[0]));
      return Promise.resolve({ rows: [] });
    }
    throw new Error(`GatewayOutboxClient: unexpected query: ${text}`);
  }
}

describe('gateway drainAfterCommand', () => {
  beforeEach(() => {
    clearRoutes();
    const route = Route as unknown as (opts: {
      method: string;
      path: string;
    }) => (cls: unknown) => unknown;

    class PokeRoute {
      aggregateType = 'PingAggregate';

      executeCommand(): Promise<PingedEvent> {
        return Promise.resolve({ type: 'Pinged' });
      }
    }
    route({ method: 'POST', path: '/pings/:id/Poke' })(PokeRoute);
  });

  afterEach(() => {
    clearRoutes();
  });

  it('drains enqueued rows in the background after a 2xx command', async () => {
    const client = new GatewayOutboxClient();
    const published: PgOutboxRow[] = [];
    const app = new Hono();
    registerPgRoutes(app, {
      client,
      runSideEffects: false,
      outbox: {
        deliverers: {
          mqtt: (row): Promise<void> => {
            published.push(row);
            return Promise.resolve();
          },
        },
      },
    });

    const res = await app.request('/pings/p1/Poke', { method: 'POST', body: '{}' });
    expect(res.status).toBe(200);

    // The drain is scheduled as a background promise — let it settle.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(published).toHaveLength(1);
    expect(published[0]?.kind).toBe('mqtt');
    expect(client.deleted).toEqual([7]);
  });

  it('delivers an outboxed event through the relay instead of appending it twice', async () => {
    const storedEvent = {
      aggregateType: 'PingAggregate',
      aggregateId: 'p1',
      version: 2,
      type: 'Pinged',
      timestamp: '2026-01-02T03:04:05.000Z',
      orgId: 'org-1',
      event: { type: 'Pinged' },
    };
    const client = new GatewayOutboxClient();
    // PostgreSQL committed the event to the outbox with the state write.
    client.commandResult = {
      status: 200,
      body: { success: true, aggregateId: 'p1', version: 2, event: null },
      event: storedEvent,
      eventOutboxId: '42',
    };
    client.claimRow = {
      id: 42,
      kind: 'event',
      aggregate_type: 'PingAggregate',
      aggregate_id: 'p1',
      request: storedEvent,
      attempts: 0,
    };

    const sink = new InMemoryEventSink();
    const app = new Hono();
    // No explicit `outbox` option: an outboxed event must still be drained,
    // with the gateway's own eventSink wired into the relay automatically.
    registerPgRoutes(app, { client, eventSink: sink, runSideEffects: false });

    const res = await app.request('/pings/p1/Poke', { method: 'POST', body: '{}' });
    expect(res.status).toBe(200);

    await new Promise((resolve) => setTimeout(resolve, 0));
    // Delivered exactly once — by the relay, which also removed the row.
    expect(sink.events).toEqual([storedEvent]);
    expect(client.deleted).toEqual([42]);
  });
});
