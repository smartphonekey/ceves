/**
 * Unit tests for the thin Hono gateway: same OpenAPI paths, execution in
 * PostgreSQL (faked SQL client), events shipped to the EventSink.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Hono } from 'hono';
import { z } from 'zod';
import { Route, clearRoutes } from '../../routing/Route';
import { EventHandler, clearEventHandlers, type IEventHandler } from '../../decorators';
import { BaseState } from '../../schemas/State';
import type { StoredEvent } from '../../storage/interfaces';
import { registerPgRoutes, type PgQueryClient } from './gateway';
import { InMemoryEventSink } from './eventSink';
import type { PgDispatchResult } from './types';

class WidgetState extends BaseState {
  name = '';
}

interface WidgetRenamedEvent {
  type: 'WidgetRenamed';
  name: string;
}

const sideEffectCalls: unknown[] = [];

class RenameWidgetRoute {
  aggregateType = 'WidgetAggregate';
  schema = {
    request: {
      body: {
        content: {
          'application/json': { schema: z.object({ name: z.string().min(1) }) },
        },
      },
    },
  };

  executeCommand(command: { name: string }): Promise<WidgetRenamedEvent> {
    return Promise.resolve({ type: 'WidgetRenamed', name: command.name });
  }
}

class GetWidgetQuery {
  aggregateType = 'WidgetAggregate';

  executeQuery(state: WidgetState): Promise<{ name: string }> {
    return Promise.resolve({ name: state.name });
  }
}

@EventHandler
class WidgetRenamedHandler implements IEventHandler<WidgetState, WidgetRenamedEvent> {
  eventType = 'WidgetRenamed';
  aggregateType = 'WidgetAggregate';

  apply(state: WidgetState, event: WidgetRenamedEvent): WidgetState {
    return { ...state, name: event.name };
  }

  sideEffects(event: WidgetRenamedEvent): Promise<void> {
    sideEffectCalls.push(event);
    return Promise.resolve();
  }
}

/** Fake async SQL client that records calls and returns a canned result. */
class FakeClient implements PgQueryClient {
  calls: { text: string; params: unknown[] }[] = [];
  nextResult: PgDispatchResult = { status: 200, body: { success: true }, event: null };

  query(text: string, params: unknown[] = []): Promise<{ rows: Record<string, unknown>[] }> {
    this.calls.push({ text, params });
    return Promise.resolve({ rows: [{ result: this.nextResult }] });
  }
}

function sampleEvent(): StoredEvent {
  return {
    aggregateType: 'WidgetAggregate',
    aggregateId: 'w1',
    version: 3,
    type: 'WidgetRenamed',
    timestamp: '2026-01-02T03:04:05.000Z',
    orgId: 'org-9',
    event: { type: 'WidgetRenamed', name: 'gizmo' },
  };
}

describe('registerPgRoutes', () => {
  let app: Hono;
  let client: FakeClient;
  let sink: InMemoryEventSink;

  beforeEach(() => {
    clearRoutes();
    clearEventHandlers();
    sideEffectCalls.length = 0;
    const route = Route as unknown as (opts: {
      method: string;
      path: string;
    }) => (cls: unknown) => unknown;
    route({ method: 'POST', path: '/widgets/:id/Rename' })(RenameWidgetRoute);
    route({ method: 'GET', path: '/widgets/:id/details' })(GetWidgetQuery);
    EventHandler(WidgetRenamedHandler);

    app = new Hono();
    client = new FakeClient();
    sink = new InMemoryEventSink();
  });

  afterEach(() => {
    clearRoutes();
    clearEventHandlers();
  });

  it('mounts every command/query route at its original path', () => {
    const mounted = registerPgRoutes(app, { client, eventSink: sink });
    expect(mounted).toEqual({ commands: 1, queries: 1 });
  });

  it('executes a command via the PostgreSQL function and preserves the endpoint', async () => {
    registerPgRoutes(app, { client, eventSink: sink, env: { REGION: 'eu' } });
    client.nextResult = {
      status: 200,
      body: { success: true, aggregateId: 'w1', version: 3, event: null },
      event: null,
    };

    const res = await app.request('/widgets/w1/Rename', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'gizmo' }),
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ success: true, aggregateId: 'w1' });

    const call = client.calls[0]!;
    expect(call.text).toContain('SELECT ceves.execute_command(');
    expect(call.params[0]).toBe('WidgetAggregate');
    expect(call.params[1]).toBe('w1');
    expect(call.params[2]).toBe('POST:/widgets/:id/Rename');
    expect(JSON.parse(String(call.params[3]))).toEqual({ name: 'gizmo' });
    expect(JSON.parse(String(call.params[5]))).toEqual({ REGION: 'eu' });
  });

  it('ships the returned event to the sink and runs sideEffects wrapper-side', async () => {
    registerPgRoutes(app, { client, eventSink: sink });
    client.nextResult = {
      status: 200,
      body: { success: true, aggregateId: 'w1', version: 3, event: null },
      event: sampleEvent(),
    };

    const res = await app.request('/widgets/w1/Rename', {
      method: 'POST',
      body: JSON.stringify({ name: 'gizmo' }),
    });

    expect(res.status).toBe(200);
    // Event appended to the external log (fire-and-forget → flush microtasks).
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(sink.events).toHaveLength(1);
    expect(sink.events[0]?.type).toBe('WidgetRenamed');
    // sideEffects executed with the domain event.
    expect(sideEffectCalls).toEqual([{ type: 'WidgetRenamed', name: 'gizmo' }]);
  });

  it('rejects an invalid body with the route Zod schema before touching PostgreSQL', async () => {
    registerPgRoutes(app, { client, eventSink: sink });

    const res = await app.request('/widgets/w1/Rename', {
      method: 'POST',
      body: JSON.stringify({ name: '' }),
    });

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ success: false });
    expect(client.calls).toHaveLength(0);
  });

  it('rejects malformed JSON with the DO-shaped InvalidRequestBody error', async () => {
    registerPgRoutes(app, { client, eventSink: sink });

    const res = await app.request('/widgets/w1/Rename', { method: 'POST', body: '{oops' });

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ success: false, error: 'InvalidRequestBody' });
  });

  it('passes error statuses from PostgreSQL through unchanged', async () => {
    registerPgRoutes(app, { client, eventSink: sink });
    client.nextResult = {
      status: 404,
      body: { success: false, errors: [{ code: 404, message: 'not found' }] },
      event: null,
    };

    const res = await app.request('/widgets/w1/Rename', {
      method: 'POST',
      body: JSON.stringify({ name: 'gizmo' }),
    });
    expect(res.status).toBe(404);
  });

  it('executes a query with URL params + query string merged', async () => {
    registerPgRoutes(app, { client, eventSink: sink });
    client.nextResult = { status: 200, body: { name: 'gizmo' }, event: null };

    const res = await app.request('/widgets/w1/details?verbose=1');

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ name: 'gizmo' });
    const call = client.calls[0]!;
    expect(call.text).toContain('SELECT ceves.execute_query(');
    expect(JSON.parse(String(call.params[3]))).toEqual({ id: 'w1', verbose: '1' });
  });

  it('propagates the authContext middleware contract into the auth payload', async () => {
    registerPgRoutes(app, { client, eventSink: sink });
    // Simulate the auth middleware the worker installs ahead of the routes.
    const authedApp = new Hono();
    authedApp.use('*', async (c, next) => {
      c.set('authContext', { authType: 'api-key', orgId: 'org-42', isSuper: true });
      await next();
    });
    authedApp.route('/', app);

    await authedApp.request('/widgets/w1/Rename', {
      method: 'POST',
      body: JSON.stringify({ name: 'gizmo' }),
    });

    const call = client.calls[0]!;
    expect(JSON.parse(String(call.params[4]))).toEqual({ orgId: 'org-42', isSuper: true });
  });

  it('rejects an unsafe schema name', () => {
    expect(() => registerPgRoutes(app, { client, schema: 'x; DROP' })).toThrow(
      /Invalid PostgreSQL schema name/u,
    );
  });
});
