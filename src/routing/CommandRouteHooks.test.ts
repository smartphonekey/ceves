/**
 * Tests for the `beforeCommit` and `afterCommit` lifecycle hooks added
 * to `CommandRoute`.
 *
 * Mocking note: `Chanfana`'s `OpenAPIRoute.getValidatedData()` and
 * `c.req.raw` access make end-to-end exercising of the full handler
 * heavy. The tests here cover the hook contract through a thin subclass
 * that overrides `buildCommand` / `getDurableObjectStub` / `getValidatedData`
 * so each hook's behaviour is verified in isolation.
 */
import { describe, it, expect, vi } from 'vitest';
import { CommandRoute } from './CommandRoute';
import { CevesError } from '../errors/CevesError';

class FakeDOStub {
  constructor(private readonly response: Response) {}
  fetch(_req: Request): Promise<Response> {
    return Promise.resolve(this.response.clone());
  }
}

function makeContext(): Parameters<CommandRoute['handle']>[0] {
  const headers = new Headers({ 'content-type': 'application/json' });
  const vars: Record<string, unknown> = {};
  const ctx: Record<string, unknown> = {
    req: {
      param: (k: string) => (k === 'id' ? 'lock-1' : undefined),
      raw: new Request('https://w.invalid/locks/lock-1/create', {
        method: 'POST',
        headers,
        body: JSON.stringify({ foo: 'bar' }),
      }),
    },
    env: { DB: undefined, LOCK: undefined },
    get: (key: string) => vars[key],
    set: () => undefined,
    json: (body: unknown, status = 200) =>
      new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }),
  };
  return ctx as unknown as Parameters<CommandRoute['handle']>[0];
}

const OK_RESPONSE = (): Response =>
  new Response(
    JSON.stringify({
      success: true,
      aggregateId: 'lock-1',
      version: 1,
      event: { type: 'X', data: { hello: 'world' } },
    }),
    { status: 200, headers: { 'Content-Type': 'application/json' } },
  );

class TestRoute extends CommandRoute<{ foo: string }, { lockId: number }, never> {
  doResponse: Response = OK_RESPONSE();
  aggregateType = 'LockAggregate';
  schema = {} as never;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  executeCommand(): Promise<any> {
    return Promise.resolve({} as never);
  }

  // Stub heavy framework deps so we can exercise handle() directly.
  override buildCommand(): Promise<{ foo: string }> {
    return Promise.resolve({ foo: 'bar' });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected override getDurableObjectStub(): any {
    return new FakeDOStub(this.doResponse);
  }

  // Bypass Chanfana validation — return the pre-built command body directly.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getValidatedData(): Promise<any> {
    return Promise.resolve({ body: { foo: 'bar' } });
  }
}

describe('CommandRoute hooks', () => {
  it('does NOT call afterCommit when the DO returns a non-2xx response', async () => {
    const after = vi.fn();
    class R extends TestRoute {
      protected override afterCommit = after;
    }
    const route = new R({} as never);
    route.doResponse = new Response(JSON.stringify({ success: false, errors: [] }), {
      status: 409,
      headers: { 'Content-Type': 'application/json' },
    });

    const res = await route.handle(makeContext());
    expect(res.status).toBe(409);
    expect(after).not.toHaveBeenCalled();
  });

  it('calls afterCommit with (c, eventData, command) after a 2xx DO response', async () => {
    const after = vi.fn().mockResolvedValue(undefined);
    class R extends TestRoute {
      protected override afterCommit = after;
    }
    const route = new R({} as never);
    const res = await route.handle(makeContext());

    expect(res.status).toBe(200);
    expect(after).toHaveBeenCalledOnce();
    const firstCall = after.mock.calls[0] ?? [];
    expect(firstCall[1]).toEqual({ hello: 'world' });
    expect(firstCall[2]).toEqual({ foo: 'bar' });
  });

  it('merges the afterCommit return value into the response body', async () => {
    class R extends TestRoute {
      protected override afterCommit(): Promise<Record<string, unknown>> {
        return Promise.resolve({ tempKeyUuid: 'inject-me' });
      }
    }
    const route = new R({} as never);
    const res = await route.handle(makeContext());
    expect(res.status).toBe(200);
    const body: Record<string, unknown> = await res.json();
    expect(body.success).toBe(true);
    expect(body.aggregateId).toBe('lock-1');
    expect(body.tempKeyUuid).toBe('inject-me');
  });

  it('swallows afterCommit errors — response still 2xx (event is already durable)', async () => {
    class R extends TestRoute {
      protected override afterCommit(): Promise<void> {
        return Promise.reject(new Error('D1 unavailable'));
      }
    }
    const route = new R({} as never);
    const res = await route.handle(makeContext());
    expect(res.status).toBe(200);
  });

  it('beforeCommit throwing a CevesError short-circuits with the typed response', async () => {
    class R extends TestRoute {
      protected override beforeCommit(): Promise<void> {
        return Promise.reject(new CevesError('Resource missing', 404));
      }
    }
    const route = new R({} as never);
    const res = await route.handle(makeContext());
    expect(res.status).toBe(404);
    const body: { success: boolean; errors: unknown[] } = await res.json();
    expect(body.success).toBe(false);
    expect(body.errors).toBeDefined();
  });

  it('beforeCommit throwing a plain Error bubbles to the framework error handler', async () => {
    class R extends TestRoute {
      protected override beforeCommit(): Promise<void> {
        return Promise.reject(new Error('Boom'));
      }
    }
    const route = new R({} as never);
    await expect(route.handle(makeContext())).rejects.toThrow('Boom');
  });

  it('routes without hooks pass through unchanged (no extra body fields)', async () => {
    const route = new TestRoute({} as never);
    const res = await route.handle(makeContext());
    expect(res.status).toBe(200);
    const body: Record<string, unknown> = await res.json();
    expect(Object.keys(body).sort((a, b) => a.localeCompare(b))).toEqual([
      'aggregateId',
      'event',
      'success',
      'version',
    ]);
  });
});
