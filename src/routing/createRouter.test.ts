/**
 * Tests for createRouter factory
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { OpenAPIRoute } from 'chanfana';
import { createRouter } from './createRouter.js';
import { Route, clearRoutes } from './Route.js';
import { z } from 'zod';

describe('createRouter', () => {
  beforeEach(() => {
    // Clear routes before each test
    clearRoutes();
  });

  it('should create a router with default configuration', () => {
    const router = createRouter();

    expect(router).toBeDefined();
    expect(router.fetch).toBeDefined();
  });

  it('should create a router with custom OpenAPI metadata', () => {
    const router = createRouter({
      openapi: {
        title: 'Test API',
        version: '2.0.0',
        description: 'Test API description',
      },
    });

    expect(router).toBeDefined();
    expect(router.fetch).toBeDefined();
  });

  it('should auto-discover and register decorated routes', async () => {
    @Route
    class TestGetRoute extends OpenAPIRoute {
      schema = {
        request: {
          params: z.object({
            id: z.string(),
          }),
        },
        responses: {
          200: {
            description: 'Success',
            content: {
              'application/json': {
                schema: z.object({
                  id: z.string(),
                  message: z.string(),
                }),
              },
            },
          },
        },
      };

      async handle() {
        const data = await this.getValidatedData<typeof this.schema>();
        return {
          id: data.params.id,
          message: 'Test response',
        };
      }
    }
    TestGetRoute; // Side-effect: registers route

    const router = createRouter({
      openapi: {
        title: 'Test API',
        version: '1.0.0',
      },
    });

    expect(router).toBeDefined();
  });

  it('should expose OpenAPI schema at configured path', async () => {
    @Route
    class SimpleRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {
          200: {
            description: 'Success',
            content: {
              'application/json': {
                schema: z.object({ success: z.boolean() }),
              },
            },
          },
        },
      };

      async handle() {
        return { success: true };
      }
    }
    SimpleRoute; // Side-effect: registers route

    const router = createRouter({
      schemaPath: '/api-schema.json',
      openapi: {
        title: 'Schema Test API',
        version: '1.0.0',
      },
    });

    // Create a mock request to the schema endpoint
    const request = new Request('http://localhost/api-schema.json');
    const response = await router.fetch(request);

    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('application/json');

    const schema = await response.json();
    expect(schema).toBeDefined();
    expect(schema.info).toBeDefined();
    expect(schema.info.title).toBe('Schema Test API');
    expect(schema.info.version).toBe('1.0.0');
  });

  it('should support custom base path configuration', () => {
    const router = createRouter({
      basePath: '/api/v2',
      openapi: {
        title: 'V2 API',
        version: '2.0.0',
      },
    });

    expect(router).toBeDefined();
  });

  it('should allow disabling Swagger UI', () => {
    const router = createRouter({
      enableDocs: false,
      openapi: {
        title: 'No Docs API',
        version: '1.0.0',
      },
    });

    expect(router).toBeDefined();
  });

  it('should handle multiple routes with different HTTP methods', () => {
    @Route
    class GetUsersRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {
          200: {
            description: 'List of users',
            content: {
              'application/json': {
                schema: z.array(z.object({ id: z.string() })),
              },
            },
          },
        },
      };

      async handle() {
        return [{ id: '1' }, { id: '2' }];
      }
    }

    @Route
    class CreateUserRoute extends OpenAPIRoute {
      schema = {
        request: {
          body: {
            content: {
              'application/json': {
                schema: z.object({
                  name: z.string(),
                  email: z.email(),
                }),
              },
            },
          },
        },
        responses: {
          201: {
            description: 'User created',
            content: {
              'application/json': {
                schema: z.object({ id: z.string(), name: z.string() }),
              },
            },
          },
        },
      };

      async handle() {
        return { id: '123', name: 'Test User' };
      }
    }
    GetUsersRoute; // Side-effect: registers route
    CreateUserRoute; // Side-effect: registers route

    const router = createRouter({
      openapi: {
        title: 'Multi-Method API',
        version: '1.0.0',
      },
    });

    expect(router).toBeDefined();
  });

  describe('AA-119: onError fallback never re-throws', () => {
    it('coerces a bare Error into a 500 JSON envelope with the actual message', async () => {
      const router = createRouter();
      router.get('/throw-error', () => {
        throw new Error('something concrete went wrong');
      });

      const response = await router.fetch(
        new Request('https://test/throw-error'),
        {},
        {} as unknown as ExecutionContext,
      );

      expect(response.status).toBe(500);
      const body = (await response.json());
      expect(body.success).toBe(false);
      expect(body.errors[0].code).toBe(500);
      // Before AA-119 this used to be `throw err` → Hono's default
      // handler stripped the message. Now it survives.
      expect(body.errors[0].message).toBe('something concrete went wrong');
    });

    it('coerces a non-Error throw (string, object) into a 500 with non-empty message', async () => {
      const router = createRouter();
      router.get('/throw-string', () => {
        // Domain code that does `throw "uh oh"` — historically produced
        // empty Sentry messages (AA-111). Must not happen anymore.
        throw 'uh oh';
      });

      const response = await router.fetch(
        new Request('https://test/throw-string'),
        {},
        {} as unknown as ExecutionContext,
      );

      expect(response.status).toBe(500);
      const body = (await response.json());
      // Crucial: not empty, not "[object Object]", contains the payload
      expect(body.errors[0].message).toContain('uh oh');
    });

    it('coerces a plain object throw into a 500 with JSON-serialised payload in message', async () => {
      const router = createRouter();
      router.get('/throw-object', () => {
        throw { reason: 'rate-limited', retryAfter: 30 };
      });

      const response = await router.fetch(
        new Request('https://test/throw-object'),
        {},
        {} as unknown as ExecutionContext,
      );

      expect(response.status).toBe(500);
      const body = (await response.json());
      expect(body.errors[0].message).toContain('rate-limited');
      expect(body.errors[0].message).toContain('30');
    });

    /**
     * AA-111 regression. SPK-API-K logged 19 unhandled Errors on
     * `POST /locks/:id/delete-temp-key` with **empty** `.message` over a
     * week — the symptom of D1 rejecting via a bare envelope (no
     * `.message` field) inside `findTempKeyByUuid`. The pre-PR-#107
     * onError handler re-threw, Hono's default surfaced an empty Error,
     * Sentry captured it with no diagnostic content. After the fix the
     * envelope's own fields must end up in the response message so the
     * triage path is greppable.
     */
    it('AA-111: D1-style envelope rejection (no .message, but has .code / .cause) produces actionable 500', async () => {
      const router = createRouter();
      // Models what `db.prepare(...).first()` rejects with when the
      // table doesn't exist or a binding is missing. The runtime turns
      // this back into a plain object on the worker side — no
      // `.message`, no Error prototype.
      router.get('/d1-envelope', () => {
        throw { code: 'D1_ERROR', cause: 'no such table: temp_keys' };
      });

      const response = await router.fetch(
        new Request('https://test/d1-envelope'),
        {},
        {} as unknown as ExecutionContext,
      );

      expect(response.status).toBe(500);
      const body = (await response.json());
      // The envelope fields MUST survive into the response — that's the
      // whole point of the fix. Empty messages used to leak straight to
      // Sentry; now the cause and code are visible.
      expect(body.errors[0].message).toContain('D1_ERROR');
      expect(body.errors[0].message).toContain('no such table');
      // And never an empty string — the bug we're regressing against.
      expect(body.errors[0].message.length).toBeGreaterThan(3);
    });
  });

  /**
   * AA-193 crash class: a response sent while the incoming request body was
   * never read destabilizes wrangler's ProxyWorker in local dev ("Can't read
   * from request stream after response has been sent" → Broken pipe →
   * "Network connection lost" → worker death; reproduced 5/5 in
   * docs/ci-evidence/AA-193-local-workerd-failure.md). The DO side gained
   * `drainRequestBody` in ceves; this guard is the WORKER-side equivalent —
   * the outermost middleware in every router, so no early-return
   * path (auth rejection, 404, thrown error, plain route) can leave a live
   * unread body behind. Site-by-site drains (unifiedAuth #285) remain as
   * defense in depth; this makes the property structural.
   */
  describe('AA-193: request-body drain guard', () => {
    /** POST request whose body is a live stream with an observable cancel. */
    const streamedRequest = (url: string) => {
      let cancelled = false;
      let pulled = false;
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          pulled = true;
          controller.enqueue(new TextEncoder().encode('{"k":"v"}'));
          controller.close();
        },
        cancel() {
          cancelled = true;
        },
      });
      const request = new Request(url, {
        method: 'POST',
        body,
        headers: { 'Content-Type': 'application/json' },
        // @ts-expect-error duplex is required for stream bodies but missing from the lib types
        duplex: 'half',
      });
      return { request, wasCancelled: () => cancelled, wasPulled: () => pulled };
    };

    it('drains the body when a handler responds without reading it (early-401 shape)', async () => {
      const router = createRouter();
      router.post('/early-reject', (c) => c.json({ error: 'Unauthorized' }, 401));

      const { request, wasCancelled } = streamedRequest('https://test/early-reject');
      const response = await router.fetch(request, {}, {} as unknown as ExecutionContext);

      expect(response.status).toBe(401);
      expect(wasCancelled()).toBe(true);
    });

    it('drains the body on an unmatched route (404 shape)', async () => {
      const router = createRouter();

      const { request, wasCancelled } = streamedRequest('https://test/no-such-route');
      const response = await router.fetch(request, {}, {} as unknown as ExecutionContext);

      expect(response.status).toBe(404);
      expect(wasCancelled()).toBe(true);
    });

    it('drains the body when the handler throws and onError builds the response', async () => {
      const router = createRouter();
      router.post('/throws', () => {
        throw new Error('boom before reading the body');
      });

      const { request, wasCancelled } = streamedRequest('https://test/throws');
      const response = await router.fetch(request, {}, {} as unknown as ExecutionContext);

      expect(response.status).toBe(500);
      expect(wasCancelled()).toBe(true);
    });

    it('leaves a consumed body alone (normal command shape)', async () => {
      const router = createRouter();
      router.post('/consumes', async (c) => {
        const parsed = (await c.req.json());
        return c.json({ echoed: parsed.k });
      });

      const { request, wasCancelled } = streamedRequest('https://test/consumes');
      const response = await router.fetch(request, {}, {} as unknown as ExecutionContext);

      expect(response.status).toBe(200);
      expect((await response.json())).toEqual({ echoed: 'v' });
      expect(wasCancelled()).toBe(false);
    });

    it('is a no-op for bodyless requests', async () => {
      const router = createRouter();
      router.get('/plain', (c) => c.json({ ok: true }));

      const response = await router.fetch(
        new Request('https://test/plain'),
        {},
        {} as unknown as ExecutionContext,
      );

      expect(response.status).toBe(200);
    });
  });
});
