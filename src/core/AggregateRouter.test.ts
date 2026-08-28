/**
 * Tests for AggregateRouter routing and auth-header injection.
 *
 * Routing is a closed allowlist over configured aggregate types — the
 * generalized replacement for what used to be hardcoded domain maps:
 * - plural → singular: strip a trailing 's' (`users` → `user`), with
 *   configurable overrides for irregular plurals;
 * - singular type → DO binding: uppercase the singular (`user` → env.USER)
 *   for types listed in `allowedTypes`, with `bindingNames` overrides;
 * - anything not registered via configure() gets a 400.
 *
 * The auth-header names injected here MUST stay aligned with what
 * AggregateObject.fetch parses back out (X-Org-Id / X-User-Id /
 * X-User-Email / X-Is-Admin / X-Super-Access) — the last test pins that
 * contract.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { AggregateRouter, type AuthContext } from './AggregateRouter';

interface CapturedFetch {
  namespaceName: string;
  idName: string;
  request: Request;
}

function makeEnv(bindings: string[], captured: CapturedFetch[]) {
  const env: Record<string, unknown> = {};
  for (const name of bindings) {
    env[name] = {
      idFromName: (idName: string) => ({ idName }),
      get: (id: { idName: string }) => ({
        fetch: (request: Request) => {
          captured.push({ namespaceName: name, idName: id.idName, request });
          return Promise.resolve(new Response('ok', { status: 200 }));
        },
      }),
    };
  }
  return env;
}

describe('AggregateRouter', () => {
  beforeEach(() => {
    AggregateRouter.resetConfig();
  });

  describe('routing conventions', () => {
    it('routes the legacy plurals exactly as the old hardcoded maps did', async () => {
      // users→USER, locks→LOCK, hubs→HUB, tempkeys→TEMPKEY were the four
      // hardcoded entries; with the types registered, the conventions must
      // preserve that routing bit-for-bit.
      AggregateRouter.configure({
        allowedTypes: ['user', 'lock', 'hub', 'tempkey'],
      });
      const cases: Array<[string, string]> = [
        ['users', 'USER'],
        ['locks', 'LOCK'],
        ['hubs', 'HUB'],
        ['tempkeys', 'TEMPKEY'],
      ];
      for (const [plural, binding] of cases) {
        const captured: CapturedFetch[] = [];
        const env = makeEnv([binding], captured);
        const res = await AggregateRouter.forward(
          env,
          new Request(`https://x.example/${plural}/agg-1/DoThing`, { method: 'POST' })
        );
        expect(res.status).toBe(200);
        expect(captured).toHaveLength(1);
        expect(captured[0]!.namespaceName).toBe(binding);
        expect(captured[0]!.idName).toBe('agg-1');
      }
    });

    it('routes an allowed type to the uppercased singular binding', async () => {
      AggregateRouter.configure({ allowedTypes: ['widget'] });
      const captured: CapturedFetch[] = [];
      const env = makeEnv(['WIDGET'], captured);
      const res = await AggregateRouter.forward(
        env,
        new Request('https://x.example/widgets/w-9/Spin', { method: 'POST' })
      );
      expect(res.status).toBe(200);
      expect(captured[0]!.namespaceName).toBe('WIDGET');
    });

    it('refuses unregistered types even when a matching DO binding exists', async () => {
      // The allowlist is what keeps internal DO namespaces (rate limiters,
      // coordinators, …) unreachable over HTTP.
      const captured: CapturedFetch[] = [];
      const env = makeEnv(['RATE_LIMITER'], captured);
      const res = await AggregateRouter.forward(
        env,
        new Request('https://x.example/rate_limiters/k-1/AnyCommand', { method: 'POST' })
      );
      expect(res.status).toBe(400);
      expect(captured).toHaveLength(0);
      const body = (await res.json());
      expect(body.error).toBe('InvalidAggregateType');
      // The error reports the literal URL segment, not the mangled singular.
      expect(body.message).toContain('rate_limiters');
    });

    it('honours configured irregular plurals and binding names', async () => {
      // A bindingNames entry allows the type by itself — no allowedTypes needed.
      AggregateRouter.configure({
        pluralToSingular: { people: 'person' },
        bindingNames: { person: 'PERSON_DO' },
      });
      const captured: CapturedFetch[] = [];
      const env = makeEnv(['PERSON_DO'], captured);
      const res = await AggregateRouter.forward(
        env,
        new Request('https://x.example/people/p-1/Greet', { method: 'POST' })
      );
      expect(res.status).toBe(200);
      expect(captured[0]!.namespaceName).toBe('PERSON_DO');
    });

    it('returns 400 InvalidAggregateType for a type never registered', async () => {
      AggregateRouter.configure({ allowedTypes: ['user'] });
      const env = makeEnv(['USER'], []);
      const res = await AggregateRouter.forward(
        env,
        new Request('https://x.example/gizmos/g-1/Do', { method: 'POST' })
      );
      expect(res.status).toBe(400);
      const body = (await res.json());
      expect(body.error).toBe('InvalidAggregateType');
      expect(body.message).toContain('gizmos');
    });

    it('returns 400 for an allowed type whose binding is missing from env', async () => {
      AggregateRouter.configure({ allowedTypes: ['user'] });
      const res = await AggregateRouter.forward(
        {},
        new Request('https://x.example/users/u-1/Do', { method: 'POST' })
      );
      expect(res.status).toBe(400);
    });

    it('returns 400 InvalidPathFormat when the path is too short', async () => {
      const env = makeEnv(['USER'], []);
      const res = await AggregateRouter.forward(
        env,
        new Request('https://x.example/users', { method: 'POST' })
      );
      expect(res.status).toBe(400);
      const body = (await res.json());
      expect(body.error).toBe('InvalidPathFormat');
    });
  });

  describe('auth-header contract with AggregateObject', () => {
    it('injects exactly the headers AggregateObject.fetch parses', async () => {
      AggregateRouter.configure({ allowedTypes: ['user'] });
      const captured: CapturedFetch[] = [];
      const env = makeEnv(['USER'], captured);
      const auth: AuthContext = {
        orgId: 'org-1',
        userId: 'user-1',
        email: 'a@example.com',
        isAdmin: true,
        isSuper: true,
      };
      await AggregateRouter.forward(
        env,
        new Request('https://x.example/users/u-1/Do', { method: 'POST' }),
        auth
      );
      const h = captured[0]!.request.headers;
      // These five names are read back by AggregateObject.fetch — keep in sync.
      expect(h.get('X-Org-Id')).toBe('org-1');
      expect(h.get('X-User-Id')).toBe('user-1');
      expect(h.get('X-User-Email')).toBe('a@example.com');
      expect(h.get('X-Is-Admin')).toBe('true');
      expect(h.get('X-Super-Access')).toBe('true');
    });

    it('omits auth headers that are not in the context', async () => {
      AggregateRouter.configure({ allowedTypes: ['user'] });
      const captured: CapturedFetch[] = [];
      const env = makeEnv(['USER'], captured);
      await AggregateRouter.forward(
        env,
        new Request('https://x.example/users/u-1/Do', { method: 'POST' }),
        { userId: 'user-1' }
      );
      const h = captured[0]!.request.headers;
      expect(h.get('X-User-Id')).toBe('user-1');
      expect(h.get('X-Org-Id')).toBeNull();
      expect(h.get('X-Is-Admin')).toBeNull();
      expect(h.get('X-Super-Access')).toBeNull();
    });
  });

  it('forwards the request untouched when no auth context is given', async () => {
    AggregateRouter.configure({ allowedTypes: ['user'] });
    const captured: CapturedFetch[] = [];
    const env = makeEnv(['USER'], captured);
    const original = new Request('https://x.example/users/u-1/Do', { method: 'POST' });
    await AggregateRouter.forward(env, original);
    expect(captured[0]!.request).toBe(original);
  });
});
