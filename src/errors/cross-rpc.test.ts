/**
 * Unit tests for the AA-119 cross-RPC error coercion helpers.
 *
 * Covers the four shapes the AA-119 spec calls out:
 *   1. Typed `CevesError` survives the DO → worker round-trip with name,
 *      message, httpStatusCode, and aggregate context intact.
 *   2. Non-Error throws (string / number / plain object) produce a
 *      response with a non-empty, greppable message — not "Unknown" or
 *      "[object Object]".
 *   3. Subclass-specific extras (e.g. `eventVersion`, `eventIndex`) are
 *      preserved on the rehydrated error so callers can read them off
 *      without re-parsing the response body.
 *   4. `stubFetchWithTypedErrors` rebuilds and throws the typed error so
 *      app code can route on it (`try/catch` instead of polling status
 *      codes).
 *
 * Existing `ErrorPropagation.test.ts` covers in-process error properties
 * (status code preservation, chanfana format compatibility). This file
 * extends that to the actual DO RPC boundary by mocking `stub.fetch`.
 */

import { describe, it, expect } from 'vitest';
import { CevesError } from './CevesError';
import { BusinessRuleViolationError } from './BusinessRuleViolationError';
import { AggregateAlreadyExistsError } from './AggregateAlreadyExistsError';
import { StateRestorationError } from './StateRestorationError';
import {
  CROSS_RPC_ERROR_HEADER,
  rehydrateErrorFromResponse,
  serializeErrorToResponse,
  stubFetchWithTypedErrors,
} from './cross-rpc';

describe('AA-119: cross-RPC error coercion', () => {
  describe('serializeErrorToResponse', () => {
    it('emits chanfana-format body + X-Ceves-Error header for typed errors', async () => {
      const err = new BusinessRuleViolationError(
        'Insufficient funds',
        'BankAccount',
        'acc-123',
      );

      const response = serializeErrorToResponse(err);

      expect(response.status).toBe(err.httpStatusCode);
      expect(response.headers.get(CROSS_RPC_ERROR_HEADER)).toBe('1');
      expect(response.headers.get('Content-Type')).toBe('application/json');

      const body: Record<string, unknown> = await response.json();
      // chanfana wire shape (preserves existing client contract)
      expect(body.success).toBe(false);
      expect(body.errors).toEqual([
        { code: err.httpStatusCode, message: 'Insufficient funds' },
      ]);
      // typed payload for rehydration
      expect(body.__ceves).toMatchObject({
        name: 'BusinessRuleViolationError',
        message: 'Insufficient funds',
        httpStatusCode: err.httpStatusCode,
        aggregateType: 'BankAccount',
        aggregateId: 'acc-123',
      });
    });

    it('produces a 500 envelope with a real message for non-Error throws', async () => {
      const response = serializeErrorToResponse({ reason: 'literal object throw' });

      expect(response.status).toBe(500);
      expect(response.headers.get(CROSS_RPC_ERROR_HEADER)).toBe('1');

      const body: Record<string, unknown> = await response.json();
      const errors = body.errors as Array<{ code: number; message: string }>;
      expect(errors[0].code).toBe(500);
      // Crucial: non-empty, contains the original payload — proves
      // AA-111/-112's "empty stripped message" mode can't reach the wire.
      expect(errors[0].message).toContain('Unknown error');
      expect(errors[0].message).toContain('literal object throw');
    });

    it('handles bare Error with message intact', async () => {
      const response = serializeErrorToResponse(new Error('something concrete'));
      const body: Record<string, unknown> = await response.json();
      const errors = body.errors as Array<{ code: number; message: string }>;
      expect(errors[0].message).toBe('something concrete');
    });

    it('handles string throws (regression: never log "Unknown error: undefined")', async () => {
      const response = serializeErrorToResponse('boom');
      const body: Record<string, unknown> = await response.json();
      const errors = body.errors as Array<{ code: number; message: string }>;
      expect(errors[0].message).toContain('boom');
    });
  });

  describe('rehydrateErrorFromResponse', () => {
    it('returns null when the response is not Ceves-encoded', async () => {
      const response = new Response(JSON.stringify({ success: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
      expect(await rehydrateErrorFromResponse(response)).toBeNull();
    });

    it('rebuilds a typed error with name, message, status, aggregate context intact', async () => {
      const original = new BusinessRuleViolationError(
        'Insufficient funds',
        'BankAccount',
        'acc-123',
      );
      const response = serializeErrorToResponse(original);

      const rebuilt = await rehydrateErrorFromResponse(response);

      expect(rebuilt).toBeInstanceOf(CevesError);
      expect(rebuilt?.name).toBe('BusinessRuleViolationError');
      expect(rebuilt?.message).toBe('Insufficient funds');
      expect(rebuilt?.httpStatusCode).toBe(original.httpStatusCode);
      expect(rebuilt?.aggregateType).toBe('BankAccount');
      expect(rebuilt?.aggregateId).toBe('acc-123');
    });

    it('preserves subclass-specific extras (eventIndex, eventVersion, …)', async () => {
      const original = new StateRestorationError(
        {
          aggregateType: 'TestAggregate',
          aggregateId: 'agg-1',
          eventType: 'TestUpdated',
          eventVersion: 17,
          eventIndex: 16,
          totalEvents: 342,
        },
        new Error('underlying'),
      );
      const response = serializeErrorToResponse(original);
      const rebuilt = (await rehydrateErrorFromResponse(response)) as
        | (CevesError & {
            eventType?: unknown;
            eventVersion?: unknown;
            eventIndex?: unknown;
            totalEvents?: unknown;
          })
        | null;

      expect(rebuilt?.name).toBe('StateRestorationError');
      expect(rebuilt?.eventType).toBe('TestUpdated');
      expect(rebuilt?.eventVersion).toBe(17);
      expect(rebuilt?.eventIndex).toBe(16);
      expect(rebuilt?.totalEvents).toBe(342);
    });

    it('returns null when header is present but body cannot be parsed', async () => {
      const response = new Response('not json', {
        status: 500,
        headers: { [CROSS_RPC_ERROR_HEADER]: '1', 'Content-Type': 'text/plain' },
      });
      expect(await rehydrateErrorFromResponse(response)).toBeNull();
    });

    it('returns null when header is present but __ceves payload is missing', async () => {
      const response = new Response(JSON.stringify({ success: false }), {
        status: 500,
        headers: { [CROSS_RPC_ERROR_HEADER]: '1', 'Content-Type': 'application/json' },
      });
      expect(await rehydrateErrorFromResponse(response)).toBeNull();
    });

    it('falls back to CevesError for unknown subclass names', async () => {
      // Simulate a payload that names a class our registry doesn't know
      // about (e.g. an older worker calling a newer DO that defines a
      // brand-new error subclass).
      const body = {
        success: false,
        errors: [{ code: 418, message: "I'm a teapot" }],
        __ceves: {
          name: 'TeapotError',
          message: "I'm a teapot",
          httpStatusCode: 418,
        },
      };
      const response = new Response(JSON.stringify(body), {
        status: 418,
        headers: { [CROSS_RPC_ERROR_HEADER]: '1', 'Content-Type': 'application/json' },
      });

      const rebuilt = await rehydrateErrorFromResponse(response);
      expect(rebuilt).toBeInstanceOf(CevesError);
      // Name still discoverable for grouping / discrimination, even
      // though the subclass isn't in our registry.
      expect(rebuilt?.name).toBe('TeapotError');
      expect(rebuilt?.httpStatusCode).toBe(418);
      expect(rebuilt?.message).toBe("I'm a teapot");
    });
  });

  describe('stubFetchWithTypedErrors', () => {
    it('returns the response untransformed when no Ceves error header', async () => {
      const okResponse = new Response(JSON.stringify({ success: true }), { status: 200 });
      const stub = { fetch: () => Promise.resolve(okResponse) };
      const got = await stubFetchWithTypedErrors(
        stub,
        new Request('https://internal/x'),
      );
      expect(got.status).toBe(200);
    });

    it('throws a rebuilt typed error when the DO returned one', async () => {
      const original = new AggregateAlreadyExistsError('LockAggregate', 'lock-99');
      const stub = { fetch: () => Promise.resolve(serializeErrorToResponse(original)) };

      await expect(
        stubFetchWithTypedErrors(stub, new Request('https://internal/x')),
      ).rejects.toMatchObject({
        name: 'AggregateAlreadyExistsError',
        httpStatusCode: 409,
        aggregateType: 'LockAggregate',
        aggregateId: 'lock-99',
      });
    });

    it('lets app code branch on error name (the prior CF "internal error; reference =" black hole)', async () => {
      // Before AA-119, this branch was impossible: the caller saw a
      // generic Error("internal error; reference = ...") with no way
      // to tell it from any other DO failure. With cross-RPC coercion
      // the typed error makes it back across.
      const stub = {
        fetch: () =>
          serializeErrorToResponse(new AggregateAlreadyExistsError('Lock', 'L1')),
      };

      let routedAsExpected = false;
      try {
        await stubFetchWithTypedErrors(stub, new Request('https://internal/x'));
      } catch (err) {
        if (err instanceof CevesError && err.name === 'AggregateAlreadyExistsError') {
          routedAsExpected = true;
        }
      }
      expect(routedAsExpected).toBe(true);
    });

    /**
     * AA-117 regression. SPK-API-P logged `MatterDevice DO RPC failed
     * (op=LoadMatterDeviceState ...): internal error; reference = ...`
     * because state restoration crashed inside the DO and the failure
     * surfaced as Cloudflare's opaque wrapper. With PR #105's
     * `StateRestorationError` + this PR's cross-RPC coercion, the
     * worker-side caller sees an error carrying:
     *
     *   - name === 'StateRestorationError'
     *   - aggregateType / aggregateId — which DO crashed
     *   - eventType / eventVersion — which event broke
     *   - eventIndex / totalEvents — replay position
     *
     * No more CF reference id leaking into Sentry — the actual cause is
     * named in the error itself.
     */
    it('AA-117: state-restoration failure crosses RPC boundary with full event-replay context', async () => {
      const original = new StateRestorationError(
        {
          aggregateType: 'MatterDeviceAggregate',
          aggregateId: '82917676-0c95-2e67-962d-6b9fa1e40d8b',
          eventType: 'ReportedShadowUpdated',
          eventVersion: 375,
          eventIndex: 374,
          totalEvents: 375,
        },
        new Error('Expected string, received undefined'),
      );
      const stub = { fetch: () => Promise.resolve(serializeErrorToResponse(original)) };

      let caught: unknown;
      try {
        await stubFetchWithTypedErrors(stub, new Request('https://internal/x'));
      } catch (err) {
        caught = err;
      }

      expect(caught).toBeInstanceOf(CevesError);
      const err = caught as CevesError & {
        eventType?: unknown;
        eventVersion?: unknown;
        eventIndex?: unknown;
        totalEvents?: unknown;
      };

      // Subclass identity preserved by name (cross-RPC rehydration
      // doesn't restore prototype chain, but name discriminator survives).
      expect(err.name).toBe('StateRestorationError');

      // Aggregate context: which DO crashed.
      expect(err.aggregateType).toBe('MatterDeviceAggregate');
      expect(err.aggregateId).toBe('82917676-0c95-2e67-962d-6b9fa1e40d8b');

      // Event-replay context: which event broke.
      expect(err.eventType).toBe('ReportedShadowUpdated');
      expect(err.eventVersion).toBe(375);
      expect(err.eventIndex).toBe(374);
      expect(err.totalEvents).toBe(375);

      // Underlying cause is in the message tail. The bug we're regressing
      // against: pre-AA-119 the message was `internal error; reference = ...`
      // — opaque, useless. Now it's the actual ZodError / handler-throw.
      expect(err.message).toContain('Expected string, received undefined');
      expect(err.message).not.toMatch(/internal error;\s*reference\s*=/);
    });
  });
});
