/**
 * Unit tests for the success-response composition logic added in AA-82.
 *
 * The framework includes the just-emitted event in the success response under
 * `event: { type, data }`, where `data` is the event payload **without** the
 * `type` field — the outer `event.type` is the only carrier of the
 * discriminator. `event: null` for the NO_EVENT path. When a route declares
 * `static readonly eventSchema`, the framework also runs a runtime safeParse
 * of the stripped data — a parse failure means the handler returned
 * malformed data and must throw loudly.
 *
 * These tests reproduce the relevant snippets from
 * `AggregateObject.applyAndPersistEvent()` and
 * `AggregateObject.handleCommandExecution()` (NO_EVENT path) without standing
 * up the full DO. The logic under test is:
 *
 *   1. event-bearing path → response contains `event: { type, data }` with
 *      `data` not carrying `type`
 *   2. NO_EVENT path → response contains `event: null` (no `noEvent` key)
 *   3. eventSchema set + handler returns valid data → no throw
 *   4. eventSchema set + handler returns malformed data → throws clearly
 *   5. eventSchema absent → no parse, no throw (backward compat)
 */

import { describe, it, expect } from 'vitest';
import { z } from 'zod';

interface DomainEventLike {
  type: string;
  [k: string]: unknown;
}

function describeParseError(error: unknown): string {
  if (error == null) return 'unknown';
  if (typeof error === 'object' && 'message' in error) {
    return String((error as { message?: unknown }).message);
  }
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

/** Mirrors the logic block in AggregateObject.applyAndPersistEvent() */
function buildSuccessResponse(
  aggregateId: string,
  version: number,
  event: DomainEventLike,
  HandlerClass?: { name?: string; eventSchema?: { safeParse: (d: unknown) => { success: boolean; error?: unknown } } }
): { success: true; aggregateId: string; version: number; event: { type: string; data: Record<string, unknown> } } {
  const { type: eventType, ...eventData } = event;

  if (HandlerClass?.eventSchema) {
    const parseResult = HandlerClass.eventSchema.safeParse(eventData);
    if (!parseResult.success) {
      const reason = describeParseError(parseResult.error);
      throw new Error(
        `Handler ${HandlerClass.name ?? 'unknown'} emitted an event whose data ` +
          `failed its declared eventSchema (event type: ${eventType}). ` +
          `This is a handler bug — it returned data inconsistent with its ` +
          `static eventSchema. Underlying error: ${reason}`
      );
    }
  }

  return {
    success: true,
    aggregateId,
    version,
    event: { type: eventType, data: eventData },
  };
}

/** Mirrors the NO_EVENT response built in AggregateObject.handleCommandExecution() */
function buildNoEventResponse(aggregateId: string, version: number): {
  success: true;
  aggregateId: string;
  version: number;
  event: null;
} {
  return { success: true, aggregateId, version, event: null };
}

describe('Command success response composition (AA-82)', () => {
  describe('event-bearing path', () => {
    it('includes the emitted event under `event: { type, data }` with `type` stripped from `data`', () => {
      const event = { type: 'AccountOpened', owner: 'Alice', initialDeposit: 100 };

      const response = buildSuccessResponse('acc-1', 1, event);

      expect(response).toEqual({
        success: true,
        aggregateId: 'acc-1',
        version: 1,
        event: {
          type: 'AccountOpened',
          data: { owner: 'Alice', initialDeposit: 100 },
        },
      });
    });

    it('does NOT duplicate `type` inside `event.data`', () => {
      const event = { type: 'MoneyDeposited', amount: 50 };
      const response = buildSuccessResponse('acc-1', 2, event);

      expect(response.event.data).not.toHaveProperty('type');
      expect(response.event.type).toBe('MoneyDeposited');
    });

    it('does NOT include a `noEvent` field on success', () => {
      const event = { type: 'MoneyDeposited', amount: 50 };
      const response = buildSuccessResponse('acc-1', 2, event);

      expect(response).not.toHaveProperty('noEvent');
    });
  });

  describe('NO_EVENT path', () => {
    it('returns `event: null` (no `noEvent` field)', () => {
      const response = buildNoEventResponse('acc-1', 5);

      expect(response).toEqual({
        success: true,
        aggregateId: 'acc-1',
        version: 5,
        event: null,
      });
      expect(response).not.toHaveProperty('noEvent');
    });
  });

  describe('runtime parse-on-response when eventSchema is set', () => {
    // Data-only schema — no `type` field. The outer `event.type` is the
    // discriminator; `event.data` is the pure data payload.
    const AccountOpenedDataSchema = z.object({
      owner: z.string(),
      initialDeposit: z.number(),
    });

    it('passes a well-formed event through unchanged', () => {
      const event = { type: 'AccountOpened', owner: 'Alice', initialDeposit: 100 };

      const response = buildSuccessResponse('acc-1', 1, event, {
        name: 'OpenAccountRoute',
        eventSchema: AccountOpenedDataSchema,
      });

      expect(response.event).toEqual({
        type: 'AccountOpened',
        data: { owner: 'Alice', initialDeposit: 100 },
      });
    });

    it('throws clearly when the handler returns a malformed event-data payload', () => {
      // owner is missing — handler bug
      const malformedEvent = { type: 'AccountOpened', initialDeposit: 100 } as DomainEventLike;

      expect(() =>
        buildSuccessResponse('acc-1', 1, malformedEvent, {
          name: 'OpenAccountRoute',
          eventSchema: AccountOpenedDataSchema,
        })
      ).toThrow(/OpenAccountRoute/);

      expect(() =>
        buildSuccessResponse('acc-1', 1, malformedEvent, {
          name: 'OpenAccountRoute',
          eventSchema: AccountOpenedDataSchema,
        })
      ).toThrow(/eventSchema/);
    });

    it('does NOT validate the `type` field — that is the framework\'s concern', () => {
      // The schema is data-only, so a "wrong" type literal at the top level
      // is irrelevant to the schema. (Discriminator validity is a separate
      // concern handled by the EventHandler registry / typed return of
      // executeCommand.) Sanity-check: this should NOT throw.
      const event = { type: 'WrongType', owner: 'Alice', initialDeposit: 100 };

      expect(() =>
        buildSuccessResponse('acc-1', 1, event, {
          name: 'OpenAccountRoute',
          eventSchema: AccountOpenedDataSchema,
        })
      ).not.toThrow();
    });
  });

  describe('backward compat: no eventSchema declared', () => {
    it('skips the runtime parse and still builds the response', () => {
      // Existing routes haven't migrated — no eventSchema. Whatever the handler
      // returns is included as-is (minus `type`, which always moves to the
      // outer envelope). This is the path every b2c-api route hits today.
      const event = { type: 'KeyAdded', keyUuid: 'arbitrary', extraField: true };

      const response = buildSuccessResponse('lock-1', 7, event, {
        name: 'AddKeyRoute',
        // no eventSchema
      });

      expect(response).toEqual({
        success: true,
        aggregateId: 'lock-1',
        version: 7,
        event: {
          type: 'KeyAdded',
          data: { keyUuid: 'arbitrary', extraField: true },
        },
      });
    });

    it('does not parse even if the event is malformed', () => {
      // No schema means no validation — the framework can't know what's right.
      const event = { type: 'AnythingGoes' } as DomainEventLike;

      expect(() => buildSuccessResponse('lock-1', 7, event)).not.toThrow();
    });
  });
});
