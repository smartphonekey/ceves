import { describe, it, expect } from 'vitest';
import { StateRestorationError } from './StateRestorationError';
import { CevesError } from './CevesError';

describe('StateRestorationError', () => {
  const baseArgs = {
    aggregateType: 'TestAggregate',
    aggregateId: 'aggregate-1',
    eventType: 'CounterIncremented',
    eventVersion: 42,
    eventIndex: 16,
    totalEvents: 342,
  };

  it('extends CevesError with 500 status', () => {
    const err = new StateRestorationError(baseArgs, new Error('inner'));
    expect(err).toBeInstanceOf(CevesError);
    expect(err.httpStatusCode).toBe(500);
    expect(err.aggregateType).toBe('TestAggregate');
    expect(err.aggregateId).toBe('aggregate-1');
  });

  it('formats a one-line, greppable message with replay position + event id + cause', () => {
    const cause = new Error('Expected string, received undefined');
    const err = new StateRestorationError(baseArgs, cause);
    // We rely on operators grepping Sentry/Axiom by these substrings.
    expect(err.message).toContain('TestAggregate/aggregate-1');
    expect(err.message).toContain('event 17/342');
    expect(err.message).toContain('type=CounterIncremented');
    expect(err.message).toContain('version=42');
    expect(err.message).toContain('Expected string, received undefined');
  });

  it('preserves the cause via ES2022 Error.cause', () => {
    const cause = new Error('zod parse fail');
    const err = new StateRestorationError(baseArgs, cause) as Error & { cause?: unknown };
    expect(err.cause).toBe(cause);
  });

  it('handles non-Error causes gracefully (string, etc.)', () => {
    const err = new StateRestorationError(baseArgs, 'literal string cause') as Error & {
      cause?: unknown;
    };
    expect(err.message).toContain('literal string cause');
    expect(err.cause).toBe('literal string cause');
  });

  it('exposes the per-event fields for telemetry callers', () => {
    const err = new StateRestorationError(baseArgs, new Error('x'));
    expect(err.eventType).toBe('CounterIncremented');
    expect(err.eventVersion).toBe(42);
    expect(err.eventIndex).toBe(16);
    expect(err.totalEvents).toBe(342);
  });
});
