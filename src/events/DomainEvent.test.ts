/**
 * Tests for DomainEvent interface and example implementations
 *
 * These tests verify:
 * - DomainEvent interface can be implemented
 * - Concrete event classes are immutable
 * - Type field uses literal types (as const)
 * - Events contain only business data (no infrastructure)
 *
 * @packageDocumentation
 */

import { describe, it, expect, expectTypeOf } from 'vitest';
import type { DomainEvent } from './DomainEvent';

// Example domain event implementations for testing
class TestAccountOpenedEvent implements DomainEvent {
  readonly type = 'AccountOpened' as const;

  constructor(
    public readonly owner: string,
    public readonly initialDeposit: number
  ) {}
}

class TestMoneyDepositedEvent implements DomainEvent {
  readonly type = 'MoneyDeposited' as const;

  constructor(public readonly amount: number) {}
}

describe('DomainEvent', () => {
  describe('interface compliance', () => {
    it('should allow implementation with type field', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);

      expect(event).toHaveProperty('type');
      expect(event.type).toBe('AccountOpened');
    });

    it('should support TypeScript literal types', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);

      // TypeScript compile-time check: event.type should be 'AccountOpened', not string
      const eventType: 'AccountOpened' = event.type;
      expect(eventType).toBe('AccountOpened');
    });
  });

  describe('immutability', () => {
    it('should have readonly type field (TypeScript compile-time check)', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);

      // TypeScript compile-time check: should not allow reassignment
      // Note: readonly is TypeScript-only, not enforced at runtime
      // @ts-expect-error - type is readonly (compile-time check)
      const _attemptReassign = () => { event.type = 'SomethingElse'; };
      _attemptReassign;

      // The type field exists and has the correct value
      expect(event.type).toBe('AccountOpened');
    });

    it('should have readonly business data fields (TypeScript compile-time check)', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);

      // TypeScript compile-time checks: should not allow reassignment
      // Note: readonly is TypeScript-only, not enforced at runtime
      // @ts-expect-error - owner is readonly (compile-time check)
      const _attemptOwner = () => { event.owner = 'jane@example.com'; };
      _attemptOwner;

      // @ts-expect-error - initialDeposit is readonly (compile-time check)
      const _attemptDeposit = () => { event.initialDeposit = 200; };
      _attemptDeposit;

      // The fields exist and have the correct values
      expect(event.owner).toBe('john@example.com');
      expect(event.initialDeposit).toBe(100);
    });
  });

  describe('pure business data', () => {
    it('should contain only business data (no aggregateId)', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);

      expect(event).not.toHaveProperty('aggregateId');
      expect(event).not.toHaveProperty('version');
      expect(event).not.toHaveProperty('timestamp');
      expect(event).not.toHaveProperty('orgId');
    });

    it('should contain only business-relevant fields', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);
      const keys = Object.keys(event);

      // Should only have: type, owner, initialDeposit. Key ORDER is
      // incidental (it depends on how the transpiler sequences class-field
      // vs parameter-property initialization), so compare as a set.
      expect(keys.sort()).toEqual(['initialDeposit', 'owner', 'type']);
    });
  });

  describe('discriminated unions', () => {
    it('should enable type-safe discriminated unions', () => {
      type BankEvent = TestAccountOpenedEvent | TestMoneyDepositedEvent;

      const events: BankEvent[] = [
        new TestAccountOpenedEvent('john@example.com', 100),
        new TestMoneyDepositedEvent(50),
      ];

      // Type discrimination based on type field
      events.forEach((event) => {
        if (event.type === 'AccountOpened') {
          // TypeScript knows this is TestAccountOpenedEvent
          expect(event.owner).toBeDefined();
          expect(event.initialDeposit).toBeDefined();
        } else if (event.type === 'MoneyDeposited') {
          // TypeScript knows this is TestMoneyDepositedEvent
          expect(event.amount).toBeDefined();
        }
      });
    });
  });

  describe('construction', () => {
    it('should create AccountOpened events with business data', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);

      expect(event.type).toBe('AccountOpened');
      expect(event.owner).toBe('john@example.com');
      expect(event.initialDeposit).toBe(100);
    });

    it('should create MoneyDeposited events with business data', () => {
      const event = new TestMoneyDepositedEvent(50);

      expect(event.type).toBe('MoneyDeposited');
      expect(event.amount).toBe(50);
    });

    it('should enforce required fields at construction (TypeScript compile-time check)', () => {
      // TypeScript compile-time checks - these would fail at compile time
      // @ts-expect-error - Missing required parameters (compile-time check)
      const _attemptNoParams = () => new TestAccountOpenedEvent();
      _attemptNoParams; // Intentional: compile-time check only

      // @ts-expect-error - Missing required parameter (compile-time check)
      const _attemptNoAmount = () => new TestMoneyDepositedEvent();
      _attemptNoAmount; // Intentional: compile-time check only

      // At runtime, TypeScript ensures correct parameters are passed
      const validEvent = new TestAccountOpenedEvent('john@example.com', 100);
      expect(validEvent.owner).toBe('john@example.com');
    });
  });

  describe('type inference', () => {
    it('should infer correct type from constructor', () => {
      const event1 = new TestAccountOpenedEvent('john@example.com', 100);
      const event2 = new TestMoneyDepositedEvent(50);

      // TypeScript should infer specific event types, not generic DomainEvent
      expectTypeOf(event1).toEqualTypeOf<TestAccountOpenedEvent>();
      expectTypeOf(event2).toEqualTypeOf<TestMoneyDepositedEvent>();
    });

    it('should allow assignment to DomainEvent interface', () => {
      const event: DomainEvent = new TestAccountOpenedEvent('john@example.com', 100);

      expect(event.type).toBe('AccountOpened');
    });
  });

  describe('serialization', () => {
    it('should serialize to JSON correctly', () => {
      const event = new TestAccountOpenedEvent('john@example.com', 100);
      const json = JSON.stringify(event);
      const parsed = JSON.parse(json);

      expect(parsed).toEqual({
        type: 'AccountOpened',
        owner: 'john@example.com',
        initialDeposit: 100,
      });
    });

    it('should deserialize from JSON (manual reconstruction)', () => {
      const json = JSON.stringify({
        type: 'MoneyDeposited',
        amount: 50,
      });
      const parsed = JSON.parse(json) as { type: string; amount: number };

      // Manual reconstruction required (classes don't auto-deserialize)
      const event = new TestMoneyDepositedEvent(parsed.amount);

      expect(event.type).toBe('MoneyDeposited');
      expect(event.amount).toBe(50);
    });
  });
});
