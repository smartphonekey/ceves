/**
 * MoneyDepositedHandler — unit test for the apply() function.
 *
 * Verifies the handler increments balance immutably and preserves all other
 * state fields.
 */

import { describe, it, expect } from 'vitest';
import { MoneyDepositedHandler } from '../MoneyDepositedHandler';
import {
  EventTypes,
  type MoneyDepositedEventData,
} from '../../types';
import type { EventMetadata } from 'ceves';
import { AccountState } from '../../aggregates/BankAccountAggregate.js';

describe('MoneyDepositedHandler', () => {
  const handler = new MoneyDepositedHandler();

  const baseState = (balance: number): AccountState => ({
    id: 'acc-123',
    orgId: 'org-1',
    version: 1,
    timestamp: '2026-01-01T00:00:00Z',
    owner: 'Alice',
    balance,
  });

  const metadata: EventMetadata = {
    aggregateId: 'acc-123',
    version: 2,
    timestamp: '2026-01-02T00:00:00Z',
    orgId: 'org-1',
  };

  it('credits the balance', () => {
    const event: MoneyDepositedEventData = {
      type: EventTypes.MONEY_DEPOSITED,
      amount: 50,
    };

    const newState = handler.apply(baseState(100), event, metadata);

    expect(newState.balance).toBe(150);
  });

  it('does not mutate the input state', () => {
    const state = baseState(100);
    const event: MoneyDepositedEventData = {
      type: EventTypes.MONEY_DEPOSITED,
      amount: 25,
    };

    const newState = handler.apply(state, event, metadata);

    expect(state.balance).toBe(100); // input untouched
    expect(newState.balance).toBe(125);
    expect(newState).not.toBe(state); // new object
  });

  it('preserves non-balance fields', () => {
    const state = baseState(100);
    const event: MoneyDepositedEventData = {
      type: EventTypes.MONEY_DEPOSITED,
      amount: 10,
    };

    const newState = handler.apply(state, event, metadata);

    expect(newState.id).toBe(state.id);
    expect(newState.orgId).toBe(state.orgId);
    expect(newState.owner).toBe(state.owner);
  });
});
