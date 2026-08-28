/**
 * AccountOpenedHandler — unit test for the apply() function.
 *
 * Event handlers are pure functions, so they're easy to test without standing
 * up a Durable Object: instantiate the handler, hand it (state, event, metadata),
 * and assert on the returned state.
 */

import { describe, it, expect } from 'vitest';
import { AccountOpenedHandler } from '../AccountOpenedHandler';
import { EventTypes, type AccountOpenedEventData } from '../../types';
import type { EventMetadata } from 'ceves';
import { AccountState } from '../../aggregates/BankAccountAggregate.js';

describe('AccountOpenedHandler', () => {
  const handler = new AccountOpenedHandler();

  const metadata: EventMetadata = {
    aggregateId: 'acc-123',
    version: 1,
    timestamp: '2026-01-01T00:00:00Z',
    orgId: 'org-1',
  };

  it('builds initial state from the empty state + AccountOpened event', () => {
    const event: AccountOpenedEventData = {
      type: EventTypes.ACCOUNT_OPENED,
      owner: 'Alice',
      initialDeposit: 100,
    };

    const newState = handler.apply(AccountState.empty(), event, metadata);

    expect(newState.id).toBe('acc-123');
    expect(newState.orgId).toBe('org-1');
    expect(newState.owner).toBe('Alice');
    expect(newState.balance).toBe(100);
  });

  it('accepts a zero initial deposit', () => {
    const event: AccountOpenedEventData = {
      type: EventTypes.ACCOUNT_OPENED,
      owner: 'Bob',
      initialDeposit: 0,
    };

    const newState = handler.apply(AccountState.empty(), event, metadata);

    expect(newState.owner).toBe('Bob');
    expect(newState.balance).toBe(0);
  });
});
