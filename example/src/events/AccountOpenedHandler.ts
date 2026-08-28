/**
 * AccountOpenedHandler — applies AccountOpened to build initial account state.
 *
 * Modern Ceves event handlers:
 * - Implement `IEventHandler<TState, TEventData>`
 * - Are decorated with the bare `@EventHandler` class decorator (no arguments)
 * - Set `eventType` to a string literal from EventTypes
 * - Set `aggregateType` to the DO class name (e.g. 'BankAccountAggregate')
 * - Receive a NEVER-NULL state (empty for the first event, per ADR-009)
 * - Receive separate `metadata` carrying aggregateId / version / timestamp / orgId
 */

import { EventHandler, type IEventHandler, type EventMetadata } from 'ceves';
import { EventTypes, type AccountOpenedEventData } from '../types';
import { AccountState } from '../aggregates/BankAccountAggregate.js';

@EventHandler
export class AccountOpenedHandler
  implements IEventHandler<AccountState, AccountOpenedEventData>
{
  eventType = EventTypes.ACCOUNT_OPENED;
  aggregateType = 'BankAccountAggregate';

  /**
   * Apply AccountOpened to produce the initial state.
   *
   * @param state    Empty state for the first event (NEVER null).
   * @param event    Pure business data describing the AccountOpened fact.
   * @param metadata Infrastructure metadata supplied by the framework.
   * @returns        The new AccountState. Framework auto-sets version & timestamp.
   */
  apply(
    state: AccountState,
    event: AccountOpenedEventData,
    metadata: EventMetadata
  ): AccountState {
    return {
      ...state,
      id: metadata.aggregateId, // business: the aggregate's identity
      orgId: metadata.orgId,    // business: which tenant this account belongs to
      owner: event.owner,
      balance: event.initialDeposit,
      // version + timestamp are set by Ceves AFTER this returns
    };
  }
}
