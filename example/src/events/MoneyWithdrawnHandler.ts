/**
 * MoneyWithdrawnHandler — debits the account balance.
 *
 * Note: business-rule validation (insufficient funds) lives in the COMMAND
 * route (`WithdrawRoute.executeCommand`), not here. By the time an event
 * exists, it is a fact; event handlers must apply it without re-validating.
 */

import { EventHandler, type IEventHandler, type EventMetadata } from 'ceves';
import { EventTypes, type MoneyWithdrawnEventData } from '../types';
import { AccountState } from '../aggregates/BankAccountAggregate.js';

@EventHandler
export class MoneyWithdrawnHandler
  implements IEventHandler<AccountState, MoneyWithdrawnEventData>
{
  eventType = EventTypes.MONEY_WITHDRAWN;
  aggregateType = 'BankAccountAggregate';

  apply(
    state: AccountState,
    event: MoneyWithdrawnEventData,
    _metadata: EventMetadata
  ): AccountState {
    return {
      ...state,
      balance: state.balance - event.amount,
    };
  }
}
