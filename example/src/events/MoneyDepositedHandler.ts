/**
 * MoneyDepositedHandler — credits the account balance.
 *
 * Trivial state transformation: existing state + amount → new state. The
 * framework guarantees `state` is non-null because Deposit is an UPDATE
 * command (CommandRoute, not CreateCommandRoute).
 */

import { EventHandler, type IEventHandler, type EventMetadata } from 'ceves';
import { EventTypes, type MoneyDepositedEventData } from '../types';
import { AccountState } from '../aggregates/BankAccountAggregate.js';

@EventHandler
export class MoneyDepositedHandler
  implements IEventHandler<AccountState, MoneyDepositedEventData>
{
  eventType = EventTypes.MONEY_DEPOSITED;
  aggregateType = 'BankAccountAggregate';

  apply(
    state: AccountState,
    event: MoneyDepositedEventData,
    _metadata: EventMetadata
  ): AccountState {
    return {
      ...state,
      balance: state.balance + event.amount,
    };
  }
}
