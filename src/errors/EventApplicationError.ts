/**
 * Event Application Error for Ceves Event Sourcing Library
 */

import { CevesError, CevesErrorJSON } from './CevesError';

/**
 * JSON representation of EventApplicationError
 */
interface EventApplicationErrorJSON extends CevesErrorJSON {
  eventType: string;
  eventVersion: number;
}

/**
 * Error thrown when applying an event to state fails.
 *
 * This error occurs when an event's apply method throws an exception
 * or when the state transformation fails for any reason.
 * Returns HTTP 500 Internal Server Error responses (indicates a bug in event handler).
 *
 * @example
 * ```typescript
 * import { EventApplicationError, EventHandler, type IEventHandler } from 'ceves';
 *
 * @EventHandler
 * class MoneyWithdrawnHandler implements IEventHandler<AccountState, MoneyWithdrawnEventData> {
 *   eventType = 'MoneyWithdrawn';
 *   aggregateType = 'BankAccountAggregate';
 *
 *   apply(state: AccountState, event: MoneyWithdrawnEventData, metadata: EventMetadata): AccountState {
 *     if (state.balance < event.amount) {
 *       throw new EventApplicationError(
 *         'Insufficient funds',
 *         this.eventType,
 *         metadata.version,
 *         this.aggregateType,
 *         metadata.aggregateId
 *       );
 *     }
 *     return { ...state, balance: state.balance - event.amount };
 *   }
 * }
 * ```
 */
export class EventApplicationError extends CevesError {
  /**
   * Create an event application error.
   *
   * Returns HTTP 500 status (event application failures indicate bugs in event handlers).
   *
   * @param message - Human-readable error description
   * @param eventType - Type of event that failed to apply (e.g., 'MoneyWithdrawn')
   * @param eventVersion - Version number of the event
   * @param aggregateType - Optional aggregate type
   * @param aggregateId - Optional aggregate instance ID
   */
  constructor(
    message: string,
    public readonly eventType: string,
    public readonly eventVersion: number,
    aggregateType?: string,
    aggregateId?: string
  ) {
    super(message, 500, aggregateType, aggregateId);
  }

  /**
   * Serialize error to JSON with event application details.
   */
  override toJSON() {
    return {
      ...super.toJSON(),
      eventType: this.eventType,
      eventVersion: this.eventVersion,
    };
  }

  /**
   * Reconstruct EventApplicationError from JSON.
   */
  static override fromJSON(json: EventApplicationErrorJSON): EventApplicationError {
    const error = new EventApplicationError(json.message, json.eventType, json.eventVersion, json.aggregateType, json.aggregateId);

    if (json.stack) {
      error.stack = json.stack;
    }

    return error;
  }
}
