/**
 * Aggregate Not Found Error for Ceves Event Sourcing Library
 */

import { CevesError, CevesErrorJSON } from './CevesError';

/**
 * JSON representation of AggregateNotFoundError
 * Uses base CevesErrorJSON since it only needs aggregateType and aggregateId
 */
type AggregateNotFoundErrorJSON = CevesErrorJSON;

/**
 * Error thrown when an aggregate cannot be found in storage.
 *
 * This error is typically thrown when attempting to retrieve an aggregate
 * by ID but no events or snapshots exist for that aggregate.
 * Returns HTTP 404 Not Found responses.
 *
 * @example
 * ```typescript
 * import { AggregateNotFoundError } from './AggregateNotFoundError';
 *
 * async function loadAggregate(aggregateType: string, aggregateId: string) {
 *   const events = await eventStore.getEvents(aggregateType, aggregateId);
 *
 *   if (events.length === 0) {
 *     throw new AggregateNotFoundError(aggregateType, aggregateId);
 *   }
 *
 *   // ... replay events to rebuild state (see AggregateObject for the
 *   // canonical implementation).
 * }
 * ```
 */
export class AggregateNotFoundError extends CevesError {
  /**
   * Create an aggregate not found error.
   *
   * Automatically generates a descriptive error message from the aggregate type and ID.
   * Returns HTTP 404 Not Found status.
   *
   * @param aggregateType - Type of aggregate (e.g., 'account', 'order')
   * @param aggregateId - Aggregate instance ID that was not found
   */
  constructor(aggregateType: string, aggregateId: string) {
    super(
      `Aggregate not found: ${aggregateType}/${aggregateId}`,
      404,
      aggregateType,
      aggregateId
    );
  }

  /**
   * Reconstruct AggregateNotFoundError from JSON.
   */
  static override fromJSON(json: AggregateNotFoundErrorJSON): AggregateNotFoundError {
    const error = new AggregateNotFoundError(json.aggregateType ?? '', json.aggregateId ?? '');

    if (json.stack) {
      error.stack = json.stack;
    }

    return error;
  }
}
