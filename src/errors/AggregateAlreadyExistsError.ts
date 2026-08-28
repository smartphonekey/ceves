/**
 * Aggregate Already Exists Error
 *
 * Thrown when a create command is executed on an aggregate that already exists.
 * Returns HTTP 409 Conflict responses.
 */

import { CevesError } from './CevesError';

/**
 * Error thrown when a create command targets an aggregate that already exists.
 *
 * This is semantically different from BusinessRuleViolationError (400) because
 * the resource already exists — the correct HTTP status is 409 Conflict.
 *
 * @example
 * ```typescript
 * if (state !== null) {
 *   throw new AggregateAlreadyExistsError('UserAggregate', 'user-123');
 * }
 * ```
 */
export class AggregateAlreadyExistsError extends CevesError {
  constructor(
    aggregateType?: string,
    aggregateId?: string
  ) {
    const message = aggregateType && aggregateId
      ? `${aggregateType} ${aggregateId} already exists. Cannot execute create command.`
      : 'Aggregate already exists. Cannot execute create command.';
    super(message, 409, aggregateType, aggregateId);
  }
}
