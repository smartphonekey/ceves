/**
 * Version Mismatch Error for Ceves Event Sourcing Library
 */

import { CevesError, CevesErrorJSON } from './CevesError';

/**
 * JSON representation of VersionMismatchError
 */
interface VersionMismatchErrorJSON extends CevesErrorJSON {
  expectedVersion: number;
  actualVersion: number;
}

/**
 * Error thrown when state version doesn't match the last event version after restoration.
 *
 * This error indicates a bug in an event handler's apply() method - the handler failed
 * to correctly update the state.version field to match the event.version.
 * Returns HTTP 409 Conflict responses.
 *
 * Version tracking is critical for:
 * - Incremental event loading (using state.version as afterVersion filter)
 * - Detecting state corruption bugs early
 * - Ensuring state consistency across restoration cycles
 *
 * @example
 * ```typescript
 * import { VersionMismatchError } from './VersionMismatchError';
 *
 * // After applying events, validate version consistency
 * const lastEvent = events[events.length - 1];
 * if (state.version !== lastEvent.version) {
 *   throw new VersionMismatchError(
 *     `Version mismatch after state restoration for aggregate "${lastEvent.aggregateId}": expected version ${lastEvent.version} (from last event), but state.version is ${state.version}. This indicates a bug in the event handler's apply() method.`,
 *     lastEvent.version,
 *     state.version,
 *     lastEvent.aggregateType,
 *     lastEvent.aggregateId
 *   );
 * }
 * ```
 */
export class VersionMismatchError extends CevesError {
  /**
   * Create a version mismatch error.
   *
   * Returns HTTP 409 Conflict status.
   *
   * @param message - Human-readable error description
   * @param expectedVersion - Expected version number (from last event)
   * @param actualVersion - Actual version number (from state)
   * @param aggregateType - Optional aggregate type
   * @param aggregateId - Optional aggregate instance ID
   */
  constructor(
    message: string,
    public readonly expectedVersion: number,
    public readonly actualVersion: number,
    aggregateType?: string,
    aggregateId?: string
  ) {
    super(message, 409, aggregateType, aggregateId);
  }

  /**
   * Serialize error to JSON with version mismatch details.
   */
  override toJSON() {
    return {
      ...super.toJSON(),
      expectedVersion: this.expectedVersion,
      actualVersion: this.actualVersion,
    };
  }

  /**
   * Reconstruct VersionMismatchError from JSON.
   */
  static override fromJSON(json: VersionMismatchErrorJSON): VersionMismatchError {
    const error = new VersionMismatchError(json.message, json.expectedVersion, json.actualVersion, json.aggregateType, json.aggregateId);

    if (json.stack) {
      error.stack = json.stack;
    }

    return error;
  }
}
