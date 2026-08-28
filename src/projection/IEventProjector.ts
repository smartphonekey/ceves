/**
 * Interface for event projectors. Implement this in consumer apps
 * to receive domain events after they are persisted.
 *
 * Register via registerProjector() before any AggregateObject instances are constructed.
 */
export interface IEventProjector {
  /** Unique stable identifier for this projector (used as cursor storage key). */
  readonly id: string;

  /**
   * Send one event to the projection target. Must be idempotent — the framework
   * may call this more than once for the same event during catch-up/retry.
   * @throws Any error signals the event was not delivered.
   */
  sendEvent(event: ProjectedEvent): Promise<void>;

  /**
   * Ask the projection what version it has for this aggregate.
   * Return 0 if the projection is empty or has been reset.
   * Called during catch-up to detect replay needs.
   */
  getLastProjectedVersion(
    aggregateType: string,
    aggregateId: string
  ): Promise<number>;
}

/** The event shape passed to projectors */
export interface ProjectedEvent {
  aggregateType: string;
  aggregateId: string;
  version: number;
  type: string;
  timestamp: number;
  data: unknown;
  /** Organization/tenant ID from the event envelope (empty string if not set) */
  orgId: string;
}
