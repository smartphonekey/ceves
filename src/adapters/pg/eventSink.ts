/**
 * Event sinks for the PG variant.
 *
 * The PostgreSQL dispatch functions RETURN the emitted event instead of
 * storing it (no events in PostgreSQL by design). The HTTP wrapper hands the
 * event to an {@link EventSink}, which appends it to the real event log —
 * R2 on Cloudflare, S3 on Node — reusing the existing `IEventStore`
 * implementations (`R2EventStore` / `S3EventStore`).
 *
 * @packageDocumentation
 */

import type { IEventStore, StoredEvent } from '../../storage/interfaces';

/** Destination for events emitted by PostgreSQL-dispatched commands. */
export interface EventSink {
  append(event: StoredEvent): Promise<void>;
}

/** Adapt any `IEventStore` (R2EventStore, S3EventStore, ...) to an EventSink. */
export class EventStoreSink implements EventSink {
  constructor(private readonly store: IEventStore) {}

  append(event: StoredEvent): Promise<void> {
    return this.store.save(event);
  }
}

/** Collects events in memory. For tests and local development. */
export class InMemoryEventSink implements EventSink {
  readonly events: StoredEvent[] = [];

  append(event: StoredEvent): Promise<void> {
    this.events.push(event);
    return Promise.resolve();
  }
}
