/**
 * `NatsAggregateContext` — the Durable-Object-state stand-in handed to an
 * `AggregateObject` subclass when it runs on the NATS runtime.
 *
 * It implements exactly the `DurableObjectState` surface Ceves'
 * `AggregateObject` touches:
 * - `id` — an object whose `toString()`/`name` return the aggregate name
 *   (better than Cloudflare, where `ctx.id` is an opaque hex hash and the
 *   human-readable name has to be recovered from the URL later),
 * - `storage` — a {@link NatsKvStorage} scoped to this aggregate,
 * - `blockConcurrencyWhile` — serialized against the actor's dispatch queue,
 * - `waitUntil` — tracks fire-and-forget promises so a graceful shutdown
 *   can drain them.
 */

import type { NatsKvStorage } from './NatsKvStorage';

/** Duck-typed `DurableObjectId` carrying the human-readable aggregate name. */
export interface NatsAggregateId {
  readonly name: string;
  toString(): string;
  equals(other: { toString(): string }): boolean;
}

export function aggregateIdFor(name: string): NatsAggregateId {
  return {
    name,
    toString: () => name,
    equals: (other) => other.toString() === name,
  };
}

export class NatsAggregateContext {
  readonly id: NatsAggregateId;

  /**
   * Chain of `blockConcurrencyWhile` sections. The hosting stub awaits
   * {@link whenReady} before dispatching a request, which reproduces the
   * Durable Object guarantee that constructor-time state loading finishes
   * before the first `fetch()` runs.
   */
  private readyChain: Promise<void> = Promise.resolve();

  /** Promises registered via `waitUntil`, drained on shutdown. */
  private readonly pending = new Set<Promise<unknown>>();

  constructor(
    name: string,
    readonly storage: NatsKvStorage
  ) {
    this.id = aggregateIdFor(name);
  }

  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
    const result = this.readyChain.then(callback);
    // Failures inside the callback are the callback owner's concern
    // (AggregateObject catches its own); never wedge the actor's queue.
    this.readyChain = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  /** Resolves when all queued `blockConcurrencyWhile` sections finished. */
  whenReady(): Promise<void> {
    return this.readyChain;
  }

  waitUntil(promise: Promise<unknown>): void {
    const tracked = promise.catch(() => undefined);
    this.pending.add(tracked);
    void tracked.finally(() => this.pending.delete(tracked));
  }

  /** Await all `waitUntil` promises (graceful shutdown). */
  async drain(): Promise<void> {
    while (this.pending.size > 0) {
      await Promise.all([...this.pending]);
    }
  }
}
