/**
 * NATS KV implementation of the subset of the Durable Object storage API
 * that `AggregateObject` (and the projection cursor store) actually use:
 * `get` / `put` / `delete` / `deleteAll` / `list` plus the alarm trio.
 *
 * Every aggregate actor owns one instance, scoped by a key prefix
 * `<aggregateType>.<aggregateId>` inside a shared KV bucket. Values are
 * stored as JSON, which matches what DO storage's structured clone
 * round-trip gives back for the state objects Ceves persists (plain data,
 * no class prototypes).
 *
 * Cross-process safety: the `state` key is written with a compare-and-swap
 * on the KV revision observed by this actor's last read/write. If another
 * process wrote a newer state in between, the CAS fails (JetStream error
 * 10071), the write throws `VersionConflictError`, and `conflicted` is
 * latched so the hosting stub can evict this actor and rebuild it from
 * storage. All other keys (replay markers, projection cursors, the stored
 * aggregate name) are advisory and use unconditional puts.
 */

import type { KV } from '@nats-io/kv';
import { VersionConflictError } from '../../errors/VersionConflictError';
import { encodeToken, decodeToken } from './naming';
import { isWrongLastSequence } from './jetstream-errors';

/** The one storage key whose writes are CAS-protected. */
const STATE_KEY = 'state';

/** Best-effort `version` extraction from a state value, for error context. */
function extractVersion(value: unknown): number {
  if (typeof value !== 'object' || value === null) return 0;
  const candidate = (value as { version?: unknown }).version;
  return typeof candidate === 'number' ? candidate : 0;
}

/** Alarm scheduling callbacks provided by the hosting actor. */
export interface AlarmHandle {
  set(scheduledTime: number | Date): void;
  get(): number | null;
  delete(): void;
}

/** No-op alarm handle for hosts that don't schedule alarms. */
export const NULL_ALARM_HANDLE: AlarmHandle = {
  set: () => undefined,
  get: () => null,
  delete: () => undefined,
};

export class NatsKvStorage {
  /** KV revision last observed per full key — the CAS token for updates. */
  private readonly revisions = new Map<string, number>();

  /**
   * Latched when a CAS write lost against a concurrent writer. The hosting
   * stub checks this after each dispatch and evicts the actor so the next
   * request reloads fresh state.
   */
  conflicted = false;

  constructor(
    private readonly kv: KV,
    /** `<aggregateType>.<aggregateId>` — see `storageKeyPrefixFor`. */
    private readonly keyPrefix: string,
    private readonly alarms: AlarmHandle = NULL_ALARM_HANDLE
  ) {}

  private fullKey(key: string): string {
    return `${this.keyPrefix}.${encodeToken(key)}`;
  }

  async get<T = unknown>(key: string): Promise<T | undefined>;
  async get<T = unknown>(keys: string[]): Promise<Map<string, T>>;
  async get<T = unknown>(key: string | string[]): Promise<(T | undefined) | Map<string, T>> {
    // DO storage's multi-key overload: a Map containing only found keys.
    if (Array.isArray(key)) {
      const found = new Map<string, T>();
      for (const one of key) {
        const value = await this.get<T>(one);
        if (value !== undefined) found.set(one, value);
      }
      return found;
    }
    const fullKey = this.fullKey(key);
    const entry = await this.kv.get(fullKey);
    if (!entry || entry.operation !== 'PUT') {
      this.revisions.delete(fullKey);
      return undefined;
    }
    this.revisions.set(fullKey, entry.revision);
    return entry.json<T>();
  }

  async put<T = unknown>(key: string, value: T): Promise<void>;
  async put(entries: Record<string, unknown>): Promise<void>;
  async put<T = unknown>(key: string | Record<string, unknown>, value?: T): Promise<void> {
    // DO storage's multi-entry overload. Unlike Durable Objects, NATS KV
    // has no cross-key transaction — entries are written one by one
    // (the `state` key still goes through its CAS path).
    if (typeof key !== 'string') {
      for (const [entryKey, entryValue] of Object.entries(key)) {
        await this.put(entryKey, entryValue);
      }
      return;
    }
    const fullKey = this.fullKey(key);
    const data = JSON.stringify(value);

    if (key !== STATE_KEY) {
      const revision = await this.kv.put(fullKey, data);
      this.revisions.set(fullKey, revision);
      return;
    }

    try {
      const known = this.revisions.get(fullKey);
      const revision =
        known !== undefined
          ? await this.kv.update(fullKey, data, known)
          : await this.kv.create(fullKey, data);
      this.revisions.set(fullKey, revision);
    } catch (error) {
      if (isWrongLastSequence(error)) {
        this.conflicted = true;
        this.revisions.delete(fullKey);
        const version = extractVersion(value);
        throw new VersionConflictError(
          `Concurrent state write detected for ${this.keyPrefix}: ` +
            'another process updated this aggregate in the meantime',
          version,
          version
        );
      }
      throw error;
    }
  }

  async delete(key: string): Promise<boolean>;
  async delete(keys: string[]): Promise<number>;
  async delete(key: string | string[]): Promise<boolean | number> {
    // DO storage's multi-key overload: returns the number of keys deleted.
    if (Array.isArray(key)) {
      let deleted = 0;
      for (const one of key) {
        if (await this.delete(one)) deleted += 1;
      }
      return deleted;
    }
    const fullKey = this.fullKey(key);
    const entry = await this.kv.get(fullKey);
    const existed = Boolean(entry && entry.operation === 'PUT');
    await this.kv.purge(fullKey);
    this.revisions.delete(fullKey);
    return existed;
  }

  async deleteAll(): Promise<void> {
    for (const key of await this.ownKeys()) {
      await this.kv.purge(key);
    }
    this.revisions.clear();
  }

  /**
   * List this aggregate's entries as a Map of decoded key → value.
   * DO storage list options (prefix/start/end/reverse/limit) are not
   * implemented — fail fast rather than silently ignoring them.
   */
  async list<T = unknown>(options?: Record<string, unknown>): Promise<Map<string, T>> {
    if (options && Object.keys(options).length > 0) {
      throw new TypeError(
        `NatsKvStorage.list: options are not supported by the NATS adapter yet ` +
          `(got: ${Object.keys(options).join(', ')})`
      );
    }
    const result = new Map<string, T>();
    for (const fullKey of await this.ownKeys()) {
      const entry = await this.kv.get(fullKey);
      if (entry && entry.operation === 'PUT') {
        this.revisions.set(fullKey, entry.revision);
        result.set(decodeToken(fullKey.slice(this.keyPrefix.length + 1)), entry.json<T>());
      }
    }
    return result;
  }

  private async ownKeys(): Promise<string[]> {
    const keys: string[] = [];
    const iterator = await this.kv.keys(`${this.keyPrefix}.>`);
    for await (const key of iterator) {
      keys.push(key);
    }
    return keys;
  }

  // --- Alarm API (delegated to the hosting actor's scheduler) -------------

  setAlarm(scheduledTime: number | Date): Promise<void> {
    this.alarms.set(scheduledTime);
    return Promise.resolve();
  }

  getAlarm(): Promise<number | null> {
    return Promise.resolve(this.alarms.get());
  }

  deleteAlarm(): Promise<void> {
    this.alarms.delete();
    return Promise.resolve();
  }

  /** DO storage `sync()` — KV writes are already acknowledged; no-op. */
  sync(): Promise<void> {
    return Promise.resolve();
  }
}
