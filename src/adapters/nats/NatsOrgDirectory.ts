/**
 * `NatsOrgDirectory` — the aggregate → home-org mapping that makes
 * tenant-partitioned NATS structure routable.
 *
 * One KV bucket (default `ceves_org_dir`) maps `<aggregateType>.<id>` to
 * the aggregate's HOME ORG: the tenant it was created under. The entry is
 * written exactly once, at creation, with a KV CAS `create()` — which
 * doubles as a **global aggregate-id lock**: the same id can never be
 * minted under two orgs, preserving Cloudflare's "aggregateId is globally
 * unique" identity semantics while still partitioning storage per tenant.
 *
 * Consumers:
 * - `NatsEventStore` resolves the home org to build the canonical event
 *   subject `ceves.events.<homeOrg>.<type>.<id>` on save AND load — so
 *   `IEventStore.load(type, id)` needs no org parameter, and traffic that
 *   carries no tenant claim (B2C JWTs, super API keys, temp-key access)
 *   still reaches the right stream.
 * - The REST gateway resolves it FIRST when routing commands, so a wrong
 *   claimed org can neither fork a stream nor miss it; the caller's claim
 *   only matters on create, where it mints the home org.
 *
 * The home org changes ONLY through the explicit transfer operation
 * ({@link transfer}, driven by `NatsAggregateNamespace.transferOut` — the
 * "sell the lock to another org" flow). Everyday tenancy changes like a
 * lock's `SetOrganization` update the aggregate's CURRENT org in state and
 * in the routed stream, not its storage partition. Entries are cached
 * in-process; call {@link startWatching} in every long-lived process
 * (gateways, aggregate services) so a transfer elsewhere invalidates the
 * cache — without a watch, a cached entry can go stale until the process
 * restarts (writes stay safe regardless: the sealed old stream rejects
 * appends).
 */

import type { KV, KvWatchEntry } from '@nats-io/kv';
import { createLogger } from '../../logger';
import { VersionConflictError } from '../../errors/VersionConflictError';
import { encodeToken } from './naming';
import { isWrongLastSequence } from './jetstream-errors';

const logger = createLogger({ component: 'NatsOrgDirectory' });

export const DEFAULT_ORG_DIRECTORY_BUCKET = 'ceves_org_dir';

/**
 * The slice of `QueuedIterator<KvWatchEntry>` the directory's cache watch
 * uses (structural — avoids a direct `@nats-io/nats-core` dependency).
 */
interface KvWatcherLike extends AsyncIterable<KvWatchEntry> {
  stop(err?: Error): void;
}

export class NatsOrgDirectory {
  /**
   * Positive lookups cache in-process. Coherent for the process lifetime
   * when {@link startWatching} is active; otherwise entries can go stale
   * after a cross-process {@link transfer}.
   */
  private readonly cache = new Map<string, string>();

  /** The live KV watch feeding the cache, when started. */
  private watcher: KvWatcherLike | null = null;

  constructor(private readonly kv: KV) {}

  private keyFor(aggregateType: string, aggregateId: string): string {
    return `${encodeToken(aggregateType)}.${encodeToken(aggregateId)}`;
  }

  /** The aggregate's home org, or `null` when it has never been created. */
  async resolve(aggregateType: string, aggregateId: string): Promise<string | null> {
    const key = this.keyFor(aggregateType, aggregateId);
    const cached = this.cache.get(key);
    if (cached !== undefined) return cached;

    const entry = await this.kv.get(key);
    if (!entry || entry.operation !== 'PUT') return null;
    const homeOrg = entry.string();
    this.cache.set(key, homeOrg);
    return homeOrg;
  }

  /**
   * Like {@link resolve} but always reads through to KV, refreshing the
   * cache. Used where a stale cached entry must not decide anything —
   * e.g. determining the CURRENT org at the start of a transfer.
   */
  async resolveFresh(aggregateType: string, aggregateId: string): Promise<string | null> {
    const key = this.keyFor(aggregateType, aggregateId);
    const entry = await this.kv.get(key);
    if (!entry || entry.operation !== 'PUT') {
      this.cache.delete(key);
      return null;
    }
    const homeOrg = entry.string();
    this.cache.set(key, homeOrg);
    return homeOrg;
  }

  /**
   * Claim the home org for a NEW aggregate. Exactly one claim wins (KV
   * CAS create); every caller gets back the AUTHORITATIVE home org — which
   * may differ from `orgId` when another tenant claimed the id first.
   * Callers must treat a mismatch as an id collision, not proceed under
   * their own org.
   */
  async claim(aggregateType: string, aggregateId: string, orgId: string): Promise<string> {
    const key = this.keyFor(aggregateType, aggregateId);
    const cached = this.cache.get(key);
    if (cached !== undefined) return cached;

    try {
      await this.kv.create(key, orgId);
      this.cache.set(key, orgId);
      return orgId;
    } catch (error) {
      if (!isWrongLastSequence(error)) throw error;
      // Lost the race (or the entry already existed) — the stored org wins.
      const existing = await this.resolve(aggregateType, aggregateId);
      if (existing !== null) {
        if (existing !== orgId) {
          logger.warn('Aggregate id already claimed by another org', {
            aggregateType,
            aggregateId,
            claimedOrg: orgId,
            homeOrg: existing,
          });
        }
        return existing;
      }
      throw error;
    }
  }

  /**
   * Repoint the aggregate's home org from `expectedFromOrg` to `toOrg` —
   * the directory step of an org transfer. CAS-guarded on the entry's KV
   * revision so a concurrent transfer can't be silently overwritten, and
   * idempotent: an entry already at `toOrg` reports `'already-transferred'`.
   *
   * Callers (`NatsAggregateNamespace.transferOut`) MUST have sealed the old
   * org's event stream and purged the aggregate's KV state first — this
   * method only moves the pointer.
   */
  async transfer(
    aggregateType: string,
    aggregateId: string,
    expectedFromOrg: string,
    toOrg: string
  ): Promise<'transferred' | 'already-transferred'> {
    const key = this.keyFor(aggregateType, aggregateId);
    const entry = await this.kv.get(key);
    if (!entry || entry.operation !== 'PUT') {
      throw new Error(
        `Cannot transfer ${aggregateType}/${aggregateId}: no org-directory entry exists`
      );
    }
    const current = entry.string();
    if (current === toOrg) {
      this.cache.set(key, toOrg);
      return 'already-transferred';
    }
    if (current !== expectedFromOrg) {
      throw new Error(
        `Cannot transfer ${aggregateType}/${aggregateId} from "${expectedFromOrg}": ` +
          `the directory entry is "${current}"`
      );
    }

    try {
      await this.kv.update(key, toOrg, entry.revision);
      this.cache.set(key, toOrg);
      return 'transferred';
    } catch (error) {
      if (!isWrongLastSequence(error)) throw error;
      // Lost a CAS race — acceptable only if the winner made the same move.
      const now = await this.resolveFresh(aggregateType, aggregateId);
      if (now === toOrg) return 'already-transferred';
      throw new VersionConflictError(
        `Concurrent org-directory change for ${aggregateType}/${aggregateId}: ` +
          `expected "${expectedFromOrg}", found "${now ?? '(deleted)'}"`,
        0,
        0,
        aggregateType,
        aggregateId
      );
    }
  }

  /**
   * Keep the cache coherent across processes: a KV watch replays current
   * entries (warming the cache) and then applies live updates — so a
   * {@link transfer} performed anywhere invalidates this process's cache
   * within the watch's propagation delay. Idempotent; call once per
   * process. If the watch ever dies, the cache is CLEARED (resolves fall
   * back to KV reads) rather than left to rot.
   */
  async startWatching(): Promise<void> {
    if (this.watcher !== null) return;
    const watcher = await this.kv.watch();
    this.watcher = watcher;
    void this.consumeWatch(watcher);
  }

  /** Stop the watch started by {@link startWatching} (idempotent). */
  stopWatching(): void {
    this.watcher?.stop();
  }

  private async consumeWatch(watcher: KvWatcherLike): Promise<void> {
    try {
      for await (const entry of watcher) {
        if (entry.operation === 'PUT') {
          this.cache.set(entry.key, entry.string());
        } else {
          this.cache.delete(entry.key);
        }
      }
    } catch (error) {
      logger.warn('Org-directory watch failed — clearing the cache', {
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      if (this.watcher === watcher) {
        this.watcher = null;
        // Without live invalidation, cached entries could silently go
        // stale after a transfer — drop them so resolves re-read KV.
        this.cache.clear();
      }
    }
  }
}
