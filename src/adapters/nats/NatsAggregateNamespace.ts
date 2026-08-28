/**
 * `NatsAggregateNamespace` — a duck-typed `DurableObjectNamespace` that hosts
 * Ceves aggregates as in-process actors backed by NATS.
 *
 * Ceves' `CommandRoute` / `QueryRoute` resolve `c.env[<BINDING>]` and call
 * `namespace.idFromName(aggregateId)` + `namespace.get(id).fetch(request)`.
 * This class satisfies that exact contract, so the routing layer runs
 * unmodified: instead of a Cloudflare DO stub, `fetch` dispatches to a local
 * actor — an instance of the app's own `AggregateObject` subclass constructed
 * with a {@link NatsAggregateContext} (NATS-KV-backed storage) and wired to a
 * JetStream-backed event store via `setStores()`.
 *
 * Concurrency model:
 * - Within one process, requests to the same aggregate are serialized on a
 *   per-actor promise queue — the Durable Object single-threading guarantee.
 * - Across processes, the KV compare-and-swap on the `state` key plus
 *   JetStream's per-subject expected-last-sequence are the backstop: a losing
 *   writer gets a 409 and its actor is evicted, so the next request rebuilds
 *   from the authoritative store. Run one host per aggregate subset (or a
 *   single host) for conflict-free operation; conflicts are detected, not
 *   silently merged.
 * - Alarms (`storage.setAlarm`) are scheduled with an in-process timer and
 *   run through the same serial queue as requests.
 */

import type { KV } from '@nats-io/kv';
import type { IEventStore, StoredEvent } from '../../storage/interfaces';
import type { ITenantResolver } from '../../tenancy/TenantResolver';
import { HeaderTenantResolver } from '../../tenancy/HeaderTenantResolver';
import { createLogger } from '../../logger';
import { NatsAggregateContext, aggregateIdFor, type NatsAggregateId } from './NatsAggregateContext';
import { NatsKvStorage, type AlarmHandle } from './NatsKvStorage';
import type { NatsOrgDirectory } from './NatsOrgDirectory';
import { storageKeyPrefixFor } from './naming';
import {
  AGGREGATE_TRANSFERRED_OUT_EVENT,
  isSealedStreamConflict,
  registerTransferredOutHandler,
  type AggregateTransferredOutEvent,
  type AggregateTransferSummary,
} from './transfer';

const logger = createLogger({ component: 'NatsAggregateNamespace' });

/**
 * The aggregate surface the namespace drives. Structurally satisfied by any
 * `AggregateObject` subclass — the class itself is typed loosely (`never`
 * constructor params) so app aggregate classes, whose constructors are
 * declared against Cloudflare's `DurableObjectState`, assign cleanly.
 */
export interface NatsAggregateInstance {
  fetch(request: Request): Promise<Response>;
  alarm?(): Promise<void>;
  setStores(eventStore: IEventStore, tenantResolver: ITenantResolver): void;
}

export type NatsAggregateClass = new (
  ctx: never,
  env: never
) => NatsAggregateInstance;

/** Dependencies handed to the namespace by the runtime bootstrap. */
export interface NatsAggregateNamespaceDeps {
  aggregateType: string;
  AggregateClass: NatsAggregateClass;
  kv: KV;
  /**
   * Event store for the hosted aggregates. When it exposes
   * `waitForPendingSaves` (as `NatsEventStore` does), the host awaits it
   * after each command so the response only returns once the event is
   * durably in the log — surfacing publish failures to the caller instead
   * of letting the core's fire-and-forget persist swallow them.
   */
  eventStore: IEventStore & {
    waitForPendingSaves?(aggregateType: string, aggregateId: string): Promise<void>;
    saveToOrg?(event: StoredEvent, homeOrg: string): Promise<void>;
    takeLastSaved?(aggregateType: string, aggregateId: string): StoredEvent | undefined;
  };
  /** Lazily resolved so namespaces can be built before the env object is complete. */
  getEnv: () => Record<string, unknown>;
  /**
   * Tenant resolver injected into hosted aggregates. Defaults to
   * `HeaderTenantResolver('X-Org-Id', env.DEFAULT_ORG_ID ?? 'default-org')`
   * — the same default `AggregateObject.initializeStores` applies. Pass a
   * custom resolver here when the app needs one; note `setStores()`
   * overrides whatever an aggregate subclass constructor installed.
   */
  tenantResolver?: ITenantResolver;
  /**
   * Aggregate → home-org directory. Required for {@link
   * NatsAggregateNamespace.transferOut} (org transfers only make sense in
   * home-org-partitioned mode); everything else works without it.
   */
  orgDirectory?: NatsOrgDirectory;
  /**
   * Event types whose commit means "this aggregate changed org" — the
   * app's SetOrganization-style sale/claim endpoint. When a command
   * commits one of these, the host runs the full org transfer (seal →
   * purge → seed → repoint, see {@link NatsAggregateNamespace.transferOut})
   * to the org named by the committed DOMAIN EVENT's `orgId` field (the
   * production `OrganizationSet` data shape; the envelope's org can't be
   * used — the core stamps it from the pre-event state, i.e. the seller)
   * before returning the response. So the endpoint IS the sale: its 200
   * means routing, partitioning, and the buyer's fresh seeded stream are
   * all in place. WHO may run it stays the aggregate's business: enforce
   * "only the current org can sell; an unowned aggregate can be claimed"
   * in `checkAuthorization`. Requires `orgDirectory` and a store exposing
   * `saveToOrg`/`takeLastSaved`.
   */
  orgTransferOn?: string[];
}

/** Storage key (per aggregate) persisting the pending alarm time. */
const ALARM_STORAGE_KEY = '__alarm';

/** Alarm retry policy — mirrors the CF runtime's retry-on-throw contract. */
const MAX_ALARM_RETRIES = 6;
const ALARM_RETRY_BASE_MILLIS = 30_000;
const ALARM_RETRY_MAX_MILLIS = 300_000;

interface ActorEntry {
  context: NatsAggregateContext;
  instance: NatsAggregateInstance;
  /** Serial dispatch queue — the DO single-threading guarantee. */
  queue: Promise<void>;
  /**
   * Set when this actor was evicted. Requests still queued on it are
   * re-dispatched to the current actor instead of executing against the
   * stale instance, and its alarm handle goes inert.
   */
  poisoned: boolean;
  alarmAt: number | null;
  alarmTimer: ReturnType<typeof setTimeout> | null;
  alarmRetries: number;
}

/**
 * Bound on transparent re-dispatches of one request when its actor was
 * evicted before the request ran. Each hop executes against a freshly
 * rebuilt actor, so more than a couple only happens under pathological
 * conflict storms — at that point the request runs where it is.
 */
const MAX_REDISPATCHES = 5;

/**
 * The HTTP status a Ceves error already carries, when it is a client-side
 * (4xx) outcome. Duck-typed on `httpStatusCode` so it survives bundling.
 */
function clientErrorStatus(error: unknown): number | null {
  const code = (error as { httpStatusCode?: unknown } | null)?.httpStatusCode;
  return typeof code === 'number' && code >= 400 && code < 500 ? code : null;
}

/**
 * Response for a command whose event the log refused AFTER the state
 * commit. Deliberately never 500: the app did nothing wrong. A rejected
 * append is the domain's own outcome (409 — a competing writer, or the
 * aggregate moved org), and everything else means the log was unreachable
 * (503 + Retry-After), which is a dependency being down, not a defect.
 */
function postCommitFailureResponse(
  error: unknown,
  conflictMessage: string,
  unavailableMessage: string
): Response {
  const status = clientErrorStatus(error) ?? 503;
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (status === 503) headers['Retry-After'] = '2';
  return new Response(
    JSON.stringify({
      success: false,
      errors: [{ code: status, message: status === 503 ? unavailableMessage : conflictMessage }],
    }),
    { status, headers }
  );
}

/** Duck-typed `DurableObjectStub`. */
export interface NatsAggregateStub {
  readonly id: NatsAggregateId;
  readonly name: string;
  fetch(input: Request | string, init?: RequestInit): Promise<Response>;
}

export class NatsAggregateNamespace {
  private readonly actors = new Map<string, ActorEntry>();

  /**
   * Org transfers whose command already committed but whose storage move
   * did not finish (aggregateId → target org). The next request to the
   * aggregate completes it before running — see
   * {@link maybeTriggerOrgTransfer}. Cross-process recovery does not
   * depend on this map: the seal event in the log carries the same
   * information (see {@link resumeInterruptedTransfer}).
   */
  private readonly pendingTransfers = new Map<string, string>();

  constructor(private readonly deps: NatsAggregateNamespaceDeps) {
    // A sealed stream (aggregate transferred to another org) must stay
    // replayable — register the audit-only seal handler for this type.
    registerTransferredOutHandler(deps.aggregateType);
  }

  idFromName(name: string): NatsAggregateId {
    return aggregateIdFor(name);
  }

  idFromString(id: string): NatsAggregateId {
    return aggregateIdFor(id);
  }

  newUniqueId(): NatsAggregateId {
    return aggregateIdFor(crypto.randomUUID());
  }

  get(id: NatsAggregateId | string): NatsAggregateStub {
    const name = typeof id === 'string' ? id : id.name;
    return {
      id: aggregateIdFor(name),
      name,
      fetch: (input, init) => {
        const request = typeof input === 'string' ? new Request(input, init) : input;
        return this.dispatch(name, request);
      },
    };
  }

  /** Requests for one aggregate run strictly one at a time, in order. */
  private dispatch(name: string, request: Request, hop = 0): Promise<Response> {
    const entry = this.actorFor(name);
    const run = async (): Promise<Response> => {
      // The actor this request was queued on may have been evicted by an
      // earlier request's conflict/failure. Executing against the stale
      // instance would serve outdated state — re-dispatch to the current
      // actor instead (bounded; see MAX_REDISPATCHES).
      if (entry.poisoned && hop < MAX_REDISPATCHES) {
        return this.dispatch(name, request, hop + 1);
      }
      // Finish an org transfer whose storage move failed after its
      // command committed — this request must not run against a
      // half-moved aggregate. Completing it evicts this actor, so the
      // request re-dispatches onto the rebuilt one.
      if (this.pendingTransfers.has(name)) {
        await this.completePendingTransfer(name, entry);
        if (entry.poisoned && hop < MAX_REDISPATCHES) {
          return this.dispatch(name, request, hop + 1);
        }
      }
      await entry.context.whenReady();
      let response: Response;
      try {
        response = await entry.instance.fetch(request);
      } catch (error) {
        // AggregateObject.fetch handles its own errors; anything escaping
        // here left the actor in an unknown state — rebuild it next time.
        this.evict(name, entry);
        throw error;
      }
      response = await this.flushEventSaves(name, entry, response);
      if (!entry.context.storage.conflicted && response.ok) {
        response = await this.maybeTriggerOrgTransfer(name, entry, response);
      }
      if (entry.context.storage.conflicted) {
        logger.warn('Evicting aggregate actor after storage write conflict', {
          aggregateType: this.deps.aggregateType,
          aggregateId: name,
        });
        this.evict(name, entry);
      } else if (response.status >= 500) {
        // A 5xx can mean state restoration failed mid-way (e.g. a transient
        // NATS outage during the first load). AggregateObject latches
        // `stateLoaded` before restoring, so a poisoned actor would keep
        // serving wrong 404s/500s forever — rebuild it on the next request.
        logger.warn('Evicting aggregate actor after 5xx response', {
          aggregateType: this.deps.aggregateType,
          aggregateId: name,
          status: response.status,
        });
        this.evict(name, entry);
      }
      return response;
    };
    const result = entry.queue.then(run, run);
    entry.queue = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  private actorFor(name: string): ActorEntry {
    const existing = this.actors.get(name);
    if (existing) return existing;

    const entry: Partial<ActorEntry> = {
      queue: Promise.resolve(),
      poisoned: false,
      alarmAt: null,
      alarmTimer: null,
      alarmRetries: 0,
    };
    const alarms = this.alarmHandleFor(name, entry as ActorEntry);
    const storage = new NatsKvStorage(
      this.deps.kv,
      storageKeyPrefixFor(this.deps.aggregateType, name),
      alarms
    );
    const context = new NatsAggregateContext(name, storage);
    const env = this.deps.getEnv();
    const instance = new this.deps.AggregateClass(context as never, env as never);
    instance.setStores(this.deps.eventStore, this.tenantResolverFor(env));

    entry.context = context;
    entry.instance = instance;
    const complete = entry as ActorEntry;
    this.actors.set(name, complete);

    // Re-arm a persisted alarm (survives evictions and process restarts).
    // Chained via blockConcurrencyWhile so it serializes before the first
    // request, mirroring DO construction semantics.
    void context.blockConcurrencyWhile(async () => {
      try {
        const at = await storage.get<number>(ALARM_STORAGE_KEY);
        if (typeof at === 'number' && !complete.poisoned) {
          this.armAlarmTimer(name, complete, at);
        }
      } catch (error) {
        logger.warn('Failed to restore persisted alarm', {
          aggregateType: this.deps.aggregateType,
          aggregateId: name,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    });
    return complete;
  }

  private tenantResolverFor(env: Record<string, unknown>): ITenantResolver {
    if (this.deps.tenantResolver) return this.deps.tenantResolver;
    const defaultOrgId =
      typeof env['DEFAULT_ORG_ID'] === 'string' ? env['DEFAULT_ORG_ID'] : 'default-org';
    return new HeaderTenantResolver('X-Org-Id', defaultOrgId);
  }

  /**
   * Await the event-log publishes the command pipeline started
   * (fire-and-forget in `AggregateObject.applyAndPersistEvent`). On
   * failure the state commit and the log have diverged: evict the actor
   * (its in-memory state reflects an event that never reached the log)
   * and, when the pipeline had reported success, replace the response
   * rather than acknowledging a write the log lost.
   *
   * The replacement is never a 500 — nothing here is a bug in the app:
   * a rejected append means another writer won or the aggregate moved
   * org (409, retryable against fresh state), and anything else means the
   * log itself was unreachable (503 + Retry-After). A sealed stream also
   * triggers {@link resumeInterruptedTransfer}, so an org transfer that
   * died half-way finishes instead of wedging the aggregate.
   */
  private async flushEventSaves(
    name: string,
    entry: ActorEntry,
    response: Response
  ): Promise<Response> {
    if (!this.deps.eventStore.waitForPendingSaves) return response;
    try {
      await this.deps.eventStore.waitForPendingSaves(this.deps.aggregateType, name);
      return response;
    } catch (error) {
      logger.error('Event log write failed after the state commit', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        error: error instanceof Error ? error.message : String(error),
      });
      await this.repairDivergedState(name, entry);
      this.evict(name, entry);
      if (isSealedStreamConflict(error)) {
        // The aggregate was transferred out from under this write. Finish
        // the transfer if it was left half-done, so the NEXT request lands
        // on the new org instead of hitting the same wall forever.
        await this.resumeInterruptedTransfer(name);
      }
      if (!response.ok) return response;
      return postCommitFailureResponse(
        error,
        'The event log rejected this write — the aggregate moved on (another writer, ' +
          'or an org transfer); reload it and retry',
        'The event log is unavailable — the command was not recorded; retry shortly'
      );
    }
  }

  /**
   * The state commit landed but its event never reached the log. Evicting
   * alone does not undo that: the rebuilt actor reloads the committed
   * snapshot (`ensureStateLoaded` trusts any stored `version > 0`), so it
   * sits one version ahead of the log, every later command appends at a
   * gapped version, and the aggregate conflicts forever. Dropping the
   * snapshot makes the rebuild replay the LOG, which is the source of
   * truth. Safe when the publish actually landed and only its ack was
   * lost — replay then reconstructs exactly the same state.
   */
  private async repairDivergedState(name: string, entry: ActorEntry): Promise<void> {
    try {
      await entry.context.storage.delete(['state', 'r2_replay_in_progress']);
      logger.warn('Dropped state the event log never received — rebuilding from the log', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
      });
    } catch (error) {
      // The aggregate stays diverged; the next command surfaces it as a
      // conflict rather than silently accepting a gapped append.
      logger.error('Could not repair state after a failed event publish', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Alarm scheduling: `storage.setAlarm()` arms an in-process timer AND
   * persists the scheduled time under an advisory storage key, so a
   * pending alarm survives actor eviction and process restarts (re-armed
   * in `actorFor`). Firing runs the aggregate's `alarm()` through the same
   * serial queue as requests; a throwing handler is retried with capped
   * exponential backoff, mirroring the CF runtime's retry contract.
   */
  private alarmHandleFor(name: string, entry: ActorEntry): AlarmHandle {
    return {
      set: (scheduledTime) => {
        if (entry.poisoned) return; // an evicted actor must not arm timers
        const at = typeof scheduledTime === 'number' ? scheduledTime : scheduledTime.getTime();
        entry.alarmRetries = 0;
        void entry.context.storage.put(ALARM_STORAGE_KEY, at).catch((error: unknown) => {
          logger.warn('Failed to persist alarm time', {
            aggregateType: this.deps.aggregateType,
            aggregateId: name,
            error: error instanceof Error ? error.message : String(error),
          });
        });
        this.armAlarmTimer(name, entry, at);
      },
      get: () => entry.alarmAt,
      delete: () => {
        if (entry.alarmTimer !== null) clearTimeout(entry.alarmTimer);
        entry.alarmAt = null;
        entry.alarmTimer = null;
        void entry.context.storage.delete(ALARM_STORAGE_KEY).catch(() => undefined);
      },
    };
  }

  /** Arm the in-process timer only (persistence handled by the caller). */
  private armAlarmTimer(name: string, entry: ActorEntry, at: number): void {
    if (entry.alarmTimer !== null) clearTimeout(entry.alarmTimer);
    entry.alarmAt = at;
    entry.alarmTimer = setTimeout(() => {
      entry.alarmAt = null;
      entry.alarmTimer = null;
      this.fireAlarm(name, entry);
    }, Math.max(0, at - Date.now()));
  }

  private fireAlarm(name: string, entry: ActorEntry): void {
    const run = async (): Promise<void> => {
      // A poisoned actor never runs the handler; the persisted alarm key
      // survives, so the replacement actor re-arms it on construction.
      if (entry.poisoned || !entry.instance.alarm) return;
      await entry.context.whenReady();
      try {
        await entry.instance.alarm();
        entry.alarmRetries = 0;
        // Clear the persisted alarm only if the handler didn't arm a new one.
        if (entry.alarmAt === null) {
          await entry.context.storage.delete(ALARM_STORAGE_KEY).catch(() => undefined);
        }
      } catch (error) {
        this.retryAlarm(name, entry, error);
      }
    };
    entry.queue = entry.queue.then(run, run);
  }

  /** Re-arm a failed alarm with capped exponential backoff, then give up loudly. */
  private retryAlarm(name: string, entry: ActorEntry, error: unknown): void {
    entry.alarmRetries += 1;
    const attempt = entry.alarmRetries;
    if (attempt > MAX_ALARM_RETRIES) {
      logger.error('Aggregate alarm handler failed permanently — giving up', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        attempts: attempt - 1,
        error: error instanceof Error ? error.message : String(error),
      });
      void entry.context.storage.delete(ALARM_STORAGE_KEY).catch(() => undefined);
      return;
    }
    const backoff = Math.min(
      ALARM_RETRY_BASE_MILLIS * 2 ** (attempt - 1),
      ALARM_RETRY_MAX_MILLIS
    );
    logger.error('Aggregate alarm handler failed — retrying with backoff', {
      aggregateType: this.deps.aggregateType,
      aggregateId: name,
      attempt,
      retryInMillis: backoff,
      error: error instanceof Error ? error.message : String(error),
    });
    this.armAlarmTimer(name, entry, Date.now() + backoff);
  }

  /**
   * Remove an actor so the next request rebuilds it from storage. When the
   * caller passes the entry it acted on, eviction is skipped if the map
   * already holds a NEWER actor for the name — a late request that was
   * queued on an already-evicted (conflicted) actor must not tear down its
   * healthy replacement.
   */
  private evict(name: string, expected?: ActorEntry): void {
    if (expected) expected.poisoned = true;
    const entry = this.actors.get(name);
    if (!entry || (expected !== undefined && entry !== expected)) return;
    entry.poisoned = true;
    if (entry.alarmTimer !== null) clearTimeout(entry.alarmTimer);
    this.actors.delete(name);
  }

  /**
   * The org-change endpoint IS the sale (see `orgTransferOn`): when the
   * event a command just committed is a registered org-changing type, run
   * the full transfer to the envelope's org before returning the response
   * — still inside this actor's serial-queue run, so no command
   * interleaves and the endpoint's 200 means the sale is complete.
   * Setting the org the aggregate already lives under is a no-op
   * (`performTransferOut` short-circuits on `fromOrg === toOrg`).
   *
   * A transfer failure AFTER the commit does NOT fail the command: the org
   * change is real — applied to state, durable in the log, and already
   * governing authorization — and which storage partition holds the
   * aggregate is this runtime's internal business (Cloudflare has no
   * partition at all). So the caller keeps its success response, the
   * failure is logged, and the move is remembered as pending: the next
   * request to this aggregate finishes it (see
   * {@link completePendingTransfer}), as does any other host that trips
   * over the seal.
   */
  private async maybeTriggerOrgTransfer(
    name: string,
    entry: ActorEntry,
    response: Response
  ): Promise<Response> {
    const triggers = this.deps.orgTransferOn;
    const directory = this.deps.orgDirectory;
    const takeLastSaved = this.deps.eventStore.takeLastSaved?.bind(this.deps.eventStore);
    const saveToOrg = this.deps.eventStore.saveToOrg?.bind(this.deps.eventStore);
    if (!triggers || triggers.length === 0 || !directory || !takeLastSaved || !saveToOrg) {
      return response;
    }
    const committed = takeLastSaved(this.deps.aggregateType, name);
    if (!committed || !triggers.includes(committed.type)) {
      return response;
    }
    // The target org comes from the DOMAIN EVENT's `orgId` field (the
    // production OrganizationSet shape) — NOT the envelope's orgId, which
    // the core stamps from the PRE-event state, i.e. the seller.
    const eventData = committed.event as { orgId?: unknown };
    const toOrg = typeof eventData.orgId === 'string' && eventData.orgId ? eventData.orgId : null;
    if (toOrg === null) {
      logger.warn('Org-transfer trigger event has no string `orgId` in its data — not transferring', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        eventType: committed.type,
      });
      return response;
    }
    try {
      await this.performTransferOut(directory, saveToOrg, name, toOrg, entry);
      return response;
    } catch (error) {
      logger.error('Org change committed but the org transfer failed', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        toOrg,
        error: error instanceof Error ? error.message : String(error),
      });
      this.pendingTransfers.set(name, toOrg);
      this.evict(name, entry);
      return response;
    }
  }

  /** Run work on an aggregate's serial queue (callers outside a run). */
  private enqueue<T>(entry: ActorEntry, run: () => Promise<T>): Promise<T> {
    const result = entry.queue.then(run, run);
    entry.queue = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  /**
   * Finish a transfer this process recorded as pending. Called from inside
   * the actor's serial run, so it drives `performTransferOut` directly.
   * Still failing? Keep it pending and log — the next request retries; the
   * seal in the log means any other host recovers it too.
   */
  private async completePendingTransfer(name: string, entry: ActorEntry): Promise<void> {
    const toOrg = this.pendingTransfers.get(name);
    const directory = this.deps.orgDirectory;
    const saveToOrg = this.deps.eventStore.saveToOrg?.bind(this.deps.eventStore);
    if (toOrg === undefined) return;
    if (!directory || !saveToOrg) {
      this.pendingTransfers.delete(name);
      return;
    }
    try {
      await this.performTransferOut(directory, saveToOrg, name, toOrg, entry);
      this.pendingTransfers.delete(name);
      logger.info('Completed a pending org transfer before serving the next request', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        toOrg,
      });
    } catch (error) {
      logger.error('Pending org transfer still failing — the next request will retry', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        toOrg,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Cross-process recovery: a write was refused because the stream is
   * SEALED. If the transfer that sealed it never finished (the directory
   * still points here), finish it — the seal event carries the target org,
   * so any host can complete a transfer another host started, and an
   * aggregate can't be stranded by whichever process happened to crash.
   * A transfer that DID finish leaves nothing to do (this host's load
   * follows the repointed directory).
   */
  private async resumeInterruptedTransfer(name: string): Promise<void> {
    const directory = this.deps.orgDirectory;
    const saveToOrg = this.deps.eventStore.saveToOrg?.bind(this.deps.eventStore);
    if (!directory || !saveToOrg) return;
    try {
      const events = await this.deps.eventStore.load(this.deps.aggregateType, name);
      const last = events[events.length - 1];
      if (!last || last.type !== AGGREGATE_TRANSFERRED_OUT_EVENT) return;
      const seal = last.event as AggregateTransferredOutEvent;
      if (typeof seal.toOrg !== 'string' || !seal.toOrg) return;
      const entry = this.actorFor(name);
      await this.enqueue(entry, () =>
        this.performTransferOut(directory, saveToOrg, name, seal.toOrg, entry)
      );
      logger.info('Finished an org transfer another run left incomplete', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        toOrg: seal.toOrg,
      });
    } catch (error) {
      logger.error('Could not finish the interrupted org transfer — a later request will retry', {
        aggregateType: this.deps.aggregateType,
        aggregateId: name,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Transfer this aggregate to another org — the "sell the lock" flow.
   * Runs on the aggregate's serial queue so no in-process command
   * interleaves:
   *
   * flush pending saves → SEAL the old stream (final audit event) →
   * PURGE the KV state → SEED the new org's stream with the original
   * creation event → REPOINT the org directory → EVICT the actor.
   *
   * Fresh-start semantics with preserved identity: the buyer gets the
   * aggregate carrying exactly the values it was created with (its
   * id/number/uuid — whatever the creation event holds) and none of the
   * seller's accumulated state; the sealed old stream stays in place as
   * the seller's audit record. Idempotent — a crashed transfer is
   * completed by calling it again with the same target org.
   */
  async transferOut(name: string, toOrg: string): Promise<AggregateTransferSummary> {
    const directory = this.deps.orgDirectory;
    if (!directory) {
      throw new Error(
        'transferOut requires the aggregate → home-org directory ' +
          '(home-org-partitioned mode; pass orgDirectory to the namespace/runtime)'
      );
    }
    const saveToOrg = this.deps.eventStore.saveToOrg?.bind(this.deps.eventStore);
    if (!saveToOrg) {
      throw new Error('transferOut requires an event store exposing saveToOrg (NatsEventStore)');
    }
    const entry = this.actorFor(name);
    return this.enqueue(entry, () =>
      this.performTransferOut(directory, saveToOrg, name, toOrg, entry)
    );
  }

  private async performTransferOut(
    directory: NatsOrgDirectory,
    saveToOrg: (event: StoredEvent, homeOrg: string) => Promise<void>,
    name: string,
    toOrg: string,
    entry: ActorEntry
  ): Promise<AggregateTransferSummary> {
    const aggregateType = this.deps.aggregateType;
    await entry.context.whenReady();
    await this.deps.eventStore.waitForPendingSaves?.(aggregateType, name);

    // The directory (read fresh — a stale cache must not decide a transfer)
    // and the event log are the truth here, never the actor's memory.
    const fromOrg = await directory.resolveFresh(aggregateType, name);
    if (fromOrg === null) {
      throw new Error(
        `Cannot transfer ${aggregateType}/${name}: the aggregate has never been created ` +
          '(no org-directory entry)'
      );
    }
    const events = await this.deps.eventStore.load(aggregateType, name);
    const last = events[events.length - 1];
    const seal =
      last !== undefined && last.type === AGGREGATE_TRANSFERRED_OUT_EVENT ? last : undefined;
    if (seal !== undefined) {
      const sealData = seal.event as AggregateTransferredOutEvent;
      if (sealData.toOrg !== toOrg) {
        throw new Error(
          `Cannot transfer ${aggregateType}/${name} to "${toOrg}": an unfinished transfer ` +
            `to "${sealData.toOrg}" sealed the stream — retry THAT transfer to complete it`
        );
      }
    }
    if (fromOrg === toOrg) {
      return this.finishUnderTargetOrg(name, toOrg, entry, events.length > 0);
    }

    // 1. Seal the old stream (skipped on a resume where the seal already
    //    landed, and for a claim-only aggregate with no events).
    let sealedVersion = seal?.version ?? null;
    if (seal === undefined && last !== undefined) {
      const sealData: AggregateTransferredOutEvent = {
        type: AGGREGATE_TRANSFERRED_OUT_EVENT,
        fromOrg,
        toOrg,
      };
      const sealEvent: StoredEvent = {
        aggregateType,
        aggregateId: name,
        version: last.version + 1,
        type: AGGREGATE_TRANSFERRED_OUT_EVENT,
        timestamp: new Date().toISOString(),
        orgId: fromOrg,
        event: sealData,
      };
      await saveToOrg(sealEvent, fromOrg);
      sealedVersion = sealEvent.version;
    }

    // 2. Purge the KV state. Storage keys are org-free, so a surviving
    //    snapshot would hand the seller's state to the buyer.
    await entry.context.storage.deleteAll();

    // 3. Seed the buyer's stream with the ORIGINAL creation event: identity
    //    (id/number/uuid) survives the sale, accumulated state does not. On
    //    a re-sale events[0] is the previous sale's seed, so the original
    //    creation data is carried forward unchanged. Seeding happens BEFORE
    //    the repoint so the directory never points at a half-built stream.
    const creation = events[0];
    let seeded = false;
    if (creation !== undefined && creation.type !== AGGREGATE_TRANSFERRED_OUT_EVENT) {
      await saveToOrg(
        {
          aggregateType,
          aggregateId: name,
          version: 1,
          type: creation.type,
          // The original timestamp is preserved deliberately: it keeps the
          // creation date truthful AND makes a retried seed byte-identical,
          // so the server absorbs it instead of conflicting.
          timestamp: creation.timestamp,
          orgId: toOrg,
          event: creation.event,
        },
        toOrg
      );
      seeded = true;
    }

    // 4. Repoint the directory — from here commands and loads go to toOrg.
    await directory.transfer(aggregateType, name, fromOrg, toOrg);

    // 5. Purge again: a cross-process seller command racing the transfer
    //    can rebuild a snapshot between purge and repoint (its event append
    //    is rejected by the sealed stream, but the core commits state
    //    first). The second purge shrinks that window to nearly nothing;
    //    if it catches an early buyer snapshot instead, that's harmless —
    //    snapshots rebuild from the event log.
    await entry.context.storage.deleteAll();
    this.evict(name, entry);

    logger.info('Aggregate transferred to another org', {
      aggregateType,
      aggregateId: name,
      fromOrg,
      toOrg,
      sealedVersion,
      seeded,
    });
    return {
      aggregateType,
      aggregateId: name,
      fromOrg,
      toOrg,
      sealedVersion,
      seeded,
      alreadyTransferred: false,
    };
  }

  /**
   * The directory already points at the target org: either the transfer
   * completed earlier (or the aggregate was simply created there) — a
   * no-op — or a create claimed the directory and crashed before its first
   * event, in which case the cleanup is finished defensively.
   */
  private async finishUnderTargetOrg(
    name: string,
    toOrg: string,
    entry: ActorEntry,
    hasEvents: boolean
  ): Promise<AggregateTransferSummary> {
    if (!hasEvents) {
      await entry.context.storage.deleteAll();
      this.evict(name, entry);
    }
    return {
      aggregateType: this.deps.aggregateType,
      aggregateId: name,
      fromOrg: toOrg,
      toOrg,
      sealedVersion: null,
      seeded: false,
      alreadyTransferred: true,
    };
  }

  /** Await in-flight work across all actors (graceful shutdown). */
  async drain(): Promise<void> {
    for (const entry of this.actors.values()) {
      await entry.queue;
      await entry.context.drain();
    }
  }
}
