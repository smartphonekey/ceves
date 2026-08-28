/**
 * End-to-end pipeline test: a real `AggregateObject` subclass (the same class
 * that would run as a Cloudflare Durable Object) hosted by
 * `NatsAggregateNamespace` with in-memory NATS fakes.
 *
 * Covers the whole DO-request pipeline on the NATS runtime: routing via the
 * @Route registry, create/update semantics (incl. the AA-92 idempotent
 * duplicate create), event application, KV state persistence, JetStream event
 * persistence, `__state` queries, restart restoration from KV, replay
 * restoration from the event log, in-process serialization, and cross-host
 * conflict detection with actor eviction.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { z } from 'zod';
import type { KV } from '@nats-io/kv';
import type { JetStreamClient, JetStreamManager } from '@nats-io/jetstream';
import { Route } from '../../../routing/Route';
import { CommandRoute, CreateCommandRoute } from '../../../routing/CommandRoute';
import { EventHandler, type IEventHandler } from '../../../decorators';
import { AggregateObject, type AggregateObjectEnv } from '../../../core/AggregateObject';
import { BaseState } from '../../../schemas/State';
import type { EventMetadata } from '../../../events/EventMetadata';
import { VersionConflictError } from '../../../errors/VersionConflictError';
import {
  registerProjector,
  clearProjectors,
  type IEventProjector,
  type ProjectedEvent,
} from '../../../projection';
import { NatsAggregateNamespace } from '../NatsAggregateNamespace';
import { NatsEventStore } from '../NatsEventStore';
import { storageKeyPrefixFor } from '../naming';
import { FakeJetStream, FakeJetStreamManager, FakeKv } from './fakes';

// --- Test domain: a Wallet with Open + Credit commands ----------------------

class WalletState extends BaseState {
  owner = '';
  balance = 0;
}

interface WalletOpenedEvent {
  type: 'WalletOpened';
  owner: string;
  initial: number;
}

interface WalletCreditedEvent {
  type: 'WalletCredited';
  amount: number;
}

@Route({ method: 'POST', path: '/wallets/:id/OpenWallet' })
class OpenWalletRoute extends CreateCommandRoute<
  { owner: string; initial: number },
  WalletState,
  WalletOpenedEvent
> {
  static readonly eventSchema = z.object({ owner: z.string(), initial: z.number() });
  aggregateType = 'WalletAggregate';
  schema = {};

  executeCommand(command: { owner: string; initial: number }): Promise<WalletOpenedEvent> {
    return Promise.resolve({ type: 'WalletOpened', owner: command.owner, initial: command.initial });
  }
}

@Route({ method: 'POST', path: '/wallets/:id/Credit' })
class CreditRoute extends CommandRoute<{ amount: number }, WalletState, WalletCreditedEvent> {
  aggregateType = 'WalletAggregate';
  schema = {};

  executeCommand(
    command: { amount: number },
    _state: WalletState
  ): Promise<WalletCreditedEvent> {
    return Promise.resolve({ type: 'WalletCredited', amount: command.amount });
  }
}

@EventHandler
class WalletOpenedHandler implements IEventHandler<WalletState, WalletOpenedEvent> {
  eventType = 'WalletOpened';
  aggregateType = 'WalletAggregate';

  apply(state: WalletState, event: WalletOpenedEvent, metadata: EventMetadata): WalletState {
    return {
      ...state,
      id: metadata.aggregateId,
      orgId: metadata.orgId,
      owner: event.owner,
      balance: event.initial,
    };
  }
}

@EventHandler
class WalletCreditedHandler implements IEventHandler<WalletState, WalletCreditedEvent> {
  eventType = 'WalletCredited';
  aggregateType = 'WalletAggregate';

  apply(state: WalletState, event: WalletCreditedEvent): WalletState {
    return { ...state, balance: state.balance + event.amount };
  }
}

class WalletAggregate extends AggregateObject<WalletState> {
  constructor(ctx: DurableObjectState, env: AggregateObjectEnv) {
    super(ctx, env, WalletState);
  }
}

// --- Harness ----------------------------------------------------------------

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

async function bodyOf(response: Response): Promise<Record<string, unknown>> {
  const body: Record<string, unknown> = await response.json();
  return body;
}

async function stateOf(response: Response): Promise<WalletState> {
  const state: WalletState = await response.json();
  return state;
}

interface StoredEnvelope {
  aggregateType: string;
  aggregateId: string;
  version: number;
  type: string;
  orgId: string;
}

function parseEnvelope(data: string): StoredEnvelope {
  const parsed: StoredEnvelope = JSON.parse(data);
  return parsed;
}

function makeNamespace(kv: FakeKv, js: FakeJetStream): NatsAggregateNamespace {
  const eventStore = new NatsEventStore(
    js as unknown as JetStreamClient,
    new FakeJetStreamManager(js) as unknown as JetStreamManager
  );
  const env: Record<string, unknown> = { DEFAULT_ORG_ID: 'org-test' };
  return new NatsAggregateNamespace({
    aggregateType: 'WalletAggregate',
    AggregateClass: WalletAggregate,
    kv: kv as unknown as KV,
    eventStore,
    getEnv: () => env,
  });
}

function post(namespace: NatsAggregateNamespace, id: string, action: string, body: unknown) {
  return namespace.get(namespace.idFromName(id)).fetch(
    new Request(`http://aggregate.local/wallets/${id}/${action}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  );
}

function getState(namespace: NatsAggregateNamespace, id: string) {
  return namespace
    .get(namespace.idFromName(id))
    .fetch(new Request(`http://aggregate.local/wallets/${id}/__state`, { method: 'GET' }));
}

describe('NatsAggregateNamespace pipeline', () => {
  it('registers the test routes and handlers via decorators', () => {
    // Referencing the classes also documents that their *registration* (the
    // @Route / @EventHandler side effects) is what the pipeline tests rely on.
    expect(
      [OpenWalletRoute, CreditRoute, WalletOpenedHandler, WalletCreditedHandler].map((c) => c.name)
    ).toEqual(['OpenWalletRoute', 'CreditRoute', 'WalletOpenedHandler', 'WalletCreditedHandler']);
  });

  let kv: FakeKv;
  let js: FakeJetStream;
  let namespace: NatsAggregateNamespace;

  beforeEach(() => {
    kv = new FakeKv();
    js = new FakeJetStream();
    namespace = makeNamespace(kv, js);
  });

  it('creates an aggregate: 201, event in response, state + event persisted', async () => {
    const response = await post(namespace, 'w-1', 'OpenWallet', { owner: 'ada', initial: 100 });
    expect(response.status).toBe(201);
    const body = await bodyOf(response);
    expect(body).toMatchObject({
      success: true,
      aggregateId: 'w-1',
      version: 1,
      event: { type: 'WalletOpened', data: { owner: 'ada', initial: 100 } },
    });

    await flush();
    const stored = js.messages.map((m) => parseEnvelope(m.data));
    expect(stored).toHaveLength(1);
    expect(stored[0]).toMatchObject({
      aggregateType: 'WalletAggregate',
      aggregateId: 'w-1',
      version: 1,
      type: 'WalletOpened',
      orgId: 'org-test',
    });

    const stateKey = `${storageKeyPrefixFor('WalletAggregate', 'w-1')}.state`;
    expect(kv.entries.has(stateKey)).toBe(true);
  });

  it('treats a duplicate create as an idempotent no-op (200, event: null)', async () => {
    await post(namespace, 'w-2', 'OpenWallet', { owner: 'ada', initial: 5 });
    const again = await post(namespace, 'w-2', 'OpenWallet', { owner: 'eve', initial: 999 });
    expect(again.status).toBe(200);
    expect(await bodyOf(again)).toMatchObject({ success: true, version: 1, event: null });

    const state = await stateOf(await getState(namespace, 'w-2'));
    expect(state.owner).toBe('ada');
  });

  it('applies update commands against loaded state', async () => {
    await post(namespace, 'w-3', 'OpenWallet', { owner: 'ada', initial: 10 });
    const response = await post(namespace, 'w-3', 'Credit', { amount: 32 });
    expect(response.status).toBe(200);
    expect(await bodyOf(response)).toMatchObject({ success: true, version: 2 });

    const state = await stateOf(await getState(namespace, 'w-3'));
    expect(state.balance).toBe(42);
    expect(state.version).toBe(2);
  });

  it('404s update commands on a non-existent aggregate', async () => {
    const response = await post(namespace, 'missing', 'Credit', { amount: 1 });
    expect(response.status).toBe(404);
  });

  it('404s state queries for a non-existent aggregate', async () => {
    const response = await getState(namespace, 'missing');
    expect(response.status).toBe(404);
  });

  it('serializes concurrent commands to one aggregate (DO single-threading)', async () => {
    await post(namespace, 'w-4', 'OpenWallet', { owner: 'ada', initial: 0 });
    const responses = await Promise.all([
      post(namespace, 'w-4', 'Credit', { amount: 1 }),
      post(namespace, 'w-4', 'Credit', { amount: 2 }),
      post(namespace, 'w-4', 'Credit', { amount: 4 }),
    ]);
    expect(responses.map((r) => r.status)).toEqual([200, 200, 200]);

    const state = await stateOf(await getState(namespace, 'w-4'));
    expect(state.balance).toBe(7);
    expect(state.version).toBe(4);

    await flush();
    const versions = js.messages.map((m) => parseEnvelope(m.data).version);
    expect(versions).toEqual([1, 2, 3, 4]);
  });

  it('restores state from KV after a "process restart"', async () => {
    await post(namespace, 'w-5', 'OpenWallet', { owner: 'ada', initial: 11 });
    await post(namespace, 'w-5', 'Credit', { amount: 6 });
    await flush();

    const restarted = makeNamespace(kv, js);
    const state = await stateOf(await getState(restarted, 'w-5'));
    expect(state).toMatchObject({ owner: 'ada', balance: 17, version: 2 });
  });

  it('replays state from the JetStream event log when KV state is gone', async () => {
    await post(namespace, 'w-6', 'OpenWallet', { owner: 'ada', initial: 1 });
    await post(namespace, 'w-6', 'Credit', { amount: 2 });
    await post(namespace, 'w-6', 'Credit', { amount: 3 });
    await flush();

    // Lose the KV snapshot — the event log is the source of truth.
    kv.entries.delete(`${storageKeyPrefixFor('WalletAggregate', 'w-6')}.state`);

    const rebuilt = makeNamespace(kv, js);
    const state = await stateOf(await getState(rebuilt, 'w-6'));
    expect(state).toMatchObject({ owner: 'ada', balance: 6, version: 3 });

    // A follow-up command continues the stream where the replay ended.
    const response = await post(rebuilt, 'w-6', 'Credit', { amount: 10 });
    expect(await bodyOf(response)).toMatchObject({ version: 4 });
  });

  it('rebuilds an actor whose state restoration failed transiently (5xx eviction)', async () => {
    await post(namespace, 'w-9', 'OpenWallet', { owner: 'ada', initial: 30 });
    await flush();
    // Lose the KV snapshot so the next host must replay from the event log.
    kv.entries.delete(`${storageKeyPrefixFor('WalletAggregate', 'w-9')}.state`);

    // A fresh host whose event store fails ONCE (transient NATS outage
    // during the first restore) then recovers.
    const realStore = new NatsEventStore(
      js as unknown as JetStreamClient,
      new FakeJetStreamManager(js) as unknown as JetStreamManager
    );
    let failuresLeft = 1;
    const flakyStore: typeof realStore = Object.create(realStore) as typeof realStore;
    flakyStore.load = (...args) => {
      if (failuresLeft > 0) {
        failuresLeft -= 1;
        return Promise.reject(new Error('simulated NATS outage'));
      }
      return realStore.load(...args);
    };
    const env: Record<string, unknown> = { DEFAULT_ORG_ID: 'org-test' };
    const flakyHost = new NatsAggregateNamespace({
      aggregateType: 'WalletAggregate',
      AggregateClass: WalletAggregate,
      kv: kv as unknown as KV,
      eventStore: flakyStore,
      getEnv: () => env,
    });

    // First request hits the outage → 5xx. Without eviction the actor would
    // be poisoned forever (stateLoaded latched with null state → wrong 404s).
    const during = await getState(flakyHost, 'w-9');
    expect(during.status).toBeGreaterThanOrEqual(500);

    // Next request gets a rebuilt actor that replays successfully.
    const after = await getState(flakyHost, 'w-9');
    expect(after.status).toBe(200);
    const state = await stateOf(after);
    expect(state).toMatchObject({ owner: 'ada', balance: 30, version: 1 });
  });

  it('re-dispatches writes queued on a stale actor to its replacement', async () => {
    const hostA = makeNamespace(kv, js);
    const hostB = makeNamespace(kv, js);

    await post(hostA, 'w-8', 'OpenWallet', { owner: 'ada', initial: 0 });
    expect((await getState(hostB, 'w-8')).status).toBe(200); // B warms at v1
    await post(hostA, 'w-8', 'Credit', { amount: 5 }); // B is now stale
    await flush();

    // Two writes land on host B's stale actor back to back. The first
    // conflicts (409) and evicts the actor; the second was queued on the
    // now-poisoned actor and is transparently re-dispatched to a freshly
    // rebuilt one, where it succeeds against current state.
    const [first, second] = await Promise.all([
      post(hostB, 'w-8', 'Credit', { amount: 10 }),
      post(hostB, 'w-8', 'Credit', { amount: 20 }),
    ]);
    expect(first.status).toBe(409);
    expect(second.status).toBe(200);
    expect(await bodyOf(second)).toMatchObject({ version: 3 });

    const state = await stateOf(await getState(hostB, 'w-8'));
    expect(state).toMatchObject({ version: 3, balance: 25 });
  });

  it('detects a cross-host race: stale host 409s, then recovers after eviction', async () => {
    const hostA = makeNamespace(kv, js);
    const hostB = makeNamespace(kv, js);

    await post(hostA, 'w-7', 'OpenWallet', { owner: 'ada', initial: 0 });
    // Host B warms up on version 1.
    expect((await getState(hostB, 'w-7')).status).toBe(200);
    // Host A advances to version 2; host B's actor is now stale.
    await post(hostA, 'w-7', 'Credit', { amount: 5 });
    await flush();

    const conflicted = await post(hostB, 'w-7', 'Credit', { amount: 100 });
    expect(conflicted.status).toBe(409);

    // The conflicted actor was evicted — the retry reloads and succeeds.
    const retry = await post(hostB, 'w-7', 'Credit', { amount: 100 });
    expect(retry.status).toBe(200);
    expect(await bodyOf(retry)).toMatchObject({ version: 3 });

    await flush();
    const versions = js.messages
      .filter((m) => m.subject.endsWith('.w-7'))
      .map((m) => parseEnvelope(m.data).version);
    expect(versions).toEqual([1, 2, 3]);

    // Host B (the last writer) serves the up-to-date state.
    const fresh = await stateOf(await getState(hostB, 'w-7'));
    expect(fresh.balance).toBe(105);

    // Documented caveat of multi-host operation: host A's warm actor still
    // serves its cached (stale) state for READS — it only discovers the race
    // on its next WRITE, which conflicts and evicts it. Run one host per
    // aggregate subset for strongly consistent reads.
    const stale = await stateOf(await getState(hostA, 'w-7'));
    expect(stale.version).toBe(2);
    const afterConflict = await post(hostA, 'w-7', 'Credit', { amount: 1 });
    expect(afterConflict.status).toBe(409);
    const recovered = await stateOf(await getState(hostA, 'w-7'));
    expect(recovered).toMatchObject({ version: 3, balance: 105 });
  });
});

/**
 * Codex review (PR #354, thread r3840613381): when a publish fails AFTER the
 * KV state commit, evicting the actor is not enough — the replacement reloads
 * the already-advanced state (ensureStateLoaded trusts any version > 0), so
 * the aggregate stays one version ahead of its log and every later command
 * conflicts forever. The committed-but-unlogged state must be repaired.
 */
describe('divergence repair after a failed event publish', () => {
  it('rebuilds from the event log instead of trusting state the log never got', async () => {
    const kv = new FakeKv();
    const js = new FakeJetStream();
    const realPublish = js.publish.bind(js);
    let failNext = false;
    js.publish = (subject, payload, opts) => {
      if (failNext) {
        failNext = false;
        return Promise.reject(new Error('publish down'));
      }
      return realPublish(subject, payload, opts);
    };
    const namespace = makeNamespace(kv, js);

    await post(namespace, 'w-div', 'OpenWallet', { owner: 'ada', initial: 10 });
    await flush();

    // v2 commits to KV, then its publish fails: state says 2, the log says 1.
    failNext = true;
    const failed = await post(namespace, 'w-div', 'Credit', { amount: 5 });
    expect(failed.status).toBeGreaterThanOrEqual(400);

    // The aggregate must come back at the LOG's version, not the orphaned one.
    const state = await stateOf(await getState(namespace, 'w-div'));
    expect(state).toMatchObject({ balance: 10, version: 1 });

    // And it must still accept writes — the divergence is gone, not latent.
    const recovered = await post(namespace, 'w-div', 'Credit', { amount: 7 });
    expect(recovered.status).toBe(200);
    expect(await bodyOf(recovered)).toMatchObject({ version: 2 });

    await flush();
    const versions = js.messages
      .filter((m) => m.subject.endsWith('.w-div'))
      .map((m) => parseEnvelope(m.data).version);
    expect(versions).toEqual([1, 2]);
  });
});

describe('event-log durability at the actor boundary', () => {
  it('returns 409 and evicts when the log REJECTS the event after the state commit', async () => {
    const kv = new FakeKv();
    const failure = new VersionConflictError('log rejected the event', 1, 1);
    let pendingFailure: Error | null = null;
    const brokenStore = {
      save: (): Promise<void> => {
        pendingFailure = failure;
        return Promise.reject(failure);
      },
      load: (): Promise<never[]> => Promise.resolve([]),
      loadAll: (): Promise<never[]> => Promise.resolve([]),
      waitForPendingSaves: (): Promise<void> => {
        const error = pendingFailure;
        pendingFailure = null;
        return error ? Promise.reject(error) : Promise.resolve();
      },
    };
    const env: Record<string, unknown> = { DEFAULT_ORG_ID: 'org-test' };
    const namespace = new NatsAggregateNamespace({
      aggregateType: 'WalletAggregate',
      AggregateClass: WalletAggregate,
      kv: kv as unknown as KV,
      eventStore: brokenStore,
      getEnv: () => env,
    });

    const response = await post(namespace, 'w-log', 'OpenWallet', { owner: 'ada', initial: 1 });
    // A rejected append is the domain's own outcome, not a bug: 409, the
    // same status the conflict would carry had it surfaced directly.
    expect(response.status).toBe(409);
    expect(await bodyOf(response)).toMatchObject({
      success: false,
      errors: [{ code: 409 }],
    });

    // Documented core ordering: the state commit happened before the publish
    // failed — the 409 signals the divergence instead of acknowledging it,
    // and the orphaned snapshot is dropped so the rebuild replays the log
    // rather than trusting state the log never received.
    expect(kv.entries.has(`${storageKeyPrefixFor('WalletAggregate', 'w-log')}.state`)).toBe(false);

    // The actor was evicted and the orphaned state dropped, so the rebuild
    // reflects the LOG: this create never landed, so the aggregate does not
    // exist — rather than lingering as state no event supports.
    const after = await getState(namespace, 'w-log');
    expect(after.status).toBe(404);
  });

  it('returns 503 with Retry-After when the log is UNREACHABLE after the state commit', async () => {
    const kv = new FakeKv();
    const outage = new Error('connection refused');
    let pendingFailure: Error | null = null;
    const downStore = {
      save: (): Promise<void> => {
        pendingFailure = outage;
        return Promise.reject(outage);
      },
      load: (): Promise<never[]> => Promise.resolve([]),
      loadAll: (): Promise<never[]> => Promise.resolve([]),
      waitForPendingSaves: (): Promise<void> => {
        const error = pendingFailure;
        pendingFailure = null;
        return error ? Promise.reject(error) : Promise.resolve();
      },
    };
    const env: Record<string, unknown> = { DEFAULT_ORG_ID: 'org-test' };
    const namespace = new NatsAggregateNamespace({
      aggregateType: 'WalletAggregate',
      AggregateClass: WalletAggregate,
      kv: kv as unknown as KV,
      eventStore: downStore,
      getEnv: () => env,
    });

    const response = await post(namespace, 'w-down', 'OpenWallet', { owner: 'ada', initial: 1 });
    // A dependency being down is not a bug in the app — 503, retryable.
    expect(response.status).toBe(503);
    expect(response.headers.get('Retry-After')).toBe('2');
    expect(await bodyOf(response)).toMatchObject({ success: false, errors: [{ code: 503 }] });
  });
});

class RecordingProjector implements IEventProjector {
  readonly id = 'recording-projector';
  readonly events: ProjectedEvent[] = [];
  failuresLeft = 0;

  sendEvent(event: ProjectedEvent): Promise<void> {
    if (this.failuresLeft > 0) {
      this.failuresLeft -= 1;
      return Promise.reject(new Error('projection target down'));
    }
    this.events.push(event);
    return Promise.resolve();
  }

  getLastProjectedVersion(): Promise<number> {
    return Promise.resolve(0);
  }
}

describe('projections on the NATS runtime', () => {
  afterEach(() => {
    clearProjectors();
  });

  it('dispatches persisted events to registered projectors', async () => {
    const projector = new RecordingProjector();
    registerProjector(projector);

    const kv = new FakeKv();
    const js = new FakeJetStream();
    const namespace = makeNamespace(kv, js);

    await post(namespace, 'w-proj', 'OpenWallet', { owner: 'ada', initial: 3 });
    await post(namespace, 'w-proj', 'Credit', { amount: 4 });
    await namespace.drain(); // projection dispatch rides waitUntil

    expect(projector.events.map((e) => ({ type: e.type, version: e.version }))).toEqual([
      { type: 'WalletOpened', version: 1 },
      { type: 'WalletCredited', version: 2 },
    ]);
    expect(projector.events[0]).toMatchObject({
      aggregateType: 'WalletAggregate',
      aggregateId: 'w-proj',
      orgId: 'org-test',
    });

    // The projection cursor is persisted in the aggregate's KV storage.
    const cursorKey = [...kv.entries.keys()].find(
      (k) => k.startsWith('WalletAggregate.w-proj.') && k.includes('projection')
    );
    expect(cursorKey).toBeDefined();
  });

  it('a failing projector arms a persisted retry alarm', async () => {
    const projector = new RecordingProjector();
    projector.failuresLeft = 1;
    registerProjector(projector);

    const kv = new FakeKv();
    const js = new FakeJetStream();
    const namespace = makeNamespace(kv, js);

    await post(namespace, 'w-retry', 'OpenWallet', { owner: 'ada', initial: 1 });
    await namespace.drain();

    // The dispatch failure scheduled a retry alarm, persisted under the
    // aggregate's advisory alarm key so it survives eviction/restart.
    const alarmKey = `${storageKeyPrefixFor('WalletAggregate', 'w-retry')}.__alarm`;
    expect(kv.entries.has(alarmKey)).toBe(true);
  });
});
