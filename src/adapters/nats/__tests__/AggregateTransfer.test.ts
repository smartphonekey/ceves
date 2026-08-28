/**
 * Org transfer (aggregate "sale") — fresh-start semantics with preserved
 * creation identity.
 *
 * Covers the full sale flow on the actor host (seal → purge → seed →
 * repoint → evict), the invariants that make it safe (sealed streams
 * reject appends even from stale processes; the buyer inherits the
 * creation-time identity — lock number/uuid — but none of the seller's
 * accumulated state), idempotent retries including resume after a
 * mid-transfer crash, re-sales carrying the ORIGINAL creation data
 * forward, the directory's CAS transfer + watch-based cache invalidation,
 * and the gateway re-routing commands to the new org after a transfer.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { z } from 'zod';
import type { KV } from '@nats-io/kv';
import type { JetStreamClient, JetStreamManager } from '@nats-io/jetstream';
import { Route } from '../../../routing/Route';
import { CommandRoute, CreateCommandRoute } from '../../../routing/CommandRoute';
import { EventHandler, type IEventHandler } from '../../../decorators';
import { AggregateObject, type AggregateObjectEnv } from '../../../core/AggregateObject';
import { BaseState } from '../../../schemas/State';
import type { EventMetadata } from '../../../events/EventMetadata';
import type { StoredEvent } from '../../../storage/interfaces';
import { VersionConflictError } from '../../../errors/VersionConflictError';
import { UnauthorizedError } from '../../../errors/UnauthorizedError';
import { NatsAggregateNamespace } from '../NatsAggregateNamespace';
import { NatsEventStore } from '../NatsEventStore';
import { NatsOrgDirectory } from '../NatsOrgDirectory';
import { NatsRequestReplyNamespace } from '../NatsRequestReplyNamespace';
import { AGGREGATE_TRANSFERRED_OUT_EVENT } from '../transfer';
import { FakeJetStream, FakeJetStreamManager, FakeKv, FakeNatsConnection } from './fakes';

// --- Test domain: a sellable Lock with identity fields ----------------------

class SaleLockState extends BaseState {
  lockNumber = 0;
  uuid = '';
  keys: string[] = [];
}

interface SaleLockCreatedEvent {
  type: 'SaleLockCreated';
  lockNumber: number;
  uuid: string;
}

interface SaleKeyAddedEvent {
  type: 'SaleKeyAdded';
  keyUuid: string;
}

interface SaleLockOrgSetEvent {
  type: 'SaleLockOrgSet';
  orgId: string;
}

@Route({ method: 'POST', path: '/salelocks/:id/CreateSaleLock' })
class CreateSaleLockRoute extends CreateCommandRoute<
  { lockNumber: number; uuid: string },
  SaleLockState,
  SaleLockCreatedEvent
> {
  static readonly eventSchema = z.object({ lockNumber: z.number(), uuid: z.string() });
  aggregateType = 'SaleLockAggregate';
  schema = {};

  executeCommand(command: { lockNumber: number; uuid: string }): Promise<SaleLockCreatedEvent> {
    return Promise.resolve({
      type: 'SaleLockCreated',
      lockNumber: command.lockNumber,
      uuid: command.uuid,
    });
  }
}

@Route({ method: 'POST', path: '/salelocks/:id/AddSaleKey' })
class AddSaleKeyRoute extends CommandRoute<{ keyUuid: string }, SaleLockState, SaleKeyAddedEvent> {
  aggregateType = 'SaleLockAggregate';
  schema = {};

  executeCommand(command: { keyUuid: string }): Promise<SaleKeyAddedEvent> {
    return Promise.resolve({ type: 'SaleKeyAdded', keyUuid: command.keyUuid });
  }
}

/**
 * The org-change endpoint — the production `SetOrganization` shape. On the
 * NATS runtime, committing its event IS the sale: the host's
 * `orgTransferOn: ['SaleLockOrgSet']` trigger runs the full transfer to
 * the new org before this command's response is returned.
 */
@Route({ method: 'POST', path: '/salelocks/:id/SetSaleLockOrg' })
class SetSaleLockOrgRoute extends CommandRoute<{ orgId: string }, SaleLockState, SaleLockOrgSetEvent> {
  aggregateType = 'SaleLockAggregate';
  schema = {};

  executeCommand(command: { orgId: string }): Promise<SaleLockOrgSetEvent> {
    return Promise.resolve({ type: 'SaleLockOrgSet', orgId: command.orgId });
  }
}

@EventHandler
class SaleLockCreatedHandler implements IEventHandler<SaleLockState, SaleLockCreatedEvent> {
  eventType = 'SaleLockCreated';
  aggregateType = 'SaleLockAggregate';

  apply(state: SaleLockState, event: SaleLockCreatedEvent, metadata: EventMetadata): SaleLockState {
    return {
      ...state,
      id: metadata.aggregateId,
      orgId: metadata.orgId,
      lockNumber: event.lockNumber,
      uuid: event.uuid,
      keys: [],
    };
  }
}

@EventHandler
class SaleKeyAddedHandler implements IEventHandler<SaleLockState, SaleKeyAddedEvent> {
  eventType = 'SaleKeyAdded';
  aggregateType = 'SaleLockAggregate';

  apply(state: SaleLockState, event: SaleKeyAddedEvent): SaleLockState {
    return { ...state, keys: [...state.keys, event.keyUuid] };
  }
}

@EventHandler
class SaleLockOrgSetHandler implements IEventHandler<SaleLockState, SaleLockOrgSetEvent> {
  eventType = 'SaleLockOrgSet';
  aggregateType = 'SaleLockAggregate';

  apply(state: SaleLockState, event: SaleLockOrgSetEvent): SaleLockState {
    // Mirrors the production OrganizationSetHandler: the new org lands in
    // state.orgId; the org-transfer trigger reads it from the event DATA's
    // `orgId` field (the envelope org is stamped pre-event = the seller).
    return { ...state, orgId: event.orgId };
  }
}

class SaleLockAggregate extends AggregateObject<SaleLockState> {
  constructor(ctx: DurableObjectState, env: AggregateObjectEnv) {
    super(ctx, env, SaleLockState);
  }

  /**
   * The sale/claim authorization rule: only the CURRENT org may command
   * an owned lock (selling included); an unowned lock (no org in state —
   * e.g. factory-provisioned) can be claimed by anyone, and creates pass
   * because there is no state yet. Reads stay open for the test's
   * `__state` inspection.
   */
  protected override checkAuthorization(request: Request): void {
    if (new URL(request.url).pathname.endsWith('/__state')) return;
    const currentOrg = this.state?.orgId;
    if (currentOrg && request.headers.get('X-Org-Id') !== currentOrg) {
      throw new UnauthorizedError('Only the lock’s current org may run commands on it');
    }
  }
}

// --- Harness ----------------------------------------------------------------

const TYPE = 'SaleLockAggregate';
const OLD_SUBJECT = 'ceves.events.acme.SaleLockAggregate.lock-1';
const NEW_SUBJECT = 'ceves.events.globex.SaleLockAggregate.lock-1';

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

interface StoredEnvelope {
  aggregateType: string;
  aggregateId: string;
  version: number;
  type: string;
  timestamp: string;
  orgId: string;
  event: Record<string, unknown>;
}

function parseEnvelope(data: string): StoredEnvelope {
  const parsed: StoredEnvelope = JSON.parse(data);
  return parsed;
}

interface Harness {
  namespace: NatsAggregateNamespace;
  store: NatsEventStore;
  directory: NatsOrgDirectory;
}

function makeHarness(kv: FakeKv, js: FakeJetStream): Harness {
  const directory = new NatsOrgDirectory(kv as unknown as KV);
  const store = new NatsEventStore(
    js as unknown as JetStreamClient,
    new FakeJetStreamManager(js) as unknown as JetStreamManager,
    { orgDirectory: directory, routedSubjectPrefix: 'ceves.evt' }
  );
  const env: Record<string, unknown> = { DEFAULT_ORG_ID: 'default-org' };
  const namespace = new NatsAggregateNamespace({
    aggregateType: TYPE,
    AggregateClass: SaleLockAggregate,
    kv: kv as unknown as KV,
    eventStore: store,
    getEnv: () => env,
    orgDirectory: directory,
    // The org-change endpoint IS the sale: committing SaleLockOrgSet runs
    // the full transfer to the event's new org.
    orgTransferOn: ['SaleLockOrgSet'],
  });
  return { namespace, store, directory };
}

function post(
  namespace: NatsAggregateNamespace,
  id: string,
  action: string,
  body: unknown,
  orgId: string
) {
  return namespace.get(namespace.idFromName(id)).fetch(
    new Request(`https://aggregate.local/salelocks/${id}/${action}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Org-Id': orgId },
      body: JSON.stringify(body),
    })
  );
}

async function stateOf(namespace: NatsAggregateNamespace, id: string): Promise<SaleLockState> {
  const response = await namespace
    .get(namespace.idFromName(id))
    .fetch(new Request(`https://aggregate.local/salelocks/${id}/__state`, { method: 'GET' }));
  expect(response.status).toBe(200);
  const state: SaleLockState = await response.json();
  return state;
}

/** Sell lock-1: created under acme (v1), one key added (v2). */
async function sellSetup(harness: Harness): Promise<void> {
  const created = await post(
    harness.namespace,
    'lock-1',
    'CreateSaleLock',
    { lockNumber: 42, uuid: 'abc-123' },
    'acme'
  );
  expect(created.status).toBe(201);
  const keyed = await post(harness.namespace, 'lock-1', 'AddSaleKey', { keyUuid: 'key-1' }, 'acme');
  expect(keyed.status).toBe(200);
  await flush();
}

describe('org transfer — selling an aggregate', () => {
  it('registers the test routes and handlers via decorators', () => {
    expect(
      [
        CreateSaleLockRoute,
        AddSaleKeyRoute,
        SetSaleLockOrgRoute,
        SaleLockCreatedHandler,
        SaleKeyAddedHandler,
        SaleLockOrgSetHandler,
      ].map((c) => c.name)
    ).toEqual([
      'CreateSaleLockRoute',
      'AddSaleKeyRoute',
      'SetSaleLockOrgRoute',
      'SaleLockCreatedHandler',
      'SaleKeyAddedHandler',
      'SaleLockOrgSetHandler',
    ]);
  });

  let kv: FakeKv;
  let js: FakeJetStream;
  let harness: Harness;

  beforeEach(() => {
    kv = new FakeKv();
    js = new FakeJetStream();
    harness = makeHarness(kv, js);
  });

  it('runs the full sale: seal, purge, seed with the original identity, repoint', async () => {
    await sellSetup(harness);

    const summary = await harness.namespace.transferOut('lock-1', 'globex');
    expect(summary).toEqual({
      aggregateType: TYPE,
      aggregateId: 'lock-1',
      fromOrg: 'acme',
      toOrg: 'globex',
      sealedVersion: 3,
      seeded: true,
      alreadyTransferred: false,
    });

    // The old stream stays intact and is closed by the seal audit event.
    const oldStream = js.messages
      .filter((m) => m.subject === OLD_SUBJECT)
      .map((m) => parseEnvelope(m.data));
    expect(oldStream.map((e) => [e.version, e.type])).toEqual([
      [1, 'SaleLockCreated'],
      [2, 'SaleKeyAdded'],
      [3, AGGREGATE_TRANSFERRED_OUT_EVENT],
    ]);
    expect(oldStream[2]).toMatchObject({
      orgId: 'acme',
      event: { fromOrg: 'acme', toOrg: 'globex' },
    });

    // The new stream is seeded with the ORIGINAL creation event: same type,
    // same domain data, same original timestamp — only the org differs.
    const newStream = js.messages
      .filter((m) => m.subject === NEW_SUBJECT)
      .map((m) => parseEnvelope(m.data));
    expect(newStream).toHaveLength(1);
    const original = oldStream[0];
    expect(newStream[0]).toMatchObject({
      version: 1,
      type: 'SaleLockCreated',
      orgId: 'globex',
      event: { lockNumber: 42, uuid: 'abc-123' },
    });
    expect(newStream[0]?.timestamp).toBe(original?.timestamp);
    expect(newStream[0]?.event).toEqual(original?.event);

    // Directory repointed; the seller's KV state is gone.
    expect(await harness.directory.resolve(TYPE, 'lock-1')).toBe('globex');
    expect([...kv.entries.keys()].filter((k) => k.startsWith('SaleLockAggregate.lock-1.'))).toEqual(
      []
    );

    // The seal is fanned out to the SELLER's routed feed, the seed to the
    // buyer's — each org's projections see their side of the sale.
    const routed = js.messages.filter((m) => m.subject.startsWith('ceves.evt.'));
    expect(routed.map((m) => m.subject)).toContain(
      `ceves.evt.acme.SaleLockAggregate.${AGGREGATE_TRANSFERRED_OUT_EVENT}.lock-1`
    );
    expect(routed.map((m) => m.subject)).toContain(
      'ceves.evt.globex.SaleLockAggregate.SaleLockCreated.lock-1'
    );

    // The buyer sees the lock with its creation identity and NO seller keys…
    const state = await stateOf(harness.namespace, 'lock-1');
    expect(state).toMatchObject({
      lockNumber: 42,
      uuid: 'abc-123',
      keys: [],
      orgId: 'globex',
      version: 1,
    });

    // …and continues the fresh stream from version 2.
    const buyerKey = await post(
      harness.namespace,
      'lock-1',
      'AddSaleKey',
      { keyUuid: 'buyer-key' },
      'globex'
    );
    expect(buyerKey.status).toBe(200);
    await flush();
    const afterBuyer = js.messages.filter((m) => m.subject === NEW_SUBJECT);
    expect(afterBuyer.map((m) => parseEnvelope(m.data).version)).toEqual([1, 2]);
  });

  it('the org-change endpoint IS the sale: committing the event runs the transfer', async () => {
    await sellSetup(harness);

    const sale = await post(
      harness.namespace,
      'lock-1',
      'SetSaleLockOrg',
      { orgId: 'globex' },
      'acme'
    );
    expect(sale.status).toBe(200);

    // Old stream: the org-change event is the seller's last business
    // event, immediately closed by the seal — all within the one request.
    const oldStream = js.messages
      .filter((m) => m.subject === OLD_SUBJECT)
      .map((m) => parseEnvelope(m.data));
    expect(oldStream.map((e) => [e.version, e.type])).toEqual([
      [1, 'SaleLockCreated'],
      [2, 'SaleKeyAdded'],
      [3, 'SaleLockOrgSet'],
      [4, AGGREGATE_TRANSFERRED_OUT_EVENT],
    ]);

    // Directory repointed; the buyer sees the creation identity, no keys.
    expect(await harness.directory.resolve(TYPE, 'lock-1')).toBe('globex');
    const state = await stateOf(harness.namespace, 'lock-1');
    expect(state).toMatchObject({
      lockNumber: 42,
      uuid: 'abc-123',
      keys: [],
      orgId: 'globex',
      version: 1,
    });

    // Ownership followed the sale: the buyer commands, the seller cannot.
    const buyer = await post(harness.namespace, 'lock-1', 'AddSaleKey', { keyUuid: 'bk' }, 'globex');
    expect(buyer.status).toBe(200);
    const seller = await post(harness.namespace, 'lock-1', 'AddSaleKey', { keyUuid: 'sk' }, 'acme');
    expect(seller.status).toBe(401);
  });

  it('only the current org may sell: a rival org-change is rejected untouched', async () => {
    await sellSetup(harness);
    const rejected = await post(
      harness.namespace,
      'lock-1',
      'SetSaleLockOrg',
      { orgId: 'rival' },
      'rival'
    );
    expect(rejected.status).toBe(401);
    expect(await harness.directory.resolve(TYPE, 'lock-1')).toBe('acme');
    expect(js.messages.filter((m) => m.subject === OLD_SUBJECT)).toHaveLength(2);
  });

  it('setting the org the lock already lives under is not a sale', async () => {
    await sellSetup(harness);
    const same = await post(
      harness.namespace,
      'lock-1',
      'SetSaleLockOrg',
      { orgId: 'acme' },
      'acme'
    );
    expect(same.status).toBe(200);
    await flush();

    expect(await harness.directory.resolve(TYPE, 'lock-1')).toBe('acme');
    const types = js.messages
      .filter((m) => m.subject === OLD_SUBJECT)
      .map((m) => parseEnvelope(m.data).type);
    expect(types).toEqual(['SaleLockCreated', 'SaleKeyAdded', 'SaleLockOrgSet']); // no seal
    const state = await stateOf(harness.namespace, 'lock-1');
    expect(state).toMatchObject({ keys: ['key-1'], orgId: 'acme', version: 3 });
  });

  it('keeps the command successful when the storage move fails, and finishes it next request', async () => {
    await sellSetup(harness);

    // The move's first write (the seal) fails — a NATS blip right after
    // the org change committed.
    const realSaveToOrg = harness.store.saveToOrg.bind(harness.store);
    let failNext = true;
    (harness.store as { saveToOrg: NatsEventStore['saveToOrg'] }).saveToOrg = (event, org) => {
      if (failNext) {
        failNext = false;
        return Promise.reject(new Error('NATS unavailable'));
      }
      return realSaveToOrg(event, org);
    };

    // The endpoint still succeeds: the org change IS applied — in state,
    // in the log, and in authorization. Which partition holds the
    // aggregate is this runtime's internal business, never the caller's
    // error to handle.
    const sale = await post(
      harness.namespace,
      'lock-1',
      'SetSaleLockOrg',
      { orgId: 'globex' },
      'acme'
    );
    expect(sale.status).toBe(200);
    expect(await sale.json()).toMatchObject({ success: true, version: 3 });

    // The move genuinely did not happen yet.
    expect(await harness.directory.resolveFresh(TYPE, 'lock-1')).toBe('acme');
    expect(js.messages.filter((m) => m.subject === NEW_SUBJECT)).toHaveLength(0);

    // The next request completes it first, then runs against the buyer's
    // fresh aggregate — no operator action, no stuck lock.
    const buyerKey = await post(
      harness.namespace,
      'lock-1',
      'AddSaleKey',
      { keyUuid: 'buyer-key' },
      'globex'
    );
    expect(buyerKey.status).toBe(200);
    expect(await buyerKey.json()).toMatchObject({ version: 2 });

    expect(await harness.directory.resolveFresh(TYPE, 'lock-1')).toBe('globex');
    const oldStream = js.messages
      .filter((m) => m.subject === OLD_SUBJECT)
      .map((m) => parseEnvelope(m.data).type);
    expect(oldStream).toEqual([
      'SaleLockCreated',
      'SaleKeyAdded',
      'SaleLockOrgSet',
      AGGREGATE_TRANSFERRED_OUT_EVENT,
    ]);
    const state = await stateOf(harness.namespace, 'lock-1');
    expect(state).toMatchObject({ lockNumber: 42, uuid: 'abc-123', keys: ['buyer-key'] });
  });

  it('another host finishes a transfer left incomplete, instead of wedging on the seal', async () => {
    await sellSetup(harness);
    // A transfer that sealed the old stream and then died (the process
    // holding it crashed) — no repoint, no seed.
    await harness.store.saveToOrg(
      {
        aggregateType: TYPE,
        aggregateId: 'lock-1',
        version: 3,
        type: AGGREGATE_TRANSFERRED_OUT_EVENT,
        timestamp: new Date().toISOString(),
        orgId: 'acme',
        event: {
          type: AGGREGATE_TRANSFERRED_OUT_EVENT,
          fromOrg: 'acme',
          toOrg: 'globex',
        } as unknown as StoredEvent['event'],
      },
      'acme'
    );

    // A DIFFERENT host takes a write. It can't append (sealed), so the
    // caller gets a conflict — not a 500 — and the host completes the
    // transfer the dead one started, using the target org in the seal.
    const other = makeHarness(kv, js);
    const blocked = await post(other.namespace, 'lock-1', 'AddSaleKey', { keyUuid: 'x' }, 'acme');
    expect(blocked.status).toBe(409);

    expect(await other.directory.resolveFresh(TYPE, 'lock-1')).toBe('globex');
    expect(js.messages.filter((m) => m.subject === NEW_SUBJECT)).toHaveLength(1);

    // The buyer's aggregate is live and clean on any host.
    const buyerKey = await post(other.namespace, 'lock-1', 'AddSaleKey', { keyUuid: 'bk' }, 'globex');
    expect(buyerKey.status).toBe(200);
    const state = await stateOf(other.namespace, 'lock-1');
    expect(state).toMatchObject({ lockNumber: 42, uuid: 'abc-123', keys: ['bk'], orgId: 'globex' });
  });

  it('a sealed stream rejects a stale process’s write with a version conflict', async () => {
    await sellSetup(harness);

    // A second host warms up on the pre-sale state (stale directory cache
    // AND stale actor — the realistic multi-instance scenario).
    const staleHost = makeHarness(kv, js);
    expect((await stateOf(staleHost.namespace, 'lock-1')).version).toBe(2);

    await harness.namespace.transferOut('lock-1', 'globex');

    // The stale host still routes to the old stream — its write must NOT
    // land after the seal, whatever its cached token says.
    const rejected = await post(
      staleHost.namespace,
      'lock-1',
      'AddSaleKey',
      { keyUuid: 'sneaky' },
      'acme'
    );
    expect(rejected.status).toBe(409);
    const oldStream = js.messages.filter((m) => m.subject === OLD_SUBJECT);
    expect(oldStream.map((m) => parseEnvelope(m.data).type)).toEqual([
      'SaleLockCreated',
      'SaleKeyAdded',
      AGGREGATE_TRANSFERRED_OUT_EVENT,
    ]);
  });

  it('a cold event store refuses to append to a sealed stream', async () => {
    await sellSetup(harness);
    await harness.namespace.transferOut('lock-1', 'globex');

    // Simulate a process that never saw the transfer trying a direct save
    // to the old partition (fresh token cache — it must fetch and see the
    // seal).
    const coldStore = makeHarness(kv, js).store;
    await expect(
      coldStore.saveToOrg(
        {
          aggregateType: TYPE,
          aggregateId: 'lock-1',
          version: 4,
          type: 'SaleKeyAdded',
          timestamp: new Date().toISOString(),
          orgId: 'acme',
          event: { type: 'SaleKeyAdded', keyUuid: 'late' } as unknown as StoredEvent['event'],
        },
        'acme'
      )
    ).rejects.toThrow(/sealed/);
  });

  it('retrying a completed transfer is a no-op (alreadyTransferred)', async () => {
    await sellSetup(harness);
    await harness.namespace.transferOut('lock-1', 'globex');
    const messagesAfterFirst = js.messages.length;

    const retry = await harness.namespace.transferOut('lock-1', 'globex');
    expect(retry).toMatchObject({ fromOrg: 'globex', toOrg: 'globex', alreadyTransferred: true });
    expect(js.messages.length).toBe(messagesAfterFirst);
    expect(await harness.directory.resolve(TYPE, 'lock-1')).toBe('globex');
  });

  it('resumes a transfer that crashed between the seal and the repoint', async () => {
    await sellSetup(harness);

    // Simulate the crash: the seal landed, nothing else happened.
    await harness.store.saveToOrg(
      {
        aggregateType: TYPE,
        aggregateId: 'lock-1',
        version: 3,
        type: AGGREGATE_TRANSFERRED_OUT_EVENT,
        timestamp: new Date().toISOString(),
        orgId: 'acme',
        event: {
          type: AGGREGATE_TRANSFERRED_OUT_EVENT,
          fromOrg: 'acme',
          toOrg: 'globex',
        } as unknown as StoredEvent['event'],
      },
      'acme'
    );

    // The retry (possibly from a fresh process) completes the transfer
    // without sealing twice.
    const resumed = await makeHarness(kv, js).namespace.transferOut('lock-1', 'globex');
    expect(resumed).toMatchObject({
      fromOrg: 'acme',
      toOrg: 'globex',
      sealedVersion: 3,
      seeded: true,
      alreadyTransferred: false,
    });
    const seals = js.messages
      .filter((m) => m.subject === OLD_SUBJECT)
      .map((m) => parseEnvelope(m.data))
      .filter((e) => e.type === AGGREGATE_TRANSFERRED_OUT_EVENT);
    expect(seals).toHaveLength(1);
    expect(await harness.directory.resolveFresh(TYPE, 'lock-1')).toBe('globex');
    expect(js.messages.filter((m) => m.subject === NEW_SUBJECT)).toHaveLength(1);
  });

  it('refuses to transfer toward a different org while an unfinished transfer holds the seal', async () => {
    await sellSetup(harness);
    await harness.store.saveToOrg(
      {
        aggregateType: TYPE,
        aggregateId: 'lock-1',
        version: 3,
        type: AGGREGATE_TRANSFERRED_OUT_EVENT,
        timestamp: new Date().toISOString(),
        orgId: 'acme',
        event: {
          type: AGGREGATE_TRANSFERRED_OUT_EVENT,
          fromOrg: 'acme',
          toOrg: 'globex',
        } as unknown as StoredEvent['event'],
      },
      'acme'
    );

    await expect(harness.namespace.transferOut('lock-1', 'hooli')).rejects.toThrow(
      /unfinished transfer to "globex"/
    );
  });

  it('a re-sale carries the ORIGINAL creation data forward', async () => {
    await sellSetup(harness);
    await harness.namespace.transferOut('lock-1', 'globex');
    await post(harness.namespace, 'lock-1', 'AddSaleKey', { keyUuid: 'globex-key' }, 'globex');
    await flush();

    const resold = await harness.namespace.transferOut('lock-1', 'hooli');
    expect(resold).toMatchObject({ fromOrg: 'globex', toOrg: 'hooli', seeded: true });

    const hooliStream = js.messages
      .filter((m) => m.subject === 'ceves.events.hooli.SaleLockAggregate.lock-1')
      .map((m) => parseEnvelope(m.data));
    expect(hooliStream).toHaveLength(1);
    expect(hooliStream[0]).toMatchObject({
      version: 1,
      type: 'SaleLockCreated',
      event: { lockNumber: 42, uuid: 'abc-123' },
    });

    const state = await stateOf(harness.namespace, 'lock-1');
    expect(state).toMatchObject({ lockNumber: 42, uuid: 'abc-123', keys: [], orgId: 'hooli' });
  });

  it('rejects transferring an aggregate that has never been created', async () => {
    await expect(harness.namespace.transferOut('ghost', 'globex')).rejects.toThrow(
      /never been created/
    );
  });

  it('rejects transferOut without an org directory', async () => {
    const store = new NatsEventStore(
      js as unknown as JetStreamClient,
      new FakeJetStreamManager(js) as unknown as JetStreamManager
    );
    const env: Record<string, unknown> = {};
    const bare = new NatsAggregateNamespace({
      aggregateType: TYPE,
      AggregateClass: SaleLockAggregate,
      kv: kv as unknown as KV,
      eventStore: store,
      getEnv: () => env,
    });
    await expect(bare.transferOut('lock-1', 'globex')).rejects.toThrow(/org directory/);
  });
});

describe('NatsOrgDirectory.transfer', () => {
  let kv: FakeKv;
  let directory: NatsOrgDirectory;

  beforeEach(() => {
    kv = new FakeKv();
    directory = new NatsOrgDirectory(kv as unknown as KV);
  });

  it('repoints the entry with CAS and reports idempotent retries', async () => {
    await directory.claim(TYPE, 'lock-1', 'acme');
    expect(await directory.transfer(TYPE, 'lock-1', 'acme', 'globex')).toBe('transferred');
    expect(await directory.resolve(TYPE, 'lock-1')).toBe('globex');
    expect(await directory.transfer(TYPE, 'lock-1', 'acme', 'globex')).toBe('already-transferred');
  });

  it('refuses when the entry is missing or under an unexpected org', async () => {
    await expect(directory.transfer(TYPE, 'ghost', 'acme', 'globex')).rejects.toThrow(
      /no org-directory entry/
    );
    await directory.claim(TYPE, 'lock-1', 'acme');
    await expect(directory.transfer(TYPE, 'lock-1', 'rival', 'globex')).rejects.toThrow(
      /entry is "acme"/
    );
  });

  it('surfaces a concurrent CAS loser unless the winner made the same move', async () => {
    await directory.claim(TYPE, 'lock-1', 'acme');
    // Another process repoints to hooli between our read and update.
    const original = kv.update.bind(kv);
    kv.update = async (key: string, value: string, rev: number) => {
      kv.update = original;
      await original(key, 'hooli', rev); // the concurrent transfer wins
      return original(key, value, rev); // our CAS is now stale → rejects
    };
    await expect(directory.transfer(TYPE, 'lock-1', 'acme', 'globex')).rejects.toBeInstanceOf(
      VersionConflictError
    );
  });
});

describe('directory cache invalidation via KV watch', () => {
  it('a watching directory sees a transfer performed elsewhere; a non-watching one goes stale', async () => {
    const kv = new FakeKv();
    const watching = new NatsOrgDirectory(kv as unknown as KV);
    const stale = new NatsOrgDirectory(kv as unknown as KV);
    const admin = new NatsOrgDirectory(kv as unknown as KV);
    await watching.startWatching();

    await admin.claim(TYPE, 'lock-1', 'acme');
    await flush();
    // Both warm their caches on the pre-sale org.
    expect(await watching.resolve(TYPE, 'lock-1')).toBe('acme');
    expect(await stale.resolve(TYPE, 'lock-1')).toBe('acme');

    await admin.transfer(TYPE, 'lock-1', 'acme', 'globex');
    await flush();

    // The watch invalidated the cache; the cache-only directory is stale —
    // the documented reason every long-lived process should watch.
    expect(await watching.resolve(TYPE, 'lock-1')).toBe('globex');
    expect(await stale.resolve(TYPE, 'lock-1')).toBe('acme');

    // Stopping the watch clears the cache: resolves re-read KV.
    watching.stopWatching();
    await flush();
    expect(await watching.resolve(TYPE, 'lock-1')).toBe('globex');
  });

  it('the gateway re-routes commands to the new org after a transfer', async () => {
    const nats = new FakeNatsConnection();
    const kv = new FakeKv();
    const gatewayDirectory = new NatsOrgDirectory(kv as unknown as KV);
    await gatewayDirectory.startWatching();
    const adminDirectory = new NatsOrgDirectory(kv as unknown as KV);
    await adminDirectory.claim(TYPE, 'lock-1', 'acme');
    await flush();

    const seen: string[] = [];
    const originalRequest = nats.request.bind(nats);
    nats.request = (subject, payload, opts) => {
      seen.push(subject);
      return originalRequest(subject, payload, opts);
    };
    const sub = nats.subscribe('ceves.cmd.>');
    void (async () => {
      for await (const msg of sub) {
        msg.respond(JSON.stringify({ status: 200, headers: {}, body: '{}' }));
      }
    })();

    const gateway = new NatsRequestReplyNamespace({
      connection: nats,
      aggregateType: TYPE,
      subjectPrefix: 'ceves.cmd',
      timeoutMillis: 1000,
      orgDirectory: gatewayDirectory,
    });
    const send = () =>
      gateway.get(gateway.idFromName('lock-1')).fetch(
        new Request('https://gw.local/salelocks/lock-1/AddSaleKey', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
        })
      );

    await send();
    expect(seen[0]).toBe('ceves.cmd.acme.SaleLockAggregate.lock-1');

    await adminDirectory.transfer(TYPE, 'lock-1', 'acme', 'globex');
    await flush();

    await send();
    expect(seen[1]).toBe('ceves.cmd.globex.SaleLockAggregate.lock-1');

    await sub.drain();
  });
});
