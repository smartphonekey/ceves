/**
 * Live integration test against a real nats-server (JetStream enabled).
 *
 * Skipped unless NATS_TEST_URL is set, e.g.:
 *   NATS_TEST_URL=nats://localhost:4222 vitest run --project unit src/adapters/nats
 *
 * Purpose: validate the adapter's duck-typed assumptions against the real
 * @nats-io client and server — ordered-consumer option naming, MSG.GET
 * `last_by_subj` behavior, the 10071 wrong-last-sequence rejection shape,
 * KV revision CAS, and Nats-Msg-Id dedup.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { NatsEventStore } from '../NatsEventStore';
import { NatsKvStorage } from '../NatsKvStorage';
import { NatsOrgDirectory } from '../NatsOrgDirectory';
import { AGGREGATE_TRANSFERRED_OUT_EVENT } from '../transfer';
import { VersionConflictError } from '../../../errors/VersionConflictError';
import type { StoredEvent } from '../../../storage/interfaces';
import { storageKeyPrefixFor } from '../naming';

const url = process.env['NATS_TEST_URL'];

describe.skipIf(!url)('NatsEventStore against a live nats-server', () => {
  // Dynamic imports so the suite loads (and skips) without the transport installed.
  let nc: { drain(): Promise<void> };
  let jsClient: import('@nats-io/jetstream').JetStreamClient;
  let store: NatsEventStore;
  let makeStore: () => NatsEventStore;
  let makeStorage: (aggregateId: string) => NatsKvStorage;
  let makeDirectory: () => NatsOrgDirectory;
  const streamName = `CEVES_TEST_${Date.now()}`;
  const runId = `live-${Date.now()}`;

  beforeAll(async () => {
    const { connect } = await import('@nats-io/transport-node');
    const { jetstream, jetstreamManager } = await import('@nats-io/jetstream');
    const { Kvm } = await import('@nats-io/kv');

    const connection = await connect({ servers: url });
    nc = connection;
    const js = jetstream(connection);
    jsClient = js;
    const jsm = await jetstreamManager(connection);
    await jsm.streams.add({
      name: streamName,
      subjects: [`cevestest.${runId}.>`],
      allow_direct: true,
    });
    await jsm.streams.add({
      name: `${streamName}_ROUTED`,
      subjects: [`cevestestevt.${runId}.>`],
    });
    const kvm = new Kvm(js);
    const kv = await kvm.create(`cevestest_${Date.now()}`, { history: 1 });
    const dirKv = await kvm.create(`cevestestdir_${Date.now()}`, { history: 1 });
    makeDirectory = () => new NatsOrgDirectory(dirKv);

    makeStore = () =>
      new NatsEventStore(js, jsm, {
        streamName,
        subjectPrefix: `cevestest.${runId}`,
        routedSubjectPrefix: `cevestestevt.${runId}`,
      });
    makeStorage = (aggregateId: string) =>
      new NatsKvStorage(kv, storageKeyPrefixFor('LiveWallet', aggregateId));
    store = makeStore();
  }, 20000);

  afterAll(async () => {
    await nc?.drain();
  });

  const eventAt = (version: number, aggregateId = 'live-1'): StoredEvent => ({
    aggregateType: 'LiveWallet',
    aggregateId,
    version,
    type: 'Credited',
    timestamp: new Date().toISOString(),
    orgId: 'org-live',
    event: { type: 'Credited', amount: version } as StoredEvent['event'],
  });

  it('saves and loads a stream of events', async () => {
    await store.save(eventAt(1));
    await store.save(eventAt(2));
    await store.save(eventAt(3));

    const all = await makeStore().loadAll('LiveWallet', 'live-1');
    expect(all.map((e) => e.version)).toEqual([1, 2, 3]);

    const tail = await makeStore().load('LiveWallet', 'live-1', 1, 1);
    expect(tail.map((e) => e.version)).toEqual([2]);
  });

  it('rejects a conflicting same-version write from a stale writer with 409', async () => {
    await store.save(eventAt(1, 'live-2'));
    const stale = makeStore();
    await stale.loadAll('LiveWallet', 'live-2'); // warm cache at v1
    await store.save(eventAt(2, 'live-2'));

    await expect(
      stale.save({ ...eventAt(2, 'live-2'), event: { type: 'Credited', amount: 999 } as StoredEvent['event'] })
    ).rejects.toBeInstanceOf(VersionConflictError);
  });

  it('absorbs a byte-identical duplicate save (Nats-Msg-Id dedup)', async () => {
    const event = eventAt(1, 'live-3');
    await store.save(event);
    await store.save(event); // identical payload → duplicate ack, no second message
    const all = await makeStore().loadAll('LiveWallet', 'live-3');
    expect(all).toHaveLength(1);
  });

  it('returns empty for unknown aggregates', async () => {
    expect(await store.load('LiveWallet', 'live-none')).toEqual([]);
  });

  it('KV storage: state CAS works against the real bucket', async () => {
    const a = makeStorage('live-kv');
    const b = makeStorage('live-kv');

    await a.put('state', { id: 'live-kv', version: 1 });
    await b.get('state');
    await b.put('state', { id: 'live-kv', version: 2 });

    await expect(a.put('state', { id: 'live-kv', version: 2 })).rejects.toBeInstanceOf(
      VersionConflictError
    );
    expect(a.conflicted).toBe(true);

    // Re-observe and continue.
    await a.get('state');
    await a.put('state', { id: 'live-kv', version: 3 });
    expect(await b.get<{ version: number }>('state')).toMatchObject({ version: 3 });
  });

  it('routed fan-out: consumers filter one EVENT TYPE via subject wildcards, in order', async () => {
    await store.save(eventAt(1, 'live-routed'));
    await store.save({ ...eventAt(2, 'live-routed'), type: 'Debited' });
    await store.save(eventAt(3, 'live-routed'));

    const readRouted = async (filter: string): Promise<number[]> => {
      const consumer = await jsClient.consumers.get(`${streamName}_ROUTED`, {
        filter_subjects: [filter],
      });
      const versions: number[] = [];
      const info = await consumer.info(true);
      if (info.num_pending > 0) {
        const messages = await consumer.consume();
        for await (const m of messages) {
          const envelope: StoredEvent = m.json();
          versions.push(envelope.version);
          if (m.info.pending === 0) break;
        }
        messages.stop();
      }
      await consumer.delete().catch(() => undefined);
      return versions;
    };

    // Only the Credited events, still in per-aggregate order.
    expect(
      await readRouted(`cevestestevt.${runId}.org-live.LiveWallet.Credited.live-routed`)
    ).toEqual([1, 3]);
    // The aggregate's FULL history via a single-token wildcard on event type.
    expect(
      await readRouted(`cevestestevt.${runId}.org-live.LiveWallet.*.live-routed`)
    ).toEqual([1, 2, 3]);
  });

  it('home-org partitioning against the real server: claim, route, load, collide', async () => {
    const directory = makeDirectory();
    const partitioned = new NatsEventStore(jsClient, await (await import('@nats-io/jetstream')).jetstreamManager(nc as never), {
      streamName,
      subjectPrefix: `cevestest.${runId}`,
      orgDirectory: directory,
    });
    await partitioned.save(eventAt(1, 'live-dir'));
    await partitioned.save(eventAt(2, 'live-dir'));

    // The canonical stream carries the home-org token.
    const consumer = await jsClient.consumers.get(streamName, {
      filter_subjects: [`cevestest.${runId}.org-live.LiveWallet.live-dir`],
    });
    const info = await consumer.info(true);
    expect(info.num_pending).toBe(2);
    await consumer.delete().catch(() => undefined);

    // A cold store + cold directory resolves and replays without an org param.
    const cold = new NatsEventStore(jsClient, await (await import('@nats-io/jetstream')).jetstreamManager(nc as never), {
      streamName,
      subjectPrefix: `cevestest.${runId}`,
      orgDirectory: makeDirectory(),
    });
    expect((await cold.loadAll('LiveWallet', 'live-dir')).map((e) => e.version)).toEqual([1, 2]);

    // Same id under another org → global id lock rejects the create.
    await expect(
      cold.save({ ...eventAt(1, 'live-dir'), orgId: 'org-other' })
    ).rejects.toBeInstanceOf(VersionConflictError);
  });

  it('org transfer at the store level: seal guard, seed, and repoint against the real server', async () => {
    const { jetstreamManager } = await import('@nats-io/jetstream');
    const jsm = await jetstreamManager(nc as never);
    const options = { streamName, subjectPrefix: `cevestest.${runId}` };
    const directory = makeDirectory();
    const seller = new NatsEventStore(jsClient, jsm, { ...options, orgDirectory: directory });

    await seller.save(eventAt(1, 'live-sale'));
    await seller.save(eventAt(2, 'live-sale'));

    // Seal the old stream (what transferOut appends), retry included —
    // the byte-identical retry must be absorbed, not conflict.
    const seal: StoredEvent = {
      aggregateType: 'LiveWallet',
      aggregateId: 'live-sale',
      version: 3,
      type: AGGREGATE_TRANSFERRED_OUT_EVENT,
      timestamp: new Date().toISOString(),
      orgId: 'org-live',
      event: {
        type: AGGREGATE_TRANSFERRED_OUT_EVENT,
        fromOrg: 'org-live',
        toOrg: 'org-buyer',
      } as unknown as StoredEvent['event'],
    };
    await seller.saveToOrg(seal, 'org-live');
    await seller.saveToOrg(seal, 'org-live');

    // A cold process (fresh token cache) cannot append past the seal.
    const cold = new NatsEventStore(jsClient, jsm, {
      ...options,
      orgDirectory: makeDirectory(),
    });
    await expect(cold.save(eventAt(4, 'live-sale'))).rejects.toThrow(/sealed/);

    // Seed the buyer's stream with the original creation event, repoint.
    const original = (await seller.loadAll('LiveWallet', 'live-sale'))[0];
    if (original === undefined) throw new Error('expected the original creation event');
    await seller.saveToOrg({ ...original, orgId: 'org-buyer' }, 'org-buyer');
    await directory.transfer('LiveWallet', 'live-sale', 'org-live', 'org-buyer');

    // A cold store + cold directory now sees ONLY the fresh seeded stream;
    // the sealed history stays intact under the old org's subject.
    const buyer = new NatsEventStore(jsClient, jsm, {
      ...options,
      orgDirectory: makeDirectory(),
    });
    const fresh = await buyer.loadAll('LiveWallet', 'live-sale');
    expect(fresh.map((e) => [e.version, e.orgId])).toEqual([[1, 'org-buyer']]);
    expect(fresh[0]?.event).toEqual(original.event);

    const sealedConsumer = await jsClient.consumers.get(streamName, {
      filter_subjects: [`cevestest.${runId}.org-live.LiveWallet.live-sale`],
    });
    expect((await sealedConsumer.info(true)).num_pending).toBe(3);
    await sealedConsumer.delete().catch(() => undefined);
  }, 15000);

  it('org directory: a KV watch invalidates a cross-process cache after a transfer', async () => {
    const watching = makeDirectory();
    await watching.startWatching();
    const admin = makeDirectory();

    await admin.claim('LiveWallet', 'live-watch', 'org-a');
    expect(await watching.resolve('LiveWallet', 'live-watch')).toBe('org-a');

    await admin.transfer('LiveWallet', 'live-watch', 'org-a', 'org-b');
    // Watch delivery is asynchronous — poll briefly.
    let seen: string | null = null;
    for (let i = 0; i < 40; i++) {
      seen = await watching.resolve('LiveWallet', 'live-watch');
      if (seen === 'org-b') break;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    expect(seen).toBe('org-b');
    watching.stopWatching();
  }, 15000);

  it('KV storage: deleteAll clears only the aggregate prefix', async () => {
    const target = makeStorage('live-del');
    const neighbour = makeStorage('live-keep');
    await target.put('state', { version: 1 });
    await neighbour.put('state', { version: 1 });

    await target.deleteAll();
    expect(await target.get('state')).toBeUndefined();
    expect(await neighbour.get('state')).toBeDefined();
  });
});
