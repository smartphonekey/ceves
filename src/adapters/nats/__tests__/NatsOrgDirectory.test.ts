/**
 * Home-org directory + home-org-partitioned canonical event log.
 *
 * Covers the directory itself (CAS claim = global id lock, cached
 * entries), the event store in directory mode (org-partitioned subjects
 * on save/load, cross-org id collisions, the home org staying put under a
 * SetOrganization-style current-org change — it moves only via the
 * explicit org transfer, see AggregateTransfer.test.ts), and the
 * gateway's directory-first routing ladder — including the B2C case: a
 * caller with NO tenant claim still reaches an org-partitioned aggregate.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import type { KV } from '@nats-io/kv';
import type { JetStreamClient, JetStreamManager } from '@nats-io/jetstream';
import { NatsOrgDirectory } from '../NatsOrgDirectory';
import { NatsEventStore } from '../NatsEventStore';
import { NatsRequestReplyNamespace } from '../NatsRequestReplyNamespace';
import { VersionConflictError } from '../../../errors/VersionConflictError';
import type { StoredEvent } from '../../../storage/interfaces';
import { FakeJetStream, FakeJetStreamManager, FakeKv, FakeNatsConnection } from './fakes';

function eventAt(
  version: number,
  orgId: string,
  overrides: Partial<StoredEvent> = {}
): StoredEvent {
  return {
    aggregateType: 'LockAggregate',
    aggregateId: 'lock-1',
    version,
    type: 'KeyAdded',
    timestamp: new Date(2026, 0, version).toISOString(),
    orgId,
    event: { type: 'KeyAdded', n: version } as unknown as StoredEvent['event'],
    ...overrides,
  };
}

describe('NatsOrgDirectory', () => {
  let kv: FakeKv;
  let directory: NatsOrgDirectory;

  beforeEach(() => {
    kv = new FakeKv();
    directory = new NatsOrgDirectory(kv as unknown as KV);
  });

  it('resolves null for unknown aggregates', async () => {
    expect(await directory.resolve('LockAggregate', 'nope')).toBeNull();
  });

  it('claim mints the home org exactly once; later claims get the original', async () => {
    expect(await directory.claim('LockAggregate', 'lock-1', 'acme')).toBe('acme');
    // A second claim under another org loses — the stored org wins.
    expect(await directory.claim('LockAggregate', 'lock-1', 'rival')).toBe('acme');
    expect(await directory.resolve('LockAggregate', 'lock-1')).toBe('acme');
  });

  it('a cross-process directory sees the same entry (no cache leakage)', async () => {
    await directory.claim('LockAggregate', 'lock-1', 'acme');
    const other = new NatsOrgDirectory(kv as unknown as KV);
    expect(await other.resolve('LockAggregate', 'lock-1')).toBe('acme');
    expect(await other.claim('LockAggregate', 'lock-1', 'rival')).toBe('acme');
  });

  it('keys are scoped per aggregate type', async () => {
    await directory.claim('LockAggregate', 'x', 'acme');
    expect(await directory.resolve('UserAggregate', 'x')).toBeNull();
  });
});

describe('NatsEventStore in home-org-partitioned mode', () => {
  let js: FakeJetStream;
  let kv: FakeKv;

  function makeStore(): NatsEventStore {
    return new NatsEventStore(
      js as unknown as JetStreamClient,
      new FakeJetStreamManager(js) as unknown as JetStreamManager,
      {
        orgDirectory: new NatsOrgDirectory(kv as unknown as KV),
        routedSubjectPrefix: 'ceves.evt',
      }
    );
  }

  beforeEach(() => {
    js = new FakeJetStream();
    kv = new FakeKv();
  });

  it('partitions canonical subjects by home org and loads through the directory', async () => {
    const store = makeStore();
    await store.save(eventAt(1, 'acme'));
    await store.save(eventAt(2, 'acme'));

    const canonical = js.messages.filter((m) => m.subject.startsWith('ceves.events.'));
    expect(canonical.map((m) => m.subject)).toEqual([
      'ceves.events.acme.LockAggregate.lock-1',
      'ceves.events.acme.LockAggregate.lock-1',
    ]);

    // A cold store (fresh cache) finds the stream via the directory —
    // load(type, id) needs no org parameter.
    const events = await makeStore().loadAll('LockAggregate', 'lock-1');
    expect(events.map((e) => e.version)).toEqual([1, 2]);
  });

  it('returns empty for never-created aggregates without touching the stream', async () => {
    expect(await makeStore().load('LockAggregate', 'ghost')).toEqual([]);
  });

  it('rejects creating the same aggregate id under a second org (global id lock)', async () => {
    const store = makeStore();
    await store.save(eventAt(1, 'acme'));
    await expect(
      makeStore().save(eventAt(1, 'rival', { event: { type: 'KeyAdded', n: 99 } as never }))
    ).rejects.toBeInstanceOf(VersionConflictError);
    expect(js.messages.filter((m) => m.subject.startsWith('ceves.events.'))).toHaveLength(1);
  });

  it('rejects a v>1 save with no directory entry (state/directory divergence)', async () => {
    await expect(makeStore().save(eventAt(2, 'acme'))).rejects.toBeInstanceOf(
      VersionConflictError
    );
    expect(js.messages).toHaveLength(0);
  });

  it('a SetOrganization-style current-org change keeps the storage partition', async () => {
    const store = makeStore();
    await store.save(eventAt(1, 'acme'));
    // The aggregate's CURRENT org changes (envelope orgId), as after
    // /locks/:id/SetOrganization — but the storage partition must not move.
    await store.save(eventAt(2, 'globex', { type: 'OrganizationSet' }));
    await store.save(eventAt(3, 'globex'));

    const canonical = js.messages
      .filter((m) => m.subject.startsWith('ceves.events.'))
      .map((m) => m.subject);
    expect(new Set(canonical)).toEqual(new Set(['ceves.events.acme.LockAggregate.lock-1']));

    // The ROUTED stream keys on the CURRENT org: history is attributed to
    // the tenant that owned the aggregate at the time of each event.
    const routed = js.messages
      .filter((m) => m.subject.startsWith('ceves.evt.'))
      .map((m) => m.subject.split('.')[2]);
    expect(routed).toEqual(['acme', 'globex', 'globex']);

    // Replay still returns the full, dense history.
    const events = await makeStore().loadAll('LockAggregate', 'lock-1');
    expect(events.map((e) => e.version)).toEqual([1, 2, 3]);
  });
});

describe('gateway directory-first routing', () => {
  it('routes a claimless (B2C-style) request to the aggregate home org', async () => {
    const nats = new FakeNatsConnection();
    const kv = new FakeKv();
    const directory = new NatsOrgDirectory(kv as unknown as KV);
    await directory.claim('LockAggregate', 'lock-9', 'acme');

    const seen: string[] = [];
    const originalRequest = nats.request.bind(nats);
    nats.request = (subject, payload, opts) => {
      seen.push(subject);
      return originalRequest(subject, payload, opts);
    };
    // A responder so requests complete.
    const sub = nats.subscribe('ceves.cmd.>');
    void (async () => {
      for await (const msg of sub) {
        msg.respond(JSON.stringify({ status: 200, headers: {}, body: '{}' }));
      }
    })();

    const gateway = new NatsRequestReplyNamespace({
      connection: nats,
      aggregateType: 'LockAggregate',
      subjectPrefix: 'ceves.cmd',
      timeoutMillis: 1000,
      orgDirectory: directory,
    });

    // No X-Org-Id header at all — a B2C JWT caller, a super key, temp-key
    // access. The directory routes it to the authoritative home org.
    await gateway.get(gateway.idFromName('lock-9')).fetch(
      new Request('https://gw.local/locks/lock-9/OpenDoor', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      })
    );
    expect(seen).toEqual(['ceves.cmd.acme.LockAggregate.lock-9']);

    // A WRONG claim cannot reroute an existing aggregate either.
    await gateway.get(gateway.idFromName('lock-9')).fetch(
      new Request('https://gw.local/locks/lock-9/OpenDoor', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Org-Id': 'rival' },
        body: '{}',
      })
    );
    expect(seen[1]).toBe('ceves.cmd.acme.LockAggregate.lock-9');

    // Unknown aggregate → the claim applies (create path).
    await gateway.get(gateway.idFromName('lock-new')).fetch(
      new Request('https://gw.local/locks/lock-new/CreateLock', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Org-Id': 'globex' },
        body: '{}',
      })
    );
    expect(seen[2]).toBe('ceves.cmd.globex.LockAggregate.lock-new');

    await sub.drain();
  });
});
