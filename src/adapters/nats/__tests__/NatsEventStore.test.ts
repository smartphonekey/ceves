import { describe, it, expect, beforeEach } from 'vitest';
import type { JetStreamClient, JetStreamManager } from '@nats-io/jetstream';
import { NatsEventStore } from '../NatsEventStore';
import { VersionConflictError } from '../../../errors/VersionConflictError';
import type { StoredEvent } from '../../../storage/interfaces';
import { FakeJetStream, FakeJetStreamManager } from './fakes';

function makeStore(js: FakeJetStream): NatsEventStore {
  return new NatsEventStore(
    js as unknown as JetStreamClient,
    new FakeJetStreamManager(js) as unknown as JetStreamManager
  );
}

function parseEnvelope(data: string): StoredEvent {
  const parsed: StoredEvent = JSON.parse(data);
  return parsed;
}

function eventAt(version: number, overrides: Partial<StoredEvent> = {}): StoredEvent {
  return {
    aggregateType: 'WalletAggregate',
    aggregateId: 'w-1',
    version,
    type: 'Credited',
    timestamp: new Date(2026, 0, version).toISOString(),
    orgId: 'org-1',
    event: { type: 'Credited', amount: version * 10 } as StoredEvent['event'],
    ...overrides,
  };
}

/**
 * Codex review (PR #354, thread r3840613358): a save that rejects while the
 * command pipeline is still running must stay observable. `save()` is
 * fire-and-forget from `AggregateObject.applyAndPersistEvent`, and the actor
 * host only calls `waitForPendingSaves()` after `fetch()` returns — if the
 * rejection is forgotten in between, the host acknowledges a command whose
 * event never reached the log.
 */
describe('NatsEventStore failure latching', () => {
  it('reports a publish failure that settled before the host observed it', async () => {
    const js = new FakeJetStream();
    js.publish = () => Promise.reject(new Error('publish down'));
    const store = makeStore(js);

    // Fire-and-forget, exactly as the core does.
    void store.save(eventAt(1)).catch(() => undefined);
    // Let the rejection settle BEFORE the host asks — the racing window.
    await new Promise((resolve) => setTimeout(resolve, 0));

    await expect(store.waitForPendingSaves('WalletAggregate', 'w-1')).rejects.toThrow(
      'publish down'
    );
  });

  it('clears the latch once observed, so the next command is not poisoned', async () => {
    const js = new FakeJetStream();
    const realPublish = js.publish.bind(js);
    let failNext = true;
    js.publish = (subject, payload, opts) => {
      if (failNext) {
        failNext = false;
        return Promise.reject(new Error('publish down'));
      }
      return realPublish(subject, payload, opts);
    };
    const store = makeStore(js);

    void store.save(eventAt(1)).catch(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 0));
    await expect(store.waitForPendingSaves('WalletAggregate', 'w-1')).rejects.toThrow();

    // A later, successful save must not inherit the consumed failure.
    await store.save(eventAt(1));
    await expect(store.waitForPendingSaves('WalletAggregate', 'w-1')).resolves.toBeUndefined();
  });
});

describe('NatsEventStore.save', () => {
  let js: FakeJetStream;
  let store: NatsEventStore;

  beforeEach(() => {
    js = new FakeJetStream();
    store = makeStore(js);
  });

  it('publishes version 1 as a create (subject must not exist)', async () => {
    await store.save(eventAt(1));
    expect(js.messages).toHaveLength(1);
    const first = js.messages[0];
    expect(first?.subject).toBe('ceves.events.WalletAggregate.w-1');
    expect(parseEnvelope(first?.data ?? '{}').version).toBe(1);
  });

  it('chains subsequent saves via the cached subject sequence', async () => {
    await store.save(eventAt(1));
    await store.save(eventAt(2));
    await store.save(eventAt(3));
    expect(js.messages.map((m) => parseEnvelope(m.data).version)).toEqual([1, 2, 3]);
  });

  it('absorbs an exact duplicate save via Nats-Msg-Id (idempotent retry)', async () => {
    await store.save(eventAt(1));
    await store.save(eventAt(1));
    expect(js.messages).toHaveLength(1);
  });

  it('throws VersionConflictError when another writer advanced the subject', async () => {
    const other = makeStore(js);
    await store.save(eventAt(1));
    // Other host appends ITS OWN v2 (cold cache → direct get resolves the seq).
    await other.save(eventAt(2, { event: { type: 'Credited', amount: 777 } as StoredEvent['event'] }));
    // This host still believes the last seq is the v1 message.
    await expect(store.save(eventAt(2))).rejects.toBeInstanceOf(VersionConflictError);
  });

  it('keeps separate aggregates independent', async () => {
    await store.save(eventAt(1));
    await store.save(eventAt(1, { aggregateId: 'w-2' }));
    expect(js.messages).toHaveLength(2);
  });

  it('self-heals a stale OCC token when the version is still the legitimate next one', async () => {
    await store.save(eventAt(1));
    await store.save(eventAt(2));
    // Simulate token staleness/corruption: the cached SEQUENCE points at
    // the v1 message even though v2 is the subject's last. Version 3 is
    // still the legitimate next event (nothing competes at v3), so save()
    // must refresh and retry rather than dropping the event as a conflict.
    const cache = (
      store as unknown as { tokenBySubject: Map<string, { seq: number; version: number | null }> }
    ).tokenBySubject;
    cache.set('ceves.events.WalletAggregate.w-1', { seq: 1, version: 2 });

    await store.save(eventAt(3));
    expect(js.messages.map((m) => parseEnvelope(m.data).version)).toEqual([1, 2, 3]);
  });

  it('refuses a create (v1) when the subject already has history — cold cache', async () => {
    await store.save(eventAt(1));
    await store.save(eventAt(2));
    const cold = makeStore(js);
    await expect(
      cold.save(eventAt(1, { event: { type: 'Credited', amount: 999 } as StoredEvent['event'] }))
    ).rejects.toBeInstanceOf(VersionConflictError);
    expect(js.messages).toHaveLength(2); // nothing appended
  });

  it('refuses a version the log cannot produce (empty subject, version > 1)', async () => {
    await expect(store.save(eventAt(5))).rejects.toBeInstanceOf(VersionConflictError);
    expect(js.messages).toHaveLength(0);
  });

  it('refuses a gapped version (log behind state view)', async () => {
    await store.save(eventAt(1));
    await store.save(eventAt(2));
    await expect(store.save(eventAt(4))).rejects.toBeInstanceOf(VersionConflictError);
    expect(js.messages).toHaveLength(2);
  });

  it('waitForPendingSaves resolves after successes and rethrows failures', async () => {
    await store.save(eventAt(1));
    await store.waitForPendingSaves('WalletAggregate', 'w-1'); // no pending → resolves

    // Fire-and-forget a save that will fail (gap), then flush.
    void store.save(eventAt(9)).catch(() => undefined);
    await expect(store.waitForPendingSaves('WalletAggregate', 'w-1')).rejects.toBeInstanceOf(
      VersionConflictError
    );
    // A subsequent flush with nothing pending resolves again.
    await store.waitForPendingSaves('WalletAggregate', 'w-1');
  });
});

describe('NatsEventStore routed fan-out', () => {
  function makeRoutedStore(js: FakeJetStream): NatsEventStore {
    return new NatsEventStore(
      js as unknown as JetStreamClient,
      new FakeJetStreamManager(js) as unknown as JetStreamManager,
      { routedSubjectPrefix: 'ceves.evt' }
    );
  }

  it('fans each committed event out to <org>.<aggType>.<eventType>.<id> subjects, in order', async () => {
    const js = new FakeJetStream();
    const store = makeRoutedStore(js);
    await store.save(eventAt(1, { type: 'Opened' }));
    await store.save(eventAt(2)); // type 'Credited'

    const canonical = js.messages.filter((m) => m.subject.startsWith('ceves.events.'));
    const routed = js.messages.filter((m) => m.subject.startsWith('ceves.evt.'));
    expect(canonical.map((m) => parseEnvelope(m.data).version)).toEqual([1, 2]);
    expect(routed.map((m) => m.subject)).toEqual([
      'ceves.evt.org-1.WalletAggregate.Opened.w-1',
      'ceves.evt.org-1.WalletAggregate.Credited.w-1',
    ]);
    // The routed copy carries the full envelope (dense version included).
    expect(routed.map((m) => parseEnvelope(m.data).version)).toEqual([1, 2]);
  });

  it('does not duplicate the routed copy on an idempotent retry', async () => {
    const js = new FakeJetStream();
    const store = makeRoutedStore(js);
    const event = eventAt(1);
    await store.save(event);
    await store.save(event);
    expect(js.messages.filter((m) => m.subject.startsWith('ceves.evt.'))).toHaveLength(1);
  });

  it('a fan-out failure is non-fatal — the canonical append still succeeds', async () => {
    const js = new FakeJetStream();
    const originalPublish = js.publish.bind(js);
    js.publish = (subject, payload, opts) =>
      subject.startsWith('ceves.evt.')
        ? Promise.reject(new Error('routed stream down'))
        : originalPublish(subject, payload, opts);

    const store = makeRoutedStore(js);
    await store.save(eventAt(1)); // resolves despite the fan-out failure
    expect(js.messages.filter((m) => m.subject.startsWith('ceves.events.'))).toHaveLength(1);
    expect(js.messages.filter((m) => m.subject.startsWith('ceves.evt.'))).toHaveLength(0);
  });
});

describe('NatsEventStore replay integrity', () => {
  it('drops duplicated versions (keeping the earliest append) and continues over gaps', async () => {
    const js = new FakeJetStream();
    const store = makeStore(js);
    await store.save(eventAt(1));
    await store.save(eventAt(2));
    // Corrupt the log externally: append a competing v2 and skip v3.
    await js.publish(
      'ceves.events.WalletAggregate.w-1',
      JSON.stringify(eventAt(2, { event: { type: 'Credited', amount: 666 } as StoredEvent['event'] }))
    );
    await js.publish('ceves.events.WalletAggregate.w-1', JSON.stringify(eventAt(4)));

    const events = await makeStore(js).loadAll('WalletAggregate', 'w-1');
    expect(events.map((e) => e.version)).toEqual([1, 2, 4]);
    const second = events[1]?.event as unknown as { amount: number };
    expect(second.amount).toBe(20); // the earliest v2 append won
  });
});

describe('NatsEventStore.load', () => {
  let js: FakeJetStream;
  let store: NatsEventStore;

  beforeEach(async () => {
    js = new FakeJetStream();
    store = makeStore(js);
    for (const version of [1, 2, 3, 4, 5]) {
      await store.save(eventAt(version));
    }
    // interleave a different aggregate to prove subject filtering
    await store.save(eventAt(1, { aggregateId: 'w-2' }));
  });

  it('loads all events for an aggregate in version order', async () => {
    const cold = makeStore(js); // cold cache — full consumer read
    const events = await cold.loadAll('WalletAggregate', 'w-1');
    expect(events.map((e) => e.version)).toEqual([1, 2, 3, 4, 5]);
  });

  it('filters events after a version', async () => {
    const events = await makeStore(js).load('WalletAggregate', 'w-1', 3);
    expect(events.map((e) => e.version)).toEqual([4, 5]);
  });

  it('pages with limit', async () => {
    const events = await makeStore(js).load('WalletAggregate', 'w-1', 1, 2);
    expect(events.map((e) => e.version)).toEqual([2, 3]);
  });

  it('short-circuits when the caller is already up to date', async () => {
    const events = await makeStore(js).load('WalletAggregate', 'w-1', 5);
    expect(events).toEqual([]);
  });

  it('returns empty for an unknown aggregate', async () => {
    const events = await makeStore(js).load('WalletAggregate', 'nope');
    expect(events).toEqual([]);
  });

  it('primes the OCC cache so a following save needs no direct lookup', async () => {
    const cold = makeStore(js);
    await cold.loadAll('WalletAggregate', 'w-1');
    await cold.save(eventAt(6));
    const versions = js.messages
      .filter((m) => m.subject.endsWith('.w-1'))
      .map((m) => parseEnvelope(m.data).version);
    expect(versions).toEqual([1, 2, 3, 4, 5, 6]);
  });
});
