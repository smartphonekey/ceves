import { describe, it, expect, beforeEach } from 'vitest';
import type { KV } from '@nats-io/kv';
import { NatsKvStorage } from '../NatsKvStorage';
import { VersionConflictError } from '../../../errors/VersionConflictError';
import { storageKeyPrefixFor } from '../naming';
import { FakeKv } from './fakes';

describe('NatsKvStorage', () => {
  let kv: FakeKv;
  let storage: NatsKvStorage;

  beforeEach(() => {
    kv = new FakeKv();
    storage = new NatsKvStorage(
      kv as unknown as KV,
      storageKeyPrefixFor('WalletAggregate', 'w-1')
    );
  });

  it('round-trips arbitrary JSON values', async () => {
    await storage.put('aggregateName', 'w-1');
    await storage.put('r2_replay_in_progress', true);
    expect(await storage.get<string>('aggregateName')).toBe('w-1');
    expect(await storage.get<boolean>('r2_replay_in_progress')).toBe(true);
    expect(await storage.get('missing')).toBeUndefined();
  });

  it('stores state with CAS: create on first write, revision update after', async () => {
    await storage.put('state', { id: 'w-1', version: 1 });
    await storage.put('state', { id: 'w-1', version: 2 });
    expect(await storage.get<{ version: number }>('state')).toEqual({ id: 'w-1', version: 2 });
  });

  it('rejects a state write that lost a cross-process race and latches conflicted', async () => {
    const other = new NatsKvStorage(
      kv as unknown as KV,
      storageKeyPrefixFor('WalletAggregate', 'w-1')
    );
    await storage.put('state', { id: 'w-1', version: 1 });
    await other.get('state'); // other host reads revision
    await other.put('state', { id: 'w-1', version: 2 }); // other host wins

    await expect(storage.put('state', { id: 'w-1', version: 2 })).rejects.toBeInstanceOf(
      VersionConflictError
    );
    expect(storage.conflicted).toBe(true);
  });

  it('rejects a state create when another process created it first', async () => {
    const other = new NatsKvStorage(
      kv as unknown as KV,
      storageKeyPrefixFor('WalletAggregate', 'w-1')
    );
    await other.put('state', { id: 'w-1', version: 1 });
    await expect(storage.put('state', { id: 'w-1', version: 1 })).rejects.toBeInstanceOf(
      VersionConflictError
    );
  });

  it('recovers after re-reading the current state', async () => {
    const other = new NatsKvStorage(
      kv as unknown as KV,
      storageKeyPrefixFor('WalletAggregate', 'w-1')
    );
    await other.put('state', { id: 'w-1', version: 1 });
    await storage.get('state'); // observe current revision
    await storage.put('state', { id: 'w-1', version: 2 });
    expect(await other.get<{ version: number }>('state')).toEqual({ id: 'w-1', version: 2 });
  });

  it('deleteAll only clears this aggregate, not bucket-mates', async () => {
    const neighbour = new NatsKvStorage(
      kv as unknown as KV,
      storageKeyPrefixFor('WalletAggregate', 'w-2')
    );
    await storage.put('state', { version: 1 });
    await storage.put('aggregateName', 'w-1');
    await neighbour.put('state', { version: 7 });

    await storage.deleteAll();

    expect(await storage.get('state')).toBeUndefined();
    expect(await storage.get('aggregateName')).toBeUndefined();
    expect(await neighbour.get<{ version: number }>('state')).toEqual({ version: 7 });
  });

  it('delete removes a single key', async () => {
    await storage.put('aggregateName', 'w-1');
    expect(await storage.delete('aggregateName')).toBe(true);
    expect(await storage.get('aggregateName')).toBeUndefined();
  });

  it('lists own entries with decoded key names', async () => {
    await storage.put('state', { version: 3 });
    await storage.put('aggregateName', 'w-1');
    const listed = await storage.list();
    expect([...listed.keys()].sort((a, b) => a.localeCompare(b))).toEqual([
      'aggregateName',
      'state',
    ]);
  });

  it('allows state re-create after deleteAll (admin delete then recreate)', async () => {
    await storage.put('state', { version: 1 });
    await storage.deleteAll();
    await storage.put('state', { version: 1 });
    expect(await storage.get<{ version: number }>('state')).toEqual({ version: 1 });
  });
});
