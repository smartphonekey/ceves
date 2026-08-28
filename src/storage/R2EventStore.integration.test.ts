/**
 * Integration tests for R2EventStore using Miniflare
 *
 * These tests validate R2EventStore against actual Workers runtime with real R2 bindings.
 * Unlike unit tests that use mocks, these tests verify:
 * 1. Events are correctly written to and read from real R2 storage (via Miniflare)
 * 2. Zero-padded version numbers work correctly with R2's lexicographic listing
 * 3. Storage interoperability (save with one instance, load with another)
 * 4. Edge cases specific to Workers runtime (R2 consistency, concurrent operations)
 *
 * Test Environment:
 * - Runs in actual Workers runtime (workerd) via @cloudflare/vitest-pool-workers
 * - Uses Miniflare's automatic in-memory R2 bucket (env.TEST_EVENTS_BUCKET)
 * - Each test gets a fresh, isolated R2 bucket instance
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import type { R2Bucket } from '@cloudflare/workers-types';
import { R2EventStore } from './R2EventStore';
import type { StoredEvent } from './interfaces';

describe('R2EventStore integration (Workers runtime)', () => {
  let eventStore: R2EventStore;

  // Miniflare provides fresh bindings for each test - no cleanup needed
  beforeEach(() => {
    eventStore = new R2EventStore(env.TEST_EVENTS_BUCKET as R2Bucket);
  });

  // Helper to create test event
  function createTestEvent(
    aggregateType = 'account',
    aggregateId = 'acc-123',
    version = 1,
    type = 'AccountCreated'
  ): StoredEvent {
    return {
      aggregateType,
      aggregateId,
      version,
      type,
      timestamp: '2025-11-15T10:00:00.000Z',
      data: { test: true, timestamp: Date.now() },
    };
  }

  describe('save() with real R2', () => {
    it('should save event to R2 and create object at correct path', async () => {
      // Arrange
      const event = createTestEvent('account', 'acc-123', 1);

      // Act
      await eventStore.save(event);

      // Assert - verify file was created in R2 at expected path
      const bucket = env.TEST_EVENTS_BUCKET as R2Bucket;
      const savedObject = await bucket.get(
        'account/acc-123/000000001.json'
      );
      expect(savedObject).not.toBeNull();
      expect(savedObject).toBeDefined();

      if (savedObject) {
        const savedContent = await savedObject.text();
        const savedEvent = JSON.parse(savedContent);
        expect(savedEvent).toEqual(event);
      }
    });

    it('should save multiple events with zero-padded version numbers', async () => {
      // Arrange
      const events = [
        createTestEvent('account', 'acc-456', 1, 'AccountCreated'),
        createTestEvent('account', 'acc-456', 2, 'MoneyDeposited'),
        createTestEvent('account', 'acc-456', 15, 'MoneyWithdrawn'),
      ];

      // Act
      for (const event of events) {
        await eventStore.save(event);
      }

      // Assert - use loadAll to verify all events were saved (avoids isolated storage issues)
      const loaded = await eventStore.loadAll('account', 'acc-456');
      expect(loaded).toHaveLength(3);
      expect(loaded[0].version).toBe(1);
      expect(loaded[1].version).toBe(2);
      expect(loaded[2].version).toBe(15);
    });

    it('should save events for different aggregates independently', async () => {
      // Arrange
      const accountEvent = createTestEvent('account', 'acc-100', 1);
      const userEvent = createTestEvent('user', 'usr-200', 1, 'UserCreated');

      // Act
      await eventStore.save(accountEvent);
      await eventStore.save(userEvent);

      // Assert - verify both aggregates can be loaded independently
      const accountEvents = await eventStore.loadAll('account', 'acc-100');
      const userEvents = await eventStore.loadAll('user', 'usr-200');

      expect(accountEvents).toHaveLength(1);
      expect(accountEvents[0]).toEqual(accountEvent);
      expect(userEvents).toHaveLength(1);
      expect(userEvents[0]).toEqual(userEvent);
    });
  });

  describe('load() with afterVersion filtering', () => {
    beforeEach(async () => {
      // Setup: save 5 events for same aggregate
      for (let i = 1; i <= 5; i++) {
        await eventStore.save(
          createTestEvent('account', 'acc-789', i, `Event${i}`)
        );
      }
    });

    it('should load all events when afterVersion is not specified', async () => {
      // Act
      const events = await eventStore.load('account', 'acc-789');

      // Assert
      expect(events).toHaveLength(5);
      expect(events[0].version).toBe(1);
      expect(events[4].version).toBe(5);
    });

    it('should filter events correctly with afterVersion parameter', async () => {
      // Act
      const events = await eventStore.load('account', 'acc-789', 2);

      // Assert - should only get events with version > 2 (i.e., 3, 4, 5)
      expect(events).toHaveLength(3);
      expect(events[0].version).toBe(3);
      expect(events[1].version).toBe(4);
      expect(events[2].version).toBe(5);
    });

    it('should return empty array when afterVersion is beyond all events', async () => {
      // Act
      const events = await eventStore.load('account', 'acc-789', 999);

      // Assert
      expect(events).toEqual([]);
    });

    it('should return events in ascending version order', async () => {
      // Act
      const events = await eventStore.load('account', 'acc-789');

      // Assert - verify strict ascending order
      for (let i = 0; i < events.length - 1; i++) {
        expect(events[i].version).toBeLessThan(events[i + 1].version);
      }
    });
  });

  describe('load() with bounded limit paging (startAfter)', () => {
    beforeEach(async () => {
      // 12 events so paging in batches of 5 yields 5 + 5 + 2 — the exact
      // shape the batched DO restore walks.
      for (let i = 1; i <= 12; i++) {
        await eventStore.save(createTestEvent('account', 'acc-paged', i, `Event${i}`));
      }
    });

    it('returns at most `limit` events as the first page', async () => {
      const page = await eventStore.load('account', 'acc-paged', 0, 5);
      expect(page.map((e) => e.version)).toEqual([1, 2, 3, 4, 5]);
    });

    it('returns a short final page (< limit) at the end of the stream', async () => {
      const lastPage = await eventStore.load('account', 'acc-paged', 10, 5);
      expect(lastPage.map((e) => e.version)).toEqual([11, 12]);
    });

    it('pages forward via afterVersion + limit covering the whole stream with no gaps/dupes', async () => {
      const collected: number[] = [];
      let after = 0;
      let page = await eventStore.load('account', 'acc-paged', after, 5);
      while (page.length > 0) {
        collected.push(...page.map((e) => e.version));
        if (page.length < 5) break;
        after = page[page.length - 1].version;
        page = await eventStore.load('account', 'acc-paged', after, 5);
      }
      // Proves R2 `startAfter` advances correctly across pages — every event,
      // in order, exactly once.
      expect(collected).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    });
  });

  describe('loadAll()', () => {
    it('should load all events for an aggregate in order', async () => {
      // Arrange
      const events = [
        createTestEvent('order', 'ord-999', 1, 'OrderCreated'),
        createTestEvent('order', 'ord-999', 2, 'ItemAdded'),
        createTestEvent('order', 'ord-999', 3, 'OrderSubmitted'),
      ];

      for (const event of events) {
        await eventStore.save(event);
      }

      // Act
      const loaded = await eventStore.loadAll('order', 'ord-999');

      // Assert
      expect(loaded).toHaveLength(3);
      expect(loaded[0].type).toBe('OrderCreated');
      expect(loaded[1].type).toBe('ItemAdded');
      expect(loaded[2].type).toBe('OrderSubmitted');
    });

    it('should return empty array for non-existent aggregate', async () => {
      // Act
      const events = await eventStore.loadAll('account', 'non-existent');

      // Assert
      expect(events).toEqual([]);
    });

    it('should return empty array for empty aggregate (no events yet)', async () => {
      // Act
      const events = await eventStore.loadAll('account', 'acc-empty');

      // Assert
      expect(events).toEqual([]);
    });
  });

  describe('interoperability - multiple instances', () => {
    it('should save with one instance and load with another', async () => {
      // Arrange
      const bucket = env.TEST_EVENTS_BUCKET as R2Bucket;
      const eventStore1 = new R2EventStore(bucket);
      const eventStore2 = new R2EventStore(bucket);

      const event = createTestEvent('account', 'acc-interop', 1);

      // Act - save with instance 1
      await eventStore1.save(event);

      // Assert - load with instance 2
      const loaded = await eventStore2.loadAll('account', 'acc-interop');
      expect(loaded).toHaveLength(1);
      expect(loaded[0]).toEqual(event);
    });

    it('should handle concurrent saves from different instances', async () => {
      // Arrange
      const bucket = env.TEST_EVENTS_BUCKET as R2Bucket;
      const eventStore1 = new R2EventStore(bucket);
      const eventStore2 = new R2EventStore(bucket);

      const event1 = createTestEvent('account', 'acc-concurrent', 1, 'Event1');
      const event2 = createTestEvent('account', 'acc-concurrent', 2, 'Event2');

      // Act - save concurrently from different instances
      await Promise.all([eventStore1.save(event1), eventStore2.save(event2)]);

      // Assert - both events should be retrievable
      const eventStore3 = new R2EventStore(bucket);
      const loaded = await eventStore3.loadAll('account', 'acc-concurrent');

      expect(loaded).toHaveLength(2);
      expect(loaded[0].type).toBe('Event1');
      expect(loaded[1].type).toBe('Event2');
    });
  });

  describe('Workers runtime edge cases', () => {
    it('should handle zero-padded version numbers with R2 lexicographic ordering', async () => {
      // Arrange - save events in non-sequential order to test lexicographic sorting
      const versions = [1, 10, 2, 20, 100];
      for (const version of versions) {
        await eventStore.save(
          createTestEvent('account', 'acc-lex', version, `Event${version}`)
        );
      }

      // Act
      const events = await eventStore.loadAll('account', 'acc-lex');

      // Assert - should be sorted numerically, not lexicographically
      expect(events).toHaveLength(5);
      expect(events[0].version).toBe(1);
      expect(events[1].version).toBe(2);
      expect(events[2].version).toBe(10);
      expect(events[3].version).toBe(20);
      expect(events[4].version).toBe(100);
    });

    it('should verify R2 consistency - rapid saves are all retrievable', async () => {
      // Arrange
      const eventCount = 20;
      const events: StoredEvent[] = [];

      for (let i = 1; i <= eventCount; i++) {
        events.push(
          createTestEvent('account', 'acc-consistency', i, `Event${i}`)
        );
      }

      // Act - save all events rapidly without awaiting each individually
      await Promise.all(events.map((event) => eventStore.save(event)));

      // Assert - all events should be retrievable
      const loaded = await eventStore.loadAll('account', 'acc-consistency');
      expect(loaded).toHaveLength(eventCount);

      // Verify all versions are present
      const loadedVersions = loaded.map((e) => e.version).sort((a, b) => a - b);
      const expectedVersions = Array.from(
        { length: eventCount },
        (_, i) => i + 1
      );
      expect(loadedVersions).toEqual(expectedVersions);
    });

    it('should handle events with large data payloads', async () => {
      // Arrange - create event with substantial data payload
      const largeData = {
        items: Array.from({ length: 100 }, (_, i) => ({
          id: `item-${i}`,
          name: `Item ${i}`,
          description: 'A'.repeat(100),
        })),
        metadata: {
          timestamp: Date.now(),
          version: '1.0.0',
          source: 'integration-test',
        },
      };

      const event = createTestEvent('order', 'ord-large', 1);
      event.data = largeData;

      // Act
      await eventStore.save(event);

      // Assert
      const loaded = await eventStore.loadAll('order', 'ord-large');
      expect(loaded).toHaveLength(1);
      expect(loaded[0].data).toEqual(largeData);
    });
  });
});
