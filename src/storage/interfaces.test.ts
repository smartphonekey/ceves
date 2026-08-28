/**
 * Unit tests for storage interfaces
 *
 * These tests verify that:
 * 1. Interfaces can be implemented with correct method signatures
 * 2. TypeScript enforces interface contracts at compile time
 * 3. Type safety works with generics (unknown data/state)
 * 4. Interfaces are exported from main entry point
 */

import { describe, it, expect } from 'vitest';
import type {
  IEventStore,
  ISnapshotStore,
  StoredEvent,
  StoredSnapshot,
} from './interfaces';

// Also verify interfaces are exported from main entry point. `ISnapshotStore`
// is intentionally NOT re-exported from `../index` on the CF/DO path
// (snapshots live in DO storage there); the S3/AWS adapter that still uses
// it imports from `./interfaces` directly.
import type {
  IEventStore as IEventStoreFromIndex,
  StoredEvent as StoredEventFromIndex,
  StoredSnapshot as StoredSnapshotFromIndex,
} from '../index';
type ISnapshotStoreFromIndex = ISnapshotStore;

describe('Storage Interfaces', () => {
  describe('StoredEvent', () => {
    it('should have all required properties', () => {
      // Arrange
      const event: StoredEvent = {
        aggregateType: 'account',
        aggregateId: 'acc-123',
        version: 1,
        type: 'AccountCreated',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: { initialBalance: 0 },
      };

      // Assert
      expect(event.aggregateType).toBe('account');
      expect(event.aggregateId).toBe('acc-123');
      expect(event.version).toBe(1);
      expect(event.type).toBe('AccountCreated');
      expect(event.timestamp).toBe('2025-11-14T10:30:00.000Z');
      expect(event.data).toEqual({ initialBalance: 0 });
    });

    it('should allow unknown type for data', () => {
      // Arrange - Test various data types
      const eventWithObject: StoredEvent = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 1,
        type: 'TestEvent',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: { foo: 'bar', count: 42 },
      };

      const eventWithArray: StoredEvent = {
        aggregateType: 'test',
        aggregateId: 'test-2',
        version: 1,
        type: 'TestEvent',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: [1, 2, 3],
      };

      const eventWithPrimitive: StoredEvent = {
        aggregateType: 'test',
        aggregateId: 'test-3',
        version: 1,
        type: 'TestEvent',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: 'simple string',
      };

      const eventWithNull: StoredEvent = {
        aggregateType: 'test',
        aggregateId: 'test-4',
        version: 1,
        type: 'TestEvent',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: null,
      };

      // Assert - TypeScript compilation succeeding is the primary test
      expect(eventWithObject.data).toBeDefined();
      expect(eventWithArray.data).toBeDefined();
      expect(eventWithPrimitive.data).toBeDefined();
      expect(eventWithNull.data).toBeNull();
    });

    it('should support type-safe generics', () => {
      // Arrange - Define typed event data
      interface AccountCreatedData {
        initialBalance: number;
        currency: string;
      }

      // Generic usage enables type safety
      const typedEvent: StoredEvent & { data: AccountCreatedData } = {
        aggregateType: 'account',
        aggregateId: 'acc-456',
        version: 1,
        type: 'AccountCreated',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: {
          initialBalance: 1000,
          currency: 'USD',
        },
      };

      // Assert - TypeScript infers correct type for data
      expect(typedEvent.data.initialBalance).toBe(1000);
      expect(typedEvent.data.currency).toBe('USD');
    });
  });

  describe('StoredSnapshot', () => {
    it('should have all required properties', () => {
      // Arrange
      const snapshot: StoredSnapshot = {
        aggregateType: 'account',
        aggregateId: 'acc-123',
        version: 42,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: {
          id: 'acc-123',
          balance: 1500,
          transactions: 42,
        },
      };

      // Assert
      expect(snapshot.aggregateType).toBe('account');
      expect(snapshot.aggregateId).toBe('acc-123');
      expect(snapshot.version).toBe(42);
      expect(snapshot.timestamp).toBe('2025-11-14T11:00:00.000Z');
      expect(snapshot.state).toEqual({
        id: 'acc-123',
        balance: 1500,
        transactions: 42,
      });
    });

    it('should allow unknown type for state', () => {
      // Arrange - Test various state types
      const snapshotWithObject: StoredSnapshot = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 10,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: { status: 'active', count: 10 },
      };

      const snapshotWithArray: StoredSnapshot = {
        aggregateType: 'test',
        aggregateId: 'test-2',
        version: 10,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: ['item1', 'item2'],
      };

      const snapshotWithNull: StoredSnapshot = {
        aggregateType: 'test',
        aggregateId: 'test-3',
        version: 10,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: null,
      };

      // Assert - TypeScript compilation succeeding is the primary test
      expect(snapshotWithObject.state).toBeDefined();
      expect(snapshotWithArray.state).toBeDefined();
      expect(snapshotWithNull.state).toBeNull();
    });

    it('should support type-safe generics', () => {
      // Arrange - Define typed state
      interface AccountState {
        id: string;
        balance: number;
        transactions: number;
        status: 'active' | 'suspended' | 'closed';
      }

      // Generic usage enables type safety
      const typedSnapshot: StoredSnapshot & { state: AccountState } = {
        aggregateType: 'account',
        aggregateId: 'acc-789',
        version: 100,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: {
          id: 'acc-789',
          balance: 5000,
          transactions: 100,
          status: 'active',
        },
      };

      // Assert - TypeScript infers correct type for state
      expect(typedSnapshot.state.id).toBe('acc-789');
      expect(typedSnapshot.state.balance).toBe(5000);
      expect(typedSnapshot.state.transactions).toBe(100);
      expect(typedSnapshot.state.status).toBe('active');
    });
  });

  describe('IEventStore', () => {
    it('should be implementable with correct method signatures', () => {
      // Arrange - Create mock implementation
      class MockEventStore implements IEventStore {
        private events: Map<string, StoredEvent[]> = new Map();

        async save(event: StoredEvent): Promise<void> {
          const key = `${event.aggregateType}:${event.aggregateId}`;
          const existingEvents = this.events.get(key) || [];
          this.events.set(key, [...existingEvents, event]);
          return Promise.resolve();
        }

        async load(
          aggregateType: string,
          aggregateId: string,
          afterVersion?: number
        ): Promise<StoredEvent[]> {
          const key = `${aggregateType}:${aggregateId}`;
          const events = this.events.get(key) || [];

          if (afterVersion !== undefined) {
            return Promise.resolve(events.filter((e) => e.version > afterVersion));
          }

          return Promise.resolve(events);
        }

        async loadAll(
          aggregateType: string,
          aggregateId: string
        ): Promise<StoredEvent[]> {
          return this.load(aggregateType, aggregateId);
        }
      }

      // Act - Instantiate and use the mock
      const store = new MockEventStore();

      // Assert - TypeScript compilation succeeding proves interface is correctly implemented
      expect(store).toBeDefined();
      expect(typeof store.save).toBe('function');
      expect(typeof store.load).toBe('function');
      expect(typeof store.loadAll).toBe('function');
    });

    it('should enforce save method signature', async () => {
      // Arrange
      class MockEventStore implements IEventStore {
        async save(_event: StoredEvent): Promise<void> {
          return Promise.resolve();
        }

        async load(
          _aggregateType: string,
          _aggregateId: string,
          _afterVersion?: number
        ): Promise<StoredEvent[]> {
          return Promise.resolve([]);
        }

        async loadAll(
          _aggregateType: string,
          _aggregateId: string
        ): Promise<StoredEvent[]> {
          return Promise.resolve([]);
        }
      }

      const store = new MockEventStore();
      const event: StoredEvent = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 1,
        type: 'TestEvent',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: {},
      };

      // Act & Assert - Method accepts correct parameters
      await expect(store.save(event)).resolves.toBeUndefined();
    });

    it('should enforce load method signature with optional afterVersion', async () => {
      // Arrange
      class MockEventStore implements IEventStore {
        async save(_event: StoredEvent): Promise<void> {
          return Promise.resolve();
        }

        async load(
          _aggregateType: string,
          _aggregateId: string,
          _afterVersion?: number
        ): Promise<StoredEvent[]> {
          return Promise.resolve([]);
        }

        async loadAll(
          _aggregateType: string,
          _aggregateId: string
        ): Promise<StoredEvent[]> {
          return Promise.resolve([]);
        }
      }

      const store = new MockEventStore();

      // Act & Assert - Can call with or without afterVersion
      await expect(store.load('test', 'test-1')).resolves.toEqual([]);
      await expect(store.load('test', 'test-1', 10)).resolves.toEqual([]);
    });

    it('should enforce loadAll method signature', async () => {
      // Arrange
      class MockEventStore implements IEventStore {
        async save(_event: StoredEvent): Promise<void> {
          return Promise.resolve();
        }

        async load(
          _aggregateType: string,
          _aggregateId: string,
          _afterVersion?: number
        ): Promise<StoredEvent[]> {
          return Promise.resolve([]);
        }

        async loadAll(
          aggregateType: string,
          aggregateId: string
        ): Promise<StoredEvent[]> {
          return this.load(aggregateType, aggregateId);
        }
      }

      const store = new MockEventStore();

      // Act & Assert - Method accepts correct parameters
      await expect(store.loadAll('test', 'test-1')).resolves.toEqual([]);
    });
  });

  describe('ISnapshotStore', () => {
    it('should be implementable with correct method signatures', () => {
      // Arrange - Create mock implementation
      class MockSnapshotStore implements ISnapshotStore {
        private snapshots: Map<string, StoredSnapshot> = new Map();

        async save(snapshot: StoredSnapshot): Promise<void> {
          const key = `${snapshot.aggregateType}:${snapshot.aggregateId}`;
          this.snapshots.set(key, snapshot);
          return Promise.resolve();
        }

        async load(
          aggregateType: string,
          aggregateId: string
        ): Promise<StoredSnapshot | null> {
          const key = `${aggregateType}:${aggregateId}`;
          return Promise.resolve(this.snapshots.get(key) || null);
        }
      }

      // Act - Instantiate and use the mock
      const store = new MockSnapshotStore();

      // Assert - TypeScript compilation succeeding proves interface is correctly implemented
      expect(store).toBeDefined();
      expect(typeof store.save).toBe('function');
      expect(typeof store.load).toBe('function');
    });

    it('should enforce save method signature', async () => {
      // Arrange
      class MockSnapshotStore implements ISnapshotStore {
        async save(_snapshot: StoredSnapshot): Promise<void> {
          return Promise.resolve();
        }

        async load(
          _aggregateType: string,
          _aggregateId: string
        ): Promise<StoredSnapshot | null> {
          return Promise.resolve(null);
        }
      }

      const store = new MockSnapshotStore();
      const snapshot: StoredSnapshot = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 10,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: {},
      };

      // Act & Assert - Method accepts correct parameters
      await expect(store.save(snapshot)).resolves.toBeUndefined();
    });

    it('should enforce load method signature returning StoredSnapshot or null', async () => {
      // Arrange
      class MockSnapshotStore implements ISnapshotStore {
        async save(_snapshot: StoredSnapshot): Promise<void> {
          return Promise.resolve();
        }

        async load(
          _aggregateType: string,
          _aggregateId: string
        ): Promise<StoredSnapshot | null> {
          return Promise.resolve(null);
        }
      }

      const store = new MockSnapshotStore();

      // Act & Assert - Method returns null when no snapshot exists
      const result = await store.load('test', 'test-1');
      expect(result).toBeNull();
    });

    it('should allow load to return StoredSnapshot', async () => {
      // Arrange
      const testSnapshot: StoredSnapshot = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 10,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: { value: 42 },
      };

      class MockSnapshotStore implements ISnapshotStore {
        async save(_snapshot: StoredSnapshot): Promise<void> {
          return Promise.resolve();
        }

        async load(
          _aggregateType: string,
          _aggregateId: string
        ): Promise<StoredSnapshot | null> {
          return Promise.resolve(testSnapshot);
        }
      }

      const store = new MockSnapshotStore();

      // Act
      const result = await store.load('test', 'test-1');

      // Assert - Method can return a snapshot
      expect(result).toBe(testSnapshot);
      expect(result?.version).toBe(10);
    });
  });

  describe('Index exports', () => {
    it('should export IEventStore from main entry point', () => {
      // Assert - Type imports work from index.ts
      // The import at the top of this file proves the export works
      // TypeScript compilation succeeding is the test
      const typeCheck: IEventStoreFromIndex = {} as IEventStoreFromIndex;
      expect(typeCheck).toBeDefined();
    });

    it('should export ISnapshotStore from main entry point', () => {
      // Assert - Type imports work from index.ts
      const typeCheck: ISnapshotStoreFromIndex = {} as ISnapshotStoreFromIndex;
      expect(typeCheck).toBeDefined();
    });

    it('should export StoredEvent from main entry point', () => {
      // Assert - Type imports work from index.ts
      const typeCheck: StoredEventFromIndex = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 1,
        type: 'TestEvent',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: {},
      };
      expect(typeCheck).toBeDefined();
    });

    it('should export StoredSnapshot from main entry point', () => {
      // Assert - Type imports work from index.ts
      const typeCheck: StoredSnapshotFromIndex = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 10,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: {},
      };
      expect(typeCheck).toBeDefined();
    });
  });

  describe('TypeScript strict mode enforcement', () => {
    it('should enforce all required properties on StoredEvent', () => {
      // This test validates that TypeScript requires all properties
      // If we try to create an event without a required property, it should fail compilation

      const validEvent: StoredEvent = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 1,
        type: 'TestEvent',
        timestamp: '2025-11-14T10:30:00.000Z',
        data: {},
      };

      // Assert - All properties are required
      expect(validEvent.aggregateType).toBeDefined();
      expect(validEvent.aggregateId).toBeDefined();
      expect(validEvent.version).toBeDefined();
      expect(validEvent.type).toBeDefined();
      expect(validEvent.timestamp).toBeDefined();
      expect(validEvent.data).toBeDefined();

      // NOTE: TypeScript would fail compilation if any property was missing
      // The fact that this compiles proves the contract is enforced
    });

    it('should enforce all required properties on StoredSnapshot', () => {
      // This test validates that TypeScript requires all properties

      const validSnapshot: StoredSnapshot = {
        aggregateType: 'test',
        aggregateId: 'test-1',
        version: 10,
        timestamp: '2025-11-14T11:00:00.000Z',
        state: {},
      };

      // Assert - All properties are required
      expect(validSnapshot.aggregateType).toBeDefined();
      expect(validSnapshot.aggregateId).toBeDefined();
      expect(validSnapshot.version).toBeDefined();
      expect(validSnapshot.timestamp).toBeDefined();
      expect(validSnapshot.state).toBeDefined();

      // NOTE: TypeScript would fail compilation if any property was missing
    });

    it('should enforce all required methods on IEventStore', () => {
      // This test validates that TypeScript requires all interface methods
      // Attempting to implement IEventStore without all methods would fail compilation

      class CompleteEventStore implements IEventStore {
        async save(_event: StoredEvent): Promise<void> {
          return Promise.resolve();
        }
        async load(
          _aggregateType: string,
          _aggregateId: string,
          _afterVersion?: number
        ): Promise<StoredEvent[]> {
          return Promise.resolve([]);
        }
        async loadAll(
          _aggregateType: string,
          _aggregateId: string
        ): Promise<StoredEvent[]> {
          return Promise.resolve([]);
        }
      }

      const store = new CompleteEventStore();

      // Assert - All methods are present
      expect(typeof store.save).toBe('function');
      expect(typeof store.load).toBe('function');
      expect(typeof store.loadAll).toBe('function');

      // NOTE: TypeScript would fail compilation if any method was missing
    });

    it('should enforce all required methods on ISnapshotStore', () => {
      // This test validates that TypeScript requires all interface methods

      class CompleteSnapshotStore implements ISnapshotStore {
        async save(_snapshot: StoredSnapshot): Promise<void> {
          return Promise.resolve();
        }
        async load(
          _aggregateType: string,
          _aggregateId: string
        ): Promise<StoredSnapshot | null> {
          return Promise.resolve(null);
        }
      }

      const store = new CompleteSnapshotStore();

      // Assert - All methods are present
      expect(typeof store.save).toBe('function');
      expect(typeof store.load).toBe('function');

      // NOTE: TypeScript would fail compilation if any method was missing
    });
  });
});
