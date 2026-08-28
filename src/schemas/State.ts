/**
 * State Type Definitions for Ceves Event Sourcing Library
 *
 * This module provides TypeScript type definitions for aggregate state with conventions
 * for structure and versioning. State objects are the result of applying events to an
 * aggregate and represent the current state of the aggregate at a specific version.
 *
 * Key Design Decisions:
 * - Base state type enforces id, version, and timestamp fields
 * - User-defined state types extend BaseState using intersection types
 * - State naming convention: [DomainEntity]State (e.g., BankAccountState, OrderState)
 * - State objects are immutable - replaced by apply methods, not mutated
 * - Version tracking ensures state version matches latest applied event version
 *
 * @packageDocumentation
 */

/**
 * Base state class for all aggregates in the event sourcing system.
 *
 * As of ADR-009, BaseState has been converted from an interface to a class
 * to support the empty state pattern. This eliminates null checks in event handlers
 * by guaranteeing that handlers always receive non-null state.
 *
 * Every state must include:
 * - `id`: Unique identifier for the aggregate instance (set by event handlers from metadata)
 * - `orgId`: Organization ID (business field - set by event handlers, read by framework)
 * - `version`: Current version number (infrastructure - auto-set by framework)
 * - `timestamp`: ISO 8601 datetime string (infrastructure - auto-set by framework)
 *
 * **Key Design Decisions (ADR-009):**
 * - Event handlers ALWAYS receive non-null state (empty state for first event)
 * - Event handlers SET id and orgId (business decisions)
 * - Framework AUTO-SETS version and timestamp (infrastructure fields)
 * - Framework READS orgId from state when creating StoredEvent
 *
 * **State Class Conventions:**
 * 1. **Naming**: Use `[DomainEntity]State` pattern (PascalCase with "State" suffix)
 *    - Examples: `BankAccountState`, `OrderState`, `UserProfileState`
 * 2. **Extension**: Extend BaseState class with business fields
 * 3. **Immutability**: State objects are replaced, not mutated (use spread operator)
 * 4. **Empty Factory**: Can override `empty()` for custom initialization
 *
 * @example
 * ```typescript
 * // Define a custom state class for a bank account
 * class BankAccountState extends BaseState {
 *   email: string = '';
 *   name: string = '';
 *   balance: number = 0;
 *   isActive: boolean = false;
 * }
 *
 * // Event handlers always receive non-null state
 * class AccountCreatedHandler implements IEventHandler<BankAccountState, AccountCreatedEvent> {
 *   apply(
 *     state: BankAccountState,  // NEVER null! Empty for first event.
 *     event: AccountCreatedEvent,
 *     metadata: EventMetadata
 *   ): BankAccountState {
 *     // For first event: state === BankAccountState.empty()
 *     // Handler sets id and orgId (business decisions)
 *     return {
 *       ...state,
 *       id: metadata.aggregateId,
 *       orgId: metadata.orgId,
 *       email: event.email,
 *       name: event.name,
 *       balance: 0,
 *       isActive: true
 *       // version and timestamp auto-set by framework AFTER this returns
 *     };
 *   }
 * }
 *
 * // Subsequent events just update business logic
 * class MoneyDepositedHandler implements IEventHandler<BankAccountState, MoneyDepositedEvent> {
 *   apply(
 *     state: BankAccountState,  // ALWAYS non-null!
 *     event: MoneyDepositedEvent,
 *     metadata: EventMetadata
 *   ): BankAccountState {
 *     // No null check needed!
 *     return {
 *       ...state,
 *       balance: state.balance + event.amount
 *       // Framework updates timestamp and version automatically
 *     };
 *   }
 * }
 * ```
 *
 * @example
 * ```typescript
 * // Custom empty state initialization
 * class OrderState extends BaseState {
 *   customerId: string = '';
 *   items: Array<{ productId: string; quantity: number; price: number }> = [];
 *   totalAmount: number = 0;
 *   status: 'pending' | 'confirmed' | 'shipped' | 'delivered' | 'cancelled' = 'pending';
 *
 *   // Override empty() for custom initialization
 *   static empty(): OrderState {
 *     const state = new OrderState();
 *     state.status = 'pending';  // Explicit default
 *     state.items = [];
 *     return state;
 *   }
 * }
 * ```
 */
export class BaseState {
  /**
   * Unique identifier for the aggregate instance.
   *
   * This should match the `aggregateId` field from events applied to this aggregate.
   * Set by event handlers from metadata.aggregateId.
   *
   * @example 'acc-123', 'order-456', 'user-789'
   */
  id: string = '';

  /**
   * Organization ID that this aggregate belongs to.
   *
   * **BUSINESS FIELD** - Set by event handlers (business decision).
   * Framework reads orgId from state when creating StoredEvent.
   *
   * Used for multi-tenant isolation - every aggregate must belong to an organization.
   * Commands can only access aggregates belonging to the same organization as the request.
   *
   * @example 'org-456', 'default-org'
   */
  orgId: string = '';

  /**
   * Current version number of the state.
   *
   * **INFRASTRUCTURE FIELD** - Auto-set by framework after event handler returns.
   *
   * This equals the version of the last event applied to the aggregate.
   * Used for:
   * - Event ordering (events applied in version sequence: 1, 2, 3, ...)
   * - Optimistic locking (detect concurrent modifications)
   * - Conflict detection when applying new events
   *
   * Version starts at 1 for the first event and increments by 1 for each subsequent event.
   *
   * @example 1, 2, 3, 42
   */
  version: number = 0;

  /**
   * ISO 8601 datetime string of when this state was last updated.
   *
   * **INFRASTRUCTURE FIELD** - Auto-set by framework after event handler returns.
   *
   * Used for:
   * - Auditing when the state was last modified
   * - Temporal queries (state at specific point in time)
   * - Event replay validation
   *
   * @example '2025-11-15T10:00:00Z', '2025-11-15T14:30:00.000Z'
   */
  timestamp: string = '';

  /**
   * Factory method for creating empty initial state.
   *
   * This is called by the framework when applying the first event to an aggregate.
   * Instead of passing null to the first event handler, the framework passes
   * StateClass.empty(), ensuring handlers never receive null.
   *
   * Subclasses can override this method for custom initialization logic.
   *
   * @returns New instance of the state class with default values
   *
   * @example
   * ```typescript
   * // Default usage (inherited from BaseState)
   * const emptyState = AccountState.empty();
   *
   * // Custom initialization
   * class OrderState extends BaseState {
   *   status: string = '';
   *
   *   static empty(): OrderState {
   *     const state = new OrderState();
   *     state.status = 'pending';
   *     return state;
   *   }
   * }
   * ```
   */
  static empty<T extends BaseState>(this: new () => T): T {
    return new this();
  }
}
