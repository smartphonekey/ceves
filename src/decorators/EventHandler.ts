/**
 * Event Handler Decorator & Registry for Ceves Event Sourcing Library
 *
 * Type-safe single-mode decorator for registering event handlers.
 * Event handlers transform aggregate state based on events.
 *
 * **Features:**
 * - Full type safety with generic constraints
 * - No `any` types in decorator implementation
 * - Support for scoped (with aggregateType) and unscoped (without aggregateType) handlers
 * - Runtime validation of required properties
 *
 * @packageDocumentation
 */

import type { BaseState } from '../schemas/State';
import type { StoredEvent } from '../storage/interfaces';
import type { DomainEvent } from '../events/DomainEvent';
import type { EventMetadata } from '../events/EventMetadata';
import { EventApplicationError } from '../errors/EventApplicationError';
import { createLogger } from '../logger';

const logger = createLogger({ component: 'EventHandler' });

/**
 * Metadata for event handler registration
 *
 * Contains the essential information needed to identify and route events
 * to the appropriate handler.
 */
interface EventHandlerMetadata {
  /** Event type this handler processes (e.g., 'AccountDebited') */
  eventType: string;

  /**
   * Aggregate type this event belongs to (e.g., 'bank-account')
   * Optional - when not set, handler is registered without aggregate scope
   */
  aggregateType: string;
}

/**
 * Event handler interface - all event handlers must implement this (ADR-009)
 *
 * Event handlers are pure functions that transform aggregate state based on domain events.
 * They receive pure business data (domain event) and infrastructure metadata separately,
 * maintaining clean separation of concerns.
 *
 * **ADR-009 Empty State Pattern:**
 * - Event handlers ALWAYS receive non-null state (empty state for first event)
 * - Event handlers SET id and orgId (business decisions from metadata/event)
 * - Framework AUTO-SETS timestamp and version AFTER handler returns
 * - Framework READS orgId from state when creating StoredEvent
 *
 * @template TState - Aggregate state type extending BaseState
 * @template TEvent - Domain event type extending DomainEvent
 *
 * @example
 * ```typescript
 * @EventHandler
 * class AccountOpenedHandler implements IEventHandler<AccountState, AccountOpenedEvent> {
 *   eventType = 'AccountOpened';
 *   aggregateType = 'bank-account'; // Optional - for scoped handlers
 *
 *   apply(
 *     state: AccountState,  // NEVER null! Empty for first event.
 *     event: AccountOpenedEvent,
 *     metadata: EventMetadata
 *   ): AccountState {
 *     // No null check needed! For first event: state === AccountState.empty()
 *     return {
 *       ...state,
 *       id: metadata.aggregateId,   // Handler sets id
 *       orgId: metadata.orgId,      // Handler sets orgId (business decision!)
 *       owner: event.owner,
 *       balance: event.initialDeposit
 *       // timestamp and version auto-set by framework AFTER return
 *     };
 *   }
 * }
 * ```
 *
 * @see {@link DomainEvent} - Pure business events without infrastructure fields
 * @see {@link EventMetadata} - Infrastructure metadata (aggregateId, version, timestamp, orgId)
 * @see {@link StoredEvent} - Infrastructure envelope (not passed to handlers)
 */
export interface IEventHandler<
  TState extends BaseState,
  TEvent extends DomainEvent
> {
  /**
   * Apply a domain event to aggregate state
   *
   * Event handlers transform aggregate state based on domain events. The handler
   * receives pure business data (event) and infrastructure metadata separately.
   *
   * **ADR-009 Changes:**
   * - State is NEVER null (empty state provided for first event)
   * - Handler MUST set id and orgId (business fields)
   * - Handler returns FULL state (including orgId)
   * - Framework auto-sets timestamp and version AFTER this returns
   *
   * @param state - Current aggregate state (NEVER null - empty for first event)
   * @param event - Domain event with business data only
   * @param metadata - Infrastructure metadata (aggregateId, version, timestamp, orgId)
   * @returns New state with id and orgId set (framework adds timestamp/version)
   */
  apply(
    state: TState,
    event: TEvent,
    metadata: EventMetadata
  ): TState;

  /**
   * Event type this handler processes (required)
   * Example: 'AccountOpened', 'AccountDebited'
   */
  eventType: string;

  /**
   * Aggregate type this handler belongs to (optional)
   * When set, creates scoped handler: "${aggregateType}:${eventType}"
   * When not set, creates unscoped handler: "${eventType}"
   * Example: 'bank-account'
   */
  aggregateType?: string;

  /**
   * Optional side effects to execute after event is applied
   *
   * Side effects are I/O operations that should happen after the event is persisted,
   * such as sending MQTT commands, webhooks, notifications, etc.
   *
   * **Important:**
   * - Side effects are fire-and-forget (errors are logged but don't fail the command)
   * - Side effects run AFTER the event is persisted to storage
   * - Side effects receive the domain event and environment bindings
   * - Side effects should be idempotent (may be retried on failure)
   *
   * @param event - Domain event that was applied
   * @param env - Environment bindings (for accessing queues, APIs, etc.)
   * @param metadata - Infrastructure metadata (aggregateId, version, timestamp, orgId)
   *
   * @example
   * ```typescript
   * @EventHandler
   * class KeyAddedHandler implements IEventHandler<LockState, KeyAddedEvent> {
   *   eventType = 'KeyAdded';
   *   aggregateType = 'LockAggregate';
   *
   *   apply(state, event, metadata) { ... }
   *
   *   async sideEffects(event: KeyAddedEvent, env: Env, metadata: EventMetadata) {
   *     const message = buildQueueMessage(event.uuid, `addkey:${event.keyUuid}`, 'add-key');
   *     await env.MQTT_QUEUE.send(message);
   *   }
   * }
   * ```
   */
  sideEffects?(
    event: TEvent,
    env: Record<string, unknown>,
    metadata: EventMetadata
  ): Promise<void>;
}

/**
 * Registry entry for event handlers
 *
 * Uses BaseState and DomainEvent as the registry types since we store
 * heterogeneous handlers. Type safety is maintained at handler registration
 * time through the decorator, and enforced at runtime through the handler's
 * schema validation.
 */
export interface EventHandlerEntry {
  /** Handler class constructor - typed as base interface for heterogeneous storage */
  handlerClass: new () => IEventHandler<BaseState, DomainEvent>;

  /** Metadata for event identification */
  metadata: EventHandlerMetadata;
}

/**
 * Static registry for event handlers
 * Keyed by "${aggregateType}:${eventType}" for scoped handlers
 * or "${eventType}" for unscoped handlers
 */
const EVENT_HANDLERS = new Map<string, EventHandlerEntry>();

/**
 * Event Handler decorator - type-safe single-mode decorator
 *
 * Registers event handlers with full type safety. Handler classes must implement
 * IEventHandler<TState, TEvent> and have a required `eventType` property and
 * optional `aggregateType` property.
 *
 * **Registration modes:**
 * - Scoped (with aggregateType): Registered as "${aggregateType}:${eventType}"
 * - Unscoped (no aggregateType): Registered as "${eventType}"
 *
 * **Type safety:**
 * - Full generic constraints on state and event types
 * - No `any` types used in decorator implementation
 * - Runtime validation of required properties
 *
 * @template TState - Aggregate state type extending BaseState
 * @template TEvent - Domain event type extending DomainEvent
 * @param target - Event handler class constructor
 * @throws Error if handler class is missing required `eventType` property
 *
 * @example Scoped handler (with aggregateType):
 * ```typescript
 * @EventHandler
 * export class AccountDebitedHandler implements IEventHandler<BankAccountState, AccountDebitedEvent> {
 *   eventType = 'AccountDebited';
 *   aggregateType = 'bank-account';
 *
 *   apply(state: BankAccountState, event: AccountDebitedEvent, metadata: EventMetadata): BankAccountState {
 *     return { ...state, balance: state.balance - event.amount };
 *   }
 * }
 * // Registered as: "bank-account:AccountDebited"
 * ```
 *
 * @example Unscoped handler (no aggregateType):
 * ```typescript
 * @EventHandler
 * export class AccountCreatedHandler implements IEventHandler<AccountState, AccountCreatedEvent> {
 *   eventType = 'AccountCreated';
 *
 *   apply(state: AccountState, event: AccountCreatedEvent, metadata: EventMetadata): AccountState {
 *     return { id: metadata.aggregateId, balance: 0, version: metadata.version };
 *   }
 * }
 * // Registered as: "AccountCreated"
 * ```
 */
export function EventHandler<
  TState extends BaseState,
  TEvent extends DomainEvent
>(
  target: new () => IEventHandler<TState, TEvent>
): void {
  // Create instance with full type safety
  const instance: IEventHandler<TState, TEvent> = new target();

  // Validate required property
  if (!instance.eventType) {
    throw new Error(
      `EventHandler: ${target.name} must have 'eventType' property`
    );
  }

  // Build registry key (scoped or unscoped)
  const aggregateType = instance.aggregateType || '';
  const key = aggregateType
    ? `${aggregateType}:${instance.eventType}`
    : instance.eventType;

  // Register handler with type-safe constructor
  EVENT_HANDLERS.set(key, {
    handlerClass: target,
    metadata: {
      eventType: instance.eventType,
      aggregateType: aggregateType
    }
  });

  logger.debug('Registered event handler', {
    aggregateType: aggregateType ?? 'global',
    eventType: instance.eventType,
    handler: target.name,
  });
}

/**
 * Get all event handlers registered with metadata
 *
 * @returns Map of event handlers with metadata
 */
export function getEventHandlers(): Map<string, EventHandlerEntry> {
  return EVENT_HANDLERS;
}


/**
 * Find event handler by event type and aggregate type
 *
 * @param eventType - Event type to find handler for
 * @param aggregateType - Aggregate type
 * @returns Event handler entry or undefined
 */
export function findEventHandler(
  eventType: string,
  aggregateType: string
): EventHandlerEntry | undefined {
  const key = `${aggregateType}:${eventType}`;
  return EVENT_HANDLERS.get(key);
}

/**
 * Apply event to state using registered handler
 *
 * Convenience function that finds and applies the appropriate event handler.
 * Extracts domain event from StoredEvent envelope and passes metadata separately.
 *
 * **Architecture (ADR-008 + ADR-009):**
 * - Domain events are extracted from the StoredEvent envelope
 * - Handlers receive pure business data (domain event) and infrastructure metadata separately
 * - Handlers ALWAYS receive non-null state (empty state for first event - ADR-009)
 * - Handlers SET id and orgId (business decisions)
 * - Framework AUTO-SETS timestamp and version AFTER handler returns (infrastructure fields)
 * - Framework READS orgId from state when creating StoredEvent
 *
 * @param aggregateType - Aggregate type
 * @param state - Current state (null for first event)
 * @param event - StoredEvent with domain event and metadata
 * @param StateClass - State class constructor for creating empty state
 * @returns New state after applying event
 * @throws Error if no handler registered for event type
 *
 * @see {@link StoredEvent} - Infrastructure envelope with event field
 * @see {@link DomainEvent} - Pure business events
 * @see {@link EventMetadata} - Infrastructure metadata passed to handlers
 */
export function applyEventToState<TState extends BaseState>(
  aggregateType: string,
  state: TState | null,
  event: StoredEvent,
  StateClass: new () => TState
): TState {
  const handlerEntry = findEventHandler(event.type, aggregateType);

  if (!handlerEntry) {
    throw new EventApplicationError(
      `No event handler registered for event type "${event.type}" on aggregate "${aggregateType}"`,
      event.type,
      event.version,
      aggregateType,
      event.aggregateId
    );
  }

  // Provide empty state for first event (ADR-009)
  const inputState = state ?? new StateClass();

  // Extract domain event from StoredEvent envelope (ADR-008)
  const domainEvent = event.event;

  // Create metadata from StoredEvent envelope fields
  const metadata: EventMetadata = {
    aggregateId: event.aggregateId,
    version: event.version,
    timestamp: event.timestamp,
    orgId: event.orgId
  };

  // Call handler with domain event and metadata
  // Type assertion needed: registry stores handlers as IEventHandler<BaseState, DomainEvent>
  // but callers know the specific TState type. Runtime type safety is enforced by handler's
  // schema validation. This is a documented type boundary in the framework.
  const handler = new handlerEntry.handlerClass();
  const newState = handler.apply(inputState, domainEvent, metadata) as TState;

  // Framework ONLY sets timestamp and version (infrastructure fields)
  // Handler is responsible for setting id and orgId (business fields)
  // orgId is READ from state when creating StoredEvent
  newState.timestamp = new Date().toISOString();
  newState.version = event.version;

  return newState;
}

/**
 * Execute side effects for an event handler (if defined)
 *
 * Side effects are I/O operations that should happen after the event is persisted,
 * such as sending MQTT commands, webhooks, notifications, etc.
 *
 * **Fire-and-forget:**
 * - Errors are logged but don't fail the command
 * - Side effects should be idempotent (may be retried)
 *
 * @param aggregateType - Aggregate type
 * @param event - StoredEvent with domain event and metadata
 * @param env - Environment bindings
 * @returns Promise that resolves when side effects complete (or rejects on error)
 */
export async function executeSideEffects(
  aggregateType: string,
  event: StoredEvent,
  env: Record<string, unknown>
): Promise<void> {
  const handlerEntry = findEventHandler(event.type, aggregateType);

  if (!handlerEntry) {
    // No handler - nothing to do
    return;
  }

  const handler = new handlerEntry.handlerClass();

  // Check if handler has side effects
  if (!handler.sideEffects) {
    return;
  }

  // Extract domain event from StoredEvent envelope
  const domainEvent = event.event;

  // Create metadata from StoredEvent envelope fields
  const metadata: EventMetadata = {
    aggregateId: event.aggregateId,
    version: event.version,
    timestamp: event.timestamp,
    orgId: event.orgId
  };

  // Execute side effects
  await handler.sideEffects(domainEvent, env, metadata);
}

/**
 * Clear all registered event handlers
 * FOR TESTING ONLY
 */
export function clearEventHandlers(): void {
  EVENT_HANDLERS.clear();
}
