/**
 * PG adapter registries and route introspection.
 *
 * The PostgreSQL variant has no Durable Object subclass to carry the state
 * class, so aggregates register their state class explicitly (used to build
 * the ADR-009 empty state for first events). Route classification reuses the
 * same duck-typing the Durable Object dispatcher applies to `@Route` classes.
 *
 * @packageDocumentation
 */

import { getRegisteredRoutes } from '../../routing/Route';
import { BaseState } from '../../schemas/State';
import type { DomainEvent, NO_EVENT } from '../../events/DomainEvent';
import type { PgRouteManifestEntry } from './types';

/**
 * Structural view of an instantiated `@Route` class, as seen by the PG
 * dispatcher. Same duck-typing contract as `AggregateObject`.
 */
export interface PgRouteInstance {
  aggregateType?: string;
  executeCommand?: (...args: unknown[]) => Promise<DomainEvent | typeof NO_EVENT>;
  executeQuery?: (state: unknown, query: unknown, ctx: unknown) => Promise<unknown>;
  customizeResponse?: (
    response: {
      success: true;
      aggregateId: string;
      version: number;
      event: { type: string; data: Record<string, unknown> } | null;
    },
    event: DomainEvent,
    state: unknown,
  ) => Promise<Record<string, unknown>>;
  schema?: unknown;
}

/** Static side of a `@Route` class (create semantics + event-data schema). */
export interface PgRouteClass {
  name: string;
  isCreateCommand?: boolean;
  eventSchema?: { safeParse: (data: unknown) => { success: boolean; error?: unknown } };
}

/** State-class registry keyed by aggregateType (e.g. 'LockAggregate'). */
const STATE_CLASSES = new Map<string, new () => BaseState>();

/**
 * Register the state class for an aggregate type so the PG dispatcher can
 * construct the ADR-009 empty state for first events. The PG-variant
 * equivalent of `super(ctx, env, StateClass)` in a Durable Object subclass.
 *
 * @example
 * ```typescript
 * registerPgAggregateState('LockAggregate', LockState);
 * ```
 */
export function registerPgAggregateState(
  aggregateType: string,
  stateClass: new () => BaseState,
): void {
  STATE_CLASSES.set(aggregateType, stateClass);
}

/**
 * Resolve the registered state class for an aggregate type.
 * Falls back to `BaseState` — first-event handlers then start from the bare
 * ADR-009 fields (id/orgId/version/timestamp) with no custom defaults.
 */
export function getPgAggregateStateClass(aggregateType: string): new () => BaseState {
  return STATE_CLASSES.get(aggregateType) ?? BaseState;
}

/** Clear the state-class registry. FOR TESTING ONLY. */
export function clearPgAggregateStates(): void {
  STATE_CLASSES.clear();
}

/** Classification of one registered route for the PG variant. */
export interface ClassifiedRoute {
  key: string;
  method: string;
  path: string;
  RouteClass: PgRouteClass & (new () => PgRouteInstance);
  instance: PgRouteInstance;
  kind: 'command' | 'query';
}

/**
 * Instantiate a route class defensively — decorator-registered classes are
 * heterogeneous and some may throw on construction outside a worker.
 */
function tryInstantiate(RouteClass: new () => PgRouteInstance): PgRouteInstance | null {
  try {
    return new RouteClass();
  } catch {
    return null;
  }
}

/**
 * Classify every registered `@Route` class into PG-dispatchable commands and
 * queries. Routes without `aggregateType` + `executeCommand`/`executeQuery`
 * (plain worker routes) are skipped — they stay in the HTTP layer.
 */
export function classifyRegisteredRoutes(): ClassifiedRoute[] {
  const result: ClassifiedRoute[] = [];
  for (const meta of getRegisteredRoutes()) {
    const RouteClass = meta.RouteClass as unknown as PgRouteClass & (new () => PgRouteInstance);
    const instance = tryInstantiate(RouteClass);
    if (!instance?.aggregateType) continue;

    let kind: 'command' | 'query';
    if (typeof instance.executeCommand === 'function') {
      kind = 'command';
    } else if (typeof instance.executeQuery === 'function') {
      kind = 'query';
    } else {
      continue;
    }

    result.push({
      key: meta.key,
      method: meta.method,
      path: meta.path,
      RouteClass,
      instance,
      kind,
    });
  }
  return result;
}

/** `AddKeyRoute` → `add_key`, `GetUserProfileQuery` → `get_user_profile`. */
function toSnakeFunctionName(className: string): string {
  const stripped = className.replace(/(Route|Query|Command)$/u, '') || className;
  return stripped
    .replace(/([a-z0-9])([A-Z])/gu, '$1_$2')
    .replace(/\W/gu, '_')
    .toLowerCase();
}

/**
 * Build the route manifest used by the SQL generator to emit one PostgreSQL
 * wrapper function per command/query route (REST-extension friendly names).
 * Function names are prefixed `cmd_`/`qry_` and de-duplicated.
 */
export function collectPgManifest(): PgRouteManifestEntry[] {
  const seen = new Map<string, number>();
  return classifyRegisteredRoutes().map((route) => {
    const prefix = route.kind === 'command' ? 'cmd_' : 'qry_';
    let functionName = `${prefix}${toSnakeFunctionName(route.RouteClass.name)}`;
    const count = seen.get(functionName) ?? 0;
    seen.set(functionName, count + 1);
    if (count > 0) functionName = `${functionName}_${count + 1}`;

    return {
      key: route.key,
      method: route.method,
      path: route.path,
      className: route.RouteClass.name,
      aggregateType: route.instance.aggregateType as string,
      kind: route.kind,
      isCreateCommand: route.RouteClass.isCreateCommand === true,
      functionName,
    };
  });
}
