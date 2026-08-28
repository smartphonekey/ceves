/**
 * PLV8 bootstrap for the PG dispatcher.
 *
 * The app's PG entry module (bundled by `ceves-generate-pg`) imports its
 * decorator barrel (routes + event handlers), registers state classes, and
 * calls {@link installPlv8Dispatcher}. Inside PostgreSQL the bundle is eval'd
 * by the generated `<schema>.execute_command` / `<schema>.execute_query`
 * PLV8 functions, which then call the installed `globalThis.__ceves_pg__`.
 *
 * The same entry also runs under Node during generation (to collect the
 * route manifest) — there is no `plv8` global there, so SQL access is bound
 * lazily and only errors if a dispatch is actually attempted.
 *
 * @packageDocumentation
 */

import { createPgDispatcher } from './dispatcher';
import { collectPgManifest } from './registry';
import type {
  PgCommandInput,
  PgDispatcherOptions,
  PgDispatchResult,
  PgQueryInput,
  PgRouteManifestEntry,
  PgSql,
} from './types';

/** Global key under which the dispatcher handle is installed. */
export const PLV8_GLOBAL_KEY = '__ceves_pg__';

/** Shape of the PLV8 host object we rely on. */
interface Plv8Host {
  execute(query: string, params?: unknown[]): unknown;
}

/** Handle installed on globalThis for the generated PLV8 functions. */
export interface PgPlv8Handle {
  executeCommand(input: PgCommandInput): Promise<PgDispatchResult>;
  executeQuery(input: PgQueryInput): Promise<PgDispatchResult>;
  /** Route manifest — used by the generator CLI under Node. */
  manifest(): PgRouteManifestEntry[];
}

/** Bind SQL to the plv8 host lazily so Node-side manifest collection works. */
function lazyPlv8Sql(): PgSql {
  return {
    execute(query: string, params?: unknown[]): unknown {
      const plv8 = (globalThis as Record<string, unknown>).plv8 as Plv8Host | undefined;
      if (!plv8) {
        throw new Error(
          'ceves/pg: no `plv8` global — command/query dispatch only runs inside ' +
            'PostgreSQL (PLV8). Outside the database this module is introspection-only.',
        );
      }
      return plv8.execute(query, params ?? []);
    },
  };
}

/**
 * Create the dispatcher over `plv8.execute` and install it as
 * `globalThis.__ceves_pg__`. Idempotent per isolate. Returns the handle.
 */
export function installPlv8Dispatcher(options: PgDispatcherOptions = {}): PgPlv8Handle {
  const dispatcher = createPgDispatcher(lazyPlv8Sql(), options);
  const handle: PgPlv8Handle = {
    executeCommand: (input) => dispatcher.executeCommand(input),
    executeQuery: (input) => dispatcher.executeQuery(input),
    manifest: () => collectPgManifest(),
  };
  (globalThis as Record<string, unknown>)[PLV8_GLOBAL_KEY] = handle;
  return handle;
}
