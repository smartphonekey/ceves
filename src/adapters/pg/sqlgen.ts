/**
 * SQL emission for the PG variant.
 *
 * Produces one idempotent SQL script (all `CREATE ... IF NOT EXISTS` /
 * `CREATE OR REPLACE` / upserts) containing:
 *
 * 1. the `ceves` schema with the aggregate-state table and the module table
 *    (NO events table — events are returned to the caller, never stored here),
 * 2. the bundled PG module upsert (`<schema>.modules`),
 * 3. the PLV8 dispatch functions `execute_command` / `execute_query`,
 * 4. optional per-route wrapper functions (`cmd_*` / `qry_*`) with plain
 *    named arguments, so a REST layer such as PostgREST can expose each
 *    command/query directly as `/rpc/<function>`.
 *
 * Requires the PLV8 extension, version 3.1+ (the dispatch functions return a
 * Promise, which PLV8 resolves before converting the result to jsonb).
 *
 * @packageDocumentation
 */

import type { PgRouteManifestEntry } from './types';
import { PLV8_GLOBAL_KEY } from './plv8';

/** Options for SQL generation. */
export interface PgSqlGenOptions {
  /** PostgreSQL schema for tables + functions. Default 'ceves'. */
  schema?: string;
  /** Module name row in `<schema>.modules`. Default 'ceves-app'. */
  moduleName?: string;
}

const IDENTIFIER_RE = /^[a-z_][a-z0-9_]*$/u;

function resolveOptions(options: PgSqlGenOptions): { schema: string; moduleName: string } {
  const schema = options.schema ?? 'ceves';
  const moduleName = options.moduleName ?? 'ceves-app';
  if (!IDENTIFIER_RE.test(schema)) {
    throw new Error(`Invalid PostgreSQL schema name: "${schema}"`);
  }
  return { schema, moduleName };
}

/** Escape a value for use inside a single-quoted SQL literal. */
function sqlLiteral(value: string): string {
  return `'${value.replace(/'/gu, "''")}'`;
}

/** Pick a dollar-quote tag that does not occur in the payload. */
function dollarTag(payload: string, base: string): string {
  let tag = `$${base}$`;
  let counter = 0;
  while (payload.includes(tag)) {
    counter += 1;
    tag = `$${base}_${counter}$`;
  }
  return tag;
}

/**
 * Schema DDL: state table + module table. Idempotent.
 *
 * `aggregate_state` holds ONLY the current state per aggregate — the event
 * log stays external (R2/S3) by design; do not add an events table here.
 */
export function generateSchemaSql(options: PgSqlGenOptions = {}): string {
  const { schema } = resolveOptions(options);
  return `-- ceves PG variant: schema + state/module tables (idempotent)
CREATE SCHEMA IF NOT EXISTS ${schema};

-- Current state per aggregate. The event log is intentionally NOT in
-- PostgreSQL: dispatch functions RETURN the emitted event to the caller,
-- which appends it to the external event store (R2/S3).
CREATE TABLE IF NOT EXISTS ${schema}.aggregate_state (
  aggregate_type text NOT NULL,
  aggregate_id   text NOT NULL,
  version        integer NOT NULL,
  org_id         text NOT NULL DEFAULT '',
  state          jsonb NOT NULL,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (aggregate_type, aggregate_id)
);

CREATE INDEX IF NOT EXISTS aggregate_state_org_idx
  ON ${schema}.aggregate_state (aggregate_type, org_id);

-- Bundled JS module(s) executed by the PLV8 dispatch functions.
CREATE TABLE IF NOT EXISTS ${schema}.modules (
  name       text PRIMARY KEY,
  source     text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Transactional outbox: external calls (fetch/MQTT/...) enqueued by
-- handlers DURING a command, in the same transaction as the state write.
-- A wrapper-side relay (drainPgOutbox) claims rows with FOR UPDATE SKIP
-- LOCKED, performs the real I/O, and DELETES them — this is a transient
-- queue, NOT an audit log (history belongs in the R2/S3 event store).
CREATE TABLE IF NOT EXISTS ${schema}.outbox (
  id              bigserial PRIMARY KEY,
  kind            text NOT NULL,
  aggregate_type  text NOT NULL DEFAULT '',
  aggregate_id    text NOT NULL DEFAULT '',
  request         jsonb NOT NULL,
  status          text NOT NULL DEFAULT 'pending', -- pending | inflight | dead
  attempts        integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_until    timestamptz,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_claim_idx
  ON ${schema}.outbox (status, next_attempt_at);
`;
}

/** Upsert the bundled PG module into `<schema>.modules`. */
export function generateModuleUpsertSql(
  moduleSource: string,
  options: PgSqlGenOptions = {},
): string {
  const { schema, moduleName } = resolveOptions(options);
  const tag = dollarTag(moduleSource, 'ceves_module');
  return `-- Bundled ceves PG module: ${moduleName}
INSERT INTO ${schema}.modules (name, source, updated_at)
VALUES (${sqlLiteral(moduleName)}, ${tag}
${moduleSource}
${tag}, now())
ON CONFLICT (name) DO UPDATE SET source = EXCLUDED.source, updated_at = now();
`;
}

/** Shared PLV8 preamble that loads + evals the module once per connection. */
function moduleLoaderJs(schema: string, moduleName: string): string {
  return `  var g = (0, eval)('this');
  if (!g.${PLV8_GLOBAL_KEY}) {
    var mod = plv8.execute('SELECT source FROM ${schema}.modules WHERE name = $1', [${JSON.stringify(moduleName)}]);
    if (mod.length === 0) {
      throw new Error('ceves module "${moduleName}" not found in ${schema}.modules — run the generated SQL first');
    }
    (0, eval)(mod[0].source);
    if (!g.${PLV8_GLOBAL_KEY}) {
      throw new Error('ceves module "${moduleName}" did not install globalThis.${PLV8_GLOBAL_KEY} — the PG entry must call installPlv8Dispatcher()');
    }
  }`;
}

/**
 * The two generic PLV8 dispatch functions. They return the dispatcher's
 * Promise directly — PLV8 3.1+ resolves it and converts the plain-object
 * result ({ status, body, event }) to jsonb.
 */
export function generateDispatchFunctionsSql(options: PgSqlGenOptions = {}): string {
  const { schema, moduleName } = resolveOptions(options);
  const loader = moduleLoaderJs(schema, moduleName);

  const commandBody = `${loader}
  return g.${PLV8_GLOBAL_KEY}.executeCommand({
    aggregateType: p_aggregate_type,
    aggregateId: p_aggregate_id,
    routeKey: p_route_key,
    command: p_command || {},
    auth: p_auth || {},
    env: p_env || {}
  });`;
  const queryBody = `${loader}
  return g.${PLV8_GLOBAL_KEY}.executeQuery({
    aggregateType: p_aggregate_type,
    aggregateId: p_aggregate_id,
    routeKey: p_route_key,
    query: p_query || {},
    auth: p_auth || {},
    env: p_env || {}
  });`;
  const tag = dollarTag(commandBody + queryBody, 'ceves_fn');

  return `-- Generic dispatch functions (PLV8 >= 3.1: returned Promises are awaited)
CREATE OR REPLACE FUNCTION ${schema}.execute_command(
  p_aggregate_type text,
  p_aggregate_id   text,
  p_route_key      text,
  p_command        jsonb DEFAULT '{}'::jsonb,
  p_auth           jsonb DEFAULT '{}'::jsonb,
  p_env            jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS ${tag}
${commandBody}
${tag} LANGUAGE plv8;

CREATE OR REPLACE FUNCTION ${schema}.execute_query(
  p_aggregate_type text,
  p_aggregate_id   text,
  p_route_key      text,
  p_query          jsonb DEFAULT '{}'::jsonb,
  p_auth           jsonb DEFAULT '{}'::jsonb,
  p_env            jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS ${tag}
${queryBody}
${tag} LANGUAGE plv8;
`;
}

/**
 * One thin SQL wrapper per command/query route, named `cmd_*` / `qry_*`.
 * These give a REST extension (PostgREST `/rpc/...`) a stable, discoverable
 * function per endpoint while all logic stays in the generic dispatchers.
 */
export function generateRouteWrappersSql(
  manifest: PgRouteManifestEntry[],
  options: PgSqlGenOptions = {},
): string {
  const { schema } = resolveOptions(options);
  const parts = manifest.map((entry) => {
    if (!IDENTIFIER_RE.test(entry.functionName)) {
      throw new Error(`Invalid generated function name: "${entry.functionName}"`);
    }
    const dispatcher = entry.kind === 'command' ? 'execute_command' : 'execute_query';
    const payloadParam = entry.kind === 'command' ? 'p_command' : 'p_query';
    const body = `  SELECT ${schema}.${dispatcher}(
    ${sqlLiteral(entry.aggregateType)},
    p_aggregate_id,
    ${sqlLiteral(entry.key)},
    ${payloadParam},
    p_auth,
    p_env
  );`;
    const tag = dollarTag(body, 'ceves_wrap');
    return `-- ${entry.method} ${entry.path} → ${entry.className} (${entry.aggregateType})
CREATE OR REPLACE FUNCTION ${schema}.${entry.functionName}(
  p_aggregate_id text,
  ${payloadParam} jsonb DEFAULT '{}'::jsonb,
  p_auth jsonb DEFAULT '{}'::jsonb,
  p_env  jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS ${tag}
${body}
${tag} LANGUAGE sql;
`;
  });
  return parts.join('\n');
}

/** Assemble the full idempotent deployment script. */
export function generateFullSql(args: {
  moduleSource: string;
  manifest: PgRouteManifestEntry[];
  options?: PgSqlGenOptions;
  /** Omit the per-route `cmd_*`/`qry_*` wrappers. Default false. */
  skipRouteWrappers?: boolean;
}): string {
  const options = args.options ?? {};
  const sections = [
    '-- Generated by ceves-generate-pg. Idempotent — safe to re-run.',
    '-- Requires: CREATE EXTENSION plv8; (version >= 3.1)',
    '',
    generateSchemaSql(options),
    generateModuleUpsertSql(args.moduleSource, options),
    generateDispatchFunctionsSql(options),
  ];
  if (!args.skipRouteWrappers) {
    sections.push(generateRouteWrappersSql(args.manifest, options));
  }
  return sections.join('\n');
}
