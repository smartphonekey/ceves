/**
 * Unit tests for the PG SQL emission (schema, module upsert, PLV8 dispatch
 * functions, per-route wrappers).
 */
import { describe, it, expect } from 'vitest';
import {
  generateSchemaSql,
  generateModuleUpsertSql,
  generateDispatchFunctionsSql,
  generateRouteWrappersSql,
  generateFullSql,
} from './sqlgen';
import type { PgRouteManifestEntry } from './types';

const MANIFEST: PgRouteManifestEntry[] = [
  {
    key: 'POST:/locks/:id/AddKey',
    method: 'POST',
    path: '/locks/:id/AddKey',
    className: 'AddKeyRoute',
    aggregateType: 'LockAggregate',
    kind: 'command',
    isCreateCommand: false,
    functionName: 'cmd_add_key',
  },
  {
    key: 'GET:/locks/:id/keys',
    method: 'GET',
    path: '/locks/:id/keys',
    className: 'GetKeysQuery',
    aggregateType: 'LockAggregate',
    kind: 'query',
    isCreateCommand: false,
    functionName: 'qry_get_keys',
  },
];

describe('generateSchemaSql', () => {
  it('creates the state, module, and outbox tables — and NO events table', () => {
    const sql = generateSchemaSql();
    expect(sql).toContain('CREATE SCHEMA IF NOT EXISTS ceves;');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS ceves.aggregate_state');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS ceves.modules');
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS ceves.outbox');
    expect(sql).toContain('CREATE INDEX IF NOT EXISTS outbox_claim_idx');
    expect(sql).toContain('PRIMARY KEY (aggregate_type, aggregate_id)');
    // Events live in R2/S3, never in PostgreSQL.
    expect(sql.toLowerCase()).not.toContain('create table if not exists ceves.events');
  });

  it('honours a custom schema and rejects unsafe identifiers', () => {
    expect(generateSchemaSql({ schema: 'myapp' })).toContain('myapp.aggregate_state');
    expect(() => generateSchemaSql({ schema: 'bad-name' })).toThrow(/Invalid PostgreSQL schema/u);
  });
});

describe('generateModuleUpsertSql', () => {
  it('dollar-quotes the module source and upserts by name', () => {
    const sql = generateModuleUpsertSql('var x = 1;', { moduleName: 'my-app' });
    expect(sql).toContain("VALUES ('my-app', $ceves_module$");
    expect(sql).toContain('var x = 1;');
    expect(sql).toContain('ON CONFLICT (name) DO UPDATE SET source = EXCLUDED.source');
  });

  it('picks a fresh dollar tag when the source contains the default one', () => {
    const sql = generateModuleUpsertSql('var s = "$ceves_module$";');
    expect(sql).toContain('$ceves_module_1$');
  });
});

describe('generateDispatchFunctionsSql', () => {
  it('emits both PLV8 dispatch functions wired to the module', () => {
    const sql = generateDispatchFunctionsSql({ moduleName: 'my-app' });
    expect(sql).toContain('CREATE OR REPLACE FUNCTION ceves.execute_command(');
    expect(sql).toContain('CREATE OR REPLACE FUNCTION ceves.execute_query(');
    expect(sql).toContain('LANGUAGE plv8');
    expect(sql).toContain('"my-app"');
    expect(sql).toContain('__ceves_pg__.executeCommand(');
    expect(sql).toContain('__ceves_pg__.executeQuery(');
  });
});

describe('generateRouteWrappersSql', () => {
  it('emits one wrapper per route bound to the right dispatcher', () => {
    const sql = generateRouteWrappersSql(MANIFEST);
    expect(sql).toContain('CREATE OR REPLACE FUNCTION ceves.cmd_add_key(');
    expect(sql).toContain('CREATE OR REPLACE FUNCTION ceves.qry_get_keys(');
    expect(sql).toContain("'LockAggregate'");
    expect(sql).toContain("'POST:/locks/:id/AddKey'");
    expect(sql).toContain('ceves.execute_command(');
    expect(sql).toContain('ceves.execute_query(');
  });

  it('escapes single quotes in embedded literals', () => {
    const sql = generateRouteWrappersSql([
      { ...MANIFEST[0]!, key: "POST:/x/:id/O'Brien", functionName: 'cmd_obrien' },
    ]);
    expect(sql).toContain("'POST:/x/:id/O''Brien'");
  });

  it('rejects unsafe function names', () => {
    expect(() =>
      generateRouteWrappersSql([{ ...MANIFEST[0]!, functionName: 'evil name' }]),
    ).toThrow(/Invalid generated function name/u);
  });
});

describe('generateFullSql', () => {
  it('assembles all sections idempotently', () => {
    const sql = generateFullSql({ moduleSource: 'var x = 1;', manifest: MANIFEST });
    expect(sql).toContain('Idempotent');
    expect(sql).toContain('CREATE EXTENSION plv8');
    expect(sql).toContain('ceves.aggregate_state');
    expect(sql).toContain('ceves.cmd_add_key');
    // Every DDL statement is re-runnable.
    expect(sql).not.toMatch(/^CREATE TABLE (?!IF NOT EXISTS)/mu);
  });

  it('can skip the per-route wrappers', () => {
    const sql = generateFullSql({
      moduleSource: 'var x = 1;',
      manifest: MANIFEST,
      skipRouteWrappers: true,
    });
    expect(sql).not.toContain('cmd_add_key');
  });
});
