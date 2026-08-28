#!/usr/bin/env node
/**
 * ceves-generate-pg — turn the app's command/query handlers into PostgreSQL
 * functions.
 *
 * What it does
 * ------------
 * 1. Bundles the app's PG entry (a file that imports the decorator barrel,
 *    registers state classes via `registerPgAggregateState`, and calls
 *    `installPlv8Dispatcher()`) into ONE self-contained IIFE with esbuild —
 *    this is the module PLV8 executes inside PostgreSQL.
 * 2. Builds the same entry for Node and imports it to collect the route
 *    manifest (`globalThis.__ceves_pg__.manifest()`), so the SQL and the
 *    bundled registry can never drift apart.
 * 3. Emits one idempotent SQL script: ceves schema (state + module tables —
 *    NO events table), module upsert, the generic PLV8 dispatch functions,
 *    and one `cmd_*` / `qry_*` wrapper per route (REST-extension friendly).
 *
 * The `cloudflare:workers` import that `ceves`'s index drags in is aliased to
 * the repo's inert shim — the PG dispatch path never touches Durable Objects.
 *
 * CLI flags
 * ---------
 *   --entry <path>        PG entry file (required), e.g. src/pg-entry.ts
 *   --out <path>          Output SQL file (default: ceves-pg.sql)
 *   --module-name <name>  Row name in <schema>.modules (default: ceves-app)
 *   --pg-schema <name>    PostgreSQL schema (default: ceves)
 *   --manifest <path>     Also write the route manifest as JSON
 *   --no-route-wrappers   Skip the per-route cmd_ / qry_ SQL functions
 *   --external <ids>      Extra comma-separated esbuild externals
 *   --alias <pairs>       Extra esbuild aliases, comma-separated name=target
 *                         (e.g. --alias ceves=/abs/path/to/ceves)
 *   --quiet               Don't print the success lines
 *
 * Requires `esbuild` to be installed in the app (optional peer dependency).
 */
import { writeFileSync, mkdirSync, rmSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const HERE = dirname(fileURLToPath(import.meta.url));
// The repo already ships an inert `cloudflare:workers` stand-in for running
// outside the Workers runtime (added for the NATS adapter). PLV8 needs the
// same thing, so reuse it rather than shipping a second copy.
const CF_SHIM = join(HERE, '..', 'dist', 'adapters', 'nats', 'cloudflare-workers-shim.js');
const SQLGEN = join(HERE, '..', 'dist', 'adapters', 'pg', 'index.js');

/**
 * Minimal host shims prepended to the PLV8 bundle. PLV8 is bare V8: no
 * console, no process. workerkit's logger writes through console, so map it
 * to plv8.elog (NOTICE for info-and-below, WARNING for warn/error — ERROR
 * would abort the transaction).
 */
const PLV8_BANNER = `/* ceves-generate-pg: PLV8 host shims */
var globalThis = (0, eval)('this');
if (typeof globalThis.console === 'undefined') {
  var __plv8log = function (level) {
    return function () {
      try {
        var parts = [];
        for (var i = 0; i < arguments.length; i++) {
          var a = arguments[i];
          parts.push(typeof a === 'string' ? a : JSON.stringify(a));
        }
        plv8.elog(level, parts.join(' '));
      } catch (e) { /* logging must never break dispatch */ }
    };
  };
  globalThis.console = {
    log: __plv8log(NOTICE), info: __plv8log(NOTICE), debug: __plv8log(NOTICE),
    warn: __plv8log(WARNING), error: __plv8log(WARNING), trace: __plv8log(NOTICE),
  };
}
if (typeof globalThis.process === 'undefined') {
  globalThis.process = { env: {} };
}
`;

function fail(message) {
  console.error(`[ceves-generate-pg] ${message}`);
  process.exit(1);
}

async function loadEsbuild() {
  try {
    return await import('esbuild');
  } catch {
    fail(
      'esbuild is required but not installed. Add it to the app devDependencies: pnpm add -D esbuild',
    );
  }
}

async function main() {
  const { values } = parseArgs({
    options: {
      entry: { type: 'string' },
      out: { type: 'string', default: 'ceves-pg.sql' },
      'module-name': { type: 'string', default: 'ceves-app' },
      'pg-schema': { type: 'string', default: 'ceves' },
      manifest: { type: 'string' },
      'no-route-wrappers': { type: 'boolean', default: false },
      external: { type: 'string', default: '' },
      alias: { type: 'string', default: '' },
      quiet: { type: 'boolean', default: false },
    },
  });

  if (!values.entry) fail('--entry <path> is required (the app PG entry file)');
  const entry = resolve(process.cwd(), values.entry);
  const outFile = resolve(process.cwd(), values.out);
  const esbuild = await loadEsbuild();

  const externals = values.external.split(',').map((s) => s.trim()).filter(Boolean);
  const alias = { 'cloudflare:workers': CF_SHIM };
  for (const pair of values.alias.split(',').map((s) => s.trim()).filter(Boolean)) {
    const eq = pair.indexOf('=');
    if (eq <= 0) fail(`--alias entries must be name=target (got "${pair}")`);
    alias[pair.slice(0, eq)] = pair.slice(eq + 1);
  }
  const sharedBuild = {
    entryPoints: [entry],
    bundle: true,
    write: false,
    target: 'es2022',
    logLevel: 'silent',
    alias,
    external: externals,
    define: { 'process.env.NODE_ENV': '"production"' },
  };

  // 1. The PLV8 module: single IIFE, neutral platform, host shims prepended.
  let plv8Result;
  try {
    // platform 'browser' resolves dependencies to their portable builds
    // (no `node:*` imports) — PLV8 is bare V8, closest to a browser without
    // DOM. Anything still touching Web/Node APIs at RUNTIME (crypto, fetch)
    // must not be reached from PG-dispatched handlers.
    plv8Result = await esbuild.build({
      ...sharedBuild,
      format: 'iife',
      platform: 'browser',
      banner: { js: PLV8_BANNER },
    });
  } catch (error) {
    fail(`PLV8 bundle failed: ${error.message}`);
  }
  const moduleSource = plv8Result.outputFiles[0].text;

  // 2. Node build of the same entry → import it → read the route manifest.
  const tempDir = mkdtempSync(join(tmpdir(), 'ceves-pg-'));
  let manifest;
  try {
    const nodeOut = join(tempDir, 'entry.mjs');
    await esbuild.build({
      ...sharedBuild,
      write: true,
      outfile: nodeOut,
      format: 'esm',
      platform: 'node',
    });
    await import(pathToFileURL(nodeOut).href);
    const handle = globalThis.__ceves_pg__;
    if (!handle || typeof handle.manifest !== 'function') {
      fail(
        'The entry did not install the dispatcher. It must call installPlv8Dispatcher() ' +
          "from 'ceves/pg' (after importing the decorator barrel).",
      );
    }
    manifest = handle.manifest();
  } catch (error) {
    fail(`Manifest collection failed: ${error.message}`);
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }

  if (manifest.length === 0) {
    fail(
      'No command/query routes found. The entry must import the decorator barrel ' +
        "(e.g. `import './_decoratorImports.generated'`) before installPlv8Dispatcher().",
    );
  }

  // 3. Emit the SQL script.
  const { generateFullSql } = await import(pathToFileURL(SQLGEN).href);
  const sql = generateFullSql({
    moduleSource,
    manifest,
    options: { schema: values['pg-schema'], moduleName: values['module-name'] },
    skipRouteWrappers: values['no-route-wrappers'],
  });

  mkdirSync(dirname(outFile), { recursive: true });
  writeFileSync(outFile, sql);
  if (values.manifest) {
    const manifestFile = resolve(process.cwd(), values.manifest);
    mkdirSync(dirname(manifestFile), { recursive: true });
    writeFileSync(manifestFile, `${JSON.stringify(manifest, null, 2)}\n`);
  }

  if (!values.quiet) {
    const commands = manifest.filter((m) => m.kind === 'command').length;
    const queries = manifest.length - commands;
    console.log(
      `[ceves-generate-pg] wrote ${outFile} (${commands} commands, ${queries} queries, ` +
        `module ${Math.round(moduleSource.length / 1024)} KiB, schema "${values['pg-schema']}")`,
    );
  }
}

main().catch((error) => fail(error.stack ?? String(error)));
