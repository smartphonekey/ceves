#!/usr/bin/env node
/**
 * ceves-generate-imports — emit a static barrel of decorator-bearing modules.
 *
 * Why this exists
 * ---------------
 * Decorator registration (`@Route`, `@EventHandler`) only happens when the
 * file containing the class is imported. Doing that with
 * `import.meta.glob({ eager: true })` works under Vite (which expands the glob
 * into static imports at build time) but silently expands to `{}` under
 * wrangler's esbuild bundler — which dropped every route in production
 * deploys. A checked-in static barrel works in every bundler.
 *
 * Implementation notes
 * --------------------
 * Globbing is delegated to `tinyglobby` and class detection to the TypeScript
 * compiler's own parser. Both used to be hand-rolled here, and both were
 * wrong in ways that silently produced an INCOMPLETE barrel with exit 0 —
 * i.e. exactly the `import.meta.glob → {}` failure this tool exists to
 * prevent. Specifically, the old code:
 *   - discarded path structure in `--pattern` (a single `*` matched any depth)
 *     and emitted 0 imports for a pattern with no `{a,b}` group at all;
 *   - used `.match()` not `.matchAll()`, so a file declaring two decorated
 *     classes registered only the first;
 *   - emitted `import { X }` for `export default class X`, which does not
 *     resolve;
 *   - emitted duplicate identifiers for same-named classes in different
 *     folders, producing a barrel that does not compile;
 *   - walked symlinks and node_modules;
 *   - stripped comments by naive string replacement, so a string literal
 *     containing a comment token could swallow a real decorator.
 * Since `createRouter` filters on `REGISTERED_ROUTES`, a class dropped from
 * the barrel is a route missing from production — so these now fail loudly.
 *
 * CLI flags (all optional)
 * ------------------------
 *   --src <dir>          Directory holding the worker source (default: "src")
 *   --pattern <glob>     Glob beneath <src>. Full glob syntax (tinyglobby).
 *   --out <path>         Output file (default: "<src>/_decoratorImports.generated.ts")
 *   --cwd <dir>          Resolve relative paths against this dir
 *   --route-decorator    Decorator marking a route class (default: "Route")
 *   --handler-decorator  Decorator marking an event handler (default: "EventHandler")
 *   --handler-dir        Parent folder name that means "handler" (default: "events")
 *   --routes-export      Name of the routes array (default: "REGISTERED_ROUTES")
 *   --handlers-export    Name of the handlers array (default: "REGISTERED_EVENT_HANDLERS")
 *   --allow-empty        Exit 0 when the pattern matches nothing (default: error)
 *   --quiet              Don't print the success line
 *
 * Idempotent: running twice produces byte-identical output (entries sorted,
 * no timestamp) and skips writing when content is unchanged.
 */
import { writeFileSync, readFileSync, existsSync } from 'node:fs';
import { join, relative, resolve, isAbsolute, dirname, basename, sep } from 'node:path';
import { createRequire } from 'node:module';
import { globSync } from 'tinyglobby';

const DEFAULT_SRC = 'src';
const DEFAULT_PATTERN = 'domain/**/{commands,routes,events}/*.{ts,tsx}';
const DEFAULT_OUT_BASENAME = '_decoratorImports.generated.ts';

const FLAGS = {
  '--src': 'src',
  '--pattern': 'pattern',
  '--out': 'out',
  '--cwd': 'cwd',
  '--route-decorator': 'routeDecorator',
  '--handler-decorator': 'handlerDecorator',
  '--handler-dir': 'handlerDir',
  '--routes-export': 'routesExport',
  '--handlers-export': 'handlersExport',
};

function fail(message) {
  process.stderr.write(`[ceves-generate-imports] ERROR: ${message}\n`);
  process.exit(1);
}

/** Parse argv. Unknown flags are an error — a typo used to yield an empty barrel. */
function parseArgs(argv) {
  const opts = { quiet: false, allowEmpty: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--quiet') {
      opts.quiet = true;
    } else if (a === '--allow-empty') {
      opts.allowEmpty = true;
    } else if (a === '-h' || a === '--help') {
      printHelpAndExit();
    } else if (FLAGS[a]) {
      const value = argv[++i];
      if (value === undefined || value.startsWith('--')) {
        fail(`${a} expects a value, got ${value === undefined ? '(missing)' : `"${value}"`}`);
      }
      opts[FLAGS[a]] = value;
    } else {
      fail(`unknown option ${a} (see --help)`);
    }
  }
  return opts;
}

function printHelpAndExit() {
  process.stdout.write(
    [
      'ceves-generate-imports — emit a static barrel of decorator-bearing modules',
      '',
      'Options:',
      `  --src <dir>          Source directory (default: ${DEFAULT_SRC})`,
      `  --pattern <glob>     Glob beneath --src (default: ${DEFAULT_PATTERN})`,
      `  --out <path>         Output file (default: <src>/${DEFAULT_OUT_BASENAME})`,
      '  --cwd <dir>          Resolve relative paths against this dir (default: cwd)',
      '  --route-decorator    Decorator marking a route class (default: Route)',
      '  --handler-decorator  Decorator marking an event handler (default: EventHandler)',
      '  --handler-dir        Parent folder meaning "handler" (default: events)',
      '  --routes-export      Routes array name (default: REGISTERED_ROUTES)',
      '  --handlers-export    Handlers array name (default: REGISTERED_EVENT_HANDLERS)',
      '  --allow-empty        Exit 0 when nothing matches (default: error)',
      '  --quiet              Suppress success log line',
      '  -h, --help           Show this help',
      '',
    ].join('\n'),
  );
  process.exit(0);
}

/**
 * Load the TypeScript compiler from the consuming project.
 *
 * Resolved from the project being generated (not from ceves) so the barrel is
 * parsed by the same compiler that will compile it.
 */
function loadTypeScript(cwd) {
  for (const base of [join(cwd, 'noop.js'), import.meta.url]) {
    try {
      return createRequire(base)('typescript');
    } catch {
      // try the next resolution base
    }
  }
  fail(
    'cannot resolve "typescript". It is needed to detect decorated classes — ' +
      'install it in this project (npm i -D typescript).',
  );
}

/** The bare name of a decorator: `@Route({...})` and `@EventHandler` both → "Route"/"EventHandler". */
function decoratorName(decorator, ts) {
  const expr = ts.isCallExpression(decorator.expression)
    ? decorator.expression.expression
    : decorator.expression;
  return ts.isIdentifier(expr) ? expr.text : undefined;
}

/**
 * Every exported class in `absPath` carrying `wanted` as a decorator.
 *
 * Uses the real parser, so comments and string literals can never be mistaken
 * for code, stacked decorators work, and `export default class` is reported
 * as such, so the caller can emit a DEFAULT import for it. Emitting a named
 * import would not resolve; emitting only a side-effect import would run the
 * decorator but leave the class out of the registered-routes array, and
 * `createRouter` registers only classes present in that array — so the route
 * would silently disappear.
 */
function findDecoratedClasses(absPath, wanted, ts) {
  let src;
  try {
    src = readFileSync(absPath, 'utf8');
  } catch {
    return [];
  }
  const sourceFile = ts.createSourceFile(
    absPath,
    src,
    ts.ScriptTarget.Latest,
    /* setParentNodes */ true,
    absPath.endsWith('.tsx') ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
  );

  const found = [];
  for (const statement of sourceFile.statements) {
    if (!ts.isClassDeclaration(statement)) continue;

    const modifiers = ts.canHaveModifiers(statement) ? (ts.getModifiers(statement) ?? []) : [];
    if (!modifiers.some((m) => m.kind === ts.SyntaxKind.ExportKeyword)) continue;

    const decorators = ts.canHaveDecorators(statement) ? (ts.getDecorators(statement) ?? []) : [];
    if (!decorators.some((d) => decoratorName(d, ts) === wanted)) continue;

    const isDefault = modifiers.some((m) => m.kind === ts.SyntaxKind.DefaultKeyword);
    found.push({ name: statement.name?.text, isDefault });
  }
  return found;
}

/** `./domain/x/foo-route` → `DomainXFooRoute`, for anonymous default exports. */
function identifierFromSpec(spec) {
  const ident = spec
    .replace(/^\.\//, '')
    .split(/[^A-Za-z0-9]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
  return /^[0-9]/.test(ident) ? `_${ident}` : ident;
}

/** `<srcDir>/domain/x/Foo.ts` → `./domain/x/Foo` (POSIX separators, no extension). */
function toImportSpec(absPath, srcDir) {
  const rel = relative(srcDir, absPath).split(sep).join('/');
  return `./${rel.replace(/\.(ts|tsx)$/, '')}`;
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const cwd = opts.cwd ? resolve(opts.cwd) : process.cwd();

  const srcRel = opts.src ?? DEFAULT_SRC;
  const pattern = opts.pattern ?? DEFAULT_PATTERN;
  const srcDir = isAbsolute(srcRel) ? srcRel : resolve(cwd, srcRel);

  // --out defaults RELATIVE TO --src. It used to be a fixed literal
  // "src/_decoratorImports.generated.ts" resolved independently, so
  // `--src app` wrote the barrel into a src/ that may not exist (raw ENOENT)
  // while computing every import specifier relative to app/.
  const outRel = opts.out ?? join(srcRel, DEFAULT_OUT_BASENAME);
  const outFile = isAbsolute(outRel) ? outRel : resolve(cwd, outRel);

  const routeDecorator = opts.routeDecorator ?? 'Route';
  const handlerDecorator = opts.handlerDecorator ?? 'EventHandler';
  const handlerDir = opts.handlerDir ?? 'events';
  const routesExport = opts.routesExport ?? 'REGISTERED_ROUTES';
  const handlersExport = opts.handlersExport ?? 'REGISTERED_EVENT_HANDLERS';

  if (!existsSync(srcDir)) {
    fail(`--src directory does not exist: ${srcDir}`);
  }

  const ts = loadTypeScript(cwd);

  // Real glob engine: honours `**` vs `*` depth, `{a,b}` alternation anywhere
  // (or none at all), and never descends into node_modules or follows symlinks.
  const matches = globSync(pattern, {
    cwd: srcDir,
    absolute: true,
    onlyFiles: true,
    followSymbolicLinks: false,
    ignore: ['**/node_modules/**', '**/dist/**', '**/*.d.ts'],
  });

  if (matches.length === 0 && !opts.allowEmpty) {
    fail(
      `pattern matched no files: "${pattern}" under ${srcDir}\n` +
        '  A barrel with no imports means NO decorators register at runtime — the\n' +
        '  exact failure this tool exists to prevent. Fix the pattern, or pass\n' +
        '  --allow-empty if an empty barrel really is intended.',
    );
  }

  const entries = matches
    .map((absPath) => {
      const isHandler = basename(dirname(absPath)) === handlerDir;
      const wanted = isHandler ? handlerDecorator : routeDecorator;
      const classes = findDecoratedClasses(absPath, wanted, ts);
      const spec = toImportSpec(absPath, srcDir);
      return {
        spec,
        kind: isHandler ? 'handler' : 'route',
        // Anonymous `export default class {}` has no name to bind; fall back to
        // one derived from the file so it can still enter the array.
        classes: classes.map((c) => ({
          name: c.name ?? identifierFromSpec(spec),
          isDefault: c.isDefault,
        })),
      };
    })
    .sort((a, b) => a.spec.localeCompare(b.spec));

  const routeClasses = [];
  const handlerClasses = [];
  const sideEffectOnly = [];
  for (const entry of entries) {
    if (entry.classes.length === 0) {
      sideEffectOnly.push(entry.spec);
      continue;
    }
    const bucket = entry.kind === 'handler' ? handlerClasses : routeClasses;
    for (const c of entry.classes) {
      bucket.push({ spec: entry.spec, className: c.name, isDefault: c.isDefault });
    }
  }

  // A duplicate identifier produces a barrel that does not compile
  // ("Identifier already declared"). This used to be emitted silently with a
  // success message; it is near-certain in any app with per-domain folders.
  const byName = new Map();
  for (const { spec, className } of [...routeClasses, ...handlerClasses]) {
    const seen = byName.get(className);
    if (seen) fail(`duplicate exported class name "${className}" in ${seen} and ${spec}.\n  A barrel cannot name-import both. Rename one, or exclude it via --pattern.`);
    byName.set(className, spec);
  }

  writeBarrel(outFile, { routeClasses, handlerClasses, sideEffectOnly }, {
    routeDecorator, handlerDecorator, routesExport, handlersExport, quiet: opts.quiet,
  });
}

function writeBarrel(outFile, { routeClasses, handlerClasses, sideEffectOnly }, cfg) {
  const { routeDecorator, handlerDecorator, routesExport, handlersExport, quiet } = cfg;

  const banner = `// AUTO-GENERATED by ceves-generate-imports — DO NOT EDIT.
//
// Static barrel that imports every @${routeDecorator} / @${handlerDecorator} file under src/domain
// so module-evaluation side effects (decorator registration) fire at startup.
//
// Replaces the previous \`import.meta.glob('./domain/**/{commands,routes,events}/*.{ts,tsx}', { eager: true })\`
// — wrangler's bundler does not expand glob imports, which silently dropped every
// route in production deploys. A static barrel works in every bundler.
//
// Class files use named imports + the \`${routesExport}\` /
// \`${handlersExport}\` arrays below so static-analysis tools (knip,
// ts-prune) see each class as by-name-referenced. Plain-function modules
// (the few files under routes/ that export \`handleX()\` instead of a class)
// keep side-effect imports — their functions are already named-imported
// from the worker entry, so they need no extra knip lure.
//
// Regenerate with:  npx ceves-generate-imports
// Hooked into:      prebuild (so \`npm run build\` / \`npm run deploy\` pick up changes).
`;

  const importLine = (c) =>
    c.isDefault
      ? `import ${c.className} from '${c.spec}';`
      : `import { ${c.className} } from '${c.spec}';`;
  const namedImports = [...routeClasses.map(importLine), ...handlerClasses.map(importLine)];
  const sideEffectImports = sideEffectOnly.map((spec) => `import '${spec}';`);

  const routesArray = `export const ${routesExport} = [\n${routeClasses
    .map((r) => `  ${r.className},`)
    .join('\n')}\n] as const;`;
  const handlersArray = `export const ${handlersExport} = [\n${handlerClasses
    .map((h) => `  ${h.className},`)
    .join('\n')}\n] as const;`;

  const out = `${banner}\n${[...namedImports, ...sideEffectImports].join(
    '\n',
  )}\n\n${routesArray}\n\n${handlersArray}\n`;

  const totalCount = routeClasses.length + handlerClasses.length + sideEffectOnly.length;
  const summary = `${routeClasses.length} routes, ${handlerClasses.length} handlers, ${sideEffectOnly.length} side-effect`;

  if (existsSync(outFile) && readFileSync(outFile, 'utf8') === out) {
    if (!quiet) {
      // eslint-disable-next-line no-console
      console.log(`[ceves-generate-imports] up-to-date (${summary})`);
    }
    return;
  }

  writeFileSync(outFile, out, 'utf8');
  if (!quiet) {
    // eslint-disable-next-line no-console
    console.log(
      `[ceves-generate-imports] wrote ${totalCount} imports to ${relative(process.cwd(), outFile) || outFile} (${summary})`,
    );
  }
}

main();
