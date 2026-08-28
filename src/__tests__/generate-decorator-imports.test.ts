/**
 * Tests for `ceves-generate-imports`.
 *
 * The generator had no tests at all, and every defect it carried had the same
 * shape: emit an INCOMPLETE barrel, print a success line, exit 0. Because
 * `createRouter` filters on REGISTERED_ROUTES, a class missing from the barrel
 * is a route missing from production — so the cases below are all about
 * "did it either register everything, or fail loudly?".
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { execFileSync } from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const GENERATOR = path.resolve(HERE, '../../bin/generate-decorator-imports.mjs');
const REPO_NODE_MODULES = path.resolve(HERE, '../../node_modules');

let tmp: string;

/** Run the generator in `tmp`; returns { status, stdout, stderr }. */
function run(args: string[]): { status: number; out: string } {
  try {
    const out = execFileSync(process.execPath, [GENERATOR, ...args], {
      cwd: tmp,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return { status: 0, out };
  } catch (err) {
    const e = err as { status?: number; stdout?: string; stderr?: string };
    return { status: e.status ?? 1, out: `${e.stdout ?? ''}${e.stderr ?? ''}` };
  }
}

function write(rel: string, content: string): void {
  const abs = path.join(tmp, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content);
}

beforeEach(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ceves-barrel-'));
  fs.writeFileSync(path.join(tmp, 'package.json'), '{"name":"fixture","private":true}');
  // The generator resolves `typescript` from the project being generated.
  fs.symlinkSync(REPO_NODE_MODULES, path.join(tmp, 'node_modules'), 'dir');
});

afterEach(() => fs.rmSync(tmp, { recursive: true, force: true }));

describe('class detection', () => {
  it('registers EVERY decorated class in a file, not just the first', () => {
    write('src/routes/Two.ts', '@Route({})\nexport class AlphaRoute {}\n\n@Route({})\nexport class BetaRoute {}\n');
    const { status } = run(['--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts']);
    expect(status).toBe(0);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    expect(barrel).toContain("import { AlphaRoute } from './routes/Two';");
    expect(barrel).toContain("import { BetaRoute } from './routes/Two';");
  });

  it('keeps `export default class` in the registered array via a default import', () => {
    write('src/routes/Def.ts', '@Route({})\nexport default class DefRoute {}\n');
    run(['--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts']);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    // A NAMED import of a default export does not resolve...
    expect(barrel).not.toContain('{ DefRoute }');
    // ...and a bare side-effect import would run the decorator but leave the
    // class out of REGISTERED_ROUTES, which createRouter filters on — so the
    // route would silently disappear. It must be a default import, in the array.
    expect(barrel).toContain("import DefRoute from './routes/Def';");
    expect(barrel).toMatch(/REGISTERED_ROUTES = \[\n {2}DefRoute,/);
  });

  it('gives an anonymous default export a derived identifier so it still registers', () => {
    write('src/routes/anon.ts', '@Route({})\nexport default class {}\n');
    run(['--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts']);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    expect(barrel).toMatch(/import RoutesAnon from '\.\/routes\/anon';/);
    expect(barrel).toMatch(/REGISTERED_ROUTES = \[\n {2}RoutesAnon,/);
  });

  it('is not fooled by a decorator inside a string literal or comment', () => {
    write('src/routes/Decoy.ts', 'export const help = "@Route({}) /* */ export class NotReal {}";\n');
    run(['--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts']);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    expect(barrel).toContain("import './routes/Decoy';");
    expect(barrel).not.toContain('NotReal');
  });

  it('never walks node_modules', () => {
    write('src/routes/Real.ts', '@Route({})\nexport class RealRoute {}\n');
    write('src/node_modules/pkg/routes/Vendor.ts', '@Route({})\nexport class VendorRoute {}\n');
    run(['--src', 'src', '--pattern', '**/routes/*.ts', '--out', 'out.ts']);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    expect(barrel).toContain('RealRoute');
    expect(barrel).not.toContain('VendorRoute');
  });
});

describe('globbing', () => {
  it('supports a pattern with no {a,b} alternation at all', () => {
    write('src/routes/A.ts', '@Route({})\nexport class ARoute {}\n');
    const { status } = run(['--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts']);

    expect(status).toBe(0);
    expect(fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8')).toContain('ARoute');
  });

  it('honours glob depth: a single * does not match nested directories', () => {
    write('src/routes/Shallow.ts', '@Route({})\nexport class ShallowRoute {}\n');
    write('src/routes/deep/Deep.ts', '@Route({})\nexport class DeepRoute {}\n');
    run(['--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts']);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    expect(barrel).toContain('ShallowRoute');
    expect(barrel).not.toContain('DeepRoute');
  });

  it('routes files under the handler dir into the handlers bucket', () => {
    write('src/domain/x/events/H.ts', '@EventHandler\nexport class XHandler {}\n');
    write('src/domain/x/commands/C.ts', '@Route({})\nexport class CRoute {}\n');
    run(['--src', 'src', '--pattern', 'domain/**/{commands,events}/*.ts', '--out', 'out.ts']);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    expect(barrel).toMatch(/REGISTERED_ROUTES = \[\n {2}CRoute,/);
    expect(barrel).toMatch(/REGISTERED_EVENT_HANDLERS = \[\n {2}XHandler,/);
  });
});

describe('fails loudly instead of emitting a silently incomplete barrel', () => {
  it('errors when the pattern matches nothing', () => {
    write('src/routes/A.ts', '@Route({})\nexport class ARoute {}\n');
    const { status, out } = run(['--src', 'src', '--pattern', 'nope/*.ts', '--out', 'out.ts']);

    expect(status).toBe(1);
    expect(out).toMatch(/matched no files/);
  });

  it('errors on a --src that does not exist', () => {
    const { status, out } = run(['--src', 'typo', '--out', 'out.ts']);
    expect(status).toBe(1);
    expect(out).toMatch(/does not exist/);
  });

  it('errors on duplicate exported class names that would not compile', () => {
    write('src/domain/a/commands/CreateRoute.ts', '@Route({})\nexport class CreateRoute {}\n');
    write('src/domain/b/commands/CreateRoute.ts', '@Route({})\nexport class CreateRoute {}\n');
    const { status, out } = run(['--src', 'src', '--pattern', 'domain/**/commands/*.ts', '--out', 'out.ts']);

    expect(status).toBe(1);
    expect(out).toMatch(/duplicate exported class name "CreateRoute"/);
  });

  it('errors on an unknown flag rather than ignoring it', () => {
    write('src/routes/A.ts', '@Route({})\nexport class ARoute {}\n');
    const { status, out } = run(['--src', 'src', '--patern', 'routes/*.ts', '--out', 'out.ts']);

    expect(status).toBe(1);
    expect(out).toMatch(/unknown option --patern/);
  });

  it('allows an empty barrel only when explicitly asked', () => {
    write('src/routes/A.ts', '@Route({})\nexport class ARoute {}\n');
    const { status } = run(['--src', 'src', '--pattern', 'nope/*.ts', '--out', 'out.ts', '--allow-empty']);
    expect(status).toBe(0);
  });
});

describe('output', () => {
  it('is idempotent — a second run reports up-to-date and does not rewrite', () => {
    write('src/routes/A.ts', '@Route({})\nexport class ARoute {}\n');
    const args = ['--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts'];
    run(args);
    const first = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');

    const second = run(args);
    expect(second.out).toMatch(/up-to-date/);
    expect(fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8')).toBe(first);
  });

  it('defaults --out relative to --src instead of a fixed src/ literal', () => {
    write('app/routes/A.ts', '@Route({})\nexport class ARoute {}\n');
    const { status } = run(['--src', 'app', '--pattern', 'routes/*.ts']);

    expect(status).toBe(0);
    expect(fs.existsSync(path.join(tmp, 'app', '_decoratorImports.generated.ts'))).toBe(true);
  });

  it('honours custom decorator and export names', () => {
    write('src/routes/A.ts', '@Controller({})\nexport class AController {}\n');
    run([
      '--src', 'src', '--pattern', 'routes/*.ts', '--out', 'out.ts',
      '--route-decorator', 'Controller', '--routes-export', 'REGISTERED_CONTROLLERS',
    ]);

    const barrel = fs.readFileSync(path.join(tmp, 'out.ts'), 'utf8');
    expect(barrel).toContain("import { AController } from './routes/A';");
    expect(barrel).toMatch(/REGISTERED_CONTROLLERS = \[\n {2}AController,/);
  });
});
