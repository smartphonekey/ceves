# Ceves

[![TypeScript](https://img.shields.io/badge/TypeScript-5.0%2B-blue)](https://www.typescriptlang.org/)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-orange)](https://workers.cloudflare.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Event-sourcing framework for Cloudflare Workers. An aggregate is a Durable
Object; its state is rebuilt by replaying events from R2; commands and queries
are decorated route classes with automatic OpenAPI docs.

The same application code also runs on **AWS Lambda** (S3 event log, via the
`./aws` subpath export), on **self-hosted NATS** (JetStream event log +
NATS KV state, via the `./nats` subpath export), and on **PostgreSQL**
(handlers compiled into PLV8 functions, state in a table, via the `./pg`
subpath export) — only the entry point differs per runtime. Cloudflare
Workers and NATS are the exercised targets.

## Install

```bash
npm install @sydorenkoalex/ceves
```

TypeScript projects targeting Cloudflare Workers also need the Workers types
(the root entry's declarations reference them):

```bash
npm install -D @cloudflare/workers-types
```

Code samples below import from `ceves` — alias the package if you prefer the
short specifier (`"ceves": "npm:@sydorenkoalex/ceves"` in `dependencies`),
or import from `@sydorenkoalex/ceves` directly. The bundled example uses
`"ceves": "file:.."`, which is why its imports read `from 'ceves'`.

## The API

```
@Route({ method, path })  +  CreateCommandRoute | CommandRoute | QueryRoute
@EventHandler             +  IEventHandler<TState, TEventData>
AggregateObject              the Durable Object base class
createRouter(options)        Hono + Chanfana router over every @Route class
```

| Base class | Use when | `execute*` signature |
| --- | --- | --- |
| `CreateCommandRoute` | the aggregate does not exist yet | `(command, env)` |
| `CommandRoute` | the aggregate must already exist | `(command, state, env)` |
| `QueryRoute` | read-only, emits no event | `executeQuery(state, query, c)` |

The framework enforces the create/update split, so you never write
`if (state) throw …` yourself. Note the create semantics are **idempotent, not
409** (AA-92): sending a `CreateCommandRoute` command at an aggregate that
already exists is a no-op returning **200 with `event: null`** (the NO_EVENT
response shape); a genuine create returns **201** with the event. The old 409
was removed because every idempotent caller caught and error-logged it,
burying real bugs under thousands of expected-outcome errors a week.
See `AggregateObject.ts` → "AA-92".

> **Retired APIs.** `CevesApp`, `BaseCommand`, `BaseEvent`, `@CommandHandler`,
> `@QueryHandler`, and `@EventHandler(...)` with arguments no longer exist.
> Anything showing them is stale — fix it or delete it.

## Runtimes

**Cloudflare Workers** (primary): aggregates are Durable Objects, the event
log is R2, state persists in `DurableObjectState.storage`. See
[`example/`](example/).

**NATS** (`ceves/nats`): the event log is a JetStream stream, state lives in
a NATS KV bucket with revision CAS, commands travel as NATS request-reply
messages between a thin REST gateway and a queue-group aggregate service.
Includes a home-org partitioned event log with claim-proof routing and org
transfer ("selling" an aggregate to another tenant). Install the transports:

```bash
npm install @nats-io/transport-node @nats-io/jetstream @nats-io/kv
```

Read [docs/architecture/nats.md](docs/architecture/nats.md), then run the
bank example on NATS: `example/src/nats-main.ts` +
`example/scripts/nats-e2e.sh`.

**PostgreSQL / PLV8** (`ceves/pg`): the command and query handlers run
*inside the database*. `ceves-generate-pg` bundles them into one JavaScript
module and emits an idempotent SQL script — `ceves.execute_command` /
`ceves.execute_query` PLV8 functions, a `ceves.aggregate_state` table, and
one `cmd_*` / `qry_*` function per route so a REST extension (PostgREST) can
serve `/rpc/...` directly. A thin Hono wrapper (`registerPgRoutes`) keeps the
same OpenAPI endpoints on Workers or Node.

The event log still lives in R2/S3 — never in PostgreSQL — but the emitted
event is committed to a **transactional outbox** in the same transaction as
the state write, so delivery to that log is guaranteed rather than
best-effort. Fire-and-forget `fetch` calls made by handlers are intercepted
into the same outbox (PLV8 has no network) and delivered post-commit by
`drainPgOutbox` with retries. Requires the PLV8 extension (>= 3.1) and
`esbuild` where the generator runs.

```bash
npx ceves-generate-pg --entry src/pg-entry.ts --out ceves-pg.sql
psql -f ceves-pg.sql
```

Read [docs/architecture/postgresql.md](docs/architecture/postgresql.md), then
see `example/src/pg-entry.ts` (`npm run gen:pg` in `example/`).

**AWS Lambda** (`ceves/aws`): S3 event store + optional S3 snapshot store,
`createLambdaHandler` adapts the router to API Gateway. Install
`@aws-sdk/client-s3`.

## Start from the example

[`example/`](example/) is a complete, runnable BankAccount aggregate covering
the Durable Object, all three route base classes, decorator registration,
local dev, and the NATS runtime entry point. Read
[`example/README.md`](example/README.md) before writing a new command.

## Multitenancy

`ITenantResolver` implementations resolve the caller's organization:

- `HeaderTenantResolver` — trust an `X-Org-Id`-style header set by an
  upstream auth gateway.
- `ApiKeyTenantResolver` — look the API key up in a D1 table:

```sql
CREATE TABLE api_keys (
  api_key TEXT PRIMARY KEY,
  org_id  TEXT NOT NULL,
  revoked INTEGER NOT NULL DEFAULT 0
);
```

Authorization beyond tenancy belongs on the aggregate: override
`checkAuthorization(request)` (throw `UnauthorizedError` → 401,
`ForbiddenError` → 403).

## Development

```bash
npm install
npm run build             # tsup → dist/ (ESM + d.ts, code-split chunks)
npm test                  # unit + integration projects
npm run test:unit
npm run test:integration  # R2 store against Miniflare (vitest-pool-workers)
npm run lint
```

The live NATS suite (`src/adapters/nats/__tests__/NatsEventStore.live.test.ts`)
is skipped unless `NATS_TEST_URL` points at a running `nats-server -js`.

`@cloudflare/workers-types` is pinned exactly (not `^`): newer snapshots of
that package have caused pathological type-check blowups against this
codebase, so bump it deliberately and re-run `npx tsc --noEmit` before
committing the bump.

## Decisions

- [ADR-009](docs/adr/ADR-009.md) — event handlers always receive a state value,
  never `null`; the first event gets the empty state.
- [docs/architecture/nats.md](docs/architecture/nats.md) — the NATS runtime:
  subject naming, OCC and version-continuity guards, org directory,
  transfer semantics, known limits.
- [docs/architecture/postgresql.md](docs/architecture/postgresql.md) — the
  PostgreSQL/PLV8 runtime: in-database dispatch, state row + version guard,
  the transactional outbox (guaranteed event delivery), and what cannot run
  inside the database.

## Decorator Registration (`ceves-generate-imports`)

Ceves uses class decorators (`@Route`, `@EventHandler`) that register handlers
in a global registry as a **side effect of module evaluation**. Because of
that, every decorated file in your worker must be imported at startup —
otherwise its decorator never runs and the route/handler is silently missing
from the registry.

The traditional fix is `import.meta.glob({ eager: true })`, but that's a
Vite-only feature. Wrangler's esbuild bundler expands it to an empty object,
so every decorated route is silently dropped from production deploys. To work
around this Ceves ships a tiny code generator: **`ceves-generate-imports`**.
It walks your source tree and emits a static barrel of one
`import './…';` per matching file. Plain static imports are handled
identically by every bundler.

### Wiring it into your worker

Add the CLI as a `prebuild` / `predev` hook in your worker's `package.json`
so the barrel is regenerated automatically before every build, dev run, or
deploy:

```jsonc
{
  "scripts": {
    "gen:decorator-imports": "ceves-generate-imports",
    "predev": "ceves-generate-imports",
    "prebuild": "ceves-generate-imports",
    "predeploy": "ceves-generate-imports"
  }
}
```

Then import the generated barrel **once** from your worker entry, before
`createRouter()` runs:

```typescript
// src/index.ts
import { createRouter } from 'ceves';

// Generated barrel — registers every @Route / @EventHandler in the project
// and exports them as REGISTERED_ROUTES / REGISTERED_EVENT_HANDLERS.
import { REGISTERED_ROUTES } from './_decoratorImports.generated';

export { /* your DO classes */ };
export default createRouter({
  // Explicit route surface: only barrel classes are exposed, and a class
  // missing its @Route metadata fails loudly at startup.
  routes: REGISTERED_ROUTES,
  /* ... */
});
```

Add the generated file to your `.gitignore`:

```
src/_decoratorImports.generated.ts
```

The CLI is **idempotent**: it skips writing when the output is unchanged, so
running it on every build is cheap and safe.

### CLI flags

All flags are optional. The defaults match a domain-oriented layout
(`src/domain/<aggregate>/{commands,routes,events}/*.{ts,tsx}`).

| Flag | Default | Purpose |
| ---- | ------- | ------- |
| `--src <dir>` | `src` | Source directory holding the worker code. |
| `--pattern <glob>` | `domain/**/{commands,routes,events}/*.{ts,tsx}` | Glob beneath `--src`. Full glob syntax via [tinyglobby](https://www.npmjs.com/package/tinyglobby) — `**` vs `*` depth is honoured, `{a,b}` groups may appear anywhere or not at all. |
| `--out <path>` | `<src>/_decoratorImports.generated.ts` | Output file path, relative to cwd. Defaults **relative to `--src`**. |
| `--cwd <dir>` | `process.cwd()` | Resolve relative paths against this dir. Useful when invoking from a different directory in a script. |
| `--route-decorator <name>` | `Route` | Decorator that marks a route class. |
| `--handler-decorator <name>` | `EventHandler` | Decorator that marks an event handler. |
| `--handler-dir <name>` | `events` | Parent folder name that classifies a file as a handler. |
| `--routes-export <name>` | `REGISTERED_ROUTES` | Name of the emitted routes array. |
| `--handlers-export <name>` | `REGISTERED_EVENT_HANDLERS` | Name of the emitted handlers array. |
| `--allow-empty` | off | Exit 0 when the pattern matches nothing (default: error). |
| `--quiet` | off | Suppress the "wrote N imports" log line. |

`node_modules`, `dist` and `.d.ts` files are always excluded and symlinks are
not followed. Tests under `commands/__tests__/` are excluded by the default
pattern (it matches a file's **immediate** parent directory), but tests placed
directly inside `commands/` would be picked up.

### It fails loudly

When you pass `routes: REGISTERED_ROUTES` (the wiring above), `createRouter`
filters the registry on the barrel — so a class missing from the barrel is a
route missing from production, the exact failure this tool exists to prevent.
(Omitting `routes:` falls back to "everything any import registered", which
loses that guarantee.) So the generator exits **non-zero** rather than
writing an incomplete barrel when:

- the pattern matches no files (override with `--allow-empty`);
- `--src` does not exist;
- two matched files export the same class name (the barrel could not compile);
- an unknown flag is passed (a typo used to be ignored, yielding an empty barrel).

Class detection uses the TypeScript compiler's parser, resolved from *your*
project. So multiple decorated classes per file are all registered, decorators
mentioned inside comments or string literals are ignored, and
`export default class` falls back to a side-effect import rather than emitting
a named import that cannot resolve.

### Flat layouts (no `src/domain`)

If your project doesn't nest aggregates under `src/domain/`, pass an explicit
pattern. The Ceves example app uses a flat layout:

```jsonc
"gen:decorator-imports": "ceves-generate-imports --pattern '{commands,queries,events}/*.ts'"
```

See [`example/README.md`](./example/README.md) for the full setup.
