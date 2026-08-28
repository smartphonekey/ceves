# Ceves Example — BankAccount

A minimal, runnable Cloudflare Worker that uses the [Ceves](../) event-sourcing
library end-to-end. It models a single bank-account aggregate with three
commands (Open / Deposit / Withdraw) and one query (GetBalance), backed by:

- A **Durable Object** for ordered, transactional command execution
- **R2** for the immutable event log (and optional snapshots)
- **Hono + Chanfana** (via `createRouter`) for HTTP routing and OpenAPI docs

If you just want to copy-paste a starting template, this is it.

## Layout

```
example/
├── src/
│   ├── index.ts                            # Worker entry — exports DO, imports the generated barrel
│   ├── _decoratorImports.generated.ts      # GENERATED, gitignored — see "Decorator registration" below
│   ├── types.ts                            # AccountState, EventTypes, event-data interfaces
│   ├── aggregates/
│   │   └── BankAccountAggregate.ts         # The Durable Object (extends AggregateObject)
│   ├── commands/                           # @Route + CommandRoute / CreateCommandRoute classes
│   │   ├── OpenAccountRoute.ts             # CREATE  → POST /accounts/:id/OpenAccount
│   │   ├── DepositRoute.ts                 # UPDATE  → POST /accounts/:id/Deposit
│   │   └── WithdrawRoute.ts                # UPDATE  → POST /accounts/:id/Withdraw (with business rule)
│   ├── queries/                            # @Route + QueryRoute classes
│   │   └── GetBalanceRoute.ts              # GET /accounts/:id/balance
│   └── events/                             # @EventHandler-decorated handlers (state transformations)
│       ├── AccountOpenedHandler.ts
│       ├── MoneyDepositedHandler.ts
│       ├── MoneyWithdrawnHandler.ts
│       └── __tests__/                      # Pure-function tests for the handlers above
│           ├── AccountOpenedHandler.test.ts
│           └── MoneyDepositedHandler.test.ts
├── package.json
├── tsconfig.json
├── wrangler.toml
└── README.md
```

The same layout (aggregate + commands/ + queries/ + events/) scales to
production apps that nest each aggregate under `src/domain/<aggregate>/` —
the example is deliberately a slimmed-down copy of that pattern.

> Tests live under `events/__tests__/` so the codegen pattern below picks up
> only real handler files. Anything whose immediate parent dir is `commands`,
> `queries`, or `events` is registered at startup; tests must live one level
> deeper.

## Decorator registration (`ceves-generate-imports`)

`@Route` and `@EventHandler` only register themselves when their file is
imported, so every handler module has to be touched at worker startup. We do
this with a generated barrel rather than a per-file import list:

- `npm run gen:decorator-imports` (also wired as `predev` / `prebuild` /
  `predeploy` / `pretest`) invokes the **`ceves-generate-imports`** CLI
  shipped with the `ceves` package.
- The CLI walks `src/{commands,queries,events}/*.ts` and writes one static
  `import './…';` per match into `src/_decoratorImports.generated.ts`.
- `src/index.ts` imports that barrel — done.

Why a generated barrel and not `import.meta.glob({ eager: true })`?
`import.meta.glob` is a Vite-only feature; wrangler's esbuild bundler silently
expands it to an empty object, dropping every route in production deploys. A
static barrel works in every bundler.

The generated file is **gitignored** (see the repo `.gitignore`); never
edit it by hand.

The example uses a custom pattern (`{commands,queries,events}/*.ts`) because
its handler folders live directly under `src/`. Production apps that nest
domains under `src/domain/<aggregate>/` can rely on the CLI's default pattern
(`domain/**/{commands,routes,events}/*.{ts,tsx}`) and call
`ceves-generate-imports` with no arguments.

## How a request flows through the system

1. Client sends `POST /accounts/acc-123/Deposit` with `{"amount": 50}`.

2. The Hono app (built by `createRouter()` in `src/index.ts`) matches the
   `@Route({ method: 'POST', path: '/accounts/:id/Deposit' })` on `DepositRoute`.

3. `CommandRoute.handle()` (in Ceves) runs:
   - Calls `getValidatedData()` — applies the Zod body schema, including any
     `.refine()` checks. Invalid bodies short-circuit with a 400.
   - Builds auth headers from the Hono context (`X-Org-Id`, `X-User-Id`, etc.).
   - Looks up the DO namespace by deriving the binding name from
     `aggregateType` (`BankAccountAggregate` → `BANK_ACCOUNT`).
   - Calls `stub.fetch(...)` on the validated request.

4. `BankAccountAggregate` (the Durable Object) wakes up and:
   - Restores state by replaying events from R2 (or by loading the latest
     snapshot, then replaying events on top).
   - Calls `checkAuthorization(request)` to enforce per-aggregate access rules.
   - Looks up `DepositRoute` from the route registry and calls
     `executeCommand(command, state, env)`. State is guaranteed non-null
     because `CommandRoute` is the UPDATE base class.
   - Persists the returned `MoneyDepositedEventData` to R2 with an
     incremented version.
   - Calls the matching `@EventHandler` (`MoneyDepositedHandler.apply`) to
     produce the new in-memory state. `version` and `timestamp` are stamped
     by the framework after the handler returns.
   - Returns `{ success: true, aggregateId, version }` to the client.

5. (Optional, fire-and-forget) Any registered projectors are dispatched with
   the new event so external systems — webhooks, SpacetimeDB, EventBridge —
   stay in sync. The example doesn't register projectors, but see
   `ProjectionDispatcher` and `registerProjector()` in the main Ceves API.

## The two base classes you'll subclass most often

### `CommandRoute` vs `CreateCommandRoute`

Pick based on whether the aggregate is supposed to exist already:

| Goal | Base class | `executeCommand` signature | State |
| ---- | ---------- | -------------------------- | ----- |
| Open a new account | `CreateCommandRoute` | `(command, env)` | always null — there is no prior state |
| Deposit / Withdraw on an existing account | `CommandRoute` | `(command, state, env)` | guaranteed non-null |

The framework enforces the distinction, so you never write
"`if (state) throw …`" yourself. Sending `OpenAccount` to an aggregate that
already exists is treated as an idempotent no-op: you get a `200` with
`event: null` (same body shape as the create), not a duplicate event.

### `QueryRoute`

Read-only. Receives the loaded state and any validated query params, returns
JSON. Doesn't emit events, doesn't mutate state. See `GetBalanceRoute.ts`.

## Decorators

- **`@Route({ method, path })`** — exported by Ceves. Registers the class
  with the router. Apply it
  to every command-route, query-route, and standalone HTTP-route class.

- **`@EventHandler`** — class decorator (no arguments). Registers the class
  in the event-handler registry. Each handler must set `eventType` and
  `aggregateType` as instance fields so Ceves knows when to run it.

There is no longer a `@CommandHandler` / `@QueryHandler` decorator — those
were retired in favour of the route-class pattern documented above. If you
see those in old docs, they're stale.

## Authorization

Override `checkAuthorization(request: Request)` on your aggregate to enforce
per-aggregate access rules. The hook runs automatically before every command
and query. Throw `UnauthorizedError` (→ 401) or `ForbiddenError` (→ 403).

`BankAccountAggregate.ts` shows the pattern: `/balance` is public, everything
else requires an `X-Org-Id` header, and once an account is owned by an org no
other org can touch it. The same hook scales to more elaborate multi-tenant
schemes (org-claim vs user-claim, super-key bypass, public sub-routes).

## Audit-only events

Sometimes you want an event in the stream for downstream consumers (a
webhook, a projector, an audit log) but the event doesn't actually change
the aggregate's state. The pattern is to write a handler that returns
`state` unchanged. The example doesn't include one (to keep things tight),
but it's worth knowing about.

## Running locally

```bash
# from the repo root: install and build the ceves package first
# (the example resolves `ceves` via file:.. -> ../dist)
npm install && npm run build

# from this directory
npm install
npm run dev
```

`wrangler dev` provides in-memory R2 buckets and a local SQLite-backed DO,
so no Cloudflare account is needed for local exploration.

Visit:

- `http://localhost:8787/docs` — Swagger UI auto-generated from the route schemas
- `http://localhost:8787/openapi.json` — raw OpenAPI 3 document

## Running on NATS instead of Cloudflare

The exact same app — routes, event handlers, and `BankAccountAggregate` —
also runs on NATS via the `ceves/nats` adapter. Commands travel **as NATS
messages**: the REST adapter serializes each validated request onto
`ceves.cmd.BankAccountAggregate.<id>` and a queue-group aggregate service
executes it against the JetStream event log + NATS KV state (instead of
R2 + Durable Objects). The runtime choice lives entirely in the entry
point: `src/index.ts` for Cloudflare, `src/nats-main.ts` for NATS.

```bash
# 1. a local NATS server with JetStream (see https://docs.nats.io)
nats-server -js -sd ./data/jetstream &

# 2. from this directory
npm install
npm run nats:start          # gateway + aggregate service in one process
# → [all] BankAccount on NATS — REST gateway at http://localhost:8788

# or run the roles as separate processes (commands cross NATS between them):
MODE=service npm run nats:start   # aggregate host (no HTTP)
MODE=gateway npm run nats:start   # REST adapter on :8788

# 3. same curl walk-through as below, against port 8788 — then assert it all:
./scripts/nats-e2e.sh

# 4. subscribe to events by TYPE / tenant / aggregate on the routed stream:
node scripts/nats-routed-demo.mjs acc-123
#    ceves.evt.acme.BankAccountAggregate.MoneyDeposited.>  → only deposits
#    ceves.evt.acme.BankAccountAggregate.*.acc-123         → full ordered history
```

`scripts/register-cf-shim.mjs` makes ceves' `import 'cloudflare:workers'`
resolve on plain Node (it redirects the specifier to
`ceves/nats/cloudflare-workers-shim`). Environment knobs: `NATS_URL`
(default `nats://127.0.0.1:4222`), `PORT` (default 8788).

State survives restarts via NATS KV, and the JetStream stream
`CEVES_EVENTS` is the source of truth: delete an aggregate's KV entry and
the next request rebuilds its state by replaying its events. Architecture
details: [`../docs/architecture/nats.md`](../docs/architecture/nats.md).

### A complete walk-through

```bash
# 1. Open an account
curl -X POST http://localhost:8787/accounts/acc-123/OpenAccount \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: org-demo" \
  -d '{"owner": "Alice", "initialDeposit": 100}'
# → {
#     "success": true,
#     "aggregateId": "acc-123",
#     "version": 1,
#     "event": {
#       "type": "AccountOpened",
#       "data": { "owner": "Alice", "initialDeposit": 100 }
#     }
#   }

# 2. Deposit
curl -X POST http://localhost:8787/accounts/acc-123/Deposit \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: org-demo" \
  -d '{"amount": 50}'
# → {
#     "success": true,
#     "aggregateId": "acc-123",
#     "version": 2,
#     "event": { "type": "MoneyDeposited", "data": { "amount": 50 } }
#   }

# 3. Withdraw too much (business rule trips)
curl -X POST http://localhost:8787/accounts/acc-123/Withdraw \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: org-demo" \
  -d '{"amount": 500}'
# → 400, BusinessRuleViolationError

# 4. Read the balance (no auth — /balance is whitelisted in checkAuthorization)
curl http://localhost:8787/accounts/acc-123/balance
# → { "accountId": "acc-123", "owner": "Alice", "balance": 150, "version": 2 }
```

### Response shape (AA-82)

Every successful command response includes the just-emitted event:

```jsonc
{
  "success": true,
  "aggregateId": "acc-123",
  "version": 1,
  "event": {
    "type": "AccountOpened",
    "data": { "owner": "Alice", "initialDeposit": 100 }
  }
}
```

The outer `event.type` is the only carrier of the discriminator;
`event.data` holds the pure data payload (no `type` field). Clients can
therefore use `event.type` for routing and read raw business fields off
`event.data` without unwrapping a duplicate discriminator.

For idempotent commands that return `NO_EVENT` (no event persisted, no state
change), the response is the same shape with `event: null`:

```jsonc
{ "success": true, "aggregateId": "acc-123", "version": 1, "event": null }
```

Clients can therefore branch on `event === null` to detect the "no-op" path
without juggling a separate `noEvent` field.

#### Declaring `eventSchema` on a route

Each command route in this example declares the **data-only** Zod schema as
`static readonly eventSchema` so two things happen for free:

1. **OpenAPI docs** surface the typed event-data shape under
   `responses.200.content.application/json.schema.event.data`, so consumers
   know exactly what to expect.
2. **Runtime validation** at the framework boundary: after `executeCommand()`
   returns, Ceves strips the outer `type` and runs
   `eventSchema.safeParse(eventData)`, throwing a clear error if the handler
   returned malformed data — a handler bug is caught loud rather than
   silently shipped to the client.

The schema describes exactly what's on the wire under `event.data`, so it's
declared without a `type: z.literal(...)` field. See
[`src/types.ts`](src/types.ts) for the data-only schemas (and the matching
`*Schema` extensions used as `executeCommand()` return types) and
[`src/commands/OpenAccountRoute.ts`](src/commands/OpenAccountRoute.ts) for
the static field declaration:

```typescript
import { AccountOpenedDataSchema } from '../types';

@Route({ method: 'POST', path: '/accounts/:id/OpenAccount' })
export class OpenAccountRoute extends CreateCommandRoute<...> {
  static readonly eventSchema = AccountOpenedDataSchema;
  // ...
}
```

`eventSchema` is **optional** — routes that don't declare it still work,
they just get a generic `event.data` shape in the OpenAPI doc and skip the
runtime parse. Declaring it is the recommended shape for every new command
route (the example's `OpenAccountRoute.ts` shows it end-to-end).

## Testing

```bash
npm test          # one-shot
npm run test:watch   # vitest watch mode
npm run typecheck    # tsc --noEmit
```

The two tests under `src/events/` exercise the event handlers as pure
functions — no Workers runtime needed. To exercise the full HTTP stack
including DO state, use `@cloudflare/vitest-pool-workers` (the parent
package's integration tests show the setup).

## Deploying to Cloudflare

```bash
# Create the buckets once
wrangler r2 bucket create ceves-example-events
wrangler r2 bucket create ceves-example-snapshots

# Then deploy
npm run deploy
```

## What this example deliberately leaves out

To keep the source under ~400 lines, this example skips a few features the
framework supports. Reach for them when you need:

- **Projectors** — fire-and-forget projection of events to external systems
  (`registerProjector()`, `ProjectionDispatcher`).
- **Multi-tenancy resolvers** — `ApiKeyTenantResolver`, custom
  `ITenantResolver` implementations.
- **Out-of-band snapshots (AWS Lambda only)** — `S3SnapshotStore`. The
  Cloudflare/DO variant persists state to DurableObjectState.storage, so it
  doesn't need a separate snapshot store.
- **Custom response shaping** — overriding `customizeResponse()` on a
  `CommandRoute` to inject server-generated fields into the response.

For all of those, the entry points in the framework source are:

- `src/projection/` (projectors + `ProjectionDispatcher`)
- `src/tenancy/` (`ApiKeyTenantResolver`, `HeaderTenantResolver`)
- `src/core/AggregateObject.ts` (alarms, admin delete, replay, `customizeResponse`)
- `src/storage/S3SnapshotStore.ts` (AWS-only snapshots)
