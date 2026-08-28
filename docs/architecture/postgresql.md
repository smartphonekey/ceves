# Ceves Architecture: PostgreSQL Variant

**Deployment Target:** PostgreSQL (PLV8) behind a thin HTTP wrapper (Cloudflare Workers or Node/Hono), or a REST extension (PostgREST) via generated `/rpc` functions
**Storage:** PostgreSQL for aggregate state + a transactional outbox · R2/S3 for the event log (PostgreSQL holds **no** event log)
**State Management:** One `jsonb` row per aggregate with optimistic version guard + `SELECT … FOR UPDATE` serialization
**Last Updated:** 2026-08-21

---

## Overview

The PostgreSQL variant runs the Ceves command/query pipeline **inside the
database**. The same `@Route` command/query classes and `@EventHandler` state
transformers that power the Durable Objects variant are bundled (TypeScript →
one JavaScript module via esbuild) and executed by
[PLV8](https://plv8.github.io/) — the PostgreSQL extension embedding V8, i.e.
"functions written in TS/JS in the database". A generated pair of PLV8
functions dispatches every command and query:

```
ceves.execute_command(aggregate_type, aggregate_id, route_key, command, auth, env) → jsonb
ceves.execute_query  (aggregate_type, aggregate_id, route_key, query,   auth, env) → jsonb
```

HTTP endpoints stay byte-compatible with the OpenAPI schema: a thin Hono
wrapper (`registerPgRoutes`) mounts every registered route at its original
method + path and forwards to the SQL functions. Because Hono is isomorphic,
the wrapper runs on Cloudflare Workers or a Node server unchanged.
Alternatively, the generator emits one named SQL function per route
(`ceves.cmd_add_key`, `ceves.qry_get_balance`, …) so a REST extension such as
PostgREST can expose each endpoint as `/rpc/cmd_add_key` with no wrapper code.

**Key Principle:** state and processing move into PostgreSQL; the event log
does not. A command's emitted event is committed to the **transactional
outbox** in the same transaction as the state write, and the wrapper-side
relay appends it to the external event store (R2/S3). The event log therefore
still lives in R2/S3 — mirroring the repo-wide data-store boundary that
append-only history never lives in a mutable relational store — while
delivery to it becomes **guaranteed** rather than best-effort: state and
event commit together, or neither does.

### Why PostgreSQL?

- **No Durable Objects dependency**: run the same domain logic on plain
  PostgreSQL — on-prem, RDS, Supabase, Neon.
- **Transactional state**: the state row update and the command's business
  checks execute in one database transaction.
- **SQL-native integration**: projections, reporting, and ad-hoc queries can
  join directly against `ceves.aggregate_state`.
- **REST-extension friendly**: PostgREST (or an equivalent) can serve the
  generated per-route functions with zero application servers.

---

## Architecture Decision Records

Shared with the other variants:

- **[ADR-009](../adr/ADR-009.md)**: Class-Based State with Empty State Pattern
  (the PG variant sources the empty state from `registerPgAggregateState`)

See also the [NATS variant](./nats.md), which solves the same
"run Ceves off Cloudflare" problem with actors + JetStream instead.

---

## Components (`src/adapters/pg/`, exported as `ceves/pg`)

| Piece | Role |
| --- | --- |
| `createPgDispatcher(sql, options)` | The pipeline: route lookup via the decorator registries, create/update semantics, `NO_EVENT`, event application, `eventSchema` parse, version bump. `sql` is `plv8.execute` in production, a fake in unit tests. |
| `installPlv8Dispatcher()` | Called by the app's PG entry; installs `globalThis.__ceves_pg__` for the generated PLV8 functions. |
| `registerPgAggregateState(type, StateClass)` | Replaces the DO subclass's `super(ctx, env, StateClass)` — supplies the ADR-009 empty state. |
| `generate*Sql` / `generateFullSql` | SQL emission: schema, module upsert, dispatch functions, per-route wrappers. |
| `ceves-generate-pg` (bin) | CLI: esbuild-bundles the PG entry (IIFE for PLV8 + a Node build to collect the route manifest) and writes one idempotent SQL script. |
| `registerPgRoutes(app, options)` | Thin Hono wrapper preserving the OpenAPI endpoints; ships returned events to the `EventSink`, runs `sideEffects`, drains the outbox after commands. |
| `EventStoreSink` / `InMemoryEventSink` | `EventSink` implementations — `EventStoreSink` wraps any `IEventStore` (`R2EventStore`, `S3EventStore`). |
| `getPgOutbox()` / fetch interception | Transactional outbox, in-database side: external calls become `<schema>.outbox` rows in the state-write transaction. |
| `drainPgOutbox(client, options)` | Wrapper-side relay: SKIP LOCKED claim → deliver (built-in `'event'` → EventSink and `'fetch'`, plus custom kinds) → delete, with backoff + dead-letter (never for events). |

### Database objects (generated, idempotent)

```sql
ceves.aggregate_state (aggregate_type, aggregate_id, version, org_id, state jsonb, updated_at)
ceves.modules         (name, source, updated_at)   -- the bundled JS module
ceves.outbox          (id, kind, request jsonb, status, attempts, ...)  -- transient call queue
ceves.execute_command(...) / ceves.execute_query(...)  -- PLV8 dispatchers
ceves.cmd_* / ceves.qry_*                              -- per-route wrappers
```

There is **no events table**. Adding one is an architecture defect — see
"Event log stays external" below.

---

## Command Flow

```
Client ── POST /locks/lock-1/AddKey ──▶ Hono wrapper (CF Worker or Node)
                                         │ 1. Zod-validate body (route schema)
                                         │ 2. SELECT ceves.execute_command(
                                         │      'LockAggregate','lock-1',
                                         │      'POST:/locks/:id/AddKey', body, auth, env)
                                         ▼
                              PostgreSQL / PLV8 (one transaction)
                                         │ 3. load module from ceves.modules (once per connection)
                                         │ 4. SELECT state row FOR UPDATE   ← per-aggregate serialization
                                         │ 5. create/update guard, executeCommand(), NO_EVENT
                                         │ 6. eventSchema parse (BEFORE any write)
                                         │ 7. @EventHandler.apply → new state, version+1
                                         │ 8. INSERT … ON CONFLICT DO NOTHING / UPDATE … WHERE version = n
                                         │ 9. INSERT the event into ceves.outbox  ← same transaction ⇒ atomic
                                         ▼
                              returns { status, body, event, eventOutboxId }
                                         │10. wrapper drains the outbox → EventSink appends to R2/S3
                                         │11. wrapper runs @EventHandler.sideEffects (network I/O)
                                         ▼
                              HTTP response — same wire shape as the DO variant
```

Semantics mirrored from the DO variant: AA-92 idempotent duplicate create
(200, `event: null`), 201 for a genuine create, `NO_EVENT`, the standard
`{ success, aggregateId, version, event: { type, data } }` body, the AA-119
error envelope (`success/errors/__ceves`), and the `customizeResponse` hook.

Two deliberate differences:

1. **`eventSchema` parses BEFORE the state write.** The DO validates after
   persisting; inside PostgreSQL an error envelope returned after a write
   would still commit it, so malformed events are rejected while nothing has
   been persisted.
2. **Concurrency is locks + version guards, not single-threading.**
   `SELECT … FOR UPDATE` serializes per aggregate; the insert/update carry an
   `ON CONFLICT DO NOTHING` / `WHERE version = expected` guard. A lost
   create race degrades to the idempotent no-op; a lost update race returns
   409 `VersionConflictError`.

## Event log stays external (⛔ no events in PostgreSQL)

The dispatch function **returns** the `StoredEvent` envelope; the wrapper
appends it to R2/S3 via an `EventSink`. Rationale:

- Append-only history in a mutable relational store violates the data-store
  boundaries (audit/event history → R2), and PostgreSQL rows are trivially
  UPDATE-able — the log would not be tamper-evident.
- The R2/S3 log remains the single source for replay, archival, and the
  existing admin cleanup tooling across variants.
- Delivery to that log is nonetheless **guaranteed**, because the event is
  committed to `ceves.outbox` inside the state-write transaction (kind
  `'event'`) and shipped by the relay. A wrapper that dies between commit and
  the R2 append loses nothing — the next drain (fast path or cron sweeper)
  still delivers it. This is strictly stronger than the DO variant, whose
  `persistEvent` to R2 is fire-and-forget and drops the event if the isolate
  dies first.
- The trade-offs that remain are the honest ones for any outbox:
  **at-least-once** (the sink may see the same version twice — R2 keys are
  per-version, so a repeat write is idempotent), and **eventual** rather than
  immediate (a delayed drain means the log briefly lags the state). Event
  rows are never dead-lettered — a dropped event would leave a hole in the
  stream and break replay — so they retry indefinitely at capped backoff.
- Set `outbox.events: false` to go back to the wrapper appending directly
  (and accept losing an event on a crash).

## Transactional outbox (external calls from inside PostgreSQL)

PLV8 is bare V8 — no network. Instead of forbidding external calls, the
dispatcher turns **fire-and-forget** ones into rows in `<schema>.outbox`,
written in the SAME transaction as the state update:

- **The emitted event** (kind `'event'`, on by default): the dispatcher
  enqueues the `StoredEvent` envelope right after the state write succeeds,
  so the aggregate's new state and its event are one atomic unit. The relay's
  built-in event deliverer appends it to the `EventSink`;
  `registerPgRoutes` wires that from its own `eventSink` automatically, and
  reports the row id back as `eventOutboxId` so the wrapper does not also
  append it.
- **`fetch` interception** (on by default): a `fetch(...)` made during a
  command dispatch is serialized (`{url, method, headers, body}`) and
  INSERTed as kind `'fetch'`. The caller gets a stub `202 Accepted` response
  — `ok`/`status` work, but any body read throws a descriptive error,
  because a call that *needs* its response cannot be outboxed (keep those in
  the wrapper). During queries, `fetch` throws (queries are reads).
- **Explicit enqueue** for everything else (MQTT, notifications, …):
  `getPgOutbox()?.enqueue('mqtt', { topic, payload })` inside
  `executeCommand`; the relay routes each kind to a deliverer the app
  registers.
- **Atomicity**: rows commit iff the transaction commits. Because error
  envelopes are *returned* (committing the transaction), the dispatcher
  compensates — a command that fails deletes the rows it enqueued, event row
  included. Net guarantee: **the call (or event) is queued iff the command
  succeeded.** This is
  stronger than the DO variant, where a fetch inside `executeCommand` has
  already fired even if the command then throws.
- **Delivery** is wrapper-side: `drainPgOutbox(client, { deliverers, ... })`
  claims due rows with a single `UPDATE … WHERE id IN (SELECT … FOR UPDATE
  SKIP LOCKED)` statement (any number of concurrent drainers are safe),
  performs the real I/O, DELETEs on success, retries with exponential
  backoff, and parks rows as `dead` after `maxAttempts`. The gateway drains
  automatically after each successful command (`outbox` option on
  `registerPgRoutes`); run `drainPgOutbox` on a cron as the catch-up sweeper
  for rows a crashed wrapper left behind. Delivery is **at-least-once** —
  receivers must be idempotent.

The outbox is a transient queue — rows are deleted after delivery. It is
NOT an audit log; history stays in the R2/S3 event store.

## What cannot run inside PostgreSQL

PLV8 is bare V8: **no fetch responses, no timers, no Cloudflare bindings.**

- `@EventHandler.sideEffects` (MQTT, webhooks) run in the wrapper after the
  SQL call returns — `registerPgRoutes` does this automatically.
- Handlers whose external calls need the RESPONSE (read-your-call flows)
  are not PG-dispatchable as-is; keep that I/O in wrapper hooks or on the
  DO variant. Fire-and-forget calls go through the transactional outbox
  above; pure state logic ports unchanged.
- `env` inside PostgreSQL is plain JSON config passed per call — not a
  bindings object.

## Requirements

- PostgreSQL with the PLV8 extension, **version ≥ 3.1** (the dispatch
  functions return a `Promise`, which PLV8 resolves before converting the
  result to `jsonb`).
- `esbuild` available where `ceves-generate-pg` runs (optional peer dep).

## Usage

1. Write a PG entry (canonical example:
   [`example/src/pg-entry.ts`](../../example/src/pg-entry.ts)): import the
   decorator barrel, `registerPgAggregateState(...)` per aggregate,
   `installPlv8Dispatcher()`.
2. `ceves-generate-pg --entry src/pg-entry.ts --out ceves-pg.sql`
   (`example/`: `npm run gen:pg`).
3. `psql -f ceves-pg.sql` (idempotent — re-run on every deploy).
4. Serve HTTP: a Hono app with `registerPgRoutes(app, { client, eventSink })`
   — or point PostgREST at the generated `cmd_*`/`qry_*` functions.
