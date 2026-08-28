# Ceves on NATS (JetStream + KV)

**Status:** Implemented (adapter + bank example verified against nats-server v2.14.5)
**Entry point:** `ceves/nats` (`src/adapters/nats/`)

Ceves apps can run on NATS instead of Cloudflare — same `@Route` command and
query classes, same `@EventHandler` state transformers, same
`AggregateObject` subclasses. The adapter swaps the platform layer only, so
an app chooses its runtime at the entry point:

| Concern | Cloudflare variant | NATS variant |
|---|---|---|
| Immutable event log | R2 (`R2EventStore`) | JetStream stream (`NatsEventStore`) |
| Authoritative aggregate state | Durable Object storage (SQLite) | NATS KV bucket (`NatsKvStorage`) |
| Command/query transport | Worker → DO stub subrequest | **NATS request-reply message** on `ceves.cmd.<org>.<type>.<id>` |
| Aggregate hosting / addressing | Durable Object namespace + stub | `NatsAggregateService` (queue-group subscriber) hosting `NatsAggregateNamespace` actors |
| Single-writer serialization | DO single-threading | Per-actor serial queue (+ CAS/OCC backstop, below) |
| Bindings (`env.BANK_ACCOUNT`) | `wrangler.jsonc` | `createNatsGatewayEnv()` / `startNatsAggregateService()` |
| HTTP serving | Workers runtime | thin REST adapter (e.g. `@hono/node-server`) |
| Alarms | DO alarms | persisted in KV + in-process timers with retry backoff |

## How it works

Commands are NATS messages. The main route for every command and query is:

```
REST adapter (createNatsGatewayEnv)              aggregate service (startNatsAggregateService)
Hono route → env[BINDING] stub.fetch()   ──────▶ queue-group subscriber on
  serialize HTTP → NATS request on               ceves.cmd.<org|*>.<type>.> → local actor
  ceves.cmd.<org>.<type>.<id>            ◀──────  (AggregateObject over JetStream/KV)
                                                  reply = serialized response
```

The `<org>` token is resolved per request by the gateway: the **org
directory** first (authoritative home org for existing aggregates — see
"Org directory" below), falling back to the caller's *claimed* tenant
(default: `X-Org-Id` header with a `default-org` fallback) only for
aggregates that don't exist yet. It is ROUTING metadata — broker
permissions can fence a tenant's credentials to `ceves.cmd.<org>.>`, and
services can be deployed per tenant via `orgFilter` — while authorization
stays inside the aggregate (`checkAuthorization()` + tenant resolver),
exactly as on Cloudflare.
Deploy either wildcard-org services or org-filtered services for a given
aggregate type, never both: overlapping queue groups each receive a copy.

The routing layer (`CommandRoute` / `QueryRoute` / `createRouter`) is pure
Hono + chanfana and needs no changes: it resolves `c.env[<BINDING>]` and
calls `namespace.idFromName(id)` / `namespace.get(id).fetch(request)`. On
the REST-adapter side that binding is a `NatsRequestReplyNamespace`, whose
stub serializes the validated request into an HTTP-over-NATS envelope
(`http-over-nats.ts`) and publishes it as a request. The adapter holds no
aggregate classes, no JetStream client, and no KV client — only a core
NATS connection. Transport failures map to structured responses: no
responders → 503, timeout → 504 (the `safeDOFetch` philosophy).

On the host side, `NatsAggregateService` subscribes to
`ceves.cmd.<org|*>.<aggregateType>.>` in a queue group, decodes each envelope, and
dispatches to an in-process actor via `NatsAggregateNamespace`: an
instance of the app's own `AggregateObject` subclass, constructed with a
`NatsAggregateContext` (a `DurableObjectState` stand-in whose `storage` is
a `NatsKvStorage`) and wired to the JetStream event store via
`setStores()`. The **unmodified production pipeline** in
`AggregateObject.fetch()` then runs: route lookup, create/update semantics
(incl. the AA-92 idempotent duplicate create), `checkAuthorization()`,
event application, state checkpointing, batched replay restore (AA-117) —
all of it, over NATS storage. Dispatch into the per-aggregate serial queue
happens synchronously in delivery order, so one aggregate's commands
execute in the order NATS delivered them.

For a monolith, `startNatsCevesRuntime()` still hosts the actors directly
in the REST process (no message hop) — same storage, same pipeline; the
gateway/service split is a deployment choice, not a code change.

`import { DurableObject } from 'cloudflare:workers'` in `AggregateObject`
must resolve under Node. The adapter ships a stand-in at
`ceves/nats/cloudflare-workers-shim`; point the specifier at it with a
bundler alias or a `module.registerHooks` resolve hook (Node ≥ 22.15). See
the shim's module doc and `example/scripts/register-cf-shim.mjs`.

## Routed event stream (subscribe by tenant / aggregate type / event type)

The canonical log deliberately keeps ONE subject per aggregate — that is
what per-subject OCC and single-subject ordered replay require, so the
event type never goes into the canonical subject. Consumers get their
filtering instead from a **derived** stream (`CEVES_EVENTS_ROUTED`,
enabled by default, `routedEvents: false` to disable): after every
successful canonical append, `NatsEventStore` fans the full envelope out to

```
ceves.evt.<orgId>.<aggregateType>.<eventType>.<aggregateId>
```

with every dimension as its own token:

- one tenant's everything:        `ceves.evt.acme.>`
- a tenant's aggregate type:      `ceves.evt.acme.LockAggregate.>`
- one EVENT TYPE across tenants:  `ceves.evt.*.LockAggregate.KeyAdded.>`
- one aggregate's full history:   `ceves.evt.acme.LockAggregate.*.lock-1`

Here `<orgId>` is the aggregate's authoritative tenant (the envelope's
`orgId`), not the caller's claim. Ordering: saves for one aggregate are
serialized by the actor host and the fan-out publish is awaited inside
`save()`, so the routed stream preserves per-aggregate commit order even
across a multi-subject filter (JetStream consumers deliver in stream
order). The fan-out is dedup-protected on retries but NON-fatal: the
canonical log is authoritative, so a failed fan-out logs an error and
leaves a gap consumers can detect via the envelope's dense `version`
(consumers should be idempotent on `(aggregateId, version)`). Server-side
alternatives (republish, subject transforms) can't do this — they are
token-based and cannot read the event type out of the payload.

## Org directory and home-org partitioning

A KV bucket (`ceves_org_dir`, on by default) maps
`<aggregateType>.<aggregateId>` → the aggregate's **home org**: the tenant
it was created under, written exactly once at creation via a CAS
`create()` — which doubles as a **global aggregate-id lock** (the same id
can never exist under two orgs, preserving Cloudflare's global-id
semantics). With the directory enabled, the canonical log is partitioned
per tenant: `ceves.events.<homeOrg>.<type>.<id>`.

Why: tenant infrastructure can be split on demand (per-tenant streams /
placement later, per-tenant broker ACLs on the raw log now), and routing
becomes claim-proof — the gateway consults the directory FIRST, so
end-user JWTs, super API keys, and delegated-access traffic (which carry
no tenant claim) reach the right stream, and a wrong `X-Org-Id` can neither fork a stream
nor miss it. The claim only mints the home org on create.

Three org semantics coexist, each deliberate:

| Where | Which org | Why |
|---|---|---|
| `ceves.cmd.<org>...` | caller's CLAIM (directory overrides for existing aggregates) | routing + per-tenant command ACLs |
| `ceves.events.<org>...` | HOME org (changes only via the explicit transfer operation below) | storage partition + log ACLs + replay addressing |
| `ceves.evt.<org>...` | CURRENT org (envelope) | tenant feeds see history they owned at the time |

A `SetOrganization`-style change therefore updates the current org (state,
authz, routed feed) but not the storage partition — verified by test.
Note: the aggregate STATE bucket (`ceves_state`) is not org-partitioned
yet — state is rebuildable from the partitioned log, so per-tenant state
splits are a mechanical follow-up. User-scoped aggregates (no org claim)
land in the `default-org` partition (their envelope v1 org); formalizing
per-user "personal tenants" requires the app to resolve org=userId for
JWT traffic.

## Org transfer — selling an aggregate to another tenant

`NatsAggregateNamespace.transferOut(aggregateId, toOrg)` (or the runtime's
`transferAggregate(type, id, toOrg)` convenience) moves an aggregate's home
org — the "sell the lock" flow. Semantics: **fresh start with preserved
identity**. The buyer gets the aggregate carrying exactly the values it was
created with (lock number, uuid — whatever the creation event holds) and
none of the seller's accumulated state (keys, settings, history); the
seller keeps a sealed audit trail. The operation runs on the aggregate's
serial actor queue:

1. **Seal** — a final `CevesAggregateTransferredOut` audit event
   (`{fromOrg, toOrg}`) is appended to the old stream. `NatsEventStore`
   refuses to append past a seal — the guard rides the per-subject token,
   so even a stale process (old cached sequence) is bounced to a re-read
   that sees the seal and 409s. The seal also fans out to the SELLER's
   routed feed (`ceves.evt.<fromOrg>...`), so seller-side projections learn
   the aggregate left.
2. **Purge** — the aggregate's KV state (snapshot, replay markers,
   projection cursors, alarm) is deleted. Storage keys are org-free, so a
   surviving snapshot would hand the seller's state to the buyer.
3. **Seed** — the aggregate's ORIGINAL creation event (same type, same
   domain data, same original timestamp) is re-appended as version 1 of a
   brand-new stream under `ceves.events.<toOrg>...`. Existing event
   handlers apply it, so no migration code is needed and the buyer's
   routed feed sees the creation. On a re-sale, the previous sale's seed
   is what gets carried forward — the original creation data survives any
   number of transfers.
4. **Repoint** — the org-directory entry is CAS-updated `fromOrg → toOrg`.
   From here the gateway routes commands to the new org and loads replay
   the seeded stream.
5. **Evict** — the in-process actor is dropped (and the state purged once
   more, closing the window where a racing seller command could rebuild a
   snapshot after step 2 — its event append is rejected by the seal, but
   the core commits state before publishing).

Every step is idempotent, so a crashed transfer is completed by calling
`transferOut` again with the same target (the seal records the in-flight
target; a retry toward a *different* org is refused). Reusing the original
timestamp makes a retried seed byte-identical, so server dedup absorbs it.

**The org-change endpoint IS the sale.** The app already has a
`SetOrganization`-style command; registering its event type as a transfer
trigger makes committing it run the whole transfer, inside the same
request:

```ts
startNatsAggregateService({
  connection,
  aggregates: [{ AggregateClass: LockAggregate, orgTransferOn: ['OrganizationSet'] }],
});
```

After the command's event is durably in the log, the actor host reads the
target org from the committed **event data's `orgId` field** (the
production `OrganizationSet` shape — the envelope's org can't be used, the
core stamps it from the pre-event state, i.e. the seller) and runs
seal → purge → seed → repoint before the response returns. A 200 from the
endpoint therefore means the sale is complete: routing, partitioning, and
the buyer's fresh seeded stream are all in place. Setting the org the
aggregate already lives under is a no-op (no seal, nothing moves).

**A failed storage move never fails the command.** If the transfer dies
after the commit, the caller still gets its success: the org change is
real — applied to state, durable in the log, and already governing
authorization — and which partition holds the aggregate is this runtime's
internal business (Cloudflare has no partition at all). The incomplete
move is finished automatically instead:

- the process that ran it remembers the pending target and completes it
  before serving the next request to that aggregate;
- any OTHER process recovers independently — a write refused by the seal
  triggers completion using the target org recorded *in the seal event*,
  so an aggregate is never stranded by whichever process crashed.

Nothing in this path returns a **500**: that status is reserved for bugs.
A write the log refuses is a **409** (a competing writer, or the aggregate
moved org — reload and retry), and a log that can't be reached is a
**503** with `Retry-After` (a dependency being down, not a defect). The
same rule governs the actor boundary generally: `flushEventSaves` maps a
post-commit log failure to 409 or 503, never 500.

**Authorization stays in the aggregate.** Who may sell/claim is business
logic, enforced where it always was — `checkAuthorization()`: only the
CURRENT org may command an owned aggregate (selling included), and an
unowned one (no org in state, e.g. factory-provisioned) can be claimed.
An aggregate that already enforces the current-org rule in
`checkAuthorization()` needs no change; on the
NATS runtime a claim is just a transfer out of whatever partition the
aggregate was provisioned under.

**Directory cache coherence:** home orgs are no longer cache-forever.
`NatsOrgDirectory.startWatching()` (a KV watch) keeps each process's cache
coherent; `startNatsCevesRuntime` / `startNatsAggregateService` /
`openNatsOrgDirectory` start it by default. A process without a watch keeps
serving the stale org until restart — its reads go to the sealed old stream
(stale but harmless) and its writes are rejected by the seal guard, so
correctness never depends on the watch; only routing freshness does.
Multi-host caveat (same as the general one below): a warm actor on another
host serves stale READS until its next write conflicts and evicts it.

## Event log layout and versioning

- One stream (default `CEVES_EVENTS`, subjects `ceves.events.>`), one
  concrete subject per aggregate:
  `ceves.events.<homeOrg>.<aggregateType>.<aggregateId>` with the org
  directory (default), or `ceves.events.<aggregateType>.<aggregateId>`
  without it (tokens escaped via `encodeToken` — `=XX` hex escapes keep
  arbitrary IDs legal in subjects and KV keys, where `%` is not allowed).
- The event **type stays in the payload**, never in the subject, so plain
  per-subject optimistic concurrency applies.
- Ceves versions are dense (1..N) in the stored envelope. JetStream's
  concurrency token is different: the **stream sequence** of the subject's
  last message. `NatsEventStore` caches it per subject — together with the last event's
  VERSION (primed by every load/save, standard MSG.GET `last_by_subj`
  lookup on cache miss) — and publishes with
  `Nats-Expected-Last-Subject-Sequence`. A mismatch (API error 10071/10164)
  surfaces as `VersionConflictError` (409). Before publishing, `save()`
  also enforces version CONTINUITY (`event.version === last.version + 1`):
  the sequence header alone validates stream position, not version
  uniqueness, so a writer with a divergent state view is refused instead
  of appending a colliding or gapped version. Replays run an integrity
  scan (duplicates dropped keeping the earliest append, gaps reported).
- Every publish carries a content-addressed `Nats-Msg-Id`
  (`type.id.version.<payload-hash>`): a retried save of the *same* event is
  absorbed by server dedup, while a *different* event at the same version
  (concurrent writer) is rejected by the sequence check — the hash matters
  because the server checks `Nats-Msg-Id` before the expectation header.
- Stream config (created on first run, existing streams left untouched):
  `retention: limits` with no limits (an event log is permanent history —
  never workqueue/interest), `storage: file`, 5-minute duplicate window.
  `allow_direct` is enabled for external tooling only — the store's own
  lookups use the standard MSG.GET API, which needs no flag and never
  serves stale replica data.

## State, restore, and replay

State lives in one KV bucket (default `ceves_state`, `history: 1`), key
`<aggregateType>.<aggregateId>.<storageKey>`. The `state` key is written
with revision CAS (`kv.create` / `kv.update(rev)`); all other keys
(`aggregateName`, `r2_replay_in_progress`, projection cursors) are
advisory unconditional puts.

Cold start follows the Cloudflare flow exactly: KV `state` hit → done;
miss → `AggregateObject.restoreFromR2Batched()` pages events from
JetStream (50 per batch, checkpointing between batches) and rebuilds state.
Deleting the KV entry is therefore safe — the event log is the source of
truth (verified in the example: wipe KV → restart → balance replays
correctly and the next command continues the stream).

## Consistency model (read before running multiple hosts)

Within one process, per-actor serial queues reproduce the DO
single-threading guarantee — including under concurrent HTTP requests.

Across processes there is **no single-writer guarantee**; correctness is
protected, not coordinated:

- a stale host's state write fails the KV CAS → the command gets a 409, the
  actor is evicted, and the next request rebuilds from the current state;
- a state write that somehow slipped through would still hit the JetStream
  per-subject sequence check when the event publishes;
- **reads on a warm stale actor serve cached state** until that host's next
  write conflicts (verified in the adapter's cross-host test).

Run one service instance per aggregate type (or a single host) for
strongly consistent reads; conflicts in other topologies are detected and
recovered, not silently merged. Note that NATS queue groups distribute
messages with no per-key affinity, so scaling a service horizontally means
one aggregate's commands can land on different instances — safe, but
conflict-retry churn; a partitioned subject scheme (deterministic
`<id> → partition` mapping with one subscriber per partition) restores
single-writer affinity without changing the storage design.

## Known deltas vs the Cloudflare variant

- STRONGER event durability than Cloudflare: state still commits first
  (core ordering), but the actor host awaits the event-log publish before
  returning the response (`waitForPendingSaves`), so a publish failure
  surfaces as a 500 + actor rebuild instead of being silently swallowed
  by the fire-and-forget persist. The only residual loss window is a
  process crash between the state CAS and the publish; the next write's
  continuity check then reports the divergence as a 409, and the log's
  integrity scan flags it on replay.
- Projectors ARE dispatched (the dispatcher is rebuilt when `setStores()`
  injects the JetStream event store), with cursors in KV and catch-up via
  the in-process alarm timers.
- Alarms persist under an advisory storage key and are re-armed on actor
  construction, so they survive evictions and host restarts; a throwing
  `alarm()` is retried with capped exponential backoff (6 attempts). A
  STOPPED host still fires nothing until the aggregate is touched again.
- A gateway 504 means the command may still commit after the timeout —
  retrying a non-idempotent command can double-apply it (no Retry-After
  is advertised on 504; client-supplied idempotency keys are future work).
- Admin delete (`X-Admin-Delete`) clears the aggregate's KV entries — the
  `DELETE_DO_ONLY` semantics. Event archival cleanup is app-level, as on
  Cloudflare.

## Running the example

```bash
nats-server -js -sd ./data &           # local NATS with JetStream
cd example && npm install
npm run nats:start                     # gateway + service in one process, port 8788
./scripts/nats-e2e.sh                  # end-to-end assertions

# or split across processes (commands cross NATS between them):
MODE=service npm run nats:start        # aggregate host, no HTTP
MODE=gateway npm run nats:start        # REST adapter on :8788
```

Live adapter tests against a real server:

```bash
NATS_TEST_URL=nats://localhost:4222 npx vitest run --project unit src/adapters/nats
```
