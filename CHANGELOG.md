# Changelog

## 0.4.0

Framework sync: this release replaces the 0.3.x codebase with the actively
developed line of the framework, adds the NATS runtime adapter, and makes the
package fully self-contained (the routing decorator, registry, router factory
and logger that had moved to an internal package are vendored back in).
Under 0.x semver this is a **breaking** release.

### Added

- **NATS runtime adapter** (`ceves/nats`): run unmodified ceves apps on NATS —
  JetStream event log (`NatsEventStore`, per-subject OCC + version-continuity
  guards), NATS KV state (`NatsKvStorage`, revision CAS), commands as NATS
  request-reply messages (`startNatsCevesRuntime` / gateway + queue-group
  `NatsAggregateService`), a routed event stream for tenant/type/event
  subscriptions, a home-org partitioned log with claim-proof routing
  (`NatsOrgDirectory`), and org transfer with fresh-start semantics.
  Architecture write-up: `docs/architecture/nats.md`.
- **AWS Lambda adapter** (`ceves/aws`): `createLambdaHandler`, `S3EventStore`,
  `S3SnapshotStore`.
- **Event projection** (`registerProjector`, `ProjectionDispatcher`, cursor
  persistence via the `ProjectorCursor` type, DLQ writer hook) with
  alarm-driven catch-up and `/ResyncProjections`.
- **Cross-RPC typed errors**: `serializeErrorToResponse` /
  `rehydrateErrorFromResponse` / `stubFetchWithTypedErrors` +
  `X-Ceves-Error` header, so DO-thrown `CevesError`s survive the stub
  boundary with their types intact.
- `safeDOFetch` — coerces Cloudflare's synthetic uncatchable DO errors into a
  structured 503.
- `ApiKeyTenantResolver` (D1-backed `api_keys` lookup).
- `AggregateRouter.configure()` — a closed allowlist of routable aggregate
  types (`allowedTypes` + `bindingNames`), with strip-trailing-`s` and
  uppercase-singular conventions (`users` → `user` → `env.USER`) resolving
  the details. Unregistered types get a 400, so internal DO bindings stay
  unreachable over HTTP.
- **`ceves-generate-imports` CLI** — generates the static decorator-import
  barrel (replaces both `import.meta.glob` and the 0.3.x `ceves-discover`).
- Command responses now include the emitted event:
  `{ success, aggregateId, version, event: { type, data } | null }`; command
  routes may declare `static readonly eventSchema` to surface the typed event
  in OpenAPI and runtime-parse `executeCommand`'s return value.
- Batched, resumable R2 replay with DO-storage checkpointing (AA-117);
  request-body draining on every early-return path (AA-193); exception
  capturer hook (`setExceptionCapturer`).

### Changed (breaking)

- **Create is idempotent, not 409** (AA-92): a `CreateCommandRoute` command at
  an existing aggregate returns 200 with `event: null`; a genuine create
  returns **201**. The `noEvent: true` response field is gone.
- **Snapshot stores removed from the Cloudflare path**: `R2SnapshotStore` and
  `D1SnapshotStore` no longer exist — DO state persists in
  `DurableObjectState.storage`. `setStores(eventStore, tenantResolver)` takes
  2 args (snapshot store dropped). S3 snapshots remain for the AWS path.
- **`restoration` module removed** (`restoreState` / `restoreFromEvents`):
  restoration lives inside `AggregateObject` (snapshot-free batched replay).
- **Old command/event schema base classes removed** (`schemas/Command`,
  `schemas/Event`): the `@Route` schema + `eventSchema` are the source of
  truth. `BaseState` is now a class (ADR-009: handlers never receive `null`
  state).
- `GET …/__state` on a never-created aggregate returns a 404 error envelope
  (was 200 `null`).
- Error responses embed a typed `__ceves` payload and the `X-Ceves-Error`
  header.
- `EventHandler` registry API: `getHandlersByAggregateType` and
  `EventHandlerMetadata` are gone; `@EventHandler` is a bare decorator and
  handlers set `eventType` / `aggregateType` as instance fields.
- `createRouter`: `onError` never rethrows (structured 500 envelope), non-`Error`
  throws are coerced, every response path drains the request body, and an
  explicit `routes:` array (from the generated barrel) is the recommended way
  to pin the route surface. The 0.3.x `discover` hint and `ceves-discover`
  CLI are gone.
- Dependencies: zod 4, chanfana 3 (which brings zod-to-openapi 8
  transitively). `@cloudflare/workers-types` is pinned exactly in this repo's
  devDependencies (newer snapshots have caused type-check blowups — bump
  deliberately).
- Admin cleanup endpoints (`__delete`) authorize against
  `env.ALLOWED_EMAIL_DOMAIN`, defaulting to `example.com` (set the var in any
  real deployment).
- `AggregateRouter` no longer routes any type out of the box: the old
  hardcoded `users`/`locks`/`hubs`/`tempkeys` map is gone — register your
  types at worker init via `AggregateRouter.configure({ allowedTypes })`.
- The `types/webhooks` module and its `./types/webhooks` subpath export were
  dropped (app-specific webhook event vocabulary, not framework surface).
- `ISnapshotStore` left the top-level barrel; it is re-exported from
  `ceves/aws`, the only path that still uses out-of-band snapshots.
- `@cloudflare/workers-types` moved to an optional peer dependency — install
  it as a devDependency in TypeScript projects consuming the root entry.

### Migration notes

- Replace `restoreState`/`restoreFromEvents` usage with `AggregateObject`
  subclasses (state restoration is automatic).
- Replace `ceves-discover` with `ceves-generate-imports` (see README) and pass
  the generated `REGISTERED_ROUTES` to `createRouter({ routes })`.
- Callers that treated create-on-existing 409 as "already exists" should treat
  200 + `event: null` as the same signal.
- If you relied on `AggregateRouter`'s hardcoded type maps: call
  `AggregateRouter.configure({ allowedTypes: ['user', 'lock', 'hub', 'tempkey'] })`
  at worker init to restore the old routing surface exactly; register
  irregular plurals / binding names the same way.
