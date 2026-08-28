/**
 * NATS runtime bootstrap for Ceves apps.
 *
 * `startNatsCevesRuntime()` is the NATS counterpart of a `wrangler.jsonc`
 * bindings section: it provisions the JetStream event stream and the KV
 * state bucket, then builds an `env` object in which every registered
 * aggregate appears under its Durable-Object-style binding name
 * (`BankAccountAggregate` → `BANK_ACCOUNT`) as a {@link NatsAggregateNamespace}.
 *
 * Hand that `env` to the same Hono app `createRouter()` builds for
 * Cloudflare, and the whole Ceves routing/command/query pipeline runs on
 * NATS unmodified:
 *
 * ```ts
 * import { connect } from '@nats-io/transport-node';
 * import { serve } from '@hono/node-server';
 * import { createRouter } from 'ceves';
 * import { startNatsCevesRuntime } from 'ceves/nats';
 * import { BankAccountAggregate } from './aggregates/BankAccountAggregate';
 *
 * const nc = await connect({ servers: 'nats://localhost:4222' });
 * const runtime = await startNatsCevesRuntime({
 *   connection: nc,
 *   aggregates: [{ AggregateClass: BankAccountAggregate }],
 * });
 * const app = createRouter({ openapi: { title: 'Bank API', version: '1.0.0' } });
 * serve({ port: 8788, fetch: (req) => app.fetch(req, runtime.env) });
 * ```
 *
 * Note: running the app's `AggregateObject` subclasses on Node requires the
 * `cloudflare:workers` module specifier to resolve to the shim shipped at
 * `ceves/nats/cloudflare-workers-shim` (via a bundler alias or a Node
 * resolve hook) — Ceves' DO base class imports it unconditionally.
 */

import {
  jetstream,
  jetstreamManager,
  RetentionPolicy,
  StorageType,
  DiscardPolicy,
} from '@nats-io/jetstream';
import { Kvm } from '@nats-io/kv';
import { createLogger } from '../../logger';
import {
  NatsEventStore,
  DEFAULT_EVENT_STREAM,
  DEFAULT_EVENT_SUBJECT_PREFIX,
  DEFAULT_ROUTED_EVENT_STREAM,
  DEFAULT_ROUTED_EVENT_SUBJECT_PREFIX,
} from './NatsEventStore';
import {
  NatsAggregateNamespace,
  type NatsAggregateClass,
} from './NatsAggregateNamespace';
import {
  NatsAggregateService,
  type NatsSubscribeConnectionLike,
} from './NatsAggregateService';
import { NatsOrgDirectory, DEFAULT_ORG_DIRECTORY_BUCKET } from './NatsOrgDirectory';
import {
  NatsRequestReplyNamespace,
  type NatsRequestConnectionLike,
} from './NatsRequestReplyNamespace';
import { DEFAULT_COMMAND_SUBJECT_PREFIX } from './http-over-nats';
import { aggregateTypeToBinding } from './naming';
import {
  jetStreamApiErrorCode,
  JS_ERR_STREAM_NOT_FOUND,
  JS_ERR_STREAM_NAME_IN_USE,
} from './jetstream-errors';

const logger = createLogger({ component: 'NatsCevesRuntime' });

/** The connection type `jetstream()` accepts (avoids a direct nats-core dep). */
type NatsConnectionLike = Parameters<typeof jetstream>[0];

export const DEFAULT_STATE_BUCKET = 'ceves_state';

/** One aggregate class to host on the NATS runtime. */
export interface NatsAggregateRegistration {
  /** The app's `AggregateObject` subclass (e.g. `BankAccountAggregate`). */
  AggregateClass: NatsAggregateClass;
  /**
   * Aggregate type as referenced by routes' `aggregateType`.
   * Defaults to the class name — which is what `AggregateObject` derives too.
   */
  aggregateType?: string;
  /** Env binding name. Defaults to the Ceves derivation (`BANK_ACCOUNT`). */
  binding?: string;
  /**
   * Event types whose commit means "this aggregate changed org" (the
   * app's SetOrganization-style sale/claim endpoint) — committing one runs
   * the full org transfer to the org in the event data's `orgId` field
   * before the command responds. See
   * `NatsAggregateNamespaceDeps.orgTransferOn`. Example:
   * `{ AggregateClass: LockAggregate, orgTransferOn: ['OrganizationSet'] }`.
   */
  orgTransferOn?: string[];
}

export interface StartNatsCevesRuntimeOptions {
  /** An established NATS connection (from `@nats-io/transport-node`). Caller owns its lifecycle. */
  connection: NatsConnectionLike;
  /** Aggregates to host. */
  aggregates: NatsAggregateRegistration[];
  /** Extra env entries (secrets, config) merged into the returned `env`. */
  env?: Record<string, unknown>;
  /** JetStream stream for the event log. Default `CEVES_EVENTS`. */
  streamName?: string;
  /** Subject prefix for event subjects. Default `ceves.events`. */
  subjectPrefix?: string;
  /** KV bucket holding aggregate state. Default `ceves_state`. */
  stateBucket?: string;
  /** Stream duplicate-detection window in nanoseconds. Default 5 minutes. */
  duplicateWindowNanos?: number;
  /**
   * Tenant resolver injected into hosted aggregates. Defaults to
   * `HeaderTenantResolver('X-Org-Id', env.DEFAULT_ORG_ID ?? 'default-org')`.
   * Note the injection overrides a resolver an aggregate subclass
   * constructor installed — pass it here instead on the NATS runtime.
   */
  tenantResolver?: import('../../tenancy/TenantResolver').ITenantResolver;
  /**
   * The routed (derived) event stream: every committed event is fanned
   * out to `<subjectPrefix>.<org>.<aggregateType>.<eventType>.<aggregateId>`
   * so consumers can subscribe by tenant, aggregate type, or event type.
   * Enabled by default; pass `false` to keep only the canonical
   * per-aggregate log.
   */
  routedEvents?: false | { streamName?: string; subjectPrefix?: string };
  /**
   * Aggregate → home-org directory (KV bucket, default `ceves_org_dir`):
   * partitions the canonical event log per tenant
   * (`ceves.events.<homeOrg>.<type>.<id>`), makes aggregate ids globally
   * unique across orgs via a CAS claim at creation, and lets the gateway
   * route to the authoritative tenant regardless of the caller's claim.
   * Enabled by default; pass `false` for an org-free canonical log.
   */
  orgDirectory?: false | { bucket?: string };
}

export interface NatsCevesRuntime {
  /**
   * Env for `app.fetch(request, env)`: aggregate namespaces under their
   * binding names plus everything passed via `options.env`.
   */
  env: Record<string, unknown>;
  /** The JetStream-backed event store shared by all hosted aggregates. */
  eventStore: NatsEventStore;
  /** Hosted namespaces keyed by binding name. */
  namespaces: ReadonlyMap<string, NatsAggregateNamespace>;
  /** Aggregate → home-org directory (null when disabled). */
  orgDirectory: NatsOrgDirectory | null;
  /**
   * Transfer an aggregate to another org (the "sell the lock" flow) —
   * see {@link NatsAggregateNamespace.transferOut}: seals the old org's
   * stream, purges state, seeds the new org's stream with the original
   * creation event (identity — id/number/uuid — is preserved; accumulated
   * state is not), and repoints the directory. Requires the org directory.
   */
  transferAggregate(
    aggregateType: string,
    aggregateId: string,
    toOrg: string
  ): Promise<import('./transfer').AggregateTransferSummary>;
  /** Drain in-flight actor work. Does NOT close the NATS connection. */
  close(): Promise<void>;
}

/** Provision NATS resources and build the aggregate env bindings. */
export async function startNatsCevesRuntime(
  options: StartNatsCevesRuntimeOptions
): Promise<NatsCevesRuntime> {
  const streamName = options.streamName ?? DEFAULT_EVENT_STREAM;
  const subjectPrefix = options.subjectPrefix ?? DEFAULT_EVENT_SUBJECT_PREFIX;
  const stateBucket = options.stateBucket ?? DEFAULT_STATE_BUCKET;

  const js = jetstream(options.connection);
  const jsm = await jetstreamManager(options.connection);

  await ensureEventStream(jsm, {
    streamName,
    subjects: [`${subjectPrefix}.>`],
    duplicateWindowNanos: options.duplicateWindowNanos ?? 5 * 60 * 1e9,
  });

  // Routed (derived) event stream — see StartNatsCevesRuntimeOptions.
  let routedSubjectPrefix: string | undefined;
  if (options.routedEvents !== false) {
    const routed = options.routedEvents ?? {};
    routedSubjectPrefix = routed.subjectPrefix ?? DEFAULT_ROUTED_EVENT_SUBJECT_PREFIX;
    await ensureEventStream(jsm, {
      streamName: routed.streamName ?? DEFAULT_ROUTED_EVENT_STREAM,
      subjects: [`${routedSubjectPrefix}.>`],
      duplicateWindowNanos: options.duplicateWindowNanos ?? 5 * 60 * 1e9,
    });
  }

  const kvm = new Kvm(js);
  const kv = await kvm.create(stateBucket, { history: 1 });

  let orgDirectory: NatsOrgDirectory | null = null;
  if (options.orgDirectory !== false) {
    const directoryKv = await kvm.create(
      options.orgDirectory?.bucket ?? DEFAULT_ORG_DIRECTORY_BUCKET,
      { history: 1 }
    );
    orgDirectory = new NatsOrgDirectory(directoryKv);
    // Keep the in-process cache coherent with org transfers performed by
    // other processes (see NatsOrgDirectory.startWatching).
    await orgDirectory.startWatching();
  }

  const eventStore = new NatsEventStore(js, jsm, {
    streamName,
    subjectPrefix,
    routedSubjectPrefix,
    orgDirectory: orgDirectory ?? undefined,
  });

  const env: Record<string, unknown> = { ...options.env };
  const namespaces = new Map<string, NatsAggregateNamespace>();
  const namespacesByType = new Map<string, NatsAggregateNamespace>();

  for (const registration of options.aggregates) {
    const aggregateType =
      registration.aggregateType ?? registration.AggregateClass.name;
    const binding = registration.binding ?? aggregateTypeToBinding(aggregateType);
    const namespace = new NatsAggregateNamespace({
      aggregateType,
      AggregateClass: registration.AggregateClass,
      kv,
      eventStore,
      getEnv: () => env,
      tenantResolver: options.tenantResolver,
      orgDirectory: orgDirectory ?? undefined,
      orgTransferOn: registration.orgTransferOn,
    });
    env[binding] = namespace;
    namespaces.set(binding, namespace);
    namespacesByType.set(aggregateType, namespace);
    logger.info('Registered aggregate on NATS runtime', { aggregateType, binding });
  }

  return {
    env,
    eventStore,
    namespaces,
    orgDirectory,
    transferAggregate: (aggregateType, aggregateId, toOrg) => {
      const namespace = namespacesByType.get(aggregateType);
      if (!namespace) {
        return Promise.reject(
          new Error(`transferAggregate: no aggregate registered for type "${aggregateType}"`)
        );
      }
      return namespace.transferOut(aggregateId, toOrg);
    },
    close: async () => {
      orgDirectory?.stopWatching();
      for (const namespace of namespaces.values()) {
        await namespace.drain();
      }
    },
  };
}

/**
 * Open (create-or-bind) the aggregate → home-org directory bucket.
 * Convenience for gateways, which otherwise hold only a core connection:
 * pass the result as `createNatsGatewayEnv({ orgDirectory })`.
 *
 * The directory's cache watch is started by default so an org transfer
 * performed elsewhere re-routes this gateway's commands to the new org
 * without a restart; pass `{ watch: false }` for a short-lived caller.
 */
export async function openNatsOrgDirectory(
  connection: Parameters<typeof jetstream>[0],
  bucket: string = DEFAULT_ORG_DIRECTORY_BUCKET,
  options: { watch?: boolean } = {}
): Promise<NatsOrgDirectory> {
  const kvm = new Kvm(jetstream(connection));
  const directory = new NatsOrgDirectory(await kvm.create(bucket, { history: 1 }));
  if (options.watch !== false) await directory.startWatching();
  return directory;
}

// ---------------------------------------------------------------------------
// Commands as NATS messages (the main route in a distributed topology)
// ---------------------------------------------------------------------------
//
// `startNatsCevesRuntime` above hosts aggregates in-process — fine for a
// monolith. In the distributed topology, the main route for every command
// and query is a NATS request-reply message:
//
//   REST adapter (createNatsGatewayEnv)          aggregate host (startNatsAggregateService)
//   Hono routes → env[BINDING] stub.fetch()  ──▶ queue-group subscriber on
//     serialize HTTP → request on                ceves.cmd.<type>.<id> → local actor
//     ceves.cmd.<type>.<id>                ◀──   (AggregateObject over JetStream/KV)
//                                                 reply = serialized response
//
// The gateway needs only a core NATS connection — no aggregate classes, no
// JetStream, no KV. The service owns storage and execution.

/** One aggregate type the gateway forwards to over NATS. */
export interface NatsGatewayAggregate {
  /** Aggregate type as referenced by routes' `aggregateType`. */
  aggregateType: string;
  /** Env binding name. Defaults to the Ceves derivation (`BANK_ACCOUNT`). */
  binding?: string;
}

export interface CreateNatsGatewayEnvOptions {
  /** A NATS connection (only core request-reply is used). */
  connection: NatsRequestConnectionLike;
  /** Aggregate types this gateway serves, by type name or with a custom binding. */
  aggregates: Array<NatsGatewayAggregate | string>;
  /** Extra env entries merged into the returned `env`. */
  env?: Record<string, unknown>;
  /** First tokens of command subjects. Default `ceves.cmd`. */
  commandSubjectPrefix?: string;
  /** Per-request timeout in milliseconds. Default 30_000. */
  timeoutMillis?: number;
  /**
   * Resolves the routing org token from each request (see
   * `NatsRequestReplyNamespaceDeps.tenantResolver`). Defaults to reading
   * `X-Org-Id` with `defaultOrgId` as fallback.
   */
  tenantResolver?: import('../../tenancy/TenantResolver').ITenantResolver;
  /** Fallback org token when the default resolver finds no header. Default `default-org`. */
  defaultOrgId?: string;
  /**
   * Aggregate → home-org directory (see `openNatsOrgDirectory`). Consulted
   * FIRST when routing: existing aggregates always go to their
   * authoritative home org; the caller's claim only applies to creates.
   */
  orgDirectory?: NatsOrgDirectory;
}

/**
 * Build the env for a REST adapter whose command/query routes forward to
 * aggregate services over NATS request-reply. Pure client — provisions
 * nothing.
 */
export function createNatsGatewayEnv(
  options: CreateNatsGatewayEnvOptions
): Record<string, unknown> {
  const subjectPrefix = options.commandSubjectPrefix ?? DEFAULT_COMMAND_SUBJECT_PREFIX;
  const timeoutMillis = options.timeoutMillis ?? 30_000;
  const env: Record<string, unknown> = { ...options.env };

  for (const entry of options.aggregates) {
    const aggregateType = typeof entry === 'string' ? entry : entry.aggregateType;
    const binding =
      typeof entry === 'string'
        ? aggregateTypeToBinding(aggregateType)
        : (entry.binding ?? aggregateTypeToBinding(aggregateType));
    env[binding] = new NatsRequestReplyNamespace({
      connection: options.connection,
      aggregateType,
      subjectPrefix,
      timeoutMillis,
      tenantResolver: options.tenantResolver,
      defaultOrgId: options.defaultOrgId,
      orgDirectory: options.orgDirectory,
    });
    logger.info('Registered NATS gateway binding', { aggregateType, binding });
  }
  return env;
}

export interface StartNatsAggregateServiceOptions extends StartNatsCevesRuntimeOptions {
  /** The service also needs to subscribe on the connection. */
  connection: StartNatsCevesRuntimeOptions['connection'] &
    NatsSubscribeConnectionLike;
  /** First tokens of command subjects. Default `ceves.cmd`. */
  commandSubjectPrefix?: string;
  /**
   * Host only one tenant's aggregates (subscribes
   * `<prefix>.<orgFilter>.<type>.>`). See `NatsAggregateServiceDeps.orgFilter`.
   */
  orgFilter?: string;
  /** Queue group name; one group per (org filter, aggregate type) by default. */
  queue?: string;
}

export interface NatsAggregateServiceRuntime extends NatsCevesRuntime {
  /** The running command subscribers, keyed by aggregate type. */
  services: ReadonlyMap<string, NatsAggregateService>;
}

/**
 * Host aggregates as a NATS service: provisions the event stream + state
 * bucket, builds the in-process actor host, and subscribes to each
 * aggregate type's command subject in a queue group. Commands arrive as
 * NATS messages and are answered with serialized responses.
 */
export async function startNatsAggregateService(
  options: StartNatsAggregateServiceOptions
): Promise<NatsAggregateServiceRuntime> {
  const runtime = await startNatsCevesRuntime(options);
  const subjectPrefix = options.commandSubjectPrefix ?? DEFAULT_COMMAND_SUBJECT_PREFIX;

  const services = new Map<string, NatsAggregateService>();
  for (const registration of options.aggregates) {
    const aggregateType =
      registration.aggregateType ?? registration.AggregateClass.name;
    const binding = registration.binding ?? aggregateTypeToBinding(aggregateType);
    const local = runtime.namespaces.get(binding);
    if (!local) continue;
    const service = new NatsAggregateService({
      connection: options.connection,
      aggregateType,
      local,
      subjectPrefix,
      orgFilter: options.orgFilter,
      queue: options.queue,
    });
    service.start();
    services.set(aggregateType, service);
  }

  return {
    ...runtime,
    services,
    close: async () => {
      for (const service of services.values()) {
        await service.stop();
      }
      await runtime.close();
    },
  };
}

interface EnsureStreamOptions {
  streamName: string;
  subjects: string[];
  duplicateWindowNanos: number;
}

/**
 * Create the event stream if it doesn't exist. An existing stream is left
 * untouched (its config is authoritative — mirrors how Terraform owns R2
 * buckets on the Cloudflare path).
 *
 * Startup is racy by design: every replica of a service runs this at once,
 * so on a first rollout several can see `STREAM_NOT_FOUND` and all call
 * `streams.add()`. Exactly one wins and the rest get "stream name already
 * in use" — a benign outcome (the stream exists, which is all the caller
 * wanted), so it is confirmed with a re-read rather than failing startup
 * and losing replicas arbitrarily.
 *
 * Exported for unit tests; not re-exported from the package entry point.
 */
export async function ensureEventStream(
  jsm: Awaited<ReturnType<typeof jetstreamManager>>,
  opts: EnsureStreamOptions
): Promise<void> {
  try {
    await jsm.streams.info(opts.streamName);
    return;
  } catch (error) {
    if (jetStreamApiErrorCode(error) !== JS_ERR_STREAM_NOT_FOUND) throw error;
  }

  try {
    await addEventStream(jsm, opts);
  } catch (error) {
    if (jetStreamApiErrorCode(error) !== JS_ERR_STREAM_NAME_IN_USE) throw error;
    // Another replica created it between our info() and add() — confirm it
    // is really there, then carry on.
    await jsm.streams.info(opts.streamName);
    logger.info('Event stream was created concurrently by another replica', {
      streamName: opts.streamName,
    });
    return;
  }
  logger.info('Created JetStream event stream', {
    streamName: opts.streamName,
    subjects: opts.subjects,
  });
}

async function addEventStream(
  jsm: Awaited<ReturnType<typeof jetstreamManager>>,
  opts: EnsureStreamOptions
): Promise<void> {
  await jsm.streams.add({
    name: opts.streamName,
    subjects: opts.subjects,
    // An event log is permanent history: limits retention with no limits set,
    // never workqueue/interest (those delete messages on consumption).
    retention: RetentionPolicy.Limits,
    storage: StorageType.File,
    discard: DiscardPolicy.Old,
    max_msgs: -1,
    max_bytes: -1,
    max_age: 0,
    max_msgs_per_subject: -1,
    // The store's last-message lookups go through the standard
    // $JS.API.STREAM.MSG.GET API (no allow_direct needed); allow_direct is
    // enabled so replica-served DIRECT.GET reads are available as a future
    // optimization on clustered streams.
    allow_direct: true,
    duplicate_window: opts.duplicateWindowNanos,
  });
}
