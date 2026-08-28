/**
 * Ceves NATS adapter — run Ceves event-sourced apps on NATS instead of
 * Cloudflare, with the same route classes, event handlers, and aggregate
 * classes.
 *
 * | Cloudflare piece                | NATS counterpart                       |
 * |---------------------------------|----------------------------------------|
 * | R2 event log (`R2EventStore`)   | JetStream stream ({@link NatsEventStore}) |
 * | Durable Object storage (state)  | NATS KV bucket ({@link NatsKvStorage}) |
 * | Durable Object namespace/stub   | {@link NatsAggregateNamespace} (in-process actors) |
 * | DO single-threaded execution    | per-actor serial queue + KV CAS + JetStream per-subject OCC |
 * | `wrangler.jsonc` bindings       | {@link startNatsCevesRuntime} building `env` |
 *
 * See `runtime.ts` for a usage example, and
 * `./cloudflare-workers-shim` for making `import 'cloudflare:workers'`
 * resolve under Node.
 *
 * @packageDocumentation
 */

export {
  NatsEventStore,
  DEFAULT_EVENT_STREAM,
  DEFAULT_EVENT_SUBJECT_PREFIX,
  DEFAULT_ROUTED_EVENT_STREAM,
  DEFAULT_ROUTED_EVENT_SUBJECT_PREFIX,
  type NatsEventStoreOptions,
} from './NatsEventStore';

export {
  NatsKvStorage,
  NULL_ALARM_HANDLE,
  type AlarmHandle,
} from './NatsKvStorage';

export {
  NatsAggregateContext,
  aggregateIdFor,
  type NatsAggregateId,
} from './NatsAggregateContext';

export {
  NatsAggregateNamespace,
  type NatsAggregateClass,
  type NatsAggregateInstance,
  type NatsAggregateNamespaceDeps,
  type NatsAggregateStub,
} from './NatsAggregateNamespace';

export {
  NatsOrgDirectory,
  DEFAULT_ORG_DIRECTORY_BUCKET,
} from './NatsOrgDirectory';

export {
  AGGREGATE_TRANSFERRED_OUT_EVENT,
  registerTransferredOutHandler,
  type AggregateTransferredOutEvent,
  type AggregateTransferSummary,
} from './transfer';

export {
  startNatsCevesRuntime,
  startNatsAggregateService,
  createNatsGatewayEnv,
  openNatsOrgDirectory,
  DEFAULT_STATE_BUCKET,
  type NatsAggregateRegistration,
  type NatsCevesRuntime,
  type StartNatsCevesRuntimeOptions,
  type StartNatsAggregateServiceOptions,
  type NatsAggregateServiceRuntime,
  type NatsGatewayAggregate,
  type CreateNatsGatewayEnvOptions,
} from './runtime';

export {
  NatsAggregateService,
  type NatsAggregateServiceDeps,
  type NatsCommandMessageLike,
  type NatsSubscriptionLike,
  type NatsSubscribeConnectionLike,
} from './NatsAggregateService';

export {
  NatsRequestReplyNamespace,
  type NatsRequestReplyNamespaceDeps,
  type NatsRequestConnectionLike,
  type NatsReplyMessageLike,
} from './NatsRequestReplyNamespace';

export {
  DEFAULT_COMMAND_SUBJECT_PREFIX,
  ANY_ORG,
  commandSubjectFor,
  commandSubscriptionSubjectFor,
  requestToEnvelope,
  envelopeToRequest,
  responseToEnvelope,
  envelopeToResponse,
  type NatsRequestEnvelope,
  type NatsResponseEnvelope,
} from './http-over-nats';

export {
  encodeToken,
  decodeToken,
  eventSubjectFor,
  tenantEventSubjectFor,
  routedEventSubjectFor,
  storageKeyPrefixFor,
  aggregateTypeToBinding,
} from './naming';

export {
  jetStreamApiErrorCode,
  isWrongLastSequence,
  JS_ERR_WRONG_LAST_SEQUENCE,
  JS_ERR_WRONG_LAST_SEQUENCE_UNKNOWN,
  JS_ERR_NO_MESSAGE_FOUND,
  JS_ERR_STREAM_NOT_FOUND,
} from './jetstream-errors';
