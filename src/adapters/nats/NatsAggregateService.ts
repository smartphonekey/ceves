/**
 * `NatsAggregateService` — the host side of running Ceves commands as NATS
 * messages.
 *
 * It subscribes (in a queue group) to one aggregate type's command subject
 * `<prefix>.<org|*>.<aggregateType>.>`, decodes each {@link NatsRequestEnvelope}
 * back into a `Request`, dispatches it to the local actor host (a
 * {@link NatsAggregateNamespace}, i.e. the real `AggregateObject`
 * pipeline over JetStream/KV), and replies with the serialized response.
 *
 * Ordering: messages are processed concurrently across aggregates, but the
 * dispatch into the per-aggregate serial queue happens synchronously in
 * subscription-arrival order, so one aggregate's commands execute in the
 * order NATS delivered them.
 *
 * Scaling note: NATS queue groups distribute messages with no per-key
 * affinity, so running several service instances for the same type means
 * commands for one aggregate can land on different hosts. That is safe —
 * the KV CAS + JetStream per-subject expectations reject the loser with a
 * 409 — but a single instance per aggregate type (or a partitioned
 * subject scheme) avoids conflict-retry churn.
 */

import { createLogger } from '../../logger';
import type { NatsAggregateNamespace } from './NatsAggregateNamespace';
import { decodeToken, encodeToken } from './naming';
import {
  commandSubscriptionSubjectFor,
  envelopeToRequest,
  responseToEnvelope,
  type NatsRequestEnvelope,
  type NatsResponseEnvelope,
} from './http-over-nats';

const logger = createLogger({ component: 'NatsAggregateService' });

/** The slice of a NATS message the service handles. */
export interface NatsCommandMessageLike {
  subject: string;
  respond(payload: Uint8Array | string): boolean;
  json<T>(): T;
}

/** The slice of a NATS subscription the service consumes. */
export interface NatsSubscriptionLike extends AsyncIterable<NatsCommandMessageLike> {
  drain(): Promise<void>;
}

/** The slice of a core NATS connection the service needs. */
export interface NatsSubscribeConnectionLike {
  subscribe(subject: string, opts?: { queue?: string }): NatsSubscriptionLike;
}

export interface NatsAggregateServiceDeps {
  connection: NatsSubscribeConnectionLike;
  aggregateType: string;
  /** Local actor host executing the commands. */
  local: NatsAggregateNamespace;
  /** First tokens of command subjects. Default `ceves.cmd`. */
  subjectPrefix: string;
  /**
   * Host only one tenant's aggregates: subscribes
   * `<prefix>.<orgFilter>.<type>.>` instead of the all-tenants wildcard.
   * Deploy EITHER wildcard services OR org-filtered services for a given
   * aggregate type — overlapping queue groups each receive a copy.
   */
  orgFilter?: string;
  /** Queue group name. Default `ceves-svc[-<orgFilter>]-<aggregateType>`. */
  queue?: string;
}

function errorEnvelope(status: number, message: string): NatsResponseEnvelope {
  return {
    status,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ success: false, errors: [{ code: status, message }] }),
  };
}

export class NatsAggregateService {
  private subscription: NatsSubscriptionLike | null = null;
  private readonly inFlight = new Set<Promise<void>>();
  private consumeLoop: Promise<void> | null = null;

  constructor(private readonly deps: NatsAggregateServiceDeps) {}

  /** Subscribe and start serving. Resolves once the subscription is set up. */
  start(): void {
    if (this.subscription) return;
    const subject = commandSubscriptionSubjectFor(
      this.deps.subjectPrefix,
      this.deps.aggregateType,
      this.deps.orgFilter
    );
    // The org filter is part of the queue name so differently-scoped
    // deployments never share a group (they subscribe different subjects).
    const orgPart = this.deps.orgFilter ? `-${encodeToken(this.deps.orgFilter)}` : '';
    const queue =
      this.deps.queue ?? `ceves-svc${orgPart}-${encodeToken(this.deps.aggregateType)}`;
    const subscription = this.deps.connection.subscribe(subject, { queue });
    this.subscription = subscription;
    logger.info('Aggregate service subscribed', {
      aggregateType: this.deps.aggregateType,
      subject,
      queue,
    });

    this.consumeLoop = (async () => {
      for await (const message of subscription) {
        // Dispatch synchronously (no await) so same-aggregate commands enter
        // the actor's serial queue in delivery order; completion is tracked
        // for graceful shutdown.
        const done = this.handle(message).catch((error) => {
          logger.error('Aggregate service message handling failed', {
            aggregateType: this.deps.aggregateType,
            error: error instanceof Error ? error.message : String(error),
          });
        });
        this.inFlight.add(done);
        void done.finally(() => this.inFlight.delete(done));
      }
    })().catch((error) => {
      logger.error('Aggregate service subscription loop ended with error', {
        aggregateType: this.deps.aggregateType,
        error: error instanceof Error ? error.message : String(error),
      });
    });
  }

  /**
   * Handle one command message. Everything up to the actor-queue enqueue
   * runs synchronously (see `start`); the reply is sent when the pipeline
   * finishes.
   */
  private handle(message: NatsCommandMessageLike): Promise<void> {
    let envelope: NatsRequestEnvelope;
    try {
      envelope = message.json<NatsRequestEnvelope>();
    } catch {
      message.respond(JSON.stringify(errorEnvelope(400, 'Malformed command envelope')));
      return Promise.resolve();
    }

    const subjectTokens = message.subject.split('.');
    const lastToken = subjectTokens[subjectTokens.length - 1] ?? '';
    const aggregateId = decodeToken(lastToken);

    // Synchronous up to and including the serial-queue enqueue.
    const responsePromise = this.deps.local
      .get(aggregateId)
      .fetch(envelopeToRequest(envelope));

    return responsePromise
      .then(responseToEnvelope)
      .catch((error): NatsResponseEnvelope => {
        logger.error('Aggregate execution failed', {
          aggregateType: this.deps.aggregateType,
          aggregateId,
          error: error instanceof Error ? error.message : String(error),
        });
        return errorEnvelope(500, 'Aggregate execution failed');
      })
      .then((reply) => {
        message.respond(JSON.stringify(reply));
      });
  }

  /** Stop consuming, wait for in-flight commands to finish replying. */
  async stop(): Promise<void> {
    if (this.subscription) {
      await this.subscription.drain();
      this.subscription = null;
    }
    if (this.consumeLoop) {
      await this.consumeLoop;
      this.consumeLoop = null;
    }
    while (this.inFlight.size > 0) {
      await Promise.all([...this.inFlight]);
    }
  }
}
