/**
 * `NatsRequestReplyNamespace` — the REST-adapter (gateway) side of running
 * Ceves commands as NATS messages.
 *
 * It satisfies the same duck-typed `DurableObjectNamespace` contract the
 * Ceves routing layer resolves from `c.env[<BINDING>]`, but instead of
 * dispatching to an in-process actor, `stub.fetch()` serializes the HTTP
 * request into a {@link NatsRequestEnvelope} and sends it as a NATS
 * request on `<prefix>.<aggregateType>.<aggregateId>`. Whichever
 * `NatsAggregateService` instance subscribes to that subject executes the
 * command and replies with the serialized response.
 *
 * This makes the NATS message the *main route* for every command and
 * query: the Node app holding the Hono/chanfana routes is a thin REST
 * adapter that needs no aggregate classes, no JetStream access, and no KV
 * access — only a core NATS connection.
 *
 * Failure mapping (mirrors `safeDOFetch`'s philosophy of structured
 * transport errors):
 * - no responders on the subject (no service running) → 503 with the
 *   standard chanfana error shape,
 * - request timeout → 504,
 * - anything else → rethrown for Hono's error handler.
 */

import { createLogger } from '../../logger';
import type { ITenantResolver } from '../../tenancy/TenantResolver';
import type { NatsOrgDirectory } from './NatsOrgDirectory';
import { HeaderTenantResolver } from '../../tenancy/HeaderTenantResolver';
import { aggregateIdFor, type NatsAggregateId } from './NatsAggregateContext';
import type { NatsAggregateStub } from './NatsAggregateNamespace';
import {
  commandSubjectFor,
  requestToEnvelope,
  envelopeToResponse,
  type NatsResponseEnvelope,
} from './http-over-nats';

const logger = createLogger({ component: 'NatsRequestReplyNamespace' });

/** The slice of a NATS message the gateway reads from a reply. */
export interface NatsReplyMessageLike {
  json<T>(): T;
}

/** The slice of a core NATS connection the gateway needs. */
export interface NatsRequestConnectionLike {
  request(
    subject: string,
    payload?: Uint8Array | string,
    opts?: { timeout?: number }
  ): Promise<NatsReplyMessageLike>;
}

export interface NatsRequestReplyNamespaceDeps {
  connection: NatsRequestConnectionLike;
  aggregateType: string;
  /** First tokens of command subjects. Default `ceves.cmd`. */
  subjectPrefix: string;
  /** Request timeout in milliseconds. */
  timeoutMillis: number;
  /**
   * Resolves the org token for the command subject from each request —
   * routing metadata only (authorization stays in the aggregate). Defaults
   * to `HeaderTenantResolver('X-Org-Id', defaultOrgId ?? 'default-org')`,
   * so unauthenticated requests (e.g. public queries) still route.
   */
  tenantResolver?: ITenantResolver;
  /** Fallback org when the default resolver finds no header. */
  defaultOrgId?: string;
  /**
   * Aggregate → home-org directory. When present it is consulted FIRST:
   * an existing aggregate always routes to its authoritative home org, so
   * a wrong (or absent) tenant claim can neither fork a stream nor miss
   * it — the claim only matters for aggregates that don't exist yet
   * (creates), where it mints the home org.
   */
  orgDirectory?: NatsOrgDirectory;
}

/** Walk `cause` chains looking for a matcher hit (v3 wraps request errors). */
function errorChainMatches(err: unknown, matches: (e: Record<string, unknown>) => boolean): boolean {
  let current: unknown = err;
  for (let depth = 0; depth < 5 && typeof current === 'object' && current !== null; depth++) {
    const record = current as Record<string, unknown>;
    if (matches(record)) return true;
    current = record['cause'];
  }
  return false;
}

function isNoResponders(err: unknown): boolean {
  return errorChainMatches(err, (e) => {
    if (e['name'] === 'NoRespondersError') return true;
    if (e['code'] === '503') return true;
    return typeof e['message'] === 'string' && e['message'].toLowerCase().includes('no responders');
  });
}

function isTimeout(err: unknown): boolean {
  return errorChainMatches(err, (e) => {
    if (e['name'] === 'TimeoutError') return true;
    if (e['code'] === 'TIMEOUT') return true;
    return typeof e['message'] === 'string' && e['message'].toLowerCase().includes('timeout');
  });
}

function transportErrorResponse(status: 503 | 504, message: string): Response {
  // Only the 503 (no responders) case advertises a retry: the command
  // demonstrably never reached a service. A 504 means the command MAY
  // still commit after the gateway gave up waiting — blindly retrying a
  // non-idempotent command could double-apply it.
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (status === 503) headers['Retry-After'] = '2';
  return new Response(JSON.stringify({ success: false, errors: [{ code: status, message }] }), {
    status,
    headers,
  });
}

export class NatsRequestReplyNamespace {
  private readonly tenantResolver: ITenantResolver;

  constructor(private readonly deps: NatsRequestReplyNamespaceDeps) {
    this.tenantResolver =
      deps.tenantResolver ??
      new HeaderTenantResolver('X-Org-Id', deps.defaultOrgId ?? 'default-org');
  }

  idFromName(name: string): NatsAggregateId {
    return aggregateIdFor(name);
  }

  idFromString(id: string): NatsAggregateId {
    return aggregateIdFor(id);
  }

  newUniqueId(): NatsAggregateId {
    return aggregateIdFor(crypto.randomUUID());
  }

  get(id: NatsAggregateId | string): NatsAggregateStub {
    const name = typeof id === 'string' ? id : id.name;
    return {
      id: aggregateIdFor(name),
      name,
      fetch: (input, init) => {
        const request = typeof input === 'string' ? new Request(input, init) : input;
        return this.send(name, request);
      },
    };
  }

  private async send(aggregateId: string, request: Request): Promise<Response> {
    // Routing ladder: the org directory is authoritative for existing
    // aggregates; the caller's claim (tenant resolver) only applies when
    // no directory entry exists yet. Resolve BEFORE the envelope consumes
    // the request body.
    const homeOrg = this.deps.orgDirectory
      ? await this.deps.orgDirectory.resolve(this.deps.aggregateType, aggregateId)
      : null;
    const orgId = homeOrg ?? (await this.tenantResolver.resolveOrgId(request));
    const subject = commandSubjectFor(
      this.deps.subjectPrefix,
      orgId,
      this.deps.aggregateType,
      aggregateId
    );
    const envelope = await requestToEnvelope(request);

    try {
      const reply = await this.deps.connection.request(subject, JSON.stringify(envelope), {
        timeout: this.deps.timeoutMillis,
      });
      return envelopeToResponse(reply.json<NatsResponseEnvelope>());
    } catch (error) {
      if (isNoResponders(error)) {
        logger.warn('No aggregate service responding on command subject', {
          aggregateType: this.deps.aggregateType,
          aggregateId,
          subject,
        });
        return transportErrorResponse(
          503,
          'Aggregate service unavailable — no responders on the command subject'
        );
      }
      if (isTimeout(error)) {
        logger.error('Aggregate service request timed out', {
          aggregateType: this.deps.aggregateType,
          aggregateId,
          subject,
          timeoutMillis: this.deps.timeoutMillis,
        });
        return transportErrorResponse(
          504,
          'Aggregate service request timed out — the command may still be applied'
        );
      }
      throw error;
    }
  }
}
