/**
 * HTTP-over-NATS envelopes — the wire format used to carry Ceves commands
 * and queries as NATS request-reply messages.
 *
 * On the NATS runtime the *main route* for a command is a NATS message:
 * the REST gateway (`NatsRequestReplyNamespace`) serializes the validated
 * HTTP request into a {@link NatsRequestEnvelope} and publishes it as a
 * request on `<prefix>.<orgId>.<aggregateType>.<aggregateId>`; the
 * aggregate service (`NatsAggregateService`) decodes it back into a
 * `Request`, runs the normal `AggregateObject.fetch()` pipeline, and
 * replies with a {@link NatsResponseEnvelope}.
 *
 * The org token is ROUTING metadata: it carries the caller's *claimed*
 * tenant (resolved by the gateway's tenant resolver, e.g. from the
 * `X-Org-Id` header) so broker-level permissions can fence a tenant's
 * credentials to `<prefix>.<org>.>` and services can be deployed
 * per-tenant. It is NOT an authorization decision — the aggregate's
 * `checkAuthorization()` and tenant resolver remain the enforcement
 * point, exactly as on Cloudflare.
 *
 * Bodies are carried as text — Ceves command bodies are JSON and the
 * framework's responses are JSON. Sub-1MB payloads fit NATS' default
 * message size limit comfortably.
 */

import { encodeToken } from './naming';

/** Default first tokens of command subjects. */
export const DEFAULT_COMMAND_SUBJECT_PREFIX = 'ceves.cmd';

/** Org token used when a service subscribes across all tenants. */
export const ANY_ORG = '*';

/** Serialized HTTP request carried inside a NATS request message. */
export interface NatsRequestEnvelope {
  method: string;
  url: string;
  headers: Record<string, string>;
  body?: string;
}

/** Serialized HTTP response carried inside a NATS reply message. */
export interface NatsResponseEnvelope {
  status: number;
  headers: Record<string, string>;
  body: string;
}

/**
 * Subject carrying one aggregate's commands/queries:
 * `<prefix>.<orgId>.<aggregateType>.<aggregateId>` (tokens escaped).
 */
export function commandSubjectFor(
  subjectPrefix: string,
  orgId: string,
  aggregateType: string,
  aggregateId: string
): string {
  return (
    `${subjectPrefix}.${encodeToken(orgId)}.` +
    `${encodeToken(aggregateType)}.${encodeToken(aggregateId)}`
  );
}

/**
 * Wildcard subject an aggregate service subscribes to for one type.
 * With the default `orgFilter` ({@link ANY_ORG}) the service receives
 * every tenant's commands; pass a concrete org id to host a single
 * tenant. Run EITHER wildcard-org services OR org-filtered services for
 * a given aggregate type — mixing both makes overlapping queue groups
 * each receive a copy of the same command.
 */
export function commandSubscriptionSubjectFor(
  subjectPrefix: string,
  aggregateType: string,
  orgFilter: string = ANY_ORG
): string {
  const orgToken = orgFilter === ANY_ORG ? '*' : encodeToken(orgFilter);
  return `${subjectPrefix}.${orgToken}.${encodeToken(aggregateType)}.>`;
}

function headersToRecord(headers: Headers): Record<string, string> {
  const record: Record<string, string> = {};
  headers.forEach((value, key) => {
    record[key] = value;
  });
  return record;
}

/** Serialize a `Request` (consumes its body). */
export async function requestToEnvelope(request: Request): Promise<NatsRequestEnvelope> {
  const body = request.body ? await request.text() : undefined;
  return {
    method: request.method,
    url: request.url,
    headers: headersToRecord(request.headers),
    ...(body !== undefined && body !== '' ? { body } : {}),
  };
}

/** Rebuild a `Request` from an envelope. */
export function envelopeToRequest(envelope: NatsRequestEnvelope): Request {
  return new Request(envelope.url, {
    method: envelope.method,
    headers: envelope.headers,
    ...(envelope.body !== undefined ? { body: envelope.body } : {}),
  });
}

/** Serialize a `Response` (consumes its body). */
export async function responseToEnvelope(response: Response): Promise<NatsResponseEnvelope> {
  return {
    status: response.status,
    headers: headersToRecord(response.headers),
    body: await response.text(),
  };
}

/** Rebuild a `Response` from an envelope. */
export function envelopeToResponse(envelope: NatsResponseEnvelope): Response {
  return new Response(envelope.body === '' ? null : envelope.body, {
    status: envelope.status,
    headers: envelope.headers,
  });
}
