/**
 * Transactional outbox — wrapper-side relay.
 *
 * `drainPgOutbox` claims due rows from `<schema>.outbox` and performs the
 * real I/O the in-database handlers could not: kind `'event'` rows are
 * appended to the external event log via the {@link EventSink}, kind
 * `'fetch'` rows are sent with the platform `fetch`, and other kinds route
 * to deliverers the app registers. Delivered rows are DELETED (the outbox is
 * a transient queue); failures retry with exponential backoff until
 * `maxAttempts`, then are parked as `dead` for operator inspection — except
 * events, which retry indefinitely because a dropped event would leave a
 * hole in the event stream (see `maxAttemptsByKind`).
 *
 * Because event rows commit with the aggregate state, draining them is what
 * makes event delivery GUARANTEED rather than best-effort: the wrapper can
 * crash at any point and the next drain still ships the event.
 *
 * Concurrency-safe by construction: the claim is a single
 * `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)` statement, so any
 * number of drainers (the gateway's after-command drain, a cron sweeper, a
 * second region) can run against the same table without double-delivery.
 * A drainer that crashes mid-delivery leaves rows `inflight` with an expired
 * `locked_until`; the next drain reclaims them — hence AT-LEAST-ONCE
 * delivery, and receivers must be idempotent.
 *
 * Run it from two places:
 * - after each successful command (the gateway does this automatically when
 *   `outbox` options are set) — the low-latency fast path, and
 * - on a schedule (CF cron trigger / Node interval) — the catch-up sweeper
 *   that survives wrapper crashes.
 *
 * @packageDocumentation
 */

import { createLogger } from '../../logger';
import type { PgQueryClient } from './types';
import type { StoredEvent } from '../../storage/interfaces';
import { OUTBOX_EVENT_KIND, type OutboxFetchRequest } from './outbox';
import type { EventSink } from './eventSink';

const logger = createLogger({ component: 'PgOutboxRelay' });

/** One claimed outbox row as handed to a deliverer. */
export interface PgOutboxRow {
  id: string;
  kind: string;
  aggregateType: string;
  aggregateId: string;
  /** The enqueued payload (for kind 'fetch': an {@link OutboxFetchRequest}). */
  request: unknown;
  attempts: number;
}

/** Performs the real I/O for one row. Throw to signal a retryable failure. */
export type OutboxDeliverer = (row: PgOutboxRow) => Promise<void>;

/** Options for {@link drainPgOutbox}. */
export interface PgOutboxRelayOptions {
  /** PostgreSQL schema. Default 'ceves'. */
  schema?: string;
  /** Deliverers by kind. Kinds 'fetch' and 'event' have built-in defaults. */
  deliverers?: Record<string, OutboxDeliverer>;
  /**
   * Destination for kind `'event'` rows — the external event log (R2/S3).
   * Required whenever the dispatcher outboxes events (the default), since
   * those rows are the only remaining path to the log. `registerPgRoutes`
   * wires this from its own `eventSink` automatically.
   */
  eventSink?: EventSink;
  /** Rows claimed per batch. Default 25. */
  batchSize?: number;
  /** Attempts before a row is parked as 'dead'. Default 8. */
  maxAttempts?: number;
  /**
   * Per-kind override of `maxAttempts`. Defaults to `{ event: Infinity }`:
   * an event that stops being retried is an event MISSING from the log, and
   * a hole in the event stream breaks replay — so events retry forever at
   * the capped backoff instead of being parked as dead. Override only if you
   * have another way to reconcile a dropped event.
   */
  maxAttemptsByKind?: Record<string, number>;
  /** How long a claim holds before it can be reclaimed. Default 60s. */
  lockSeconds?: number;
  /** Base for exponential backoff (base * 2^attempts, capped at 1h). Default 5s. */
  backoffBaseSeconds?: number;
  /** fetch implementation for the built-in 'fetch' deliverer (tests). */
  fetchImpl?: (url: string, init: Record<string, unknown>) => Promise<{ ok: boolean; status: number }>;
}

/** Outcome counters returned by one drain run. */
export interface PgOutboxDrainSummary {
  claimed: number;
  delivered: number;
  retried: number;
  dead: number;
}

/** Built-in deliverer for kind 'fetch' — replays the serialized request. */
function fetchDeliverer(
  fetchImpl: NonNullable<PgOutboxRelayOptions['fetchImpl']>,
): OutboxDeliverer {
  return async (row): Promise<void> => {
    const request = row.request as OutboxFetchRequest;
    if (!request || typeof request.url !== 'string') {
      throw new Error(`outbox row ${row.id}: kind 'fetch' payload has no url`);
    }
    const response = await fetchImpl(request.url, {
      method: request.method ?? 'GET',
      headers: request.headers ?? {},
      body: request.body,
    });
    if (!response.ok) {
      throw new Error(`outbox fetch to ${request.url} failed with status ${response.status}`);
    }
  };
}

/** Built-in deliverer for kind 'event' — appends to the external event log. */
function eventDeliverer(sink: EventSink): OutboxDeliverer {
  return async (row): Promise<void> => {
    const event = row.request as StoredEvent | null;
    if (!event || typeof event.type !== 'string' || typeof event.version !== 'number') {
      throw new Error(`outbox row ${row.id}: kind 'event' payload is not a StoredEvent`);
    }
    await sink.append(event);
  };
}

function toRow(raw: Record<string, unknown>): PgOutboxRow {
  const request = raw.request;
  return {
    id: String(raw.id),
    kind: String(raw.kind),
    aggregateType: String(raw.aggregate_type ?? ''),
    aggregateId: String(raw.aggregate_id ?? ''),
    request: typeof request === 'string' ? (JSON.parse(request) as unknown) : request,
    attempts: Number(raw.attempts ?? 0),
  };
}

/** Claim the next batch of due rows (single statement — SKIP LOCKED safe). */
async function claimBatch(
  client: PgQueryClient,
  schema: string,
  batchSize: number,
  lockSeconds: number,
): Promise<PgOutboxRow[]> {
  const { rows } = await client.query(
    `UPDATE ${schema}.outbox SET status = 'inflight', ` +
      `locked_until = now() + make_interval(secs => $2) ` +
      `WHERE id IN (` +
      `  SELECT id FROM ${schema}.outbox ` +
      `  WHERE (status = 'pending' AND next_attempt_at <= now()) ` +
      `     OR (status = 'inflight' AND locked_until < now()) ` +
      `  ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED` +
      `) RETURNING id, kind, aggregate_type, aggregate_id, request, attempts`,
    [batchSize, lockSeconds],
  );
  return rows.map(toRow);
}

async function settleSuccess(client: PgQueryClient, schema: string, id: string): Promise<void> {
  await client.query(`DELETE FROM ${schema}.outbox WHERE id = $1`, [id]);
}

async function settleFailure(
  client: PgQueryClient,
  schema: string,
  row: PgOutboxRow,
  error: unknown,
  options: { maxAttempts: number; backoffBaseSeconds: number },
): Promise<'retried' | 'dead'> {
  const attempts = row.attempts + 1;
  const message = (error instanceof Error ? error.message : String(error)).slice(0, 2000);
  if (attempts >= options.maxAttempts) {
    await client.query(
      `UPDATE ${schema}.outbox SET status = 'dead', attempts = $2, last_error = $3, ` +
        `locked_until = NULL WHERE id = $1`,
      [row.id, attempts, message],
    );
    return 'dead';
  }
  const backoff = Math.min(options.backoffBaseSeconds * 2 ** attempts, 3600);
  await client.query(
    `UPDATE ${schema}.outbox SET status = 'pending', attempts = $2, last_error = $3, ` +
      `locked_until = NULL, next_attempt_at = now() + make_interval(secs => $4) WHERE id = $1`,
    [row.id, attempts, message, backoff],
  );
  return 'retried';
}

/**
 * Drain due outbox rows: claim → deliver → delete (or reschedule/park).
 * Loops until a claim comes back empty (bounded at 50 batches per call).
 */
export async function drainPgOutbox(
  client: PgQueryClient,
  options: PgOutboxRelayOptions = {},
): Promise<PgOutboxDrainSummary> {
  const schema = options.schema ?? 'ceves';
  if (!/^[a-z_][a-z0-9_]*$/u.test(schema)) {
    throw new Error(`Invalid PostgreSQL schema name: "${schema}"`);
  }
  const batchSize = options.batchSize ?? 25;
  const maxAttempts = options.maxAttempts ?? 8;
  const lockSeconds = options.lockSeconds ?? 60;
  const backoffBaseSeconds = options.backoffBaseSeconds ?? 5;
  const fetchImpl =
    options.fetchImpl ??
    ((url: string, init: Record<string, unknown>): Promise<{ ok: boolean; status: number }> =>
      (globalThis as { fetch: (u: string, i: unknown) => Promise<{ ok: boolean; status: number }> })
        .fetch(url, init));
  const deliverers: Record<string, OutboxDeliverer> = {
    fetch: fetchDeliverer(fetchImpl),
    ...(options.eventSink ? { [OUTBOX_EVENT_KIND]: eventDeliverer(options.eventSink) } : {}),
    ...options.deliverers,
  };
  // Events never dead-letter by default — see maxAttemptsByKind.
  const maxAttemptsByKind: Record<string, number> = {
    [OUTBOX_EVENT_KIND]: Infinity,
    ...options.maxAttemptsByKind,
  };

  const summary: PgOutboxDrainSummary = { claimed: 0, delivered: 0, retried: 0, dead: 0 };
  for (let batch = 0; batch < 50; batch += 1) {
    const rows = await claimBatch(client, schema, batchSize, lockSeconds);
    if (rows.length === 0) break;
    summary.claimed += rows.length;
    for (const row of rows) {
      await deliverRow(client, schema, row, deliverers, {
        maxAttempts: maxAttemptsByKind[row.kind] ?? maxAttempts,
        backoffBaseSeconds,
      }, summary);
    }
  }
  return summary;
}

/** Deliver one claimed row and settle it (delete / reschedule / dead-letter). */
async function deliverRow(
  client: PgQueryClient,
  schema: string,
  row: PgOutboxRow,
  deliverers: Record<string, OutboxDeliverer>,
  settleOptions: { maxAttempts: number; backoffBaseSeconds: number },
  summary: PgOutboxDrainSummary,
): Promise<void> {
  try {
    const deliverer = deliverers[row.kind];
    if (!deliverer) {
      throw new Error(`no deliverer registered for outbox kind "${row.kind}"`);
    }
    await deliverer(row);
    await settleSuccess(client, schema, row.id);
    summary.delivered += 1;
  } catch (error) {
    const outcome = await settleFailure(client, schema, row, error, settleOptions);
    summary[outcome] += 1;
    logger.warn('Outbox delivery failed', {
      outboxId: row.id,
      kind: row.kind,
      aggregateId: row.aggregateId,
      attempts: row.attempts + 1,
      outcome,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
