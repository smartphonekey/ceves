/**
 * Transactional outbox for the PG variant — in-database side.
 *
 * Handlers running inside PostgreSQL cannot perform network I/O (PLV8 is
 * bare V8). Instead, external calls whose RESPONSE IS NOT NEEDED are written
 * to `<schema>.outbox` in the SAME transaction as the aggregate state write:
 *
 * - `fetch(...)` calls made during a command dispatch are intercepted by the
 *   dispatcher (see `createOutboxCapture`) and enqueued as kind `'fetch'`.
 *   The caller gets a stub `202 Accepted` response; reading its body throws,
 *   because a call that needs its response cannot be outboxed.
 * - Anything else (MQTT publishes, notifications, …) is enqueued explicitly
 *   via `getPgOutbox().enqueue(kind, payload)` and delivered by a matching
 *   deliverer registered on the relay.
 * - The dispatcher itself enqueues the emitted domain event as kind
 *   `'event'` ({@link OUTBOX_EVENT_KIND}), which is what makes delivery to
 *   the external event log GUARANTEED: the event row and the aggregate state
 *   write commit in one transaction, so the event cannot be lost by a
 *   wrapper that dies before appending it to R2/S3.
 *
 * This gives the classic outbox guarantee: the external call is queued IFF
 * the transaction commits. The dispatcher additionally compensates (deletes
 * the rows it enqueued) when the command fails with an error envelope, since
 * error envelopes are returned as data and therefore COMMIT. Delivery is
 * at-least-once (the wrapper-side relay in `outboxRelay.ts` drains the table
 * with retries), so receivers must be idempotent.
 *
 * The outbox is a TRANSIENT QUEUE — rows are deleted after delivery. It is
 * not an audit log and must not be treated as one (history belongs in R2/S3).
 *
 * @packageDocumentation
 */

import { CevesError } from '../../errors/CevesError';
import type { PgSql } from './types';

/**
 * Outbox kind carrying a `StoredEvent` envelope destined for the external
 * event log (R2/S3). Enqueued by the dispatcher inside the state-write
 * transaction, so the event survives a wrapper crash: it is delivered by the
 * relay's event deliverer (see `outboxRelay.ts`) rather than by a
 * fire-and-forget append that dies with the process.
 */
export const OUTBOX_EVENT_KIND = 'event';

/** Serialized fetch request stored under `request` for kind 'fetch'. */
export interface OutboxFetchRequest {
  url: string;
  method: string;
  headers: Record<string, string>;
  body?: string;
}

/** Correlation columns stamped on every enqueued row. */
export interface OutboxCorrelation {
  aggregateType: string;
  aggregateId: string;
}

/** Handle available to handlers during a dispatch (see {@link getPgOutbox}). */
export interface PgOutboxHandle {
  /**
   * Queue one external call for post-commit delivery. Returns the row id.
   * `payload` must be JSON-serializable; the relay routes it to the
   * deliverer registered for `kind`.
   */
  enqueue(kind: string, payload: unknown): string;
}

/**
 * Row writer over the synchronous PgSql handle (plv8.execute inside
 * PostgreSQL). Inserts join the surrounding transaction automatically.
 */
export class PgOutboxWriter {
  constructor(
    private readonly sql: PgSql,
    private readonly schema: string,
  ) {}

  enqueue(kind: string, payload: unknown, correlation: OutboxCorrelation): string {
    const result = this.sql.execute(
      `INSERT INTO ${this.schema}.outbox (kind, aggregate_type, aggregate_id, request) ` +
        `VALUES ($1, $2, $3, $4::jsonb) RETURNING id`,
      [kind, correlation.aggregateType, correlation.aggregateId, JSON.stringify(payload ?? null)],
    );
    const rows = Array.isArray(result) ? (result as Record<string, unknown>[]) : [];
    const id = rows[0]?.id;
    if (id === undefined || id === null) {
      throw new Error('outbox enqueue did not return an id — is the outbox table deployed?');
    }
    return String(id);
  }

  /** Compensation path: remove rows enqueued by a command that then failed. */
  deleteMany(ids: string[]): void {
    for (const id of ids) {
      this.sql.execute(`DELETE FROM ${this.schema}.outbox WHERE id = $1`, [id]);
    }
  }
}

/**
 * Ambient handle for the CURRENT dispatch, so handler code can enqueue
 * non-fetch kinds without new plumbing:
 *
 * ```typescript
 * import { getPgOutbox } from 'ceves/pg';
 * getPgOutbox()?.enqueue('mqtt', { topic, payload });
 * ```
 *
 * PLV8 executes one function call at a time, so a single ambient slot is
 * safe there. Outside PostgreSQL (unit tests), do not interleave concurrent
 * dispatches that rely on this handle.
 */
let currentOutbox: PgOutboxHandle | null = null;

/** The current dispatch's outbox, or null outside a PG dispatch. */
export function getPgOutbox(): PgOutboxHandle | null {
  return currentOutbox;
}

/** Normalize fetch(input, init) into a JSON-serializable request record. */
function serializeFetchRequest(input: unknown, init?: unknown): OutboxFetchRequest {
  const url =
    typeof input === 'string'
      ? input
      : String((input as { url?: unknown } | null | undefined)?.url ?? input);
  const options = (init ?? {}) as {
    method?: unknown;
    headers?: unknown;
    body?: unknown;
  };

  const headers: Record<string, string> = {};
  const rawHeaders = options.headers;
  if (rawHeaders && typeof (rawHeaders as { forEach?: unknown }).forEach === 'function') {
    (rawHeaders as { forEach: (cb: (value: string, key: string) => void) => void }).forEach(
      (value, key) => {
        headers[key] = value;
      },
    );
  } else if (rawHeaders && typeof rawHeaders === 'object') {
    for (const [key, value] of Object.entries(rawHeaders as Record<string, unknown>)) {
      headers[key] = String(value);
    }
  }

  let body: string | undefined;
  if (options.body !== undefined && options.body !== null) {
    if (typeof options.body !== 'string') {
      throw new CevesError(
        'Outboxed fetch supports string bodies only — JSON.stringify the payload. ' +
          'Streams/FormData cannot be persisted to the outbox.',
        500,
      );
    }
    body = options.body;
  }

  return {
    url,
    method: typeof options.method === 'string' ? options.method.toUpperCase() : 'GET',
    headers,
    body,
  };
}

/** Error thrown when handler code tries to READ an outboxed response. */
function outboxedBodyError(url: string): Error {
  return new CevesError(
    `This fetch to ${url} was queued in the transactional outbox — its response ` +
      'does not exist yet. A call whose response is needed cannot run inside ' +
      'PostgreSQL; keep it in the HTTP wrapper instead.',
    500,
  );
}

/**
 * Stub standing in for the Response of an outboxed fetch. `ok`/`status`
 * satisfy fire-and-forget code; any body read throws a descriptive error so
 * wrong data can never silently flow.
 */
function makeOutboxedResponse(url: string): Record<string, unknown> {
  const reject = (): Promise<never> => Promise.reject(outboxedBodyError(url));
  const stub: Record<string, unknown> = {
    ok: true,
    status: 202,
    statusText: 'Accepted (queued in ceves outbox)',
    url,
    redirected: false,
    outboxed: true,
    headers: { get: (): null => null, has: (): boolean => false },
    text: reject,
    json: reject,
    arrayBuffer: reject,
    blob: reject,
    formData: reject,
  };
  stub.clone = (): Record<string, unknown> => stub;
  return stub;
}

/** What the dispatcher gets back from {@link createOutboxCapture}. */
export interface OutboxCapture {
  /**
   * Enqueue a row as part of THIS dispatch — tracked for compensation, so a
   * later failure removes it along with everything the handler enqueued.
   * The dispatcher uses this for the `'event'` kind; handler code uses the
   * ambient {@link getPgOutbox} handle instead.
   */
  enqueue(kind: string, payload: unknown): string;
  /** Delete every row this dispatch enqueued (command failed → compensate). */
  rollback(): void;
  /** Restore globalThis.fetch and clear the ambient handle. ALWAYS call. */
  restore(): void;
}

interface CaptureOptions {
  writer: PgOutboxWriter;
  correlation: OutboxCorrelation;
  /** Replace globalThis.fetch with the enqueueing interceptor. */
  interceptFetch: boolean;
  /** 'command' enqueues; 'query' makes any fetch throw (queries are reads). */
  mode: 'command' | 'query';
}

/**
 * Begin outbox capture for one dispatch: expose the ambient enqueue handle
 * and (for commands) swap `globalThis.fetch` for the interceptor. The
 * returned capture MUST be `restore()`d in a finally block.
 */
export function createOutboxCapture(options: CaptureOptions): OutboxCapture {
  const { writer, correlation, interceptFetch, mode } = options;
  const enqueuedIds: string[] = [];
  const globals = globalThis as Record<string, unknown>;
  const previousFetch = globals.fetch;
  const hadFetch = Object.prototype.hasOwnProperty.call(globals, 'fetch');

  const trackedEnqueue = (kind: string, payload: unknown): string => {
    const id = writer.enqueue(kind, payload, correlation);
    enqueuedIds.push(id);
    return id;
  };

  currentOutbox = mode === 'command' ? { enqueue: trackedEnqueue } : null;

  if (interceptFetch) {
    globals.fetch =
      mode === 'command'
        ? (input: unknown, init?: unknown): Promise<Record<string, unknown>> => {
            const request = serializeFetchRequest(input, init);
            trackedEnqueue('fetch', request);
            return Promise.resolve(makeOutboxedResponse(request.url));
          }
        : (): Promise<never> =>
            Promise.reject(
              new CevesError(
                'fetch during a PG-dispatched query is not supported — queries are ' +
                  'read-only inside PostgreSQL. Move the call to the HTTP wrapper.',
                500,
              ),
            );
  }

  return {
    enqueue: trackedEnqueue,
    rollback: (): void => {
      writer.deleteMany(enqueuedIds);
      enqueuedIds.length = 0;
    },
    restore: (): void => {
      currentOutbox = null;
      if (!interceptFetch) return;
      if (hadFetch) {
        globals.fetch = previousFetch;
      } else {
        delete globals.fetch;
      }
    },
  };
}
