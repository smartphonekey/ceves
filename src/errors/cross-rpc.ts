/**
 * Cross-RPC error coercion for Durable Object boundaries.
 *
 * Background (AA-119):
 *
 * When a typed `CevesError` is thrown inside a Durable Object and crosses
 * the worker → DO RPC boundary, Cloudflare's runtime serialises the
 * exception generically. The caller sees `Error: internal error;
 * reference = <id>` with the prototype chain, message, and structured
 * context all dropped. That's the root cause of AA-108 / -111 / -112 /
 * -117 — multiple Sentry hotspots that look unrelated but all stem from
 * this single boundary-stripping behaviour.
 *
 * The fix:
 *
 * 1. DO side — `serializeErrorToResponse(err)`: catches typed errors and
 *    returns a structured `Response` instead of letting the exception
 *    cross the boundary. The response carries:
 *
 *      - `X-Ceves-Error: 1` header (sentinel for callers to opt in)
 *      - Status code from `error.httpStatusCode` (CevesError) or 500
 *      - JSON body in two-layer shape:
 *        ```
 *        {
 *          // chanfana-compatible (preserves existing client contract)
 *          success: false,
 *          errors: [{ code, message }],
 *          // typed metadata (consumed only by rehydration)
 *          __ceves: { name, message, httpStatusCode, ...extras }
 *        }
 *        ```
 *
 * 2. Worker side — `rehydrateErrorFromResponse(response)`: detects the
 *    `X-Ceves-Error` header, looks up the original error class in the
 *    registry, and rebuilds an instance via the class's `fromJSON`. The
 *    rebuilt error preserves `name`, `message`, `httpStatusCode`,
 *    `aggregateType` / `aggregateId`, plus any subclass-specific extras
 *    that the class round-trips through `toJSON`.
 *
 * 3. App code (e.g. a cross-DO routing helper) wraps `stub.fetch` with
 *    `stubFetchWithTypedErrors(stub, req)` to opt into rehydrate-and-
 *    throw semantics. CommandRoute / QueryRoute / AggregateRouter
 *    intentionally do NOT opt in — they pass the Response through to
 *    the HTTP client unchanged so the existing chanfana wire shape is
 *    preserved on the public API.
 *
 * Why the two-layer body shape:
 *
 * - The `success` / `errors` keys keep the chanfana-format wire
 *   compatibility that existing tests, OpenAPI specs, and external
 *   clients depend on. No client-visible breakage.
 * - The `__ceves` key is invisible to client SDKs (they read
 *   `errors`) but lets the rehydration path reconstruct full subclass
 *   identity, including fields like `eventType`, `eventVersion`,
 *   `eventIndex`, `totalEvents` that `errors[].message` flattens away.
 */

import { CevesError } from './CevesError';

/**
 * Sentinel header that marks a response as a serialised CevesError.
 * Presence of this header on any response with status >= 400 means the
 * body's `__ceves` field can be rehydrated into the original typed error.
 */
export const CROSS_RPC_ERROR_HEADER = 'X-Ceves-Error';

/**
 * Shape of the typed-metadata block embedded under `body.__ceves` for
 * cross-RPC rehydration. Mirrors `CevesError.toJSON()` plus any subclass-
 * specific fields the subclass chooses to round-trip.
 */
interface CrossRpcErrorPayload {
  readonly name: string;
  readonly message: string;
  readonly httpStatusCode: number;
  readonly aggregateType?: string;
  readonly aggregateId?: string;
  // Subclass-specific extras (eventType, eventVersion, etc.) — preserved
  // verbatim so a future subclass that adds new fields needs no central
  // change here.
  readonly [extra: string]: unknown;
}

/**
 * Outer envelope written to the response body. The two-layer design
 * preserves chanfana wire compatibility for client SDKs while making
 * typed metadata available to internal rehydration.
 */
interface CrossRpcErrorEnvelope {
  readonly success: false;
  readonly errors: ReadonlyArray<{ readonly code: number; readonly message: string }>;
  readonly __ceves?: CrossRpcErrorPayload;
}

/**
 * Rebuild a `CevesError` from a cross-RPC payload.
 *
 * The rehydrated error preserves `name`, `message`, `httpStatusCode`, and
 * `aggregateType` / `aggregateId`. The returned instance is of class
 * `CevesError` (not the original subclass), so callers should check
 * subclass identity by name:
 *
 *   `if (err.name === 'BusinessRuleViolationError') { ... }`
 *
 * not by `instanceof BusinessRuleViolationError`. That cost is worth
 * dodging a giant per-subclass `fromJSON` registry — every existing
 * subclass uses the inherited `toJSON()` shape, and any future subclass
 * that adds extra fields can still round-trip them as opaque properties
 * on the rebuilt CevesError if it ever needs to.
 */
function buildCevesErrorFromPayload(payload: CrossRpcErrorPayload): CevesError {
  const err = CevesError.fromJSON(payload);
  // Preserve the original class name so downstream consumers can
  // discriminate (Sentry grouping, log search, runtime branches).
  // The default `name` from CevesError.fromJSON is just 'CevesError'.
  Object.defineProperty(err, 'name', {
    value: payload.name,
    enumerable: false,
    writable: true,
    configurable: true,
  });
  // Stash subclass-specific extras (eventType, eventVersion, etc.) on
  // the rebuilt error so callers that *do* know the subclass shape can
  // read them off without re-parsing the response.
  for (const [key, value] of Object.entries(payload)) {
    if (key in err) continue;
    (err as unknown as Record<string, unknown>)[key] = value;
  }
  return err;
}

/**
 * Best-effort, never-throws stringification for non-`Error` thrown values
 * (strings, numbers, plain objects, cycles, etc.). Used in the fallback
 * path when an unknown value crosses the RPC boundary so the resulting
 * response message is *something* greppable rather than `"undefined"` or
 * `"[object Object]"`.
 */
function safeStringify(value: unknown): string {
  if (value == null) return String(value);
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    // Cycles, BigInt-in-object, etc. — fall back to native stringify.
    return Object.prototype.toString.call(value);
  }
}

/**
 * Serialise any thrown value into a structured `Response` suitable for
 * crossing the DO → worker RPC boundary without losing context.
 *
 * Use this from `AggregateObject.handleError` (or any DO `fetch()`
 * catch-all) instead of returning a plain `Response` so callers that
 * opt into rehydration (`stubFetchWithTypedErrors`) can rebuild the
 * typed error on the other side.
 */
export function serializeErrorToResponse(error: unknown): Response {
  if (error instanceof CevesError) {
    const json = error.toJSON();
    // Pick up subclass-specific own properties (eventType, eventVersion,
    // etc.) without forcing every subclass to override toJSON. We
    // iterate own keys and filter to JSON-safe values so the payload
    // round-trips through `JSON.stringify` cleanly.
    const extras: Record<string, unknown> = {};
    for (const key of Object.keys(error)) {
      // toJSON output already has these; skip non-serialisable values
      // (functions, symbols, undefined, bigint).
      if (key in json) continue;
      const value = (error as unknown as Record<string, unknown>)[key];
      const t = typeof value;
      if (
        value === null ||
        t === 'string' ||
        t === 'number' ||
        t === 'boolean' ||
        t === 'object'
      ) {
        extras[key] = value;
      }
    }
    const payload: CrossRpcErrorPayload = {
      ...json,
      ...extras,
      name: error.name,
      message: error.message,
      httpStatusCode: error.httpStatusCode,
    };
    const envelope: CrossRpcErrorEnvelope = {
      success: false,
      errors: [{ code: error.httpStatusCode, message: error.message }],
      __ceves: payload,
    };
    return new Response(JSON.stringify(envelope), {
      status: error.httpStatusCode,
      headers: {
        'Content-Type': 'application/json',
        [CROSS_RPC_ERROR_HEADER]: '1',
      },
    });
  }

  // Non-CevesError path: prefer the native error message; fall back to
  // safeStringify so a raw `throw 42` or `throw { reason: 'x' }` still
  // produces a non-empty, greppable response body.
  const message =
    error instanceof Error && error.message
      ? error.message
      : `Unknown error: ${safeStringify(error)}`;
  const name = error instanceof Error ? error.name : 'Error';

  const envelope: CrossRpcErrorEnvelope = {
    success: false,
    errors: [{ code: 500, message }],
    __ceves: { name, message, httpStatusCode: 500 },
  };
  return new Response(JSON.stringify(envelope), {
    status: 500,
    headers: {
      'Content-Type': 'application/json',
      [CROSS_RPC_ERROR_HEADER]: '1',
    },
  });
}

/**
 * Inspect a response from `stub.fetch(...)`. If it was produced by
 * `serializeErrorToResponse` (i.e. carries `X-Ceves-Error: 1`), rebuild
 * the original typed `CevesError`. Otherwise return `null` so the caller
 * can handle the response normally.
 *
 * Consumes the response body. Pass `response.clone()` if the caller
 * still needs the original.
 */
export async function rehydrateErrorFromResponse(response: Response): Promise<CevesError | null> {
  if (response.headers.get(CROSS_RPC_ERROR_HEADER) !== '1') {
    return null;
  }

  let parsed: CrossRpcErrorEnvelope;
  try {
    parsed = await response.json();
  } catch {
    // Header said "yes Ceves" but body wasn't JSON. Shouldn't happen,
    // but don't make it worse — return null and let the caller decide.
    return null;
  }

  const payload = parsed.__ceves;
  if (!payload || typeof payload.name !== 'string') {
    return null;
  }

  return buildCevesErrorFromPayload(payload);
}

/**
 * `stub.fetch(req)` plus typed-error rehydration. On a Ceves-encoded
 * error response, throws the original `CevesError` (or subclass) with
 * its `httpStatusCode`, `message`, `aggregateType` / `aggregateId`, and
 * any subclass extras intact. Otherwise returns the response
 * untransformed.
 *
 * Use from app code that wants to act on typed errors (e.g.
 * a routing helper deciding to skip a downstream call when the DO
 * returned `AggregateAlreadyExistsError`).
 *
 * Worker entry-point routes (CommandRoute / QueryRoute /
 * AggregateRouter) intentionally do NOT use this — they pass the
 * Response through so the HTTP client receives the chanfana-format
 * body unchanged.
 */
export async function stubFetchWithTypedErrors(
  stub: { fetch: (request: Request) => Promise<Response> },
  request: Request,
): Promise<Response> {
  const response = await stub.fetch(request);
  if (response.headers.get(CROSS_RPC_ERROR_HEADER) === '1') {
    const cloned = response.clone();
    const err = await rehydrateErrorFromResponse(cloned);
    if (err) {
      throw err;
    }
  }
  return response;
}
