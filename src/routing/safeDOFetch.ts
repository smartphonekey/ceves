/**
 * `safeDOFetch` — wrap a `stub.fetch()` await so Cloudflare's synthetic
 * uncatchable wrappers for DO failures become structured 503 responses
 * instead of unhandled exceptions that surface to the client as 500s.
 *
 * # The problem
 *
 * Across deploys (every push to a worker tearing down and rebuilding its
 * isolates) and on internal DO faults (storage timeout, isolate kill),
 * a Durable Object subrequest started via `stub.fetch(...)` may never
 * complete. After a few seconds of unresponsive waiting Cloudflare's
 * runtime synthesises one of these errors on the CALLING side:
 *
 *   - `Error('internal error; reference = <id>')`
 *   - `Error('Durable Object reset because its code was updated.')`
 *   - `Error('Durable Object storage operation exceeded timeout which
 *           caused object to be reset.')`
 *
 * These are thrown synchronously out of the `await stub.fetch(...)` call.
 * Without try/catch they propagate up the worker, get caught by Hono's
 * `auto.faas.hono.error_handler`, returned as opaque 500s to the client,
 * and tagged in Sentry as `cf_internal_ref=True, handled=no` — see the
 * upstream incident cluster that motivated this helper.
 *
 * # Why a wrapper, not a guard at each call site
 *
 * Every `CommandRoute` / `CreateCommandRoute` / `QueryRoute` in ceves
 * does one or more `stub.fetch()` calls. Apps that override `handle()`
 * (e.g. CreateTempKeyRoute) do their own `stub.fetch()` too. Re-deriving
 * the matching/coercion logic at every site invites drift. This helper
 * is the one place that knows the CF runtime's error vocabulary.
 *
 * # Returned shape
 *
 * On a CF transport-layer failure the caller gets back a NORMAL Response
 * with HTTP 503, `Content-Type: application/json`, `Retry-After: 2`, and
 * a body shaped like every other framework error response:
 *
 *   {
 *     "success": false,
 *     "errors": [
 *       { "code": 503, "message": "Aggregate temporarily unavailable, please retry" }
 *     ],
 *     "cfReference": "<id>"  // present if the CF synthetic msg contained one
 *   }
 *
 * Other (real) exceptions are re-thrown unchanged.
 */
const CF_INTERNAL_ERROR_RE = /internal error; reference = (\S+)/;

const CF_DO_RESET_SIGNATURES = [
  'Durable Object reset because its code was updated',
  'Durable Object storage operation exceeded timeout',
];

export async function safeDOFetch(promise: Promise<Response>): Promise<Response> {
  try {
    return await promise;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);

    const internalRef = CF_INTERNAL_ERROR_RE.exec(msg)?.[1];
    const isResetSignature = CF_DO_RESET_SIGNATURES.some((sig) => msg.includes(sig));

    if (!internalRef && !isResetSignature) {
      // Unrelated error — let the upstream handler deal with it.
      throw err;
    }

    const body: Record<string, unknown> = {
      success: false,
      errors: [
        { code: 503, message: 'Aggregate temporarily unavailable, please retry' },
      ],
    };
    if (internalRef) {
      body.cfReference = internalRef;
    }

    return new Response(JSON.stringify(body), {
      status: 503,
      headers: {
        'Content-Type': 'application/json',
        'Retry-After': '2',
      },
    });
  }
}
