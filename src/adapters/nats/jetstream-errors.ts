/**
 * Duck-typed JetStream API error detection.
 *
 * The adapter deliberately avoids `instanceof JetStreamApiError` checks:
 * `@nats-io/jetstream` is an (optional) peer dependency, and in bundled or
 * multi-copy installs the error class the app's connection throws may be a
 * different module instance than the one this adapter imported. Matching on
 * the numeric JetStream API error code (which both v2's `api_error.err_code`
 * and v3's `JetStreamApiError.code` expose) is stable across all of that —
 * and makes unit-test fakes trivial.
 */

/** `wrong last sequence: N` — per-subject optimistic-concurrency conflict. */
export const JS_ERR_WRONG_LAST_SEQUENCE = 10071;

/**
 * Variant of 10071 the server returns when it cannot determine the last
 * sequence for the subject (`StreamWrongLastSequenceUnknown`).
 */
export const JS_ERR_WRONG_LAST_SEQUENCE_UNKNOWN = 10164;

/** `no message found` — direct-get miss for a subject with no messages. */
export const JS_ERR_NO_MESSAGE_FOUND = 10037;

/** `stream not found` */
export const JS_ERR_STREAM_NOT_FOUND = 10059;

/**
 * `stream name already in use` — returned to every replica but the winner
 * when several create the same stream at once. A benign startup race, not a
 * failure: the stream the caller wanted now exists.
 */
export const JS_ERR_STREAM_NAME_IN_USE = 10058;

/**
 * Extract the JetStream API error code from a thrown error, if present.
 * Handles v3 (`JetStreamApiError.code === err_code`) and v2
 * (`NatsError.api_error.err_code`) shapes.
 */
export function jetStreamApiErrorCode(err: unknown): number | undefined {
  if (typeof err !== 'object' || err === null) return undefined;
  const e = err as {
    code?: unknown;
    err_code?: unknown;
    api_error?: { err_code?: unknown };
  };
  if (typeof e.code === 'number') return e.code;
  if (typeof e.err_code === 'number') return e.err_code;
  if (typeof e.api_error?.err_code === 'number') return e.api_error.err_code;
  return undefined;
}

/** True when the error is a per-subject expected-last-sequence conflict. */
export function isWrongLastSequence(err: unknown): boolean {
  const code = jetStreamApiErrorCode(err);
  return (
    code === JS_ERR_WRONG_LAST_SEQUENCE ||
    code === JS_ERR_WRONG_LAST_SEQUENCE_UNKNOWN
  );
}
