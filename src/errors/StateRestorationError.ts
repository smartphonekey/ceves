/**
 * State Restoration Error for Ceves Event Sourcing Library.
 *
 * Thrown when rebuilding aggregate state from the event stream fails —
 * typically because one specific event cannot be applied (corrupted
 * payload that fails Zod parse, handler `apply()` that throws on bad
 * legacy data after a refactor, etc.).
 *
 * Why a dedicated class:
 *
 * The raw error inside `applyEvent()` usually has no aggregate context —
 * it might say "Expected string, received undefined" with no clue *which*
 * event in *which* aggregate broke. Without that, the operator stares at
 * an opaque host-runtime error (e.g. Cloudflare's
 * "internal error; reference = ...") with nothing to grep for.
 *
 * `StateRestorationError` captures:
 *   - aggregateType / aggregateId — which aggregate crashed
 *   - eventType / eventVersion — which event in the stream
 *   - eventIndex / totalEvents — replay position (e.g., "event 17/342")
 *   - cause (ES2022 `Error.cause`) — the underlying failure preserved
 *
 * The thrown message is purpose-built to be one-line, greppable, and
 * actionable.
 */

import { CevesError } from './CevesError';

export class StateRestorationError extends CevesError {
  readonly eventType: string;
  readonly eventVersion: number;
  readonly eventIndex: number;
  readonly totalEvents: number;

  constructor(
    args: {
      readonly aggregateType: string;
      readonly aggregateId: string;
      readonly eventType: string;
      readonly eventVersion: number;
      readonly eventIndex: number;
      readonly totalEvents: number;
    },
    cause: unknown,
  ) {
    const causeMessage = cause instanceof Error ? cause.message : String(cause);
    const message =
      `State restoration failed for ${args.aggregateType}/${args.aggregateId} ` +
      `at event ${args.eventIndex + 1}/${args.totalEvents} ` +
      `(type=${args.eventType}, version=${args.eventVersion}): ${causeMessage}`;

    super(message, 500, args.aggregateType, args.aggregateId);

    this.eventType = args.eventType;
    this.eventVersion = args.eventVersion;
    this.eventIndex = args.eventIndex;
    this.totalEvents = args.totalEvents;

    // ES2022 `Error.cause` — preserves the underlying failure so callers
    // (and Sentry) can walk the chain to the original ZodError /
    // missing-handler / handler-`apply()` exception.
    if (cause !== undefined) {
      (this as { cause?: unknown }).cause = cause;
    }
  }
}
