/**
 * Optional exception-capture hook.
 *
 * Ceves itself stays framework-agnostic — it doesn't depend on Sentry, OTel,
 * or any other capture library. But unhandled exceptions in `handleError`
 * are otherwise invisible (logged once, returned as a 500 Response, never
 * captured anywhere). Apps can wire a capturer once at startup so those
 * exceptions land somewhere queryable:
 *
 *   import * as Sentry from '@sentry/cloudflare';
 *   import { setExceptionCapturer } from 'ceves';
 *   setExceptionCapturer((err) => Sentry.captureException(err));
 *
 * If no capturer is set, `captureException` is a no-op.
 */

type ExceptionCapturer = (error: unknown) => void;

let capturer: ExceptionCapturer | null = null;

export function setExceptionCapturer(fn: ExceptionCapturer | null): void {
  capturer = fn;
}

export function captureException(error: unknown): void {
  if (!capturer) return;
  try {
    capturer(error);
  } catch {
    // Capturer must never throw — swallow to keep handleError boring.
  }
}
