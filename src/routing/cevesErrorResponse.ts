/**
 * `handleCevesError` — convert a thrown `CevesError` (or subclass) into a
 * structured `{ success: false, errors: [...] }` Response, or return `null`
 * if the thrown value doesn't look like a CevesError. Used by route base
 * classes to give domain-thrown errors a uniform wire shape.
 *
 * Why duck-type instead of `instanceof`: when a route lives in an app
 * bundle and CevesError is also bundled into Ceves's dist/, the two copies
 * can fail `instanceof` even though they're semantically the same class.
 * Matching on the two methods every CevesError implements is bundler-safe.
 */
export function handleCevesError(error: unknown): Response | null {
  if (
    error &&
    typeof error === 'object' &&
    'httpStatusCode' in error &&
    'buildResponse' in error
  ) {
    const cevesError = error as {
      httpStatusCode: number;
      buildResponse: () => unknown;
    };
    return new Response(
      JSON.stringify({ success: false, errors: cevesError.buildResponse() }),
      {
        status: cevesError.httpStatusCode,
        headers: { 'Content-Type': 'application/json' },
      },
    );
  }
  return null;
}
