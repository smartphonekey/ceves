import { describe, it, expect } from 'vitest';
import { safeDOFetch } from './safeDOFetch';

describe('safeDOFetch', () => {
  it('returns the Response unchanged on the happy path', async () => {
    const original = new Response(JSON.stringify({ ok: true }), { status: 200 });
    const result = await safeDOFetch(Promise.resolve(original));
    expect(result).toBe(original);
    expect(result.status).toBe(200);
  });

  it('coerces `internal error; reference = ...` into a 503 with cfReference', async () => {
    const cf = new Error('internal error; reference = 2lt4mbc6cg5qm987bdghc7c1');
    const result = await safeDOFetch(Promise.reject(cf));

    expect(result.status).toBe(503);
    expect(result.headers.get('Content-Type')).toBe('application/json');
    expect(result.headers.get('Retry-After')).toBe('2');

    const body: {
      success: boolean;
      errors: Array<{ code: number; message: string }>;
      cfReference?: string;
    } = await result.json();
    expect(body.success).toBe(false);
    expect(body.errors).toEqual([
      { code: 503, message: 'Aggregate temporarily unavailable, please retry' },
    ]);
    expect(body.cfReference).toBe('2lt4mbc6cg5qm987bdghc7c1');
  });

  it('coerces `Durable Object reset because its code was updated` into a 503', async () => {
    const cf = new Error('Durable Object reset because its code was updated.');
    const result = await safeDOFetch(Promise.reject(cf));

    expect(result.status).toBe(503);
    const body: { success: boolean; cfReference?: string } = await result.json();
    expect(body.success).toBe(false);
    // No reference id in this variant — body should not synthesise one.
    expect(body.cfReference).toBeUndefined();
  });

  it('coerces `Durable Object storage operation exceeded timeout` into a 503', async () => {
    const cf = new Error(
      'Durable Object storage operation exceeded timeout which caused object to be reset.',
    );
    const result = await safeDOFetch(Promise.reject(cf));
    expect(result.status).toBe(503);
  });

  it('re-throws unrelated errors so upstream handlers still see them', async () => {
    const real = new Error('Connection refused');
    await expect(safeDOFetch(Promise.reject(real))).rejects.toBe(real);
  });

  it('re-throws TypeError unchanged (e.g. malformed Request constructor)', async () => {
    const typeErr = new TypeError('Cannot read property of undefined');
    await expect(safeDOFetch(Promise.reject(typeErr))).rejects.toBe(typeErr);
  });

  it('treats non-Error throwables that match the CF signature as 503 (defensive)', async () => {
    // CF runtime should always throw an Error, but the helper shouldn't
    // crash if someone bubbles a plain string up by mistake.
    const result = await safeDOFetch(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      Promise.reject('internal error; reference = abc123' as any),
    );
    expect(result.status).toBe(503);
    const body: { cfReference?: string } = await result.json();
    expect(body.cfReference).toBe('abc123');
  });
});
