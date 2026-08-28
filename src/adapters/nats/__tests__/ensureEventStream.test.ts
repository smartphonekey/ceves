/**
 * Codex review (PR #354, thread r3840613384): stream provisioning must
 * survive a replica startup race. Every replica runs `ensureEventStream()`
 * on boot, so on a first rollout several can see STREAM_NOT_FOUND and all
 * call `streams.add()` — only one wins, and the losers must not die.
 */

import { describe, it, expect } from 'vitest';
import type { JetStreamManager } from '@nats-io/jetstream';
import { ensureEventStream } from '../runtime';
import { JS_ERR_STREAM_NOT_FOUND, JS_ERR_STREAM_NAME_IN_USE } from '../jetstream-errors';

class FakeApiError extends Error {
  constructor(readonly code: number, message: string) {
    super(message);
    this.name = 'JetStreamApiError';
  }
}

const OPTS = {
  streamName: 'CEVES_EVENTS',
  subjects: ['ceves.events.>'],
  duplicateWindowNanos: 300_000_000_000,
};

/** A manager where the stream is missing until someone adds it. */
function makeJsm(options: { addFails?: FakeApiError } = {}) {
  const calls = { info: 0, add: 0 };
  let exists = false;
  const jsm = {
    streams: {
      info: (_name: string) => {
        calls.info += 1;
        if (!exists && !options.addFails) {
          return Promise.reject(new FakeApiError(JS_ERR_STREAM_NOT_FOUND, 'stream not found'));
        }
        if (!exists && options.addFails) {
          // The racing replica created it in the meantime.
          return calls.info === 1
            ? Promise.reject(new FakeApiError(JS_ERR_STREAM_NOT_FOUND, 'stream not found'))
            : Promise.resolve({ config: { name: OPTS.streamName } });
        }
        return Promise.resolve({ config: { name: OPTS.streamName } });
      },
      add: (_config: unknown) => {
        calls.add += 1;
        if (options.addFails) return Promise.reject(options.addFails);
        exists = true;
        return Promise.resolve({});
      },
    },
  };
  return { jsm: jsm as unknown as JetStreamManager, calls };
}

describe('ensureEventStream', () => {
  it('creates the stream when it does not exist', async () => {
    const { jsm, calls } = makeJsm();
    await ensureEventStream(jsm, OPTS);
    expect(calls.add).toBe(1);
  });

  it('leaves an existing stream untouched', async () => {
    const { jsm, calls } = makeJsm();
    await ensureEventStream(jsm, OPTS);
    await ensureEventStream(jsm, OPTS);
    expect(calls.add).toBe(1); // second run only re-read
  });

  it('survives losing the creation race to another replica', async () => {
    const { jsm, calls } = makeJsm({
      addFails: new FakeApiError(JS_ERR_STREAM_NAME_IN_USE, 'stream name already in use'),
    });
    // The losing replica must start normally, not throw.
    await expect(ensureEventStream(jsm, OPTS)).resolves.toBeUndefined();
    expect(calls.add).toBe(1);
    expect(calls.info).toBeGreaterThan(1); // re-read confirmed the stream exists
  });

  it('still fails loudly on a genuine creation error', async () => {
    const { jsm } = makeJsm({ addFails: new FakeApiError(10023, 'insufficient resources') });
    await expect(ensureEventStream(jsm, OPTS)).rejects.toThrow('insufficient resources');
  });
});
