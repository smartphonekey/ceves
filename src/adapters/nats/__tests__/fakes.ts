/**
 * In-memory fakes of the @nats-io surfaces the NATS adapter touches.
 *
 * They emulate the server behaviors the adapter's correctness rests on:
 * - one stream-wide sequence counter, per-subject "expected last sequence"
 *   checks rejecting with JetStream API error code 10071,
 * - `Nats-Msg-Id` duplicate suppression (`ack.duplicate`),
 * - KV create/update CAS on revision (same 10071 rejection),
 * - ordered-consumer reads with `num_pending` / per-message `pending`.
 */

interface FakeStoredMessage {
  subject: string;
  seq: number;
  data: string;
}

class FakeJetStreamApiError extends Error {
  constructor(
    readonly code: number,
    message: string
  ) {
    super(message);
    this.name = 'JetStreamApiError';
  }
}

export class FakeJetStream {
  messages: FakeStoredMessage[] = [];
  private seq = 0;
  private readonly msgIds = new Map<string, number>();

  private lastSeqForSubject(subject: string): number {
    for (let i = this.messages.length - 1; i >= 0; i--) {
      const message = this.messages[i];
      if (message !== undefined && message.subject === subject) return message.seq;
    }
    return 0;
  }

  publish(
    subject: string,
    payload: string,
    opts?: { msgID?: string; expect?: { lastSubjectSequence?: number } }
  ): Promise<{ seq: number; duplicate: boolean }> {
    if (opts?.msgID !== undefined) {
      const original = this.msgIds.get(opts.msgID);
      if (original !== undefined) {
        return Promise.resolve({ seq: original, duplicate: true });
      }
    }
    const expected = opts?.expect?.lastSubjectSequence;
    if (expected !== undefined && expected !== this.lastSeqForSubject(subject)) {
      return Promise.reject(
        new FakeJetStreamApiError(10071, `wrong last sequence: ${this.lastSeqForSubject(subject)}`)
      );
    }
    this.seq += 1;
    this.messages.push({ subject, seq: this.seq, data: payload });
    if (opts?.msgID !== undefined) this.msgIds.set(opts.msgID, this.seq);
    return Promise.resolve({ seq: this.seq, duplicate: false });
  }

  readonly consumers = {
    get: (_stream: string, opts: { filter_subjects: string[] }) => {
      const matching = () =>
        this.messages.filter((m) => opts.filter_subjects.includes(m.subject));
      return Promise.resolve({
        info: (_detail?: boolean) => Promise.resolve({ num_pending: matching().length }),
        consume: () => {
          const snapshot = matching();
          let stopped = false;
          const iterator = {
            stop: () => {
              stopped = true;
            },
            async *[Symbol.asyncIterator]() {
              for (let i = 0; i < snapshot.length; i++) {
                if (stopped) return;
                const message = snapshot[i];
                if (message === undefined) continue;
                yield {
                  seq: message.seq,
                  json: <T>() => JSON.parse(message.data) as T,
                  info: { pending: snapshot.length - i - 1 },
                };
              }
              // Like the real ordered consumer, never terminate on our own —
              // the reader must break on pending === 0.
              await new Promise(() => undefined);
            },
          };
          return Promise.resolve(iterator);
        },
        delete: () => Promise.resolve(true),
      });
    },
  };
}

export class FakeJetStreamManager {
  constructor(private readonly js: FakeJetStream) {}

  readonly streams = {
    // v3 contract (verified against the installed @nats-io/jetstream 3.4.0):
    // getMessage resolves to null when no message matches. The adapter also
    // handles the v2-style 10037 rejection defensively.
    getMessage: (_stream: string, opts: { last_by_subj: string }) => {
      const all = this.js.messages.filter((m) => m.subject === opts.last_by_subj);
      const last = all[all.length - 1];
      if (last === undefined) {
        return Promise.resolve(null);
      }
      return Promise.resolve({
        seq: last.seq,
        json: <T>() => JSON.parse(last.data) as T,
      });
    },
    info: (_stream: string) => Promise.resolve({}),
    add: (_config: unknown) => Promise.resolve({}),
  };
}

type FakeKvOperation = 'PUT' | 'DEL' | 'PURGE';

interface FakeKvRecord {
  value: string;
  revision: number;
  operation: FakeKvOperation;
}

/** What the org directory reads from a KV watch entry. */
export interface FakeKvWatchEntry {
  key: string;
  operation: FakeKvOperation;
  revision: number;
  string(): string;
  json<T>(): T;
}

/**
 * Duck-typed `QueuedIterator<KvWatchEntry>`: replays current entries first
 * (the real client's default), then live updates until `stop()`.
 */
export class FakeKvWatcher {
  private buffered: FakeKvWatchEntry[] = [];
  private waiters: ((result: IteratorResult<FakeKvWatchEntry>) => void)[] = [];
  private stopped = false;

  push(entry: FakeKvWatchEntry): void {
    if (this.stopped) return;
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: entry, done: false });
    else this.buffered.push(entry);
  }

  stop(): void {
    this.stopped = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ value: undefined as never, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<FakeKvWatchEntry> {
    return {
      next: (): Promise<IteratorResult<FakeKvWatchEntry>> => {
        const entry = this.buffered.shift();
        if (entry) return Promise.resolve({ value: entry, done: false });
        if (this.stopped) return Promise.resolve({ value: undefined as never, done: true });
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    };
  }
}

export class FakeKv {
  readonly entries = new Map<string, FakeKvRecord>();
  private revision = 0;
  private readonly watchers: FakeKvWatcher[] = [];

  private watchEntryFor(
    key: string,
    operation: FakeKvOperation,
    value: string,
    revision: number
  ): FakeKvWatchEntry {
    return {
      key,
      operation,
      revision,
      string: () => value,
      json: <T>() => JSON.parse(value) as T,
    };
  }

  private notifyWatchers(entry: FakeKvWatchEntry): void {
    for (const watcher of this.watchers) watcher.push(entry);
  }

  watch(): Promise<FakeKvWatcher> {
    const watcher = new FakeKvWatcher();
    for (const [key, record] of this.entries) {
      watcher.push(this.watchEntryFor(key, record.operation, record.value, record.revision));
    }
    this.watchers.push(watcher);
    return Promise.resolve(watcher);
  }

  get(key: string) {
    const record = this.entries.get(key);
    if (!record) return Promise.resolve(null);
    return Promise.resolve({
      revision: record.revision,
      operation: record.operation,
      json: <T>() => JSON.parse(record.value) as T,
      string: () => record.value,
    });
  }

  put(key: string, value: string): Promise<number> {
    this.revision += 1;
    this.entries.set(key, { value, revision: this.revision, operation: 'PUT' });
    this.notifyWatchers(this.watchEntryFor(key, 'PUT', value, this.revision));
    return Promise.resolve(this.revision);
  }

  create(key: string, value: string): Promise<number> {
    const existing = this.entries.get(key);
    if (existing && existing.operation === 'PUT') {
      return Promise.reject(new FakeJetStreamApiError(10071, 'wrong last sequence'));
    }
    return this.put(key, value);
  }

  update(key: string, value: string, previousRevision: number): Promise<number> {
    const existing = this.entries.get(key);
    if (!existing || existing.revision !== previousRevision) {
      return Promise.reject(
        new FakeJetStreamApiError(10071, `wrong last sequence: ${existing?.revision ?? 0}`)
      );
    }
    return this.put(key, value);
  }

  purge(key: string): Promise<void> {
    this.entries.delete(key);
    this.revision += 1;
    this.notifyWatchers(this.watchEntryFor(key, 'PURGE', '', this.revision));
    return Promise.resolve();
  }

  keys(filter?: string): Promise<AsyncIterable<string>> {
    const prefix = filter?.endsWith('.>') === true ? filter.slice(0, -1) : undefined;
    const filtered = [...this.entries.keys()].filter(
      (k) => prefix === undefined || k.startsWith(prefix)
    );
    const names = filtered[Symbol.iterator]();
    return Promise.resolve({
      [Symbol.asyncIterator]: () => ({
        next: () => Promise.resolve(names.next()),
      }),
    });
  }
}

// ---------------------------------------------------------------------------
// Core NATS request-reply fakes (command transport)
// ---------------------------------------------------------------------------

export interface FakeMsg {
  subject: string;
  data: string;
  respond(payload: Uint8Array | string): boolean;
  json<T>(): T;
  string(): string;
}

function subjectMatches(pattern: string, subject: string): boolean {
  const patternTokens = pattern.split('.');
  const subjectTokens = subject.split('.');
  for (let i = 0; i < patternTokens.length; i++) {
    const token = patternTokens[i];
    if (token === '>') return true;
    if (i >= subjectTokens.length) return false;
    if (token !== '*' && token !== subjectTokens[i]) return false;
  }
  return patternTokens.length === subjectTokens.length;
}

export class FakeSubscription {
  private buffered: FakeMsg[] = [];
  private waiters: ((result: IteratorResult<FakeMsg>) => void)[] = [];
  private closed = false;
  /** Count of messages delivered to this subscription (for assertions). */
  delivered = 0;

  /** Like the real client: a drained subscription no longer receives. */
  get isClosed(): boolean {
    return this.closed;
  }

  constructor(
    readonly pattern: string,
    readonly queue?: string
  ) {}

  push(msg: FakeMsg): void {
    if (this.closed) return;
    this.delivered += 1;
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: msg, done: false });
    else this.buffered.push(msg);
  }

  drain(): Promise<void> {
    this.closed = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter({ value: undefined as never, done: true });
    }
    return Promise.resolve();
  }

  [Symbol.asyncIterator](): AsyncIterator<FakeMsg> {
    return {
      next: (): Promise<IteratorResult<FakeMsg>> => {
        const msg = this.buffered.shift();
        if (msg) return Promise.resolve({ value: msg, done: false });
        if (this.closed) return Promise.resolve({ value: undefined as never, done: true });
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    };
  }
}

/** In-memory core NATS: subscribe with queue groups + request-reply. */
export class FakeNatsConnection {
  readonly subscriptions: FakeSubscription[] = [];
  private inboxCounter = 0;

  subscribe(subject: string, opts?: { queue?: string }): FakeSubscription {
    const subscription = new FakeSubscription(subject, opts?.queue);
    this.subscriptions.push(subscription);
    return subscription;
  }

  request(
    subject: string,
    payload?: Uint8Array | string,
    _opts?: { timeout?: number }
  ): Promise<FakeMsg> {
    const matching = this.subscriptions.filter(
      (s) => !s.isClosed && subjectMatches(s.pattern, subject)
    );
    // one member per queue group, all plain subscribers
    const targets: FakeSubscription[] = [];
    const seenQueues = new Set<string>();
    for (const sub of matching) {
      if (sub.queue !== undefined) {
        if (seenQueues.has(sub.queue)) continue;
        seenQueues.add(sub.queue);
      }
      targets.push(sub);
    }
    if (targets.length === 0) {
      const err = new Error('no responders available for request');
      err.name = 'NoRespondersError';
      return Promise.reject(err);
    }

    const text =
      typeof payload === 'string' ? payload : new TextDecoder().decode(payload ?? new Uint8Array());

    return new Promise((resolve) => {
      const makeMsg = (): FakeMsg => ({
        subject,
        data: text,
        json: <T>() => JSON.parse(text) as T,
        string: () => text,
        respond: (replyPayload) => {
          const reply =
            typeof replyPayload === 'string'
              ? replyPayload
              : new TextDecoder().decode(replyPayload);
          this.inboxCounter += 1;
          resolve({
            subject: `_INBOX.${this.inboxCounter}`,
            data: reply,
            json: <T>() => JSON.parse(reply) as T,
            string: () => reply,
            respond: () => false,
          });
          return true;
        },
      });
      for (const target of targets) target.push(makeMsg());
    });
  }
}
