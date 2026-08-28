/**
 * NATS JetStream implementation of the Ceves IEventStore.
 *
 * Layout: all events live in one JetStream stream (default `CEVES_EVENTS`)
 * with one concrete subject per aggregate —
 * `<subjectPrefix>.<aggregateType>.<aggregateId>` (tokens escaped, see
 * `naming.ts`). The event type stays in the payload, never in the subject,
 * so plain per-subject optimistic concurrency applies.
 *
 * Versioning / OCC: Ceves versions are dense (1..N) and live in the stored
 * envelope. JetStream's concurrency token is different — it is the *stream
 * sequence* of the subject's last message, enforced via the
 * `Nats-Expected-Last-Subject-Sequence` header (`expect.lastSubjectSequence`).
 * The store keeps a per-subject cache of that sequence (updated on every
 * load/save) and falls back to a direct `last_by_subj` lookup on cache miss.
 * A mismatch (JetStream API error 10071/10164) means a concurrent writer got
 * there first and surfaces as `VersionConflictError` (HTTP 409).
 *
 * Idempotency: every publish carries a `Nats-Msg-Id` of
 * `<aggregateType>.<aggregateId>.<version>.<payload-hash>`, so a retried
 * save of the *same* event inside the stream's duplicate window is absorbed
 * by the server (ack has `duplicate: true`) instead of appending twice.
 * The payload hash matters: the server checks `Nats-Msg-Id` before the
 * expected-last-sequence header, so without it a *different* event at the
 * same version (a concurrent writer) would be silently absorbed as a
 * duplicate instead of rejected as a conflict.
 */

import type { JetStreamClient, JetStreamManager } from '@nats-io/jetstream';
import type { IEventStore, StoredEvent } from '../../storage/interfaces';
import { EventStoreError, EventWriteError } from '../../storage/errors';
import { VersionConflictError } from '../../errors/VersionConflictError';
import { createLogger } from '../../logger';
import type { NatsOrgDirectory } from './NatsOrgDirectory';
import { eventSubjectFor, tenantEventSubjectFor, routedEventSubjectFor } from './naming';
import {
  isWrongLastSequence,
  jetStreamApiErrorCode,
  JS_ERR_NO_MESSAGE_FOUND,
} from './jetstream-errors';
import { AGGREGATE_TRANSFERRED_OUT_EVENT, markSealedStreamConflict } from './transfer';

const logger = createLogger({ component: 'NatsEventStore' });

/**
 * Deterministic 64-bit-ish content hash (two FNV-1a 32-bit passes with
 * different seeds, hex-joined). Used to make `Nats-Msg-Id` content-addressed
 * so server-side dedup only absorbs byte-identical retries — see module doc.
 */
function payloadHash(payload: string): string {
  let h1 = 0x811c9dc5;
  let h2 = 0x01000193;
  for (let i = 0; i < payload.length; i++) {
    const c = payload.charCodeAt(i);
    h1 = Math.imul(h1 ^ c, 0x01000193) >>> 0;
    h2 = Math.imul(h2 ^ c, 0x85ebca6b) >>> 0;
  }
  return h1.toString(16).padStart(8, '0') + h2.toString(16).padStart(8, '0');
}

/** Options for {@link NatsEventStore}. */
export interface NatsEventStoreOptions {
  /** JetStream stream that stores all events. Default: `CEVES_EVENTS`. */
  streamName?: string;
  /** First subject token(s) under which events are published. Default: `ceves.events`. */
  subjectPrefix?: string;
  /**
   * When set, the canonical event log is HOME-ORG partitioned:
   * subjects become `<subjectPrefix>.<homeOrg>.<aggregateType>.<aggregateId>`,
   * with the home org resolved through the directory on save AND load
   * (so `IEventStore.load(type, id)` needs no org parameter). A version-1
   * save CLAIMS the directory entry from the event envelope's `orgId`;
   * the CAS-backed claim makes aggregate ids globally unique across orgs —
   * a create under a second org is rejected as a conflict. Without a
   * directory, subjects stay org-free (`<prefix>.<type>.<id>`).
   */
  orgDirectory?: NatsOrgDirectory;
  /**
   * When set, every successfully appended event is ALSO published to the
   * routed (derived) stream under
   * `<routedSubjectPrefix>.<orgId>.<aggregateType>.<eventType>.<aggregateId>`,
   * so consumers can subscribe by tenant, aggregate type, or EVENT TYPE
   * via subject wildcards. The canonical per-aggregate subject stays the
   * write/OCC/replay path — the routed copy is derived data.
   *
   * Ordering: saves for one aggregate are serialized by the actor host and
   * the fan-out publish is awaited inside `save()`, so the routed stream
   * preserves per-aggregate commit order. A fan-out failure does NOT fail
   * the save (the canonical log already has the event) — it is logged and
   * leaves a gap consumers can detect via the envelope's dense `version`.
   */
  routedSubjectPrefix?: string;
}

export const DEFAULT_EVENT_STREAM = 'CEVES_EVENTS';
export const DEFAULT_EVENT_SUBJECT_PREFIX = 'ceves.events';
export const DEFAULT_ROUTED_EVENT_STREAM = 'CEVES_EVENTS_ROUTED';
export const DEFAULT_ROUTED_EVENT_SUBJECT_PREFIX = 'ceves.evt';

/**
 * The per-subject OCC token: the stream sequence of the subject's last
 * message (what `expect.lastSubjectSequence` needs) PLUS that message's
 * Ceves version. Tracking the version lets `save()` enforce version
 * CONTINUITY (`event.version === last.version + 1`) before publishing —
 * the sequence header alone validates stream position, not version
 * uniqueness, so without this check a writer with a divergent state view
 * could append a colliding or gapped version that corrupts later replays.
 * `version` is null only if the last message's payload can't be parsed
 * (never for messages this store wrote); an empty subject is `{seq: 0,
 * version: 0, lastEventType: null}`.
 *
 * `lastEventType` additionally lets `save()` detect a SEALED stream (last
 * event is {@link AGGREGATE_TRANSFERRED_OUT_EVENT} — the aggregate was
 * transferred to another org) and refuse to append. A process with a stale
 * cached token can't slip past this: its stale sequence is rejected by the
 * server, the refresh re-reads the last message, and the re-run sees the
 * seal.
 */
interface SubjectToken {
  seq: number;
  version: number | null;
  lastEventType: string | null;
}

export class NatsEventStore implements IEventStore {
  private readonly streamName: string;
  private readonly subjectPrefix: string;
  private readonly routedSubjectPrefix: string | null;

  /**
   * Per-subject token cache. Refreshed by every successful load/save;
   * invalidated on conflict so the next attempt re-reads the truth from
   * the server.
   */
  private readonly tokenBySubject = new Map<string, SubjectToken>();

  /**
   * In-flight `save()` promises per subject. `AggregateObject` publishes
   * events fire-and-forget after committing state; the NATS actor host
   * awaits {@link waitForPendingSaves} before returning the command
   * response, turning that into an effectively synchronous event-log
   * commit (and surfacing publish failures instead of swallowing them).
   */
  private readonly pendingSaves = new Map<string, Set<Promise<void>>>();

  /**
   * First failure per aggregate, latched until the actor host observes it in
   * {@link waitForPendingSaves}. Tracking the in-flight promise alone is not
   * enough: `save()` is fire-and-forget, so a rejection that settles while
   * the command pipeline is still running (awaited side effects, response
   * building) would be removed from `pendingSaves` before the host ever
   * looks — and the host would acknowledge a command whose event never
   * reached the log.
   */
  private readonly saveFailures = new Map<string, unknown>();

  /**
   * The envelope most recently committed through `save()` per aggregate —
   * consumed by the actor host's org-transfer trigger (see
   * {@link takeLastSaved}). One entry per aggregate, overwritten on every
   * save; bounded like the token cache.
   */
  private readonly lastSavedByAggregate = new Map<string, StoredEvent>();

  constructor(
    private readonly js: JetStreamClient,
    private readonly jsm: JetStreamManager,
    options: NatsEventStoreOptions = {}
  ) {
    this.streamName = options.streamName ?? DEFAULT_EVENT_STREAM;
    this.subjectPrefix = options.subjectPrefix ?? DEFAULT_EVENT_SUBJECT_PREFIX;
    this.routedSubjectPrefix = options.routedSubjectPrefix ?? null;
    this.orgDirectory = options.orgDirectory ?? null;
  }

  private readonly orgDirectory: NatsOrgDirectory | null;

  /**
   * Canonical subject for a SAVE. In home-org mode a version-1 save claims
   * the directory entry (from the envelope's creation-time `orgId`); any
   * later version requires an existing entry. See `subjectForLoad` for the
   * read side.
   */
  private async subjectForSave(event: StoredEvent): Promise<string> {
    if (this.orgDirectory === null) {
      return eventSubjectFor(this.subjectPrefix, event.aggregateType, event.aggregateId);
    }
    if (event.version === 1) {
      const homeOrg = await this.orgDirectory.claim(
        event.aggregateType,
        event.aggregateId,
        event.orgId
      );
      if (homeOrg !== event.orgId) {
        throw new VersionConflictError(
          `Aggregate id ${event.aggregateType}/${event.aggregateId} already exists ` +
            `under org "${homeOrg}" — ids are globally unique across tenants`,
          event.version,
          event.version,
          event.aggregateType,
          event.aggregateId
        );
      }
      return tenantEventSubjectFor(
        this.subjectPrefix,
        homeOrg,
        event.aggregateType,
        event.aggregateId
      );
    }
    const homeOrg = await this.orgDirectory.resolve(event.aggregateType, event.aggregateId);
    if (homeOrg === null) {
      throw new VersionConflictError(
        `No org-directory entry for existing aggregate ` +
          `${event.aggregateType}/${event.aggregateId} (version ${event.version}) — ` +
          `state and directory have diverged`,
        event.version,
        event.version,
        event.aggregateType,
        event.aggregateId
      );
    }
    return tenantEventSubjectFor(
      this.subjectPrefix,
      homeOrg,
      event.aggregateType,
      event.aggregateId
    );
  }

  /** Canonical subject for a LOAD, or `null` when the aggregate was never created. */
  private async subjectForLoad(
    aggregateType: string,
    aggregateId: string
  ): Promise<string | null> {
    if (this.orgDirectory === null) {
      return eventSubjectFor(this.subjectPrefix, aggregateType, aggregateId);
    }
    const homeOrg = await this.orgDirectory.resolve(aggregateType, aggregateId);
    if (homeOrg === null) return null;
    return tenantEventSubjectFor(this.subjectPrefix, homeOrg, aggregateType, aggregateId);
  }

  /** Pending-save bookkeeping key — aggregate-addressed, independent of subject layout. */
  private pendingKeyFor(aggregateType: string, aggregateId: string): string {
    return `${aggregateType}\u0000${aggregateId}`;
  }

  save(event: StoredEvent): Promise<void> {
    const pendingKey = this.pendingKeyFor(event.aggregateType, event.aggregateId);
    const operation = this.doSave(event);

    let pending = this.pendingSaves.get(pendingKey);
    if (!pending) {
      pending = new Set();
      this.pendingSaves.set(pendingKey, pending);
    }
    const tracked = pending;
    tracked.add(operation);
    void operation
      .catch((error: unknown) => {
        // Latch BEFORE the finally drops the promise from the set.
        if (!this.saveFailures.has(pendingKey)) this.saveFailures.set(pendingKey, error);
      })
      .finally(() => {
        tracked.delete(operation);
        if (tracked.size === 0) this.pendingSaves.delete(pendingKey);
      });

    return operation;
  }

  /**
   * Await every in-flight `save()` for one aggregate; rethrows the first
   * failure. The NATS actor host calls this after the command pipeline
   * finishes so the response is only returned once the event is durably in
   * the log — see the `pendingSaves` doc.
   */
  async waitForPendingSaves(aggregateType: string, aggregateId: string): Promise<void> {
    const pendingKey = this.pendingKeyFor(aggregateType, aggregateId);
    // `allSettled` never rejects, so the drain loop cannot throw.
    for (;;) {
      const pending = this.pendingSaves.get(pendingKey);
      if (!pending || pending.size === 0) break;
      await Promise.allSettled([...pending]);
    }
    // Drain first, then report: a failure latched by ANY save for this
    // aggregate surfaces even if its promise already settled and was
    // dropped from the set (see `saveFailures`). Consuming it here keeps
    // the latch one-shot, so the next command starts clean.
    const failure = this.saveFailures.get(pendingKey);
    if (failure !== undefined) {
      this.saveFailures.delete(pendingKey);
      throw failure;
    }
  }

  private async doSave(event: StoredEvent): Promise<void> {
    const subject = await this.subjectForSave(event);
    const token = await this.resolveToken(subject);
    await this.guardAndPublish(subject, event, token, true);
    this.lastSavedByAggregate.set(
      this.pendingKeyFor(event.aggregateType, event.aggregateId),
      event
    );
  }

  /**
   * Return (and clear) the envelope most recently committed for one
   * aggregate through the command path (`save()`; the org-transfer
   * internals in `saveToOrg` don't count). The actor host calls this after
   * each successful command to decide whether the committed event is a
   * registered org-transfer trigger (e.g. a lock's `OrganizationSet`) and
   * to read the target org out of the event's domain data.
   */
  takeLastSaved(aggregateType: string, aggregateId: string): StoredEvent | undefined {
    const key = this.pendingKeyFor(aggregateType, aggregateId);
    const saved = this.lastSavedByAggregate.get(key);
    this.lastSavedByAggregate.delete(key);
    return saved;
  }

  /**
   * Directory-BYPASSING save targeting an explicit home org — the org
   * transfer's tool (`NatsAggregateNamespace.transferOut`), which must
   * write to a subject the directory doesn't (or doesn't yet) point at:
   * the seal event goes to the OLD org's stream deterministically even if
   * a concurrent repoint lands mid-flight, and the seed event goes to the
   * NEW org's stream BEFORE the directory is repointed. Same OCC,
   * continuity, idempotency, and routed fan-out as `save()`.
   */
  async saveToOrg(event: StoredEvent, homeOrg: string): Promise<void> {
    const subject =
      this.orgDirectory === null
        ? eventSubjectFor(this.subjectPrefix, event.aggregateType, event.aggregateId)
        : tenantEventSubjectFor(
            this.subjectPrefix,
            homeOrg,
            event.aggregateType,
            event.aggregateId
          );
    const token = await this.resolveToken(subject);
    await this.guardAndPublish(subject, event, token, true);
  }

  /**
   * Enforce version continuity against the resolved token, then publish
   * with the sequence expectation. On a wrong-last-sequence rejection the
   * token is refreshed from the server and the whole decision re-runs once
   * (`allowRefresh`) — a stale cached sequence for a still-legitimate next
   * version self-heals, while genuine conflicts (a competing event at or
   * past this version, or a log that can't produce this version) throw
   * `VersionConflictError` WITHOUT appending, keeping the log dense.
   */
  private async guardAndPublish(
    subject: string,
    event: StoredEvent,
    token: SubjectToken,
    allowRefresh: boolean
  ): Promise<void> {
    if ((await this.checkContinuity(subject, event, token)) === 'already-stored') {
      return;
    }

    try {
      await this.publishExpecting(subject, event, token.seq);
    } catch (error) {
      // The cached token was stale — drop it so future saves re-read.
      this.tokenBySubject.delete(subject);
      if (!isWrongLastSequence(error)) {
        throw new EventWriteError(
          `Failed to publish event to JetStream: ${error instanceof Error ? error.message : String(error)}`,
          {
            aggregateType: event.aggregateType,
            aggregateId: event.aggregateId,
            version: event.version,
            cause: error instanceof Error ? error : undefined,
          }
        );
      }
      if (allowRefresh) {
        logger.warn('Retrying event publish with refreshed subject token', {
          aggregateType: event.aggregateType,
          aggregateId: event.aggregateId,
          version: event.version,
          staleExpected: token.seq,
        });
        const refreshed = await this.fetchToken(subject);
        return this.guardAndPublish(subject, event, refreshed, false);
      }
      throw new VersionConflictError(
        `Concurrent write detected for ${event.aggregateType}/${event.aggregateId}: ` +
          `JetStream rejected expected last subject sequence ${token.seq}`,
        event.version,
        event.version,
        event.aggregateType,
        event.aggregateId
      );
    }
  }

  /**
   * The version-continuity gate (see {@link SubjectToken}): the event must
   * be exactly the log's next version. A byte-identical event at the
   * CURRENT version is an at-least-once retry — reported as
   * `'already-stored'` so the caller succeeds without appending. Anything
   * else is a divergence and throws. A SEALED stream (last event is the
   * transfer-out seal) accepts nothing but a retry of the seal itself —
   * the aggregate lives under another org now.
   */
  private async checkContinuity(
    subject: string,
    event: StoredEvent,
    token: SubjectToken
  ): Promise<'publish' | 'already-stored'> {
    const isRetryOfLast =
      event.version === token.version && (await this.isIdempotentDuplicate(subject, event));
    if (token.lastEventType === AGGREGATE_TRANSFERRED_OUT_EVENT) {
      if (isRetryOfLast) return 'already-stored';
      throw markSealedStreamConflict(
        new VersionConflictError(
          `Refusing to append version ${event.version} for ` +
            `${event.aggregateType}/${event.aggregateId}: the stream is sealed by ` +
            `${AGGREGATE_TRANSFERRED_OUT_EVENT} — the aggregate was transferred to another org`,
          event.version,
          event.version,
          event.aggregateType,
          event.aggregateId
        )
      );
    }
    if (token.version === null || event.version === token.version + 1) {
      return 'publish';
    }
    if (isRetryOfLast) {
      logger.info('Duplicate event save short-circuited (already stored)', {
        aggregateType: event.aggregateType,
        aggregateId: event.aggregateId,
        version: event.version,
      });
      return 'already-stored';
    }
    throw new VersionConflictError(
      `Refusing to append version ${event.version} for ` +
        `${event.aggregateType}/${event.aggregateId}: the event log's last ` +
        `version is ${token.version} — state and log have diverged ` +
        `(a competing writer, or a previously lost event)`,
      event.version,
      event.version,
      event.aggregateType,
      event.aggregateId
    );
  }

  /** Cached token, or a server read on cache miss. */
  private async resolveToken(subject: string): Promise<SubjectToken> {
    const cached = this.tokenBySubject.get(subject);
    if (cached) return cached;
    return this.fetchToken(subject);
  }

  /** Read the subject's last message and (re)prime the token cache. */
  private async fetchToken(subject: string): Promise<SubjectToken> {
    const last = await this.lastStoredMessage(subject);
    const token: SubjectToken =
      last === null
        ? { seq: 0, version: 0, lastEventType: null }
        : {
            seq: last.seq,
            version: last.envelope?.version ?? null,
            lastEventType: last.envelope?.type ?? null,
          };
    this.tokenBySubject.set(subject, token);
    return token;
  }

  /**
   * True when the subject's last message IS this event (same version,
   * byte-identical payload) — an at-least-once caller retrying a save that
   * already succeeded. Works beyond the server's dedup window.
   */
  private async isIdempotentDuplicate(subject: string, event: StoredEvent): Promise<boolean> {
    const last = await this.lastStoredMessage(subject);
    if (last === null || last.envelope === null) return false;
    if (last.envelope.version !== event.version) return false;
    return (
      payloadHash(JSON.stringify(last.envelope)) === payloadHash(JSON.stringify(event))
    );
  }

  /** One OCC-guarded, deduplicated publish; updates the token cache on success. */
  private async publishExpecting(
    subject: string,
    event: StoredEvent,
    expectedLastSeq: number
  ): Promise<void> {
    const payload = JSON.stringify(event);
    const ack = await this.js.publish(subject, payload, {
      msgID: `${event.aggregateType}.${event.aggregateId}.${event.version}.${payloadHash(payload)}`,
      expect: { lastSubjectSequence: expectedLastSeq },
    });
    this.tokenBySubject.set(subject, {
      seq: ack.seq,
      version: event.version,
      lastEventType: event.type,
    });
    if (ack.duplicate) {
      logger.info('Duplicate event publish absorbed by server', {
        aggregateType: event.aggregateType,
        aggregateId: event.aggregateId,
        version: event.version,
      });
      // The original append already fanned out — don't duplicate the copy.
      return;
    }
    await this.fanOutRouted(event, payload);
  }

  /**
   * Publish the routed (derived) copy of a just-appended event — see
   * `routedSubjectPrefix`. Awaited so per-aggregate order is preserved,
   * but non-fatal: the canonical log is authoritative and already has the
   * event, so a fan-out failure only costs consumers a detectable gap.
   */
  private async fanOutRouted(event: StoredEvent, payload: string): Promise<void> {
    if (this.routedSubjectPrefix === null) return;
    const subject = routedEventSubjectFor(
      this.routedSubjectPrefix,
      event.orgId,
      event.aggregateType,
      event.type,
      event.aggregateId
    );
    try {
      await this.js.publish(subject, payload, {
        msgID: `routed.${event.aggregateType}.${event.aggregateId}.${event.version}.${payloadHash(payload)}`,
      });
    } catch (error) {
      logger.error('Routed event fan-out failed — consumers will see a version gap', {
        aggregateType: event.aggregateType,
        aggregateId: event.aggregateId,
        version: event.version,
        eventType: event.type,
        subject,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  async load(
    aggregateType: string,
    aggregateId: string,
    afterVersion?: number,
    limit?: number
  ): Promise<StoredEvent[]> {
    const subject = await this.subjectForLoad(aggregateType, aggregateId);
    if (subject === null) return []; // never created (org directory has no entry)
    const after = afterVersion ?? 0;

    try {
      // Warm-path shortcut: if the subject's newest event is already at or
      // below `afterVersion` there is nothing to replay — skip the consumer
      // round-trip entirely (this is the common case when state was
      // restored from the KV snapshot and is fully up to date).
      const last = await this.lastStoredMessage(subject);
      if (last === null) return [];
      this.tokenBySubject.set(subject, {
        seq: last.seq,
        version: last.envelope?.version ?? null,
        lastEventType: last.envelope?.type ?? null,
      });
      if (last.envelope !== null && last.envelope.version <= after) return [];

      const events = await this.readSubject(subject);
      // Stable sort: for a (never-expected) version collision, stream order
      // is preserved and the earliest append wins in the integrity scan.
      events.sort((a, b) => a.version - b.version);
      const verified = this.verifyReplayIntegrity(aggregateType, aggregateId, events);
      const filtered = verified.filter((e) => e.version > after);
      return limit !== undefined ? filtered.slice(0, limit) : filtered;
    } catch (error) {
      throw new EventStoreError(
        `Failed to load events from JetStream: ${error instanceof Error ? error.message : String(error)}`,
        {
          aggregateType,
          aggregateId,
          cause: error instanceof Error ? error : undefined,
        }
      );
    }
  }

  async loadAll(aggregateType: string, aggregateId: string): Promise<StoredEvent[]> {
    return this.load(aggregateType, aggregateId);
  }

  /**
   * Replay-integrity scan: a healthy log is dense and unique (1..N). The
   * `save()` continuity guard keeps it that way, so anomalies here mean
   * the log was manipulated externally or predates the guard. Duplicated
   * versions keep the EARLIEST append (later ones were losers of a race
   * that should have been rejected) and are dropped loudly; gaps are
   * reported loudly but replay continues — a partially restored aggregate
   * that can still serve reads beats an unrestorable one, and the next
   * write's continuity check will surface the divergence as a 409.
   */
  private verifyReplayIntegrity(
    aggregateType: string,
    aggregateId: string,
    events: StoredEvent[]
  ): StoredEvent[] {
    const result: StoredEvent[] = [];
    let previous: number | undefined;
    for (const event of events) {
      if (previous !== undefined && event.version === previous) {
        logger.error('Duplicate version in event log — keeping the earliest append', {
          aggregateType,
          aggregateId,
          version: event.version,
          droppedType: event.type,
        });
        continue;
      }
      if (previous !== undefined && event.version !== previous + 1) {
        logger.error('Gap in event log versions — state may be missing a lost event', {
          aggregateType,
          aggregateId,
          expectedVersion: previous + 1,
          foundVersion: event.version,
        });
      }
      result.push(event);
      previous = event.version;
    }
    return result;
  }

  /**
   * Read every message currently on `subject` via an ephemeral ordered
   * consumer, updating the OCC token cache as messages stream in.
   *
   * The whole subject is read and filtered client-side. That keeps the
   * `IEventStore` version semantics exact (the OCC token is a stream
   * sequence, so an `afterVersion` cannot be translated into
   * `opt_start_seq` without an index). Aggregate streams are short by
   * design — the aggregate host checkpoints state and rarely replays.
   */
  private async readSubject(subject: string): Promise<StoredEvent[]> {
    const consumer = await this.js.consumers.get(this.streamName, {
      filter_subjects: [subject],
    });

    const events: StoredEvent[] = [];
    try {
      const info = await consumer.info(true);
      if (info.num_pending === 0) return events;

      // The iterator on an ordered consumer never terminates on its own —
      // `m.info.pending === 0` is the canonical end-of-data signal.
      const messages = await consumer.consume();
      try {
        for await (const message of messages) {
          const envelope = message.json<StoredEvent>();
          events.push(envelope);
          this.tokenBySubject.set(subject, {
            seq: message.seq,
            version: envelope.version,
            lastEventType: envelope.type,
          });
          if (message.info.pending === 0) break;
        }
      } finally {
        messages.stop();
      }
      return events;
    } finally {
      await consumer.delete().catch(() => undefined);
    }
  }

  /**
   * Lookup of the subject's last message via the standard MSG.GET API.
   * Returns `null` when the subject has no messages. The envelope is
   * `null` when the payload can't be parsed (never expected for messages
   * this store wrote).
   */
  private async lastStoredMessage(
    subject: string
  ): Promise<{ seq: number; envelope: StoredEvent | null } | null> {
    try {
      const stored = await this.jsm.streams.getMessage(this.streamName, {
        last_by_subj: subject,
      });
      if (!stored) return null;
      let envelope: StoredEvent | null = null;
      try {
        envelope = stored.json<StoredEvent>();
      } catch {
        envelope = null;
      }
      return { seq: stored.seq, envelope };
    } catch (error) {
      if (jetStreamApiErrorCode(error) === JS_ERR_NO_MESSAGE_FOUND) return null;
      throw error;
    }
  }
}
