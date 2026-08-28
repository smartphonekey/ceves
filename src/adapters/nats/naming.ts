/**
 * Naming helpers for the NATS adapter.
 *
 * NATS subjects and KV keys have restricted alphabets:
 * - Subject tokens must not contain whitespace, `.` (token separator),
 *   `*` / `>` (wildcards), or be empty.
 * - KV keys are subject-like: `[-/_=.a-zA-Z0-9]` per token, and `%` is NOT
 *   allowed (so percent-encoding can't be used).
 *
 * `encodeToken` maps an arbitrary aggregate type / aggregate ID / storage key
 * into the common safe alphabet `[A-Za-z0-9_-]` plus `=` as the escape
 * character (`=` is legal in both subjects and KV keys). The encoding is
 * deterministic and collision-free: every character outside the safe set is
 * replaced by `=XX` (uppercase hex of the UTF-8 byte), and `=` itself is
 * escaped as `=3D`.
 */

const SAFE_TOKEN_CHAR = /[A-Za-z0-9_-]/;

/** Encode one subject/KV token from an arbitrary string (see module doc). */
export function encodeToken(raw: string): string {
  if (raw.length === 0) {
    // NATS forbids empty tokens; a lone escape marker is unambiguous because
    // a non-empty input always produces at least one character.
    return '=';
  }
  let out = '';
  for (const char of raw) {
    if (char.length === 1 && SAFE_TOKEN_CHAR.test(char)) {
      out += char;
    } else {
      // Escape every UTF-8 byte of the character as =XX.
      for (const byte of new TextEncoder().encode(char)) {
        out += `=${byte.toString(16).toUpperCase().padStart(2, '0')}`;
      }
    }
  }
  return out;
}

/** Decode a token produced by {@link encodeToken}. */
export function decodeToken(token: string): string {
  if (token === '=') return '';
  const bytes: number[] = [];
  for (let i = 0; i < token.length; i++) {
    const ch = token[i];
    if (ch === '=') {
      bytes.push(Number.parseInt(token.slice(i + 1, i + 3), 16));
      i += 2;
    } else if (ch !== undefined) {
      bytes.push(ch.charCodeAt(0));
    }
  }
  return new TextDecoder().decode(new Uint8Array(bytes));
}

/**
 * Subject carrying the events of one aggregate:
 * `<prefix>.<aggregateType>.<aggregateId>` (tokens encoded).
 */
export function eventSubjectFor(
  subjectPrefix: string,
  aggregateType: string,
  aggregateId: string
): string {
  return `${subjectPrefix}.${encodeToken(aggregateType)}.${encodeToken(aggregateId)}`;
}

/**
 * Canonical event subject in HOME-ORG-partitioned mode (org directory
 * enabled): `<prefix>.<homeOrg>.<aggregateType>.<aggregateId>`. The home
 * org is the tenant at aggregate creation and changes only through the
 * explicit org-transfer operation (`transferOut`, which seals this stream
 * and starts a fresh one under the new org) — a storage partition key,
 * deliberately distinct from the envelope's `orgId` (the CURRENT tenant,
 * which `SetOrganization`-style commands may change).
 */
export function tenantEventSubjectFor(
  subjectPrefix: string,
  homeOrg: string,
  aggregateType: string,
  aggregateId: string
): string {
  return (
    `${subjectPrefix}.${encodeToken(homeOrg)}.` +
    `${encodeToken(aggregateType)}.${encodeToken(aggregateId)}`
  );
}

/**
 * Subject a committed event is FANNED OUT to on the routed (derived)
 * stream: `<prefix>.<orgId>.<aggregateType>.<eventType>.<aggregateId>`.
 *
 * The canonical event log keeps one subject per aggregate (ordering +
 * per-subject OCC); this derived copy exists purely for consumers, with
 * every dimension a subscriber may want to filter on as its own token:
 *
 * - one tenant's everything:      `ceves.evt.acme.>`
 * - a tenant's aggregate type:    `ceves.evt.acme.LockAggregate.>`
 * - one EVENT TYPE across orgs:   `ceves.evt.*.LockAggregate.KeyAdded.>`
 * - one aggregate's full history: `ceves.evt.acme.LockAggregate.*.lock-1`
 */
export function routedEventSubjectFor(
  subjectPrefix: string,
  orgId: string,
  aggregateType: string,
  eventType: string,
  aggregateId: string
): string {
  return (
    `${subjectPrefix}.${encodeToken(orgId)}.${encodeToken(aggregateType)}.` +
    `${encodeToken(eventType)}.${encodeToken(aggregateId)}`
  );
}

/**
 * KV key prefix under which one aggregate's durable storage entries live:
 * `<aggregateType>.<aggregateId>` (tokens encoded). Individual storage keys
 * are appended as a third encoded token.
 */
export function storageKeyPrefixFor(
  aggregateType: string,
  aggregateId: string
): string {
  return `${encodeToken(aggregateType)}.${encodeToken(aggregateId)}`;
}

/**
 * Convert an aggregate type to its environment binding name — the same
 * derivation `CommandRoute` / `QueryRoute` use to look the namespace up:
 * snake_case, uppercase, drop a trailing `_AGGREGATE`.
 *
 * Examples: `BankAccountAggregate` → `BANK_ACCOUNT`, `UserAggregate` → `USER`.
 */
export function aggregateTypeToBinding(aggregateType: string): string {
  const snakeCase = aggregateType.replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase();
  return snakeCase.replace(/_AGGREGATE$/, '');
}
