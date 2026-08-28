import { describe, it, expect } from 'vitest';
import {
  encodeToken,
  decodeToken,
  eventSubjectFor,
  storageKeyPrefixFor,
  aggregateTypeToBinding,
} from '../naming';

const KV_KEY_TOKEN = /^[-/_=a-zA-Z0-9]+$/; // NATS KV key alphabet (per token)

describe('encodeToken / decodeToken', () => {
  it.each([
    'simple',
    'BankAccountAggregate',
    'a.b.c',
    'has space',
    'wild*card>and.more',
    'equals=sign',
    'ünïcödé-☂',
    'UPPER_lower-123',
  ])('round-trips %j', (raw) => {
    const token = encodeToken(raw);
    expect(decodeToken(token)).toBe(raw);
    // Safe in both subjects and KV keys: no dots, wildcards, spaces, %.
    expect(token).toMatch(KV_KEY_TOKEN);
    expect(token).not.toContain('.');
  });

  it('round-trips the empty string to a non-empty token', () => {
    const token = encodeToken('');
    expect(token.length).toBeGreaterThan(0);
    expect(decodeToken(token)).toBe('');
  });

  it('produces distinct tokens for inputs that collide naively', () => {
    // '.' vs an escaped '.' literal must not collide.
    expect(encodeToken('a.b')).not.toBe(encodeToken('a=2Eb'));
  });
});

describe('eventSubjectFor / storageKeyPrefixFor', () => {
  it('builds a three-token subject under the prefix', () => {
    expect(eventSubjectFor('ceves.events', 'LockAggregate', 'lock-1')).toBe(
      'ceves.events.LockAggregate.lock-1'
    );
  });

  it('escapes dots inside aggregate ids so they stay one token', () => {
    const subject = eventSubjectFor('es', 'A', 'id.with.dots');
    expect(subject.split('.').length).toBe(3);
  });

  it('scopes storage keys by type and id', () => {
    expect(storageKeyPrefixFor('LockAggregate', 'lock-1')).toBe('LockAggregate.lock-1');
  });
});

describe('aggregateTypeToBinding', () => {
  it.each([
    ['BankAccountAggregate', 'BANK_ACCOUNT'],
    ['UserAggregate', 'USER'],
    ['LockAggregate', 'LOCK'],
    ['MatterDeviceAggregate', 'MATTER_DEVICE'],
    ['Widget', 'WIDGET'],
  ])('derives %s → %s (matches CommandRoute/QueryRoute)', (type, binding) => {
    expect(aggregateTypeToBinding(type)).toBe(binding);
  });
});
