/**
 * Commands as NATS messages: gateway (`NatsRequestReplyNamespace`) →
 * request-reply → aggregate service (`NatsAggregateService`) → local actor
 * (`NatsAggregateNamespace` hosting a real `AggregateObject` subclass).
 *
 * Uses the in-memory core-NATS fake (queue groups + request-reply) plus the
 * JetStream/KV fakes, so this exercises the full distributed topology
 * in-process: every command observably travels as a NATS message.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import type { KV } from '@nats-io/kv';
import type { JetStreamClient, JetStreamManager } from '@nats-io/jetstream';
import { Route } from '../../../routing/Route';
import { CommandRoute, CreateCommandRoute } from '../../../routing/CommandRoute';
import { EventHandler, type IEventHandler } from '../../../decorators';
import { AggregateObject, type AggregateObjectEnv } from '../../../core/AggregateObject';
import { BaseState } from '../../../schemas/State';
import type { EventMetadata } from '../../../events/EventMetadata';
import { NatsAggregateNamespace } from '../NatsAggregateNamespace';
import { NatsAggregateService } from '../NatsAggregateService';
import { NatsRequestReplyNamespace } from '../NatsRequestReplyNamespace';
import { NatsEventStore } from '../NatsEventStore';
import {
  requestToEnvelope,
  envelopeToRequest,
  responseToEnvelope,
  envelopeToResponse,
  commandSubjectFor,
} from '../http-over-nats';
import { FakeJetStream, FakeJetStreamManager, FakeKv, FakeNatsConnection } from './fakes';

// --- Test domain: a Vault with Open + Store commands ------------------------

class VaultState extends BaseState {
  items: string[] = [];
}

interface VaultOpenedEvent {
  type: 'VaultOpened';
}

interface ItemStoredEvent {
  type: 'ItemStored';
  item: string;
}

@Route({ method: 'POST', path: '/vaults/:id/OpenVault' })
class OpenVaultRoute extends CreateCommandRoute<Record<string, never>, VaultState, VaultOpenedEvent> {
  aggregateType = 'VaultAggregate';
  schema = {};

  executeCommand(): Promise<VaultOpenedEvent> {
    return Promise.resolve({ type: 'VaultOpened' });
  }
}

@Route({ method: 'POST', path: '/vaults/:id/StoreItem' })
class StoreItemRoute extends CommandRoute<{ item: string }, VaultState, ItemStoredEvent> {
  aggregateType = 'VaultAggregate';
  schema = {};

  executeCommand(command: { item: string }): Promise<ItemStoredEvent> {
    return Promise.resolve({ type: 'ItemStored', item: command.item });
  }
}

@EventHandler
class VaultOpenedHandler implements IEventHandler<VaultState, VaultOpenedEvent> {
  eventType = 'VaultOpened';
  aggregateType = 'VaultAggregate';

  apply(state: VaultState, _event: VaultOpenedEvent, metadata: EventMetadata): VaultState {
    return { ...state, id: metadata.aggregateId, orgId: metadata.orgId, items: [] };
  }
}

@EventHandler
class ItemStoredHandler implements IEventHandler<VaultState, ItemStoredEvent> {
  eventType = 'ItemStored';
  aggregateType = 'VaultAggregate';

  apply(state: VaultState, event: ItemStoredEvent): VaultState {
    return { ...state, items: [...state.items, event.item] };
  }
}

class VaultAggregate extends AggregateObject<VaultState> {
  constructor(ctx: DurableObjectState, env: AggregateObjectEnv) {
    super(ctx, env, VaultState);
  }
}

// --- Harness ----------------------------------------------------------------

async function bodyOf(response: Response): Promise<Record<string, unknown>> {
  const body: Record<string, unknown> = await response.json();
  return body;
}

describe('envelope round-trips', () => {
  it('request → envelope → request preserves method, url, headers, body', async () => {
    const original = new Request('https://x.local/vaults/v1/StoreItem', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Org-Id': 'acme' },
      body: JSON.stringify({ item: 'gold' }),
    });
    const envelope = await requestToEnvelope(original);
    const rebuilt = envelopeToRequest(envelope);
    expect(rebuilt.method).toBe('POST');
    expect(rebuilt.url).toBe('https://x.local/vaults/v1/StoreItem');
    expect(rebuilt.headers.get('x-org-id')).toBe('acme');
    expect(await rebuilt.text()).toBe(JSON.stringify({ item: 'gold' }));
  });

  it('GET request without body round-trips', async () => {
    const envelope = await requestToEnvelope(
      new Request('https://x.local/vaults/v1/__state', { method: 'GET' })
    );
    expect(envelope.body).toBeUndefined();
    expect(envelopeToRequest(envelope).method).toBe('GET');
  });

  it('response → envelope → response preserves status, headers, body', async () => {
    const envelope = await responseToEnvelope(
      new Response('{"ok":true}', {
        status: 201,
        headers: { 'Content-Type': 'application/json' },
      })
    );
    const rebuilt = envelopeToResponse(envelope);
    expect(rebuilt.status).toBe(201);
    expect(await rebuilt.text()).toBe('{"ok":true}');
  });
});

describe('commands as NATS messages (gateway → service)', () => {
  let nats: FakeNatsConnection;
  let js: FakeJetStream;
  let kv: FakeKv;
  let service: NatsAggregateService;
  let gateway: NatsRequestReplyNamespace;

  beforeEach(() => {
    nats = new FakeNatsConnection();
    js = new FakeJetStream();
    kv = new FakeKv();

    const eventStore = new NatsEventStore(
      js as unknown as JetStreamClient,
      new FakeJetStreamManager(js) as unknown as JetStreamManager
    );
    const env: Record<string, unknown> = { DEFAULT_ORG_ID: 'org-vault' };
    const local = new NatsAggregateNamespace({
      aggregateType: 'VaultAggregate',
      AggregateClass: VaultAggregate,
      kv: kv as unknown as KV,
      eventStore,
      getEnv: () => env,
    });
    service = new NatsAggregateService({
      connection: nats,
      aggregateType: 'VaultAggregate',
      local,
      subjectPrefix: 'ceves.cmd',
    });
    service.start();

    gateway = new NatsRequestReplyNamespace({
      connection: nats,
      aggregateType: 'VaultAggregate',
      subjectPrefix: 'ceves.cmd',
      timeoutMillis: 5000,
    });
  });

  afterEach(async () => {
    await service.stop();
  });

  function post(id: string, action: string, body: unknown, orgId?: string): Promise<Response> {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (orgId !== undefined) headers['X-Org-Id'] = orgId;
    return gateway.get(gateway.idFromName(id)).fetch(
      new Request(`https://gateway.local/vaults/${id}/${action}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
      })
    );
  }

  it('routes a create command over NATS and persists through the service', async () => {
    const response = await post('v-1', 'OpenVault', {});
    expect(response.status).toBe(201);
    expect(await bodyOf(response)).toMatchObject({
      success: true,
      aggregateId: 'v-1',
      version: 1,
      event: { type: 'VaultOpened' },
    });

    // The command demonstrably traveled as a NATS message.
    expect(nats.subscriptions[0]?.delivered).toBe(1);
    expect(nats.subscriptions[0]?.pattern).toBe('ceves.cmd.*.VaultAggregate.>');
  });

  it('routes queries (__state) over NATS too', async () => {
    await post('v-2', 'OpenVault', {});
    await post('v-2', 'StoreItem', { item: 'gold' });
    const response = await gateway
      .get(gateway.idFromName('v-2'))
      .fetch(new Request('https://gateway.local/vaults/v-2/__state', { method: 'GET' }));
    expect(response.status).toBe(200);
    const state: VaultState = await response.json();
    expect(state.items).toEqual(['gold']);
    expect(state.version).toBe(2);
  });

  it('serializes concurrent commands for one aggregate across the transport', async () => {
    await post('v-3', 'OpenVault', {});
    const responses = await Promise.all([
      post('v-3', 'StoreItem', { item: 'a' }),
      post('v-3', 'StoreItem', { item: 'b' }),
      post('v-3', 'StoreItem', { item: 'c' }),
    ]);
    expect(responses.map((r) => r.status)).toEqual([200, 200, 200]);
    const versions = await Promise.all(responses.map(async (r) => (await bodyOf(r))['version']));
    expect([...versions].sort((a, b) => Number(a) - Number(b))).toEqual([2, 3, 4]);
  });

  it('propagates framework errors (404 on update of a missing aggregate)', async () => {
    const response = await post('missing', 'StoreItem', { item: 'x' });
    expect(response.status).toBe(404);
  });

  it('replies 400 to a malformed command envelope', async () => {
    const reply = await nats.request(
      commandSubjectFor('ceves.cmd', 'org-vault', 'VaultAggregate', 'v-4'),
      'this is not json'
    );
    const parsed: { status: number } = JSON.parse(reply.string());
    expect(parsed.status).toBe(400);
  });

  it('returns 503 from the gateway when no service is subscribed', async () => {
    const lonely = new NatsRequestReplyNamespace({
      connection: new FakeNatsConnection(),
      aggregateType: 'VaultAggregate',
      subjectPrefix: 'ceves.cmd',
      timeoutMillis: 100,
    });
    const response = await lonely
      .get(lonely.idFromName('v-9'))
      .fetch(
        new Request('https://gateway.local/vaults/v-9/OpenVault', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
        })
      );
    expect(response.status).toBe(503);
    expect(await bodyOf(response)).toMatchObject({
      success: false,
      errors: [{ code: 503 }],
    });
  });

  it("routes commands under the caller's claimed org token", async () => {
    const seen: string[] = [];
    const originalRequest = nats.request.bind(nats);
    nats.request = (subject, payload, opts) => {
      seen.push(subject);
      return originalRequest(subject, payload, opts);
    };

    await post('v-org', 'OpenVault', {}, 'acme');
    expect(seen).toEqual(['ceves.cmd.acme.VaultAggregate.v-org']);

    // No header → the gateway's default org token keeps the request routable.
    await post('v-org', 'StoreItem', { item: 'x' });
    expect(seen[1]).toBe('ceves.cmd.default-org.VaultAggregate.v-org');
  });

  it('an org-filtered service only receives its own tenant', async () => {
    // Stop the wildcard service so queue groups don't overlap.
    await service.stop();

    const acmeOnly = new NatsAggregateService({
      connection: nats,
      aggregateType: 'VaultAggregate',
      local: new NatsAggregateNamespace({
        aggregateType: 'VaultAggregate',
        AggregateClass: VaultAggregate,
        kv: kv as unknown as KV,
        eventStore: new NatsEventStore(
          js as unknown as JetStreamClient,
          new FakeJetStreamManager(js) as unknown as JetStreamManager
        ),
        getEnv: () => ({ DEFAULT_ORG_ID: 'acme' }),
      }),
      subjectPrefix: 'ceves.cmd',
      orgFilter: 'acme',
    });
    acmeOnly.start();
    try {
      const acme = await post('v-t1', 'OpenVault', {}, 'acme');
      expect(acme.status).toBe(201);

      // Another tenant's command has no responder on this deployment.
      const rival = await post('v-t2', 'OpenVault', {}, 'rival');
      expect(rival.status).toBe(503);
    } finally {
      await acmeOnly.stop();
    }
  });

  it('registers the vault routes and handlers via decorators', () => {
    expect(
      [OpenVaultRoute, StoreItemRoute, VaultOpenedHandler, ItemStoredHandler].map((c) => c.name)
    ).toEqual(['OpenVaultRoute', 'StoreItemRoute', 'VaultOpenedHandler', 'ItemStoredHandler']);
  });
});
