/**
 * NATS entry point — the same BankAccount app as `index.ts` (identical
 * routes, event handlers, and `BankAccountAggregate` class), hosted on NATS
 * instead of Cloudflare.
 *
 * Commands travel **as NATS messages**: the REST adapter's routes serialize
 * each validated request onto `ceves.cmd.BankAccountAggregate.<id>` and the
 * aggregate service (a queue-group subscriber) executes it against the
 * JetStream event log + NATS KV state, replying with the response. The REST
 * process is a thin adapter — it holds no aggregate classes and touches no
 * storage.
 *
 * | Cloudflare (index.ts + wrangler.toml) | NATS (this file)                    |
 * |---------------------------------------|-------------------------------------|
 * | R2 `EVENTS_BUCKET` event log          | JetStream stream `CEVES_EVENTS`     |
 * | Durable Object storage (state)        | NATS KV bucket `ceves_state`        |
 * | Worker → DO stub subrequest           | NATS request-reply on `ceves.cmd.*` |
 * | `[[durable_objects.bindings]]`        | gateway env / aggregate service     |
 * | Workers runtime serving the Hono app  | `@hono/node-server` on Node         |
 *
 * Run it (requires a local nats-server with JetStream, e.g. `nats-server -js`):
 *
 *   npm run nats:start                 # both roles in one process (default)
 *   MODE=service npm run nats:start    # aggregate host only (no HTTP)
 *   MODE=gateway npm run nats:start    # REST adapter only
 *
 * Environment:
 *   MODE      all | service | gateway (default all)
 *   NATS_URL  server to connect to (default nats://127.0.0.1:4222)
 *   PORT      HTTP port for the gateway (default 8788)
 */

import { serve } from '@hono/node-server';
import { connect } from '@nats-io/transport-node';
import { createRouter } from 'ceves';
import {
  startNatsAggregateService,
  createNatsGatewayEnv,
  openNatsOrgDirectory,
} from 'ceves/nats';

// Same decorator barrel as the Cloudflare entry — registers every
// @Route / @EventHandler class before createRouter() snapshots the registry.
import { REGISTERED_ROUTES } from './_decoratorImports.generated';

import { BankAccountAggregate } from './aggregates/BankAccountAggregate.js';

const mode = process.env.MODE ?? 'all';
const natsUrl = process.env.NATS_URL ?? 'nats://127.0.0.1:4222';
const port = Number(process.env.PORT ?? 8788);

const connection = await connect({ servers: natsUrl });

/** Aggregate host: subscribes to ceves.cmd.BankAccountAggregate.> */
const service =
  mode === 'all' || mode === 'service'
    ? await startNatsAggregateService({
        connection,
        aggregates: [{ AggregateClass: BankAccountAggregate }],
        env: {
          // The example's checkAuthorization requires X-Org-Id on commands;
          // the tenant resolver falls back to this org where it's absent.
          DEFAULT_ORG_ID: process.env.DEFAULT_ORG_ID ?? 'demo-org',
        },
      })
    : null;

/** REST adapter: forwards every command/query as a NATS request. */
const server =
  mode === 'all' || mode === 'gateway'
    ? await (async () => {
        // Aggregate → home-org directory: existing accounts always route to
        // their authoritative tenant, even when the caller sends no or a
        // wrong X-Org-Id; the claim only mints the org on create.
        const orgDirectory = await openNatsOrgDirectory(connection);
        const env = createNatsGatewayEnv({
          connection,
          aggregates: ['BankAccountAggregate'],
          orgDirectory,
        });
        const app = createRouter({
          routes: REGISTERED_ROUTES,
          openapi: {
            title: 'Ceves Example — BankAccount API (NATS runtime)',
            version: '1.0.0',
            description:
              'The same event-sourced bank-account service as the Cloudflare worker; commands travel as NATS messages to the aggregate service.',
          },
          docsPath: '/docs',
          schemaPath: '/openapi.json',
        });
        return serve({ port, fetch: (request) => app.fetch(request, env) });
      })()
    : null;

// eslint-disable-next-line no-console -- demo server banner
console.log(
  `[${mode}] BankAccount on NATS (${natsUrl})` +
    (server ? ` — REST gateway at http://localhost:${port} (docs at /docs)` : '') +
    (service ? ' — aggregate service subscribed on ceves.cmd.BankAccountAggregate.>' : '')
);

async function shutdown(): Promise<void> {
  server?.close();
  await service?.close();
  await connection.drain();
  process.exit(0);
}

process.on('SIGINT', () => void shutdown());
process.on('SIGTERM', () => void shutdown());
