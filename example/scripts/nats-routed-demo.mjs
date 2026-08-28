// Demonstrates the routed (derived) event stream: subscribe by event type,
// by tenant, or read one aggregate's full ordered history — all via subject
// wildcards, without touching the canonical per-aggregate log.
import { connect } from '@nats-io/transport-node';
import { jetstream } from '@nats-io/jetstream';

const acc = process.argv[2] ?? 'acc-routed-1';
const nc = await connect({ servers: process.env.NATS_URL ?? 'nats://127.0.0.1:4222' });
const js = jetstream(nc);

async function read(filter) {
  const consumer = await js.consumers.get('CEVES_EVENTS_ROUTED', { filter_subjects: [filter] });
  const out = [];
  const info = await consumer.info(true);
  if (info.num_pending > 0) {
    const messages = await consumer.consume();
    for await (const m of messages) {
      const e = m.json();
      out.push(`v${e.version} ${e.type} ${JSON.stringify(e.event)} [${m.subject}]`);
      if (m.info.pending === 0) break;
    }
    messages.stop();
  }
  await consumer.delete().catch(() => {});
  console.log(`\n== ${filter}`);
  for (const line of out) console.log('  ' + line);
  if (out.length === 0) console.log('  (no events)');
}

// Only deposits, org acme, any account:
await read(`ceves.evt.acme.BankAccountAggregate.MoneyDeposited.>`);
// One account's FULL history (any event type), still in order:
await read(`ceves.evt.acme.BankAccountAggregate.*.${acc}`);
// Everything the acme tenant did:
await read(`ceves.evt.acme.>`);
await nc.drain();
