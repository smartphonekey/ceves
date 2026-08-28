/**
 * Node module hook that redirects the `cloudflare:workers` builtin specifier
 * (imported unconditionally by ceves' AggregateObject) to the DurableObject
 * shim shipped with the ceves NATS adapter, so the example can run on plain
 * Node against NATS.
 *
 * Usage: node --import ./scripts/register-cf-shim.mjs dist-nats/main.mjs
 * Requires Node >= 22.15 (module.registerHooks).
 */
import { registerHooks } from 'node:module';

const shimUrl = import.meta.resolve('ceves/nats/cloudflare-workers-shim');

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === 'cloudflare:workers') {
      return { url: shimUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
