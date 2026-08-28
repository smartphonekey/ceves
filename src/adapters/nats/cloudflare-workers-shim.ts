/**
 * Stand-in for the `cloudflare:workers` builtin module, for running Ceves
 * apps outside the Cloudflare runtime (e.g. on the NATS adapter under Node).
 *
 * Ceves' `AggregateObject` does `import { DurableObject } from
 * 'cloudflare:workers'` at module load, so any process importing the root
 * `ceves` package needs that specifier to resolve. On Node it doesn't —
 * point it at this module instead, either with a bundler alias:
 *
 * ```ts
 * // esbuild
 * alias: { 'cloudflare:workers': 'ceves/nats/cloudflare-workers-shim' }
 * // (or '@sydorenkoalex/ceves/nats/cloudflare-workers-shim' when consuming
 * // the published package without an alias to the bare `ceves` name)
 * ```
 *
 * or a Node resolve hook (`module.registerHooks`, Node >= 22.15):
 *
 * ```ts
 * import { registerHooks, createRequire } from 'node:module';
 * const require = createRequire(import.meta.url);
 * const shim = require.resolve('ceves/nats/cloudflare-workers-shim');
 * registerHooks({
 *   resolve(specifier, context, nextResolve) {
 *     if (specifier === 'cloudflare:workers') {
 *       return { url: new URL(`file://${shim}`).href, shortCircuit: true };
 *     }
 *     return nextResolve(specifier, context);
 *   },
 * });
 * ```
 *
 * The class only replicates what `AggregateObject` relies on from the real
 * base: holding `ctx` and `env`. It is never instantiated directly by the
 * NATS runtime with real Cloudflare types — the "ctx" is a
 * `NatsAggregateContext`.
 */

export class DurableObject<TEnv = unknown> {
  protected ctx: unknown;
  protected env: TEnv;

  constructor(ctx: unknown, env: TEnv) {
    this.ctx = ctx;
    this.env = env;
  }
}
