import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'adapters/aws/index': 'src/adapters/aws/index.ts',
    'adapters/nats/index': 'src/adapters/nats/index.ts',
    'adapters/nats/cloudflare-workers-shim':
      'src/adapters/nats/cloudflare-workers-shim.ts',
    'adapters/pg/index': 'src/adapters/pg/index.ts',
  },
  format: ['esm'],
  dts: true, // Re-enable to check errors
  clean: true,
  sourcemap: true,
  // Shared ESM chunks so all entries (index, aws, nats) share ONE copy of the
  // error classes — with per-entry inlining, `instanceof CevesError` fails
  // across entry boundaries (an adapter-thrown VersionConflictError would
  // not be recognized by AggregateObject.handleError and become a 500).
  splitting: true,
  treeshake: true,
  minify: false,
  external: ['cloudflare:workers'],
});
