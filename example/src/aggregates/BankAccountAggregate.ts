/**
 * BankAccountAggregate — Durable Object backing one bank account.
 *
 * In modern Ceves, the aggregate DO is intentionally tiny:
 * - It extends `AggregateObject<TState>`.
 * - It tells the base class which State class to use (so `State.empty()`
 *   can be called for the first event).
 * - It optionally overrides `checkAuthorization()` to enforce per-aggregate
 *   access rules.
 *
 * All command and query *logic* lives in route classes (see ../commands and
 * ../queries). Those route classes forward the validated request to this DO,
 * which then dispatches into your `executeCommand` / `executeQuery` based on
 * the registered `@Route` metadata.
 *
 * See:
 */

import { AggregateObject, BaseState, UnauthorizedError } from 'ceves';

/**
 * Environment bindings the example needs at runtime — see wrangler.toml.
 * The `[key: string]: unknown` index signature satisfies Ceves's
 * `AggregateObjectEnv` contract while still letting wrangler-provided
 * bindings appear as typed properties.
 */
export interface ExampleEnv {
  EVENTS_BUCKET: R2Bucket;
  SNAPSHOTS_BUCKET: R2Bucket;
  BANK_ACCOUNT: DurableObjectNamespace;
  [key: string]: unknown;
}

/**
 * AA-128: state is a sibling top-level export (`AccountState`). Co-located
 * with the aggregate so the state class lives next to the aggregate that owns it.
 * ES module flat exports — no namespace, no lint override needed.
 *
 * `BaseState` provides id, orgId, version, timestamp. Event handlers
 * SET id and orgId; the framework SETS version and timestamp.
 */
export class AccountState extends BaseState {
  owner: string = '';
  balance: number = 0;
}

export class BankAccountAggregate extends AggregateObject<AccountState> {
  constructor(ctx: DurableObjectState, env: ExampleEnv) {
    super(ctx, env, AccountState);
  }

  /**
   * Authorization hook — called automatically by Ceves before running any
   * command or query against this aggregate.
   *
   * The default implementation is a no-op (everything is allowed). The
   * override below shows the pattern from LockAggregate:
   *   - Public endpoints can be allow-listed by URL.
   *   - Otherwise require an X-Org-Id header (B2B style).
   *   - Once the aggregate has an orgId, block requests from other orgs.
   *
   * For an even more permissive example you can simply delete this method.
   */
  protected override checkAuthorization(request: Request): void {
    const url = new URL(request.url);

    // Allow `/accounts/:id/balance` to be queried without auth, just to
    // demonstrate per-route public endpoints. QueryRoute forwards state
    // queries to the aggregate with `/__state` appended to the original
    // path, so strip that suffix before matching the public route.
    const pathname = url.pathname.replace(/\/__state$/, '');
    if (pathname.endsWith('/balance')) {
      return;
    }

    const orgId = request.headers.get('X-Org-Id');
    if (!orgId) {
      throw new UnauthorizedError('Missing X-Org-Id header');
    }

    // If the account has been claimed by an org, only that org may touch it.
    if (this.state?.orgId && this.state.orgId !== orgId) {
      throw new UnauthorizedError(
        `Account belongs to org ${this.state.orgId}, not ${orgId}`
      );
    }
  }
}

