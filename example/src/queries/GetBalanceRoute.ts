/**
 * GetBalance — read-only query route.
 *
 * URL: GET /accounts/:id/balance
 *
 * `QueryRoute` loads the current state from the DO (replaying events as
 * needed) and passes it to `executeQuery` along with any validated query
 * parameters. The handler is pure — it doesn't emit events or mutate state.
 */

import { Route, QueryRoute, type Context } from 'ceves';
import { z } from 'zod';
import { AccountState } from '../aggregates/BankAccountAggregate.js';

const ParamsSchema = z.object({
  id: z.string().min(1).describe('Account ID'),
});

const BalanceResponseSchema = z.object({
  accountId: z.string(),
  owner: z.string(),
  balance: z.number(),
  version: z.number(),
});

type BalanceResponse = z.infer<typeof BalanceResponseSchema>;

@Route({ method: 'GET', path: '/accounts/:id/balance' })
export class GetBalanceRoute extends QueryRoute<
  AccountState,
  Record<string, never>,
  BalanceResponse
> {
  aggregateType = 'BankAccountAggregate';

  schema = {
    request: {
      params: ParamsSchema,
    },
    responses: {
      200: {
        description: 'Current account balance',
        content: {
          'application/json': { schema: BalanceResponseSchema },
        },
      },
    },
    tags: ['BankAccount', 'Query'],
    summary: 'Get current account balance',
  };

  async executeQuery(
    state: AccountState,
    _query: Record<string, never>,
    _c: Context
  ): Promise<BalanceResponse> {
    return {
      accountId: state.id,
      owner: state.owner,
      balance: state.balance,
      version: state.version,
    };
  }
}
