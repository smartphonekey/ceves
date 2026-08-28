/**
 * Withdraw — UPDATE command route with a business-rule check.
 *
 * URL: POST /accounts/:id/Withdraw
 *
 * Demonstrates throwing `BusinessRuleViolationError` from a command handler.
 * The framework catches it, prevents the event from being persisted, and
 * returns a 400 response to the client. State is guaranteed non-null.
 */

import { Route, CommandRoute, BusinessRuleViolationError } from 'ceves';
import { z } from 'zod';
import { AccountState } from '../aggregates/BankAccountAggregate.js';
import {
  EventTypes,
  MoneyWithdrawnDataSchema,
  type MoneyWithdrawnEventData,
} from '../types';

const ParamsSchema = z.object({
  id: z.string().min(1).describe('Account ID'),
});

const WithdrawBodySchema = z.object({
  amount: z.number().positive().describe('Amount to withdraw'),
});

type WithdrawBody = z.infer<typeof WithdrawBodySchema>;

@Route({ method: 'POST', path: '/accounts/:id/Withdraw' })
export class WithdrawRoute extends CommandRoute<
  WithdrawBody,
  AccountState,
  MoneyWithdrawnEventData
> {
  aggregateType = 'BankAccountAggregate';

  static readonly eventSchema = MoneyWithdrawnDataSchema;

  schema = {
    request: {
      params: ParamsSchema,
      body: {
        content: {
          'application/json': { schema: WithdrawBodySchema },
        },
      },
    },
    responses: {
      200: {
        description: 'Withdrawal accepted',
        content: {
          'application/json': {
            schema: z.object({
              success: z.literal(true),
              aggregateId: z.string(),
              version: z.number(),
              event: z.object({
                type: z.literal(EventTypes.MONEY_WITHDRAWN),
                data: MoneyWithdrawnDataSchema,
              }),
            }),
          },
        },
      },
      400: {
        description: 'Business rule violated (e.g. insufficient funds)',
        content: {
          'application/json': {
            schema: z.object({
              success: z.literal(false),
              error: z.string(),
              message: z.string(),
            }),
          },
        },
      },
    },
    tags: ['BankAccount', 'Command'],
    summary: 'Withdraw money',
  };

  async executeCommand(
    command: WithdrawBody,
    state: AccountState,
    _env: unknown
  ): Promise<MoneyWithdrawnEventData> {
    if (state.balance < command.amount) {
      throw new BusinessRuleViolationError(
        `Insufficient funds: balance ${state.balance}, requested ${command.amount}`
      );
    }

    return {
      type: EventTypes.MONEY_WITHDRAWN,
      amount: command.amount,
    };
  }
}
