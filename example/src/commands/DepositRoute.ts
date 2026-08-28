/**
 * Deposit — UPDATE command route.
 *
 * URL: POST /accounts/:id/Deposit
 *
 * `CommandRoute` (not Create) is used because this command requires the
 * aggregate to already exist. `executeCommand` takes (command, state, env)
 * with state guaranteed non-null.
 */

import { Route, CommandRoute } from 'ceves';
import { z } from 'zod';
import { AccountState } from '../aggregates/BankAccountAggregate.js';
import {
  EventTypes,
  MoneyDepositedDataSchema,
  type MoneyDepositedEventData,
} from '../types';

const ParamsSchema = z.object({
  id: z.string().min(1).describe('Account ID'),
});

const DepositBodySchema = z.object({
  amount: z.number().positive().describe('Amount to deposit'),
});

type DepositBody = z.infer<typeof DepositBodySchema>;

@Route({ method: 'POST', path: '/accounts/:id/Deposit' })
export class DepositRoute extends CommandRoute<
  DepositBody,
  AccountState,
  MoneyDepositedEventData
> {
  aggregateType = 'BankAccountAggregate';

  static readonly eventSchema = MoneyDepositedDataSchema;

  schema = {
    request: {
      params: ParamsSchema,
      body: {
        content: {
          'application/json': { schema: DepositBodySchema },
        },
      },
    },
    responses: {
      200: {
        description: 'Deposit accepted',
        content: {
          'application/json': {
            schema: z.object({
              success: z.literal(true),
              aggregateId: z.string(),
              version: z.number(),
              event: z.object({
                type: z.literal(EventTypes.MONEY_DEPOSITED),
                data: MoneyDepositedDataSchema,
              }),
            }),
          },
        },
      },
      404: {
        description: 'Account not found',
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
    summary: 'Deposit money',
  };

  async executeCommand(
    command: DepositBody,
    _state: AccountState,
    _env: unknown
  ): Promise<MoneyDepositedEventData> {
    return {
      type: EventTypes.MONEY_DEPOSITED,
      amount: command.amount,
    };
  }
}
