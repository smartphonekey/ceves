/**
 * OpenAccount — CREATE command route.
 *
 * URL: POST /accounts/:id/OpenAccount
 *
 * `CreateCommandRoute` is used (not `CommandRoute`) because this command runs
 * against a NON-EXISTENT aggregate. As a result `executeCommand` only takes
 * (command, env) — there is no `state` argument because there is no prior state.
 */

import { Route, CreateCommandRoute } from 'ceves';
import { z } from 'zod';
import { AccountState } from '../aggregates/BankAccountAggregate.js';
import {
  EventTypes,
  AccountOpenedDataSchema,
  type AccountOpenedEventData,
} from '../types';

const ParamsSchema = z.object({
  id: z.string().min(1).describe('Account ID (used as the DO name)'),
});

const OpenAccountBodySchema = z.object({
  owner: z.string().min(1).describe('Owner name'),
  initialDeposit: z
    .number()
    .min(0)
    .describe('Starting balance, in whole units'),
});

type OpenAccountBody = z.infer<typeof OpenAccountBodySchema>;

@Route({ method: 'POST', path: '/accounts/:id/OpenAccount' })
export class OpenAccountRoute extends CreateCommandRoute<
  OpenAccountBody,
  AccountState,
  AccountOpenedEventData
> {
  aggregateType = 'BankAccountAggregate';

  /**
   * Surfaces the AccountOpened event shape in the OpenAPI doc and triggers a
   * runtime `safeParse` of the data payload returned by `executeCommand()`
   * (with the outer `type` stripped) to catch handler bugs at the framework
   * boundary. See AA-82.
   */
  static readonly eventSchema = AccountOpenedDataSchema;

  schema = {
    request: {
      params: ParamsSchema,
      body: {
        content: {
          'application/json': { schema: OpenAccountBodySchema },
        },
      },
    },
    responses: {
      // A genuine create is RESTfully a 201. The body carries the just-emitted
      // event under `event: { type, data }`.
      201: {
        description: 'Account opened',
        content: {
          'application/json': {
            schema: z.object({
              success: z.literal(true),
              aggregateId: z.string(),
              version: z.number(),
              event: z
                .object({
                  type: z.literal(EventTypes.ACCOUNT_OPENED),
                  data: AccountOpenedDataSchema,
                })
                .nullable(),
            }),
          },
        },
      },
      // AA-92: re-opening an account that already exists is an idempotent
      // no-op, NOT a 409. Same body shape with `event: null` — the framework
      // returns this automatically, so the handler needs no `if (state) throw`.
      200: {
        description: 'Account already exists — idempotent no-op (event: null)',
        content: {
          'application/json': {
            schema: z.object({
              success: z.literal(true),
              aggregateId: z.string(),
              version: z.number(),
              event: z
                .object({
                  type: z.literal(EventTypes.ACCOUNT_OPENED),
                  data: AccountOpenedDataSchema,
                })
                .nullable(),
            }),
          },
        },
      },
    },
    tags: ['BankAccount', 'Command'],
    summary: 'Open a new bank account',
  };

  async executeCommand(
    command: OpenAccountBody,
    _env: unknown
  ): Promise<AccountOpenedEventData> {
    // No null-check for state — CreateCommandRoute is only invoked when the
    // aggregate doesn't exist yet, and the framework enforces that.
    return {
      type: EventTypes.ACCOUNT_OPENED,
      owner: command.owner,
      initialDeposit: command.initialDeposit,
    };
  }
}
