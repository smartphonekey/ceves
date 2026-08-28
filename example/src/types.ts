/**
 * BankAccount Domain Types
 *
 * Defines the shape of:
 * - AccountState — what the aggregate looks like at any point in time
 * - EventTypes  — string constants for the events we emit
 * - Event-data Zod schemas + inferred TS types for each event
 *
 * Modern Ceves convention:
 * - State is a class extending BaseState (so the framework can call
 *   `AccountState.empty()` for the first event).
 * - Events are defined as **two related Zod schemas**:
 *     • `*DataSchema`     — the pure data payload (no `type` field), used
 *                           both as `static readonly eventSchema` on the
 *                           command route and as the OpenAPI `event.data`
 *                           schema. This is what's on the wire under
 *                           `event.data`.
 *     • `*Schema`         — the data schema EXTENDED with the `type` literal
 *                           discriminator. Used as the return-type Zod
 *                           schema for `executeCommand()`, which still
 *                           returns `{ type: '…', …data }`. The framework
 *                           strips `type` before placing the event on the
 *                           wire (see AA-82).
 *   Splitting the two means the runtime parse runs against the actual wire
 *   payload, and the OpenAPI doc shows exactly what clients receive.
 * - Commands are not declared here — they are inlined as Zod body schemas in
 *   each command-route file (see src/commands/).
 */

import { z } from 'zod';
import { BaseState } from 'ceves';


/**
 * Event type discriminator constants.
 *
 * Using a `const` map keeps the strings centralised so refactors are typo-safe
 * (handlers reference `EventTypes.MONEY_DEPOSITED`, not the bare literal).
 */
export const EventTypes = {
  ACCOUNT_OPENED: 'AccountOpened',
  MONEY_DEPOSITED: 'MoneyDeposited',
  MONEY_WITHDRAWN: 'MoneyWithdrawn',
} as const;

/**
 * Event-data schemas (no `type` field).
 *
 * Each `*DataSchema` describes the pure business data carried under
 * `event.data` on the wire — the outer `event.type` is the discriminator,
 * so the data payload itself doesn't need to repeat it (AA-82).
 *
 * Use these as:
 *   - `static readonly eventSchema = AccountOpenedDataSchema;` on the route,
 *     so the framework runtime-parses the just-emitted data payload.
 *   - The Zod schema for the `event.data` field in the OpenAPI response doc.
 *
 * The companion `*Schema` (further down) extends the data schema with the
 * `type` literal — used as the `executeCommand()` return-type schema since
 * handlers still return `{ type: '…', …data }`.
 */
export const AccountOpenedDataSchema = z.object({
  owner: z.string(),
  initialDeposit: z.number(),
});
export const AccountOpenedSchema = AccountOpenedDataSchema.extend({
  type: z.literal(EventTypes.ACCOUNT_OPENED),
});
export type AccountOpenedEventData = z.infer<typeof AccountOpenedSchema>;

export const MoneyDepositedDataSchema = z.object({
  amount: z.number(),
});
export const MoneyDepositedSchema = MoneyDepositedDataSchema.extend({
  type: z.literal(EventTypes.MONEY_DEPOSITED),
});
export type MoneyDepositedEventData = z.infer<typeof MoneyDepositedSchema>;

export const MoneyWithdrawnDataSchema = z.object({
  amount: z.number(),
});
export const MoneyWithdrawnSchema = MoneyWithdrawnDataSchema.extend({
  type: z.literal(EventTypes.MONEY_WITHDRAWN),
});
export type MoneyWithdrawnEventData = z.infer<typeof MoneyWithdrawnSchema>;
