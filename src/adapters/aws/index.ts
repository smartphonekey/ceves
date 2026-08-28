/**
 * AWS Lambda Adapter for Ceves Event Sourcing Library
 *
 * This module provides AWS-specific implementations and adapters for running
 * Ceves applications on AWS Lambda with S3 storage and API Gateway.
 *
 * @example
 * ```typescript
 * import { createRouter } from 'ceves';
 * import {
 *   S3EventStore,
 *   S3SnapshotStore,
 *   HeaderTenantResolver,
 *   createLambdaHandler
 * } from 'ceves/aws';
 * import { S3Client } from '@aws-sdk/client-s3';
 * import './routes'; // side-effect imports registering @Route / @EventHandler classes
 *
 * const s3 = new S3Client({ region: process.env.AWS_REGION });
 * const eventStore = new S3EventStore(s3, process.env.EVENTS_BUCKET!);
 * const tenantResolver = new HeaderTenantResolver('X-Org-Id');
 * // Wire the stores into your aggregate via setStores(eventStore, tenantResolver)
 * // (S3SnapshotStore is available for snapshot persistence on the AWS path.)
 *
 * const app = createRouter({ openapi: { title: 'My API' } });
 * export const handler = createLambdaHandler(app);
 * ```
 *
 * @packageDocumentation
 */

// Storage implementations
export { S3EventStore } from '../../storage/S3EventStore';
export { S3SnapshotStore } from '../../storage/S3SnapshotStore';

// Tenant resolution
export { HeaderTenantResolver } from '../../tenancy/HeaderTenantResolver';

// Lambda adapter
export { createLambdaHandler } from './LambdaAdapter';

// Type definitions
export type { AWSEnv } from './types';
export type { ISnapshotStore } from '../../storage/interfaces';
