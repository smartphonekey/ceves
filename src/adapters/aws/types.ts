/**
 * AWS-specific type definitions for Ceves Event Sourcing Library
 *
 * This module defines TypeScript types and interfaces specific to AWS Lambda
 * and related AWS services used by Ceves.
 *
 * @packageDocumentation
 */

/**
 * AWS environment configuration for Ceves applications running on Lambda.
 *
 * This interface defines the expected environment variables and configuration
 * needed for a Ceves app running on AWS Lambda with S3 storage.
 *
 * @example
 * ```typescript
 * // In Lambda handler
 * const config: AWSEnv = {
 *   EVENTS_BUCKET: process.env.EVENTS_BUCKET!,
 *   AWS_REGION: process.env.AWS_REGION!,
 *   DEFAULT_ORG_ID: process.env.DEFAULT_ORG_ID
 * };
 * ```
 */
export interface AWSEnv {
  /**
   * S3 bucket name for storing events and snapshots.
   * Events are stored at: {EVENTS_BUCKET}/events/{aggregateType}/{aggregateId}/{version}.json
   * Snapshots are stored at: {EVENTS_BUCKET}/snapshots/{aggregateType}/{aggregateId}/snapshot.json
   */
  EVENTS_BUCKET: string;

  /**
   * AWS region for S3 client configuration.
   * Example: 'us-east-1', 'eu-west-1'
   */
  AWS_REGION: string;

  /**
   * Optional default organization ID for local development.
   * Used when X-Org-Id header is missing (typically for testing).
   */
  DEFAULT_ORG_ID?: string;
}
