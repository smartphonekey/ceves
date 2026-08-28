/**
 * AWS Lambda Adapter for Hono Applications
 *
 * This module provides an adapter that converts between AWS API Gateway event format
 * and standard HTTP Request/Response objects used by Hono. This enables Hono applications
 * to run on AWS Lambda without code changes.
 *
 * Key Features:
 * - Converts API Gateway events to standard Request objects
 * - Converts Hono Response objects to API Gateway response format
 * - Handles multi-value headers and query parameters
 * - Supports base64-encoded bodies
 * - Preserves Lambda context for advanced use cases
 *
 * @packageDocumentation
 */

import type { Hono } from 'hono';
import type {
  APIGatewayProxyEvent,
  APIGatewayProxyResult,
  Context as LambdaContext,
} from 'aws-lambda';
import { createLogger } from '../../logger';

const logger = createLogger({ component: 'LambdaAdapter' });

/** Build URLSearchParams from API Gateway query parameters */
function buildSearchParams(event: APIGatewayProxyEvent): URLSearchParams {
  const searchParams = new URLSearchParams();

  // Handle single-value query parameters
  if (event.queryStringParameters) {
    for (const [key, value] of Object.entries(event.queryStringParameters)) {
      if (value) searchParams.append(key, value);
    }
  }

  // Handle multi-value query parameters
  if (event.multiValueQueryStringParameters) {
    for (const [key, values] of Object.entries(event.multiValueQueryStringParameters)) {
      if (values) values.forEach((v) => searchParams.append(key, v));
    }
  }

  return searchParams;
}

/** Build Headers object from API Gateway headers */
function buildHeaders(event: APIGatewayProxyEvent): Headers {
  const headers = new Headers();

  // Handle single-value headers
  if (event.headers) {
    for (const [key, value] of Object.entries(event.headers)) {
      if (value) headers.set(key, value);
    }
  }

  // Handle multi-value headers
  if (event.multiValueHeaders) {
    for (const [key, values] of Object.entries(event.multiValueHeaders)) {
      if (values) values.forEach((v) => headers.append(key, v));
    }
  }

  return headers;
}

/** Decode request body (handles base64 encoding) */
function decodeBody(event: APIGatewayProxyEvent): string | undefined {
  if (!event.body) return undefined;
  if (event.isBase64Encoded) {
    return Buffer.from(event.body, 'base64').toString('utf-8');
  }
  return event.body;
}

/** Build full URL with search params */
function buildUrl(event: APIGatewayProxyEvent, searchParams: URLSearchParams): string {
  const host = event.headers.host ?? event.headers.Host ?? event.requestContext.domainName;
  const baseUrl = `https://${host}${event.path}`;
  const queryString = searchParams.toString();
  return queryString ? `${baseUrl}?${queryString}` : baseUrl;
}

/**
 * Convert an API Gateway event to a standard HTTP Request object.
 *
 * This function reconstructs a standard Request from the API Gateway event format,
 * handling URL construction, headers (including multi-value), query parameters,
 * and base64-encoded bodies.
 *
 * @param event - API Gateway proxy event
 * @returns Standard Request object compatible with Hono
 *
 * @example
 * ```typescript
 * const event: APIGatewayProxyEvent = {
 *   httpMethod: 'POST',
 *   path: '/accounts/123/deposit',
 *   headers: { 'content-type': 'application/json', 'x-org-id': 'org-456' },
 *   body: '{"amount":100}',
 *   // ... other fields
 * };
 *
 * const request = apiGatewayEventToRequest(event);
 * // Standard Request object ready for Hono
 * ```
 */
function apiGatewayEventToRequest(event: APIGatewayProxyEvent): Request {
  const searchParams = buildSearchParams(event);
  const finalUrl = buildUrl(event, searchParams);
  const headers = buildHeaders(event);
  const body = decodeBody(event);

  return new Request(finalUrl, {
    method: event.httpMethod,
    headers,
    body: body || undefined,
  });
}

/**
 * Convert a Hono Response to API Gateway response format.
 *
 * This function transforms a standard Response object into the format expected
 * by API Gateway, including status code, headers, and body.
 *
 * @param response - Standard Response object from Hono
 * @returns API Gateway proxy result format
 *
 * @example
 * ```typescript
 * const response = new Response(JSON.stringify({ success: true }), {
 *   status: 200,
 *   headers: { 'content-type': 'application/json' }
 * });
 *
 * const result = await responseToAPIGatewayResult(response);
 * // Returns: { statusCode: 200, headers: {...}, body: '{"success":true}' }
 * ```
 */
async function responseToAPIGatewayResult(
  response: Response
): Promise<APIGatewayProxyResult> {
  const body = await response.text();

  // Convert Headers to plain object
  const headers: Record<string, string> = {};
  response.headers.forEach((value, key) => {
    headers[key] = value;
  });

  return {
    statusCode: response.status,
    headers,
    body,
    isBase64Encoded: false,
  };
}

/**
 * Create an AWS Lambda handler function from a Hono application.
 *
 * This factory function wraps a Hono app and returns an AWS Lambda handler that:
 * 1. Converts API Gateway events to standard HTTP Requests
 * 2. Passes requests to the Hono app for routing and processing
 * 3. Converts Hono Responses back to API Gateway format
 * 4. Handles errors gracefully with 500 responses
 *
 * @param honoApp - The Hono application instance to wrap
 * @returns AWS Lambda handler function
 *
 * @example
 * ```typescript
 * import { createRouter } from 'ceves';
 * import { createLambdaHandler } from 'ceves/aws';
 * // Side-effect imports registering every @Route / @EventHandler class
 * // (generate with `npx ceves-generate-imports`):
 * import './_decoratorImports.generated';
 *
 * const app = createRouter({ openapi: { title: 'My API' } });
 *
 * // Create Lambda handler from the Hono app
 * export const handler = createLambdaHandler(app);
 * ```
 *
 * @example
 * ```typescript
 * // The returned handler can be used directly as Lambda export
 * export const handler = createLambdaHandler(myHonoApp);
 *
 * // AWS Lambda will invoke it with:
 * // - event: API Gateway proxy event
 * // - context: Lambda execution context
 * ```
 */
export function createLambdaHandler(honoApp: Hono) {
  return async (
    event: APIGatewayProxyEvent,
    context: LambdaContext
  ): Promise<APIGatewayProxyResult> => {
    try {
      // Convert API Gateway event to standard Request
      const request = apiGatewayEventToRequest(event);

      // Call Hono app (handles routing, middleware, command processing)
      // Pass Lambda context as env for access in handlers if needed
      const response = await honoApp.fetch(request, {
        lambdaContext: context,
      } as Record<string, unknown>);

      // Convert Response back to API Gateway format
      return await responseToAPIGatewayResult(response);
    } catch (error) {
      // Log error for CloudWatch
      logger.error('Request processing failed', {
        error: error instanceof Error ? error.message : String(error),
      });

      // Return 500 error in API Gateway format
      return {
        statusCode: 500,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          success: false,
          error: 'Internal server error',
          message: error instanceof Error ? error.message : 'Unknown error',
        }),
        isBase64Encoded: false,
      };
    }
  };
}
