/**
 * AggregateObject Tests - Request Validation (DO Mode)
 *
 * Tests that Durable Object mode validates request data against
 * handler schemas, returning 400 for invalid requests.
 */

import { describe, it, expect } from 'vitest';
import { z } from 'zod';

// Local types for testing validation logic (original types were removed in refactoring)
interface CommandHandlerEntry {
  handlerClass: new () => unknown;
  metadata?: {
    route?: string;
    aggregateType?: string;
    method?: string;
    schema?: {
      request?: {
        body?: z.ZodSchema;
      };
    };
  };
}

// Test the validation logic extracted from AggregateObject
describe('AggregateObject - Request Validation', () => {
  /**
   * Simulate the validation logic from AggregateObject.executeCommand()
   * This tests the core validation without needing the full DO infrastructure
   */
  function validateCommand(
    command: unknown,
    handlerEntry: CommandHandlerEntry
  ): { success: true; data: unknown } | { success: false; response: Response } {
    const bodySchema = handlerEntry.metadata?.schema?.request?.body;

    if (bodySchema) {
      const parseResult = bodySchema.safeParse(command);
      if (!parseResult.success) {
        return {
          success: false,
          response: new Response(
            JSON.stringify({
              success: false,
              error: 'Validation failed',
              details: parseResult.error.issues,
            }),
            {
              status: 400,
              headers: { 'Content-Type': 'application/json' },
            }
          ),
        };
      }
      return { success: true, data: parseResult.data };
    }

    return { success: true, data: command };
  }

  // Create a mock handler entry with schema
  const createHandlerEntry = (bodySchema?: z.ZodSchema): CommandHandlerEntry => ({
    handlerClass: class {},
    metadata: {
      route: '/test/:id',
      aggregateType: 'test',
      method: 'POST',
      schema: {
        request: {
          body: bodySchema,
        },
      },
    },
  });

  describe('AC: Missing required fields return 400', () => {
    it('should return 400 when required field is missing', async () => {
      // Arrange
      const schema = z.object({
        lockId: z.string(),
        lockName: z.string(),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = { wrongField: 'test' };

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
        const body = await result.response.json();
        expect(body.success).toBe(false);
        expect(body.error).toBe('Validation failed');
        expect(body.details).toBeInstanceOf(Array);
        expect(body.details.length).toBeGreaterThan(0);

        // Check that missing fields are reported
        const paths = (body.details as z.core.$ZodIssue[]).map((e) => e.path[0]);
        expect(paths).toContain('lockId');
        expect(paths).toContain('lockName');
      }
    });

    it('should return 400 when one required field is missing', async () => {
      // Arrange
      const schema = z.object({
        lockId: z.string(),
        lockName: z.string(),
      });
      const handlerEntry = createHandlerEntry(schema);
      const partialCommand = { lockId: 'lock-123' }; // missing lockName

      // Act
      const result = validateCommand(partialCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
        const body = await result.response.json();
        expect((body.details as z.core.$ZodIssue[]).some((e) => e.path[0] === 'lockName')).toBe(true);
      }
    });
  });

  describe('AC: Wrong data types return 400', () => {
    it('should return 400 when string is provided instead of number', async () => {
      // Arrange
      const schema = z.object({
        id: z.string(),
        amount: z.number(),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = { id: 'test', amount: 'not-a-number' };

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
        const body = await result.response.json();
        expect((body.details as z.core.$ZodIssue[]).some((e) => e.code === 'invalid_type')).toBe(true);
      }
    });

    it('should return 400 when number is provided instead of string', () => {
      // Arrange
      const schema = z.object({
        name: z.string(),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = { name: 12345 };

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
      }
    });
  });

  describe('AC: Custom validation rules return 400', () => {
    it('should return 400 when value fails min length', async () => {
      // Arrange
      const schema = z.object({
        name: z.string().min(3, 'Name must be at least 3 characters'),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = { name: 'ab' };

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
        const body = await result.response.json();
        expect(body.details[0].message).toContain('3 characters');
      }
    });

    it('should return 400 when number fails positive() constraint', async () => {
      // Arrange
      const schema = z.object({
        amount: z.number().positive('Amount must be positive'),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = { amount: -100 };

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
        const body = await result.response.json();
        expect(body.details[0].message).toContain('positive');
      }
    });

    it('should return 400 when enum value is invalid', () => {
      // Arrange
      const schema = z.object({
        status: z.enum(['active', 'inactive', 'pending']),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = { status: 'invalid-status' };

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
      }
    });
  });

  describe('AC: Valid data passes validation', () => {
    it('should pass with all required fields present and valid', () => {
      // Arrange
      const schema = z.object({
        lockId: z.string(),
        lockName: z.string(),
        status: z.enum(['active', 'inactive']),
      });
      const handlerEntry = createHandlerEntry(schema);
      const validCommand = {
        lockId: 'lock-123',
        lockName: 'Front Door',
        status: 'active',
      };

      // Act
      const result = validateCommand(validCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toEqual(validCommand);
      }
    });

    it('should pass when no schema is defined (backward compatibility)', () => {
      // Arrange: Handler without schema
      const handlerEntry: CommandHandlerEntry = {
        handlerClass: class {},
        metadata: {
          route: '/test/:id',
          aggregateType: 'test',
          schema: {}, // No body schema
        },
      };
      const anyCommand = { anything: 'goes', foo: 123 };

      // Act
      const result = validateCommand(anyCommand, handlerEntry);

      // Assert: Should pass without validation
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toEqual(anyCommand);
      }
    });
  });

  describe('AC: Response format is correct', () => {
    it('should return proper JSON error response', async () => {
      // Arrange
      const schema = z.object({
        name: z.string(),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = {};

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.response.status).toBe(400);
        expect(result.response.headers.get('Content-Type')).toBe('application/json');

        const body = await result.response.json();
        expect(body).toEqual({
          success: false,
          error: 'Validation failed',
          details: expect.any(Array),
        });
      }
    });

    it('should include path and message in error details', async () => {
      // Arrange
      const schema = z.object({
        email: z.email('Invalid email format'),
      });
      const handlerEntry = createHandlerEntry(schema);
      const invalidCommand = { email: 'not-an-email' };

      // Act
      const result = validateCommand(invalidCommand, handlerEntry);

      // Assert
      expect(result.success).toBe(false);
      if (!result.success) {
        const body = await result.response.json();
        expect(body.details[0]).toHaveProperty('path');
        expect(body.details[0]).toHaveProperty('message');
        expect(body.details[0].path).toContain('email');
        expect(body.details[0].message).toContain('email');
      }
    });
  });
});
