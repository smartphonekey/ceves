/**
 * Unit tests for Ceves error classes
 *
 * Tests cover:
 * - AC-2.4.1: Base error classes extend ApiException with additional context
 * - AC-2.4.2: Domain-specific error types for common scenarios
 * - AC-2.4.3: Error serialization and deserialization support
 * - HTTP status code propagation for API responses
 * - Chanfana integration for OpenAPI schema generation
 */

import { describe, it, expect } from 'vitest';
import { ApiException } from 'chanfana';
import { CevesError } from './CevesError';
import { CommandValidationError } from './CommandValidationError';
import { EventApplicationError } from './EventApplicationError';
import { AggregateNotFoundError } from './AggregateNotFoundError';
import { VersionConflictError } from './VersionConflictError';

describe('CevesError', () => {
  describe('AC-2.4.1: Base error class extends ApiException with context', () => {
    it('should extend JavaScript Error and ApiException classes', () => {
      // Arrange & Act
      const error = new CevesError('Test error');

      // Assert
      expect(error instanceof Error).toBe(true);
      expect(error instanceof ApiException).toBe(true);
      expect(error instanceof CevesError).toBe(true);
    });

    it('should preserve error message', () => {
      // Arrange & Act
      const error = new CevesError('Something went wrong');

      // Assert
      expect(error.message).toBe('Something went wrong');
    });

    it('should set error name to class name', () => {
      // Arrange & Act
      const error = new CevesError('Test');

      // Assert
      expect(error.name).toBe('CevesError');
    });

    it('should preserve stack trace', () => {
      // Arrange & Act
      const error = new CevesError('Test');

      // Assert
      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('CevesError');
    });

    it('should default httpStatusCode to 400', () => {
      // Arrange & Act
      const error = new CevesError('Test error');

      // Assert
      expect(error.httpStatusCode).toBe(400);
    });

    it('should accept custom httpStatusCode', () => {
      // Arrange & Act
      const error = new CevesError('Not found', 404);

      // Assert
      expect(error.httpStatusCode).toBe(404);
    });

    it('should include aggregateType when provided', () => {
      // Arrange & Act
      const error = new CevesError('Test error', 400, 'account');

      // Assert
      expect(error.aggregateType).toBe('account');
    });

    it('should include aggregateId when provided', () => {
      // Arrange & Act
      const error = new CevesError('Test error', 400, 'account', 'acc-123');

      // Assert
      expect(error.aggregateId).toBe('acc-123');
    });

    it('should have undefined aggregateType and aggregateId when not provided', () => {
      // Arrange & Act
      const error = new CevesError('Test error');

      // Assert
      expect(error.aggregateType).toBeUndefined();
      expect(error.aggregateId).toBeUndefined();
    });
  });

  describe('Chanfana integration', () => {
    it('should implement buildResponse() method returning chanfana format', () => {
      // Arrange
      const error = new CevesError('Validation failed', 400);

      // Act
      const response = error.buildResponse();

      // Assert
      expect(response).toEqual([
        {
          code: 400,
          message: 'Validation failed',
        },
      ]);
    });

    it('should return correct status code in buildResponse()', () => {
      // Arrange
      const error404 = new CevesError('Not found', 404);
      const error500 = new CevesError('Server error', 500);

      // Act
      const response404 = error404.buildResponse();
      const response500 = error500.buildResponse();

      // Assert
      expect(response404[0].code).toBe(404);
      expect(response500[0].code).toBe(500);
    });

    it('should provide static schema() method for OpenAPI', () => {
      // Act
      const schema = CevesError.schema();

      // Assert
      expect(schema).toBeDefined();
      expect(typeof schema).toBe('object');
    });
  });

  describe('AC-2.4.3: Error serialization and deserialization', () => {
    it('should serialize to JSON with all fields including httpStatusCode', () => {
      // Arrange
      const error = new CevesError('Test error', 403, 'account', 'acc-123');

      // Act
      const json = error.toJSON();

      // Assert
      expect(json.name).toBe('CevesError');
      expect(json.message).toBe('Test error');
      expect(json.httpStatusCode).toBe(403);
      expect(json.stack).toBeDefined();
      expect(json.aggregateType).toBe('account');
      expect(json.aggregateId).toBe('acc-123');
    });

    it('should deserialize from JSON correctly', () => {
      // Arrange
      const original = new CevesError('Test error', 404, 'order', 'order-456');
      const json = original.toJSON();

      // Act
      const reconstructed = CevesError.fromJSON(json);

      // Assert
      expect(reconstructed.message).toBe('Test error');
      expect(reconstructed.httpStatusCode).toBe(404);
      expect(reconstructed.aggregateType).toBe('order');
      expect(reconstructed.aggregateId).toBe('order-456');
      expect(reconstructed.stack).toBe(original.stack);
    });

    it('should preserve error type information after JSON round-trip', () => {
      // Arrange
      const error = new CevesError('Test', 500);
      const json = error.toJSON();

      // Act
      const reconstructed = CevesError.fromJSON(json);

      // Assert
      expect(reconstructed instanceof Error).toBe(true);
      expect(reconstructed instanceof ApiException).toBe(true);
      expect(reconstructed instanceof CevesError).toBe(true);
      expect(reconstructed.name).toBe('CevesError');
    });

    it('should handle serialization without aggregate context', () => {
      // Arrange
      const error = new CevesError('Test error', 401);

      // Act
      const json = error.toJSON();

      // Assert
      expect(json.message).toBe('Test error');
      expect(json.httpStatusCode).toBe(401);
      expect(json.aggregateType).toBeUndefined();
      expect(json.aggregateId).toBeUndefined();
    });
  });
});

describe('CommandValidationError', () => {
  describe('AC-2.4.2: Domain-specific error for command validation', () => {
    it('should extend CevesError', () => {
      // Arrange & Act
      const error = new CommandValidationError(
        'Validation failed',
        'CreateAccount',
        []
      );

      // Assert
      expect(error instanceof Error).toBe(true);
      expect(error instanceof ApiException).toBe(true);
      expect(error instanceof CevesError).toBe(true);
      expect(error instanceof CommandValidationError).toBe(true);
    });

    it('should include commandType field', () => {
      // Arrange & Act
      const error = new CommandValidationError(
        'Validation failed',
        'CreateAccount',
        []
      );

      // Assert
      expect(error.commandType).toBe('CreateAccount');
    });

    it('should include validationErrors array', () => {
      // Arrange
      const validationErrors = [
        { path: ['email'], message: 'Invalid email' },
        { path: ['age'], message: 'Must be positive' },
      ];

      // Act
      const error = new CommandValidationError(
        'Validation failed',
        'UpdateProfile',
        validationErrors,
        'user',
        'user-123'
      );

      // Assert
      expect(error.validationErrors).toEqual(validationErrors);
      expect(error.validationErrors).toHaveLength(2);
    });

    it('should have unique error name', () => {
      // Arrange & Act
      const error = new CommandValidationError('Test', 'Cmd', []);

      // Assert
      expect(error.name).toBe('CommandValidationError');
    });

    it('should serialize with command validation details', () => {
      // Arrange
      const error = new CommandValidationError(
        'Invalid command',
        'CreateAccount',
        [{ field: 'email', error: 'required' }],
        'account',
        'acc-123'
      );

      // Act
      const json = error.toJSON();

      // Assert
      expect(json.commandType).toBe('CreateAccount');
      expect(json.validationErrors).toHaveLength(1);
      expect(json.aggregateType).toBe('account');
    });

    it('should deserialize from JSON correctly', () => {
      // Arrange
      const original = new CommandValidationError(
        'Test',
        'UpdateAccount',
        [{ error: 'test' }],
        'account',
        'acc-1'
      );
      const json = original.toJSON();

      // Act
      const reconstructed = CommandValidationError.fromJSON(json);

      // Assert
      expect(reconstructed.commandType).toBe('UpdateAccount');
      expect(reconstructed.validationErrors).toHaveLength(1);
      expect(reconstructed instanceof CommandValidationError).toBe(true);
    });
  });
});

describe('EventApplicationError', () => {
  describe('AC-2.4.2: Domain-specific error for event application', () => {
    it('should extend CevesError', () => {
      // Arrange & Act
      const error = new EventApplicationError(
        'Failed to apply',
        'AccountCreated',
        1
      );

      // Assert
      expect(error instanceof Error).toBe(true);
      expect(error instanceof ApiException).toBe(true);
      expect(error instanceof CevesError).toBe(true);
      expect(error instanceof EventApplicationError).toBe(true);
    });

    it('should include eventType field', () => {
      // Arrange & Act
      const error = new EventApplicationError(
        'Apply failed',
        'MoneyDeposited',
        5
      );

      // Assert
      expect(error.eventType).toBe('MoneyDeposited');
    });

    it('should include eventVersion field', () => {
      // Arrange & Act
      const error = new EventApplicationError(
        'Apply failed',
        'MoneyWithdrawn',
        42
      );

      // Assert
      expect(error.eventVersion).toBe(42);
    });

    it('should have unique error name', () => {
      // Arrange & Act
      const error = new EventApplicationError('Test', 'Event', 1);

      // Assert
      expect(error.name).toBe('EventApplicationError');
    });

    it('should serialize with event application details', () => {
      // Arrange
      const error = new EventApplicationError(
        'Insufficient funds',
        'MoneyWithdrawn',
        3,
        'account',
        'acc-123'
      );

      // Act
      const json = error.toJSON();

      // Assert
      expect(json.eventType).toBe('MoneyWithdrawn');
      expect(json.eventVersion).toBe(3);
      expect(json.message).toBe('Insufficient funds');
    });

    it('should deserialize from JSON correctly', () => {
      // Arrange
      const original = new EventApplicationError(
        'Test',
        'OrderPlaced',
        1,
        'order',
        'order-1'
      );
      const json = original.toJSON();

      // Act
      const reconstructed = EventApplicationError.fromJSON(json);

      // Assert
      expect(reconstructed.eventType).toBe('OrderPlaced');
      expect(reconstructed.eventVersion).toBe(1);
      expect(reconstructed instanceof EventApplicationError).toBe(true);
    });
  });
});

describe('AggregateNotFoundError', () => {
  describe('AC-2.4.2: Domain-specific error for missing aggregates', () => {
    it('should extend CevesError', () => {
      // Arrange & Act
      const error = new AggregateNotFoundError('account', 'acc-123');

      // Assert
      expect(error instanceof Error).toBe(true);
      expect(error instanceof ApiException).toBe(true);
      expect(error instanceof CevesError).toBe(true);
      expect(error instanceof AggregateNotFoundError).toBe(true);
    });

    it('should auto-generate message from aggregateType and aggregateId', () => {
      // Arrange & Act
      const error = new AggregateNotFoundError('account', 'acc-123');

      // Assert
      expect(error.message).toBe('Aggregate not found: account/acc-123');
    });

    it('should include aggregateType from constructor', () => {
      // Arrange & Act
      const error = new AggregateNotFoundError('order', 'order-456');

      // Assert
      expect(error.aggregateType).toBe('order');
    });

    it('should include aggregateId from constructor', () => {
      // Arrange & Act
      const error = new AggregateNotFoundError('order', 'order-456');

      // Assert
      expect(error.aggregateId).toBe('order-456');
    });

    it('should have unique error name', () => {
      // Arrange & Act
      const error = new AggregateNotFoundError('user', 'user-1');

      // Assert
      expect(error.name).toBe('AggregateNotFoundError');
    });

    it('should serialize correctly', () => {
      // Arrange
      const error = new AggregateNotFoundError('account', 'acc-123');

      // Act
      const json = error.toJSON();

      // Assert
      expect(json.aggregateType).toBe('account');
      expect(json.aggregateId).toBe('acc-123');
      expect(json.message).toContain('account/acc-123');
    });

    it('should deserialize from JSON correctly', () => {
      // Arrange
      const original = new AggregateNotFoundError('order', 'order-1');
      const json = original.toJSON();

      // Act
      const reconstructed = AggregateNotFoundError.fromJSON(json);

      // Assert
      expect(reconstructed.aggregateType).toBe('order');
      expect(reconstructed.aggregateId).toBe('order-1');
      expect(reconstructed instanceof AggregateNotFoundError).toBe(true);
    });
  });
});

describe('VersionConflictError', () => {
  describe('AC-2.4.2: Domain-specific error for version conflicts', () => {
    it('should extend CevesError', () => {
      // Arrange & Act
      const error = new VersionConflictError(
        'Version mismatch',
        5,
        3
      );

      // Assert
      expect(error instanceof Error).toBe(true);
      expect(error instanceof ApiException).toBe(true);
      expect(error instanceof CevesError).toBe(true);
      expect(error instanceof VersionConflictError).toBe(true);
    });

    it('should include expectedVersion field', () => {
      // Arrange & Act
      const error = new VersionConflictError(
        'Version mismatch',
        5,
        3
      );

      // Assert
      expect(error.expectedVersion).toBe(5);
    });

    it('should include actualVersion field', () => {
      // Arrange & Act
      const error = new VersionConflictError(
        'Version mismatch',
        5,
        3
      );

      // Assert
      expect(error.actualVersion).toBe(3);
    });

    it('should have unique error name', () => {
      // Arrange & Act
      const error = new VersionConflictError('Test', 1, 2);

      // Assert
      expect(error.name).toBe('VersionConflictError');
    });

    it('should serialize with version conflict details', () => {
      // Arrange
      const error = new VersionConflictError(
        'Expected version 5 but got 3',
        5,
        3,
        'account',
        'acc-123'
      );

      // Act
      const json = error.toJSON();

      // Assert
      expect(json.expectedVersion).toBe(5);
      expect(json.actualVersion).toBe(3);
      expect(json.message).toContain('Expected version 5');
    });

    it('should deserialize from JSON correctly', () => {
      // Arrange
      const original = new VersionConflictError(
        'Test',
        10,
        8,
        'order',
        'order-1'
      );
      const json = original.toJSON();

      // Act
      const reconstructed = VersionConflictError.fromJSON(json);

      // Assert
      expect(reconstructed.expectedVersion).toBe(10);
      expect(reconstructed.actualVersion).toBe(8);
      expect(reconstructed instanceof VersionConflictError).toBe(true);
    });
  });
});

describe('Error inheritance chain', () => {
  it('should support instanceof checks for all error types', () => {
    // Arrange
    const errors = [
      new CevesError('Test'),
      new CommandValidationError('Test', 'Cmd', []),
      new EventApplicationError('Test', 'Event', 1),
      new AggregateNotFoundError('type', 'id'),
      new VersionConflictError('Test', 1, 2),
    ];

    // Act & Assert
    errors.forEach((error) => {
      expect(error instanceof Error).toBe(true);
      expect(error instanceof ApiException).toBe(true);
      expect(error instanceof CevesError).toBe(true);
    });
  });

  it('should have unique error names for each type', () => {
    // Arrange
    const errors = {
      base: new CevesError('Test'),
      validation: new CommandValidationError('Test', 'Cmd', []),
      application: new EventApplicationError('Test', 'Event', 1),
      notFound: new AggregateNotFoundError('type', 'id'),
      conflict: new VersionConflictError('Test', 1, 2),
    };

    // Act & Assert
    expect(errors.base.name).toBe('CevesError');
    expect(errors.validation.name).toBe('CommandValidationError');
    expect(errors.application.name).toBe('EventApplicationError');
    expect(errors.notFound.name).toBe('AggregateNotFoundError');
    expect(errors.conflict.name).toBe('VersionConflictError');

    // All names should be unique
    const names = Object.values(errors).map((e) => e.name);
    const uniqueNames = new Set(names);
    expect(uniqueNames.size).toBe(names.length);
  });
});

describe('Integration with exports', () => {
  it('should be importable from errors/index.ts', async () => {
    // Act
    const exported = await import('./index');

    // Assert
    expect(exported.CevesError).toBeDefined();
    expect(exported.CommandValidationError).toBeDefined();
    expect(exported.EventApplicationError).toBeDefined();
    expect(exported.AggregateNotFoundError).toBeDefined();
    expect(exported.VersionConflictError).toBeDefined();
  });

  it('should be importable from main index.ts', async () => {
    // Act
    const exported = await import('../index');

    // Assert
    expect(exported.CevesError).toBeDefined();
    expect(exported.CommandValidationError).toBeDefined();
    expect(exported.EventApplicationError).toBeDefined();
    expect(exported.AggregateNotFoundError).toBeDefined();
    expect(exported.VersionConflictError).toBeDefined();
  });
});
