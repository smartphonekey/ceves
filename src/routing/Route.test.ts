/**
 * Tests for @Route decorator
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { OpenAPIRoute } from 'chanfana';
import { Route, clearRoutes, getRegisteredRoutes } from './Route.js';
import { z } from 'zod';

describe('@Route decorator', () => {
  beforeEach(() => {
    // Clear routes before each test
    clearRoutes();
  });

  it('should register a route class', () => {
    @Route({ method: 'GET', path: '/test' })
    class TestRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {
          200: {
            description: 'Success',
            content: {
              'application/json': {
                schema: z.object({ success: z.boolean() }),
              },
            },
          },
        },
      };

      async handle() {
        return { success: true };
      }
    }
    TestRoute; // Side-effect: registers route

    const routes = getRegisteredRoutes();
    expect(routes).toHaveLength(1);
    expect(routes[0]?.RouteClass.name).toBe('TestRoute');
    expect(routes[0]?.method).toBe('GET');
    expect(routes[0]?.path).toBe('/test');
  });

  it('should register multiple route classes', () => {
    @Route({ method: 'GET', path: '/users/:id' })
    class GetUserRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {},
      };

      async handle() {
        return { success: true };
      }
    }

    @Route({ method: 'POST', path: '/users' })
    class CreateUserRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {},
      };

      async handle() {
        return { success: true };
      }
    }
    GetUserRoute; // Side-effect: registers route
    CreateUserRoute; // Side-effect: registers route

    const routes = getRegisteredRoutes();
    expect(routes).toHaveLength(2);
    expect(routes.map(r => r.RouteClass.name)).toContain('GetUserRoute');
    expect(routes.map(r => r.RouteClass.name)).toContain('CreateUserRoute');
  });

  it('should throw error when different class registers same route', () => {
    @Route({ method: 'GET', path: '/duplicate' })
    class DuplicateRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {},
      };

      async handle() {
        return { success: true };
      }
    }
    DuplicateRoute; // Side-effect: registers route

    // Different class with same route should throw
    expect(() => {
      @Route({ method: 'GET', path: '/duplicate' })
      class AnotherRoute extends OpenAPIRoute {
        schema = {
          request: {},
          responses: {},
        };

        async handle() {
          return { success: true };
        }
      }
      AnotherRoute;
    }).toThrow('Route already registered: GET:/duplicate');
  });

  it('should allow same class to re-register (HMR support)', () => {
    // Simulate HMR: same class name registered twice
    // Use IIFEs to create separate scopes (simulates module re-execution)
    (() => {
      @Route({ method: 'GET', path: '/hmr-test' })
      class HmrRoute extends OpenAPIRoute {
        schema = { request: {}, responses: {} };
        async handle() {
          return { version: 1 };
        }
      }
      HmrRoute;
    })();

    // HMR reloads create new class with same name - should not throw
    (() => {
      @Route({ method: 'GET', path: '/hmr-test' })
      class HmrRoute extends OpenAPIRoute {
        schema = { request: {}, responses: {} };
        async handle() {
          return { version: 2 };
        }
      }
      HmrRoute;
    })();

    // Should still have exactly one route
    const routes = getRegisteredRoutes();
    expect(routes).toHaveLength(1);
    expect(routes[0]?.RouteClass.name).toBe('HmrRoute');
  });

  it('should allow same path with different methods', () => {
    @Route({ method: 'GET', path: '/users/:id' })
    class GetUserRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {},
      };

      async handle() {
        return { success: true };
      }
    }

    @Route({ method: 'POST', path: '/users/:id' })
    class UpdateUserRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {},
      };

      async handle() {
        return { success: true };
      }
    }
    GetUserRoute; // Side-effect: registers route
    UpdateUserRoute; // Side-effect: registers route

    const routes = getRegisteredRoutes();
    expect(routes).toHaveLength(2);
    expect(routes[0]?.method).toBe('GET');
    expect(routes[1]?.method).toBe('POST');
  });

  it('should clear all routes', () => {
    @Route({ method: 'GET', path: '/test1' })
    class TestRoute1 extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {},
      };

      async handle() {
        return { success: true };
      }
    }

    @Route({ method: 'POST', path: '/test2' })
    class TestRoute2 extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {},
      };

      async handle() {
        return { success: true };
      }
    }
    TestRoute1; // Side-effect: registers route
    TestRoute2; // Side-effect: registers route

    expect(getRegisteredRoutes()).toHaveLength(2);

    clearRoutes();

    expect(getRegisteredRoutes()).toHaveLength(0);
  });

  it('should preserve class functionality after decoration', () => {
    @Route({ method: 'GET', path: '/functional' })
    class FunctionalRoute extends OpenAPIRoute {
      schema = {
        request: {},
        responses: {
          200: {
            description: 'Success',
            content: {
              'application/json': {
                schema: z.object({ message: z.string() }),
              },
            },
          },
        },
      };

      async handle() {
        return { message: 'Hello from decorated route' };
      }
    }

    // Verify the class can still be instantiated and used
    const instance = new FunctionalRoute({
      router: {},
      raiseUnknownParameters: false,
      route: '/functional',
      urlParams: [],
    });
    expect(instance).toBeInstanceOf(OpenAPIRoute);
    expect(instance.handle).toBeDefined();
  });
});
