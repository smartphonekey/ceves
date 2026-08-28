/**
 * Router for forwarding requests to Durable Objects
 *
 * Handles:
 * - Plural → singular aggregate type conversion (users → user, locks → lock)
 * - A closed allowlist of routable aggregate types (see
 *   {@link AggregateRouterConfig} — call `AggregateRouter.configure()` at
 *   worker init to register your types; unregistered types get a 400)
 * - Authentication context forwarding
 *
 * Commands are always addressed by URL (`POST /{type}/:id/{CommandName}`); the
 * request is forwarded to the matching DO unchanged. There is no command-in-body
 * form.
 */

export interface AuthContext {
  orgId?: string;
  userId?: string;
  email?: string;
  isAdmin?: boolean;
  /**
   * True when the caller presented a "super" API key (worker forwards
   * `X-Super-Access: true`). Super keys bypass per-org ownership checks —
   * e.g. an integrator registering resources on behalf of another org.
   */
  isSuper?: boolean;
}

/**
 * Per-app routing configuration for {@link AggregateRouter}.
 *
 * Routing is a **closed allowlist**: a request only reaches a Durable Object
 * when its (singular) aggregate type is registered here — either listed in
 * `allowedTypes` (binding name derived by convention: uppercased singular,
 * `user` → `env.USER`) or given an explicit binding via `bindingNames`.
 * Everything else is a 400, so DO namespaces in the env that were never meant
 * to be HTTP-addressable (rate limiters, coordinators, …) stay unreachable.
 *
 * Plural URL segments resolve to singular types by stripping a trailing 's'
 * (`users` → `user`, case-insensitive); register irregular plurals in
 * `pluralToSingular`.
 */
export interface AggregateRouterConfig {
  /**
   * Aggregate types (singular) routable via the uppercase-singular binding
   * convention (e.g. `['user', 'lock']` → `env.USER`, `env.LOCK`).
   */
  allowedTypes?: string[];
  /** Irregular plural → singular overrides (e.g. `{ people: 'person' }`) */
  pluralToSingular?: Record<string, string>;
  /**
   * Aggregate type → env binding name overrides (e.g. `{ person: 'PERSON_DO' }`).
   * Listing a type here also allows it — no separate `allowedTypes` entry needed.
   */
  bindingNames?: Record<string, string>;
}

export class AggregateRouter {
  /** App-level overrides; set once at worker init via {@link configure}. */
  private static config: AggregateRouterConfig = {};

  /**
   * Register app-level routing overrides (irregular plurals, non-conventional
   * DO binding names). Later calls merge over earlier ones.
   */
  static configure(config: AggregateRouterConfig): void {
    this.config = {
      allowedTypes: [
        ...(this.config.allowedTypes ?? []),
        ...(config.allowedTypes ?? []),
      ],
      pluralToSingular: { ...this.config.pluralToSingular, ...config.pluralToSingular },
      bindingNames: { ...this.config.bindingNames, ...config.bindingNames },
    };
  }

  /** Clear all configured overrides (primarily for tests). */
  static resetConfig(): void {
    this.config = {};
  }

  /**
   * Forward a request to the appropriate Durable Object.
   *
   * The command is always in the URL (`POST /{type}/:id/{CommandName}`); the
   * request is forwarded unchanged, with auth headers injected when an
   * `authContext` is supplied.
   *
   * @param env - Environment bindings with DO namespaces
   * @param request - Incoming request
   * @param authContext - Optional authentication context to inject into headers
   * @returns Response from Durable Object
   */
  static async forward(
    env: Record<string, unknown>,
    request: Request,
    authContext?: AuthContext
  ): Promise<Response> {
    const url = new URL(request.url);
    const pathSegments = url.pathname.split('/').filter(Boolean);

    // Destructure first, then check for undefined: a `length < 2` test tells
    // the compiler nothing under noUncheckedIndexedAccess, which is why these
    // reads used to need non-null assertions. Same behaviour, provable.
    const [pluralType, aggregateId] = pathSegments;
    if (pluralType === undefined || aggregateId === undefined) {
      return new Response(
        JSON.stringify({
          success: false,
          error: 'InvalidPathFormat',
          message: 'Path must include aggregate type and ID (e.g., /users/:id)',
        }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }

    const aggregateType = this.pluralToSingular(pluralType);

    // Forward request as-is to the Durable Object, injecting auth when present.
    const forwardRequest = authContext
      ? this.createAuthenticatedRequest(request, authContext)
      : request;

    return await this.forwardToDurableObject(
      env,
      aggregateType,
      aggregateId,
      forwardRequest,
      pluralType
    );
  }

  /**
   * Convert plural aggregate type to singular.
   *
   * Resolution order (case-insensitive): configured override → strip a
   * trailing 's' (`users` → `user`, `locks` → `lock`) → the lowercased
   * segment unchanged.
   */
  private static pluralToSingular(plural: string): string {
    const lower = plural.toLowerCase();
    const override = this.config.pluralToSingular?.[lower];
    if (override) return override;
    if (lower.endsWith('s') && lower.length > 1) return lower.slice(0, -1);
    return lower;
  }

  /**
   * Inject auth context fields into request headers
   */
  private static injectAuthHeaders(headers: Headers, authContext: AuthContext): void {
    if (authContext.orgId) headers.set('X-Org-Id', authContext.orgId);
    if (authContext.userId) headers.set('X-User-Id', authContext.userId);
    if (authContext.email) headers.set('X-User-Email', authContext.email);
    if (authContext.isAdmin) headers.set('X-Is-Admin', 'true');
    if (authContext.isSuper) headers.set('X-Super-Access', 'true');
  }

  /**
   * Create authenticated request with auth context in headers
   */
  private static createAuthenticatedRequest(
    request: Request,
    authContext: AuthContext
  ): Request {
    const headers = new Headers(request.headers);
    this.injectAuthHeaders(headers, authContext);

    // Clone request with new headers
    return new Request(request.url, {
      method: request.method,
      headers,
      body: request.body,
      // @ts-expect-error - duplex is required for streaming bodies but not in RequestInit type
      duplex: 'half',
    });
  }

  /**
   * Forward request to Durable Object namespace
   */
  private static async forwardToDurableObject(
    env: Record<string, unknown>,
    aggregateType: string,
    aggregateId: string,
    request: Request,
    originalSegment: string
  ): Promise<Response> {
    // Closed allowlist: only types registered via configure() are routable —
    // an explicit bindingNames entry, or an allowedTypes entry resolved to
    // the uppercased singular by convention (user → env.USER). Anything else
    // is a 400, so unregistered DO bindings in the env stay unreachable.
    const explicitBinding = this.config.bindingNames?.[aggregateType];
    const bindingName =
      explicitBinding ??
      (this.config.allowedTypes?.includes(aggregateType)
        ? aggregateType.toUpperCase()
        : undefined);
    const namespace = bindingName
      ? (env[bindingName] as DurableObjectNamespace | undefined)
      : undefined;
    if (!namespace || typeof namespace.idFromName !== 'function') {
      return new Response(
        JSON.stringify({
          success: false,
          error: 'InvalidAggregateType',
          message: `Unknown aggregate type: ${originalSegment}`,
        }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Get DO stub and forward request
    const id = namespace.idFromName(aggregateId);
    const stub = namespace.get(id);
    return await stub.fetch(request);
  }
}
