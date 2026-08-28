/**
 * Structured Logger for Cloudflare Workers
 *
 * Provides structured logging with optional Axiom and Sentry integration.
 * Works in both Vite dev server and production Workers environment.
 *
 * Note: Pino was removed due to sonic-boom dependency issues with Vite.
 * sonic-boom uses node:util.inherits which doesn't work in Workers.
 */

/**
 * Logger environment bindings.
 *
 * Structural subset of a worker's env that the logger reads for enrichment
 * and transport selection. All fields are optional — a bare
 * `createLogger({ component })` with no env works everywhere.
 */
export interface LoggerEnv {
  /** Service name stamped onto every log entry */
  SERVICE_NAME?: string;
  /** Deployment environment (dev/stage/prod/...) stamped onto every entry */
  ENVIRONMENT?: string;
  /** 'console' (default; rely on platform log shipping) or 'sdk' (HTTP-push to Axiom) */
  LOG_TRANSPORT?: string;
  /** Axiom ingest token, used only when LOG_TRANSPORT === 'sdk' */
  AXIOM_TOKEN_LOGS?: string;
  /**
   * Axiom dataset name. Required for the 'sdk' transport — when unset, the
   * HTTP push is skipped entirely (console output still happens).
   */
  AXIOM_DATASET?: string;
  /**
   * Set to suppress Sentry breadcrumbs. Accepts boolean `true` or the string
   * 'true' (raw platform env vars arrive as strings).
   */
  sentryDisable?: boolean | string;
  /** Sentry DSN (breadcrumbs attach via a global `Sentry` object when present) */
  SENTRY_DSN?: string;
  [key: string]: unknown;
}

/**
 * Logger context for structured logging
 */
export interface LoggerContext {
  /** Component or module name */
  component?: string;
  /** Request ID for tracing */
  requestId?: string;
  /** User ID for user-scoped logs */
  userId?: string;
  /** Organization ID for org-scoped logs */
  organizationId?: string;
  /** Aggregate ID */
  aggregateId?: string;
  /** Custom context fields */
  [key: string]: unknown;
}

type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';

/**
 * Global default environment for loggers created without explicit env.
 * Set once at Worker init (e.g., by env validation middleware) so that
 * module-level loggers created before request context is available
 * still get SERVICE_NAME and ENVIRONMENT enrichment.
 */
let globalLoggerEnv: LoggerEnv | undefined;

/**
 * Set the global default environment for all loggers.
 * Called automatically by env validation middleware on first request.
 */
export function setGlobalLoggerEnv(env: LoggerEnv): void {
  globalLoggerEnv = env;
}

/**
 * Structured logger with Axiom and Sentry integration
 *
 * Uses console internally but provides structured JSON output.
 * Integrates with Axiom for log aggregation and Sentry for error tracking.
 */
export class WorkerkitLogger {
  private context: LoggerContext;
  private env?: LoggerEnv;

  constructor(context: LoggerContext = {}, env?: LoggerEnv) {
    // Automatically add service and env fields if available in environment
    const anyEnv = env as Record<string, unknown> | undefined;
    const enrichedContext: LoggerContext = { ...context };

    if (anyEnv?.SERVICE_NAME) {
      enrichedContext.service = anyEnv.SERVICE_NAME as string;
    }
    if (anyEnv?.ENVIRONMENT) {
      enrichedContext.env = anyEnv.ENVIRONMENT as string;
    }

    this.context = enrichedContext;
    this.env = env;
  }

  /**
   * Create a child logger with additional context
   */
  child(additionalContext: LoggerContext): WorkerkitLogger {
    return new WorkerkitLogger(
      { ...this.context, ...additionalContext },
      this.env
    );
  }

  /**
   * Format log entry as structured JSON
   */
  private formatLog(level: LogLevel, message: string, data?: Record<string, unknown>): string {
    // Dynamically enrich with global env if not already in context
    const resolvedEnv = this.env ?? globalLoggerEnv;
    const envFields: Record<string, unknown> = {};
    if (!this.context.service && resolvedEnv) {
      const anyEnv = resolvedEnv as Record<string, unknown>;
      if (anyEnv.SERVICE_NAME) envFields.service = anyEnv.SERVICE_NAME;
      if (anyEnv.ENVIRONMENT) envFields.env = anyEnv.ENVIRONMENT;
    }

    const entry = {
      level,
      time: new Date().toISOString(),
      msg: message,
      ...envFields,
      ...this.context,
      ...data,
    };
    return JSON.stringify(entry);
  }

  /**
   * Log info message
   */
  info(message: string, data?: Record<string, unknown>): void {
    const logData = { ...this.context, ...data };
    // eslint-disable-next-line no-restricted-syntax
    console.log(this.formatLog('info', message, data));
    this.sendToAxiom('info', message, logData);
    this.sendToSentry('info', message, logData);
  }

  /**
   * Log warning message
   */
  warn(message: string, data?: Record<string, unknown>): void {
    const logData = { ...this.context, ...data };
    // eslint-disable-next-line no-restricted-syntax
    console.warn(this.formatLog('warn', message, data));
    this.sendToAxiom('warn', message, logData);
    this.sendToSentry('warning', message, logData);
  }

  /**
   * Log error message
   */
  error(message: string, error?: unknown, data?: Record<string, unknown>): void {
    const logData = {
      ...this.context,
      ...data,
      error: error instanceof Error ? {
        name: error.name,
        message: error.message,
        stack: error.stack,
      } : error,
    };
    // eslint-disable-next-line no-restricted-syntax
    console.error(this.formatLog('error', message, logData));
    this.sendToAxiom('error', message, logData);
    this.sendToSentry('error', message, logData);
  }

  /**
   * Log debug message
   */
  debug(message: string, data?: Record<string, unknown>): void {
    const logData = { ...this.context, ...data };
    // eslint-disable-next-line no-restricted-syntax
    console.debug(this.formatLog('debug', message, data));
    this.sendToAxiom('debug', message, logData);
  }

  /**
   * Log fatal message
   */
  fatal(message: string, error?: unknown, data?: Record<string, unknown>): void {
    const logData = {
      ...this.context,
      ...data,
      error: error instanceof Error ? {
        name: error.name,
        message: error.message,
        stack: error.stack,
      } : error,
    };
    // eslint-disable-next-line no-restricted-syntax
    console.error(this.formatLog('fatal', message, logData));
    this.sendToAxiom('fatal', message, logData);
    this.sendToSentry('fatal', message, logData);
  }

  /**
   * Log trace message
   */
  trace(message: string, data?: Record<string, unknown>): void {
    const logData = { ...this.context, ...data };
    // eslint-disable-next-line no-restricted-syntax
    console.debug(this.formatLog('trace', message, data));
    this.sendToAxiom('trace', message, logData);
  }

  /**
   * Send log to Axiom via HTTP SDK (only when LOG_TRANSPORT='sdk')
   *
   * When LOG_TRANSPORT='console' (default), this is a no-op because logs are
   * already output to console (via formatLog) and shipped via Logpush.
   */
  private sendToAxiom(
    level: string,
    message: string,
    data: Record<string, unknown>
  ): void {
    if (!this.env) {
      return;
    }

    // Only send via HTTP when using SDK transport
    const anyEnv = this.env as Record<string, unknown>;
    if (anyEnv.LOG_TRANSPORT !== 'sdk') {
      return;
    }

    // SDK transport: send directly to Axiom via HTTP. Without a dataset
    // there is nowhere to send — skip rather than POSTing to
    // /datasets/undefined/ingest (console output already carries the line).
    const dataset = this.env.AXIOM_DATASET;
    if (!dataset) {
      return;
    }
    const axiomUrl = `https://api.axiom.co/v1/datasets/${dataset}/ingest`;

    // Fire and forget - don't await, catch handles rejections
    fetch(axiomUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${this.env.AXIOM_TOKEN_LOGS}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify([
        {
          _time: new Date().toISOString(),
          level,
          message,
          ...data,
        },
      ]),
    }).catch(() => {
      // Fail silently - don't break logging if Axiom is down
    });
  }

  /**
   * Send breadcrumb to Sentry (if configured and enabled)
   */
  private sendToSentry(
    level: 'debug' | 'info' | 'warning' | 'error' | 'fatal',
    message: string,
    data: Record<string, unknown>
  ): void {
    // Raw platform env vars are strings, so accept both the boolean and the
    // string form of the flag.
    if (!this.env || this.env.sentryDisable === true || this.env.sentryDisable === 'true') {
      return;
    }

    if (typeof globalThis !== 'undefined' && 'Sentry' in globalThis) {
      const Sentry = (globalThis as Record<string, unknown>).Sentry as {
        addBreadcrumb?: (breadcrumb: {
          type: string;
          level: string;
          message: string;
          data: Record<string, unknown>;
        }) => void;
      };
      if (Sentry?.addBreadcrumb) {
        Sentry.addBreadcrumb({
          type: 'default',
          level: level === 'fatal' ? 'error' : level,
          message,
          data,
        });
      }
    }
  }
}

/**
 * Create a logger instance
 *
 * @example
 * ```typescript
 * // Simple usage
 * const logger = createLogger({ component: 'UserService' });
 * logger.info('User created', { userId: '123' });
 *
 * // With environment for Axiom/Sentry integration
 * const logger = createLogger({ requestId: 'req-123' }, c.env);
 * logger.error('Failed to process', error, { userId: '123' });
 * ```
 */
export function createLogger(context?: LoggerContext, env?: LoggerEnv): WorkerkitLogger {
  return new WorkerkitLogger(context, env ?? globalLoggerEnv);
}
