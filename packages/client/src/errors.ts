/**
 * Typed client errors decoded STRUCTURALLY from the binding's payload —
 * never by message-string matching. The classes mirror `pa-client`'s
 * transport classes one-to-one; no auto-resend anywhere.
 */

export type TeardownCause = 'overflow' | 'closed';

interface ErrorPayload {
  kind: 'NotSent' | 'Uncertain' | 'Rejected' | 'Transport' | 'Overflow';
  message: string;
  cause?: TeardownCause;
  info?: unknown;
}

interface ClientErrorShape {
  readonly kind: string;
  readonly message: string;
  readonly cause: TeardownCause | undefined;
  readonly errorInfo: unknown | undefined;
}

class ClientError extends Error implements ClientErrorShape {
  readonly kind: string;
  readonly cause: TeardownCause | undefined;
  readonly errorInfo: unknown | undefined;
  constructor(kind: string, message: string, cause?: TeardownCause, errorInfo?: unknown) {
    super(message);
    this.name = kind;
    this.kind = kind;
    this.cause = cause;
    this.errorInfo = errorInfo;
  }
}

/** Provable non-delivery: the command never reached the wire. */
export class NotSentError extends ClientError {
  constructor(message: string, cause?: TeardownCause) {
    super('NotSent', message, cause);
  }
  static is(error: unknown): error is NotSentError {
    return error instanceof ClientError && error.kind === 'NotSent';
  }
}

/** The command may have executed; no answer arrived. Never resend. */
export class UncertainError extends ClientError {
  constructor(message: string, cause?: TeardownCause) {
    super('Uncertain', message, cause);
  }
  static is(error: unknown): error is UncertainError {
    return error instanceof ClientError && error.kind === 'Uncertain';
  }
}

/** The daemon answered a refusal; `errorInfo` preserved. */
export class RejectedError extends ClientError {
  constructor(message: string, errorInfo?: unknown) {
    super('Rejected', message, undefined, errorInfo);
  }
  static is(error: unknown): error is RejectedError {
    return error instanceof ClientError && error.kind === 'Rejected';
  }
}

/** The terminal frame-channel error after queue overflow. */
export class OverflowError extends ClientError {
  constructor(message: string) {
    super('Overflow', message, 'overflow');
  }
  static is(error: unknown): error is OverflowError {
    return error instanceof ClientError && error.kind === 'Overflow';
  }
}

/** Any other transport failure. */
export class TransportError extends ClientError {
  constructor(message: string) {
    super('Transport', message);
  }
  static is(error: unknown): error is TransportError {
    return error instanceof ClientError && error.kind === 'Transport';
  }
}

/**
 * Decodes the binding's structured payload into the typed error classes.
 * A payload that fails to decode becomes a TransportError carrying the
 * original text — it is never silently coerced into another class.
 */
export function decodeError(error: unknown): unknown {
  if (!(error instanceof Error)) return error;
  let payload: ErrorPayload | undefined;
  try {
    payload = JSON.parse(error.message) as ErrorPayload;
  } catch {
    return error;
  }
  if (!payload || typeof payload.kind !== 'string') return error;
  switch (payload.kind) {
    case 'NotSent':
      return new NotSentError(payload.message, payload.cause);
    case 'Uncertain':
      return new UncertainError(payload.message, payload.cause);
    case 'Rejected':
      return new RejectedError(payload.message, payload.info);
    case 'Overflow':
      return new OverflowError(payload.message);
    case 'Transport':
      return new TransportError(payload.message);
    default:
      return error;
  }
}
