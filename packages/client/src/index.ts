/**
 * The Prime Agent daemon client for Node 22 / Electron 44 main processes.
 *
 * The TypeScript surface is a thin wrapper over the native addon; command
 * and response types come ONLY from the generated client contract
 * package (`file:../client-contract`) — no hand-written wire mirror.
 */
import { decodeError, NotSentError, OverflowError, RejectedError, TransportError, UncertainError } from './errors.js';
import { loadAddon } from './loader.js';
// Generated wire types ONLY (the client contract package, PR3a): no
// hand-maintained TS wire mirror lives in this wrapper. Type-only import
// of the contract package (file: dependency; declarations from its own
// build), erased at runtime.
import type { DaemonCommand, DaemonResponse } from 'prime-agent-client-contract';

interface ConnectOptions {
  /** Bounded frame-queue capacity in frames (default 1024). */
  frameBuffer?: number;
  /** Inbound frame byte cap (opt-in transport cap). */
  maxFrameBytes?: number;
  /** Pending-request cap (opt-in transport cap). */
  maxPending?: number;
}

export interface StatusValue {
  type: 'open' | 'overflow' | 'closed';
  reason?: string;
}

export interface Caps {
  frameBuffer: number;
  maxFrameBytes?: number;
  maxPending?: number;
}

/** One explicit reconnect window: omitted windowMs is unbounded. */
export interface ReconnectWindow {
  windowMs?: number;
  initialDelayMs: number;
  maxDelayMs: number;
}

/**
 * An explicit reconnect policy. A null/omitted update/shutdown slot gives
 * that loss kind no special window — the core falls back to the lost
 * policy. Omit `reconnect` entirely for the Rust desktop policy.
 */
export interface ReconnectPolicy {
  update?: ReconnectWindow | null;
  shutdown?: ReconnectWindow | null;
  lost: ReconnectWindow;
}

export interface ManagedSessionOptions {
  socketPath: string;
  /** Bounded event-queue capacity in events (default 1024). */
  eventBuffer?: number;
  /** Omitted = the Rust desktop reconnect policy. */
  reconnect?: ReconnectPolicy;
  /** Default ["attach_snapshot", "event_sequence"]. */
  attachCapabilities?: string[];
  /** Registry dedupe of replayed session events (default true). */
  dedupeSessionEvents?: boolean;
  /** One restore-attach attempt's budget. REQUIRED, nonzero. */
  restoreAttemptTimeoutMs: number;
  /** Inbound frame byte cap (default 64 MiB). */
  maxFrameBytes?: number;
  /** Pending-request cap (default 128). */
  maxPending?: number;
  /** Client identity on every command envelope (default: core's daemon-tui:{pid}). */
  clientId?: string;
}

/** The managed session's synchronous status snapshot. */
export interface ManagedStatus {
  state: 'connecting' | 'connected' | 'disconnected' | 'expired';
  socketPath: string;
  appVersion?: string;
  capabilities: string[];
  error?: string;
  /** The structural io::ErrorKind label of the LAST connect failure ("not_found", "connection_refused", ...); absent on every other status. */
  errorCode?: string;
}

/** The serialized event stream (the shared Rust lifecycle's ConnectionEvent). */
export type ManagedEvent =
  | { type: 'status'; status: ManagedStatus }
  | { type: 'frame'; frame: unknown }
  | { type: 'reattachBegin'; activeSessionId: string }
  | { type: 'reattached'; activeSessionId: string; data: unknown }
  | { type: 'reattachFailed'; activeSessionId: string; error: string }
  | { type: 'reconnectExpired'; kind: 'update' | 'shutdown' | 'lost' };

export interface ManagedCaps {
  eventBuffer: number;
  maxFrameBytes?: number;
  maxPending?: number;
  restoreAttemptTimeoutMs: number;
  dedupeSessionEvents: boolean;
  attachCapabilities: string[];
  clientId?: string;
  desktopReconnectPolicy: boolean;
}

interface NativeClient {
  nextFrame(): Promise<unknown>;
  nextStatus(): Promise<StatusValue>;
  currentStatus(): StatusValue;
  request(command: DaemonCommand, timeoutMs?: number): Promise<DaemonResponse>;
  requestOk(command: DaemonCommand): Promise<unknown>;
  upgradeDirect(activeSessionId: string): Promise<boolean>;
  dropDirect(): void;
  directSessionId(): string | null;
  hello(): unknown;
  protocol(): unknown;
  supportsServerCapability(capability: string): boolean;
  socketPath(): string;
  caps(): Caps;
  close(): Promise<void>;
}

interface NativeManagedSession {
  status(): ManagedStatus;
  caps(): ManagedCaps;
  nextEvent(): Promise<ManagedEvent | null>;
  request(command: DaemonCommand): Promise<DaemonResponse>;
  requestOk(command: DaemonCommand): Promise<unknown>;
  registerAttachment(id: string, capabilities?: string[]): void;
  forgetAttachment(id: string): void;
  stop(): Promise<void>;
}

interface NativeAddon {
  connect(socketPath: string, options?: ConnectOptions): Promise<NativeClient>;
  startSession(options: ManagedSessionOptions): Promise<NativeManagedSession>;
  clientMetadata(): unknown;
}

/** The verified native addon module. */
export function nativeAddon(): NativeAddon {
  return loadAddon() as unknown as NativeAddon;
}

/** The addon's derived build metadata (schema id, harness commit, triple). */
export function clientMetadata(): Record<string, unknown> {
  return nativeAddon().clientMetadata() as Record<string, unknown>;
}

async function decode<T>(promise: Promise<T>): Promise<T> {
  try {
    return await promise;
  } catch (error) {
    throw decodeError(error);
  }
}

export class DaemonConnection {
  private constructor(private readonly native: NativeClient) {}

  /** Connect and complete the hello handshake. */
  static async connect(socketPath: string, options?: ConnectOptions): Promise<DaemonConnection> {
    const native = await decode(nativeAddon().connect(socketPath, options));
    return new DaemonConnection(native);
  }

  /** Pull the next raw application frame; null at end-of-stream. */
  nextFrame(): Promise<unknown> {
    return decode(this.native.nextFrame());
  }

  /** open initially, then changes only. */
  nextStatus(): Promise<StatusValue> {
    return decode(this.native.nextStatus());
  }

  currentStatus(): StatusValue {
    return this.native.currentStatus();
  }

  request(command: DaemonCommand, timeoutMs?: number): Promise<DaemonResponse> {
    return decode(this.native.request(command, timeoutMs));
  }

  requestOk(command: DaemonCommand): Promise<unknown> {
    return decode(this.native.requestOk(command));
  }

  upgradeDirect(activeSessionId: string): Promise<boolean> {
    return decode(this.native.upgradeDirect(activeSessionId));
  }

  dropDirect(): void {
    this.native.dropDirect();
  }

  directSessionId(): string | null {
    return this.native.directSessionId();
  }

  hello(): unknown {
    return this.native.hello();
  }

  protocol(): unknown {
    return this.native.protocol();
  }

  supportsServerCapability(capability: string): boolean {
    return this.native.supportsServerCapability(capability);
  }

  socketPath(): string {
    return this.native.socketPath();
  }

  /** The effective configuration this connection was built with. */
  caps(): Caps {
    return this.native.caps();
  }

  close(): Promise<void> {
    return decode(this.native.close());
  }
}

/**
 * The managed daemon session: reconnect, restore-attach, and dedupe are
 * owned by the shared Rust lifecycle; this wrapper only decodes errors.
 * Pull events in a single-consumer loop, send requests, and stop
 * explicitly (the finalizer schedules the same stop chain on GC).
 */
export class ManagedDaemonSession {
  private constructor(private readonly native: NativeManagedSession) {}

  /** Start the managed lifecycle (connect/reconnect owned by Rust). */
  static async start(options: ManagedSessionOptions): Promise<ManagedDaemonSession> {
    const native = await decode(nativeAddon().startSession(options));
    return new ManagedDaemonSession(native);
  }

  /** The core's synchronous status snapshot. */
  status(): ManagedStatus {
    return this.native.status();
  }

  /** The effective configuration this session was built with. */
  caps(): ManagedCaps {
    return this.native.caps();
  }

  /**
   * Pull the next serialized event; null at end of stream. Fail-closed:
   * a full event queue tears the session down and every pull rejects with
   * the sticky OverflowError, which precedes any buffered event.
   */
  nextEvent(): Promise<ManagedEvent | null> {
    return decode(this.native.nextEvent());
  }

  request(command: DaemonCommand): Promise<DaemonResponse> {
    return decode(this.native.request(command));
  }

  requestOk(command: DaemonCommand): Promise<unknown> {
    return decode(this.native.requestOk(command));
  }

  /** Register an attachment (idempotent per id) for restore-attach and dedupe. */
  registerAttachment(id: string, capabilities?: string[]): void {
    this.native.registerAttachment(id, capabilities);
  }

  /** Drop an attachment and every marker it owns. */
  forgetAttachment(id: string): void {
    this.native.forgetAttachment(id);
  }

  /**
   * The full stop, awaited on real completion (idempotent; the driver's
   * honest failure rejects). The event stream ends at the mark: drain to
   * your quiescent point BEFORE stopping.
   */
  stop(): Promise<void> {
    return decode(this.native.stop());
  }
}

export { NotSentError, OverflowError, RejectedError, TransportError, UncertainError };
export type { TeardownCause } from './errors.js';
