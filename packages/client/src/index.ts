/**
 * The Prime Agent daemon client for Node 22 / Electron 44 main processes.
 *
 * The TypeScript surface is a thin wrapper over the native addon: commands
 * and responses are typed `unknown` until the generated client contract
 * package lands (PR3a) — this wrapper deliberately keeps NO hand-written
 * wire mirror.
 */
import { decodeError, NotSentError, OverflowError, RejectedError, TransportError, UncertainError } from './errors.js';
import { loadAddon } from './loader.js';

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

interface NativeClient {
  nextFrame(): Promise<unknown>;
  nextStatus(): Promise<StatusValue>;
  currentStatus(): StatusValue;
  request(command: unknown, timeoutMs?: number): Promise<unknown>;
  requestOk(command: unknown): Promise<unknown>;
  upgradeDirect(activeSessionId: string): Promise<boolean>;
  dropDirect(): void;
  directSessionId(): string | null;
  hello(): unknown;
  protocol(): unknown;
  supportsServerCapability(capability: string): boolean;
  socketPath(): string;
  caps(): Caps;
  close(): void;
}

interface NativeAddon {
  connect(socketPath: string, options?: ConnectOptions): Promise<NativeClient>;
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

  request(command: unknown, timeoutMs?: number): Promise<unknown> {
    return decode(this.native.request(command, timeoutMs));
  }

  requestOk(command: unknown): Promise<unknown> {
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

  close(): void {
    this.native.close();
  }
}

export { NotSentError, OverflowError, RejectedError, TransportError, UncertainError };
export type { TeardownCause } from './errors.js';
