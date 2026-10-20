/* eslint-disable */
/**
 * Generated from daemon-outbound.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with `npm run gen:ts` and commit.
 */
/**
 * Supervisor -> client frames, tagged by `type`.
 */
export type DaemonOutbound =
  | {
      type: "response";
      id?: string | null;
      command: string;
      success: boolean;
      data?: unknown;
      error?: string | null;
      errorInfo?:
        | null
        | DaemonErrorInfo
        | {
            code: string;
            [k: string]: unknown;
          };
      [k: string]: unknown;
    }
  | {
      type: "session_list_progress";
      id?: string | null;
      command: string;
      activeSessionId?: string | null;
      loaded: number;
      total: number;
      [k: string]: unknown;
    }
  | {
      type: "session_list_item";
      id?: string | null;
      command: string;
      activeSessionId?: string | null;
      session: DaemonSavedSessionInfo;
      [k: string]: unknown;
    }
  | {
      type: "daemon_hello";
      socketPath: string;
      protocol: DaemonProtocolInfo;
      schemaId?: string | null;
      schemaRevision?: number | null;
      appVersion?: string | null;
      runtime?: DaemonRuntimeIdentity | null;
      supervisorGeneration?: string | null;
      supervisorPid?: number | null;
      supervisorOwnerToken?: string | null;
      supervisorProcessStartId?: string | null;
      supervisorSocketPath?: string | null;
      /**
       * The update resume contract (spec §10.3): whether the restore pass has finished, so a
       * reconnecting client knows whether to queue its attach. Rust-only extension.
       */
      updateResume?: DaemonUpdateResume | null;
      clientId: string;
      serverCapabilities: string[];
      [k: string]: unknown;
    }
  | {
      type: "daemon_closing";
      reason: DaemonClosingReason;
      [k: string]: unknown;
    }
  | {
      type: "heartbeats_changed";
      [k: string]: unknown;
    }
  | {
      type: "model_catalog_changed";
      [k: string]: unknown;
    }
  | {
      type: "roster_update";
      changed: unknown;
      removed?: string[] | null;
      resync?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "session_event";
      activeSessionId: string;
      event: unknown;
      meta?: DaemonEventMeta | null;
      [k: string]: unknown;
    }
  | {
      type: "side_question_event";
      activeSessionId: string;
      event: unknown;
      [k: string]: unknown;
    }
  | {
      type: "session_replaced";
      activeSessionId: string;
      state: unknown;
      messages: AgentMessage[];
      snapshotFollows?: boolean | null;
      meta?: DaemonEventMeta | null;
      [k: string]: unknown;
    }
  | {
      type: "session_resynced";
      activeSessionId: string;
      snapshot: DaemonSessionSnapshot;
      meta?: DaemonEventMeta | null;
      [k: string]: unknown;
    }
  | {
      type: "session_attached";
      activeSessionId: string;
      state: unknown;
      messages: AgentMessage[];
      snapshot?: DaemonSessionSnapshot | null;
      replay?: DaemonReplayInfo | null;
      lastEventSequence?: number | null;
      [k: string]: unknown;
    }
  | {
      type: "session_snapshot_begin";
      activeSessionId: string;
      snapshotId: string;
      messageCount: number;
      targetChunkBytes: number;
      purpose?: SnapshotPurpose | null;
      [k: string]: unknown;
    }
  | {
      type: "session_snapshot_chunk";
      activeSessionId: string;
      snapshotId: string;
      index: number;
      messages: AgentMessage[];
      [k: string]: unknown;
    }
  | {
      type: "session_snapshot_end";
      activeSessionId: string;
      snapshotId: string;
      chunkCount: number;
      lastEventSequence: number;
      lastEventCursor?: DaemonEventCursor | null;
      [k: string]: unknown;
    }
  | {
      type: "session_snapshot_failed";
      activeSessionId: string;
      snapshotId: string;
      error: string;
      [k: string]: unknown;
    }
  | {
      type: "session_detached";
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "session_binding";
      previousActiveSessionId: string;
      activeSessionId: string;
      sessionId?: string | null;
      sessionFile?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "session_closed";
      activeSessionId: string;
      reason: DaemonSessionClosedReason;
      meta?: DaemonEventMeta | null;
      [k: string]: unknown;
    };
/**
 * Structured failure info carried on error responses, tagged by `code`.
 */
export type DaemonErrorInfo =
  | {
      code: "missing_session_cwd";
      issue: unknown;
      [k: string]: unknown;
    }
  | {
      code: "session_import_file_not_found";
      filePath: string;
      [k: string]: unknown;
    }
  | {
      code: "session_already_active";
      sessionPath: string;
      activeSessionId?: string | null;
      [k: string]: unknown;
    }
  | {
      code: "session_recovering";
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      code: "update_restarting";
      [k: string]: unknown;
    }
  | {
      code: "command_result_uncertain";
      clientId: string;
      commandId: string;
      [k: string]: unknown;
    }
  | {
      code: "update_prepare_refused";
      activeUpdateId: string;
      [k: string]: unknown;
    }
  | {
      code: "model_provider_unauthenticated";
      provider: string;
      [k: string]: unknown;
    }
  | {
      code: "worker_overloaded";
      [k: string]: unknown;
    }
  | {
      code: "unknown";
      [k: string]: unknown;
    };
/**
 * Opaque update identifier (a `UUIDv7` in practice), typed so status records, artifact paths, and
 * prepare transactions cannot mix it up with session ids.
 */
export type UpdateId = string;
export type DaemonClosingReason = "shutdown" | "update";
/**
 * Conversation message stored in `message` entries: the provider-facing
 * messages from [`crate::ai`] plus the coding-agent-only roles.
 */
export type AgentMessage =
  | UserMessage
  | AssistantMessage
  | ToolResultMessage
  | BashExecutionMessage
  | CustomMessage
  | BranchSummaryMessage
  | CompactionSummaryMessage;
export type DaemonReplayStatus = "complete" | "partial" | "unavailable";
/**
 * Purpose tag of a `session_snapshot_begin` record; the catch-up value
 * is `resync` on the wire, never `catchup`.
 */
export type SnapshotPurpose = "attach" | "replacement" | "resync";
export type DaemonSessionClosedReason = "killed" | "shutdown" | "completed" | "replaced" | "update";

/**
 * Saved-session row pushed by `session_list_item` progress events.
 */
export interface DaemonSavedSessionInfo {
  path: string;
  id: string;
  cwd: string;
  name?: string | null;
  state?: unknown;
  parentSessionPath?: string | null;
  rlmDepth?: number | null;
  created: string;
  modified: string;
  messageCount: number;
  firstMessage: string;
  allMessagesText: string;
  usage?: unknown;
  model?: unknown;
  [k: string]: unknown;
}
export interface DaemonProtocolInfo {
  name: string;
  version: number;
  [k: string]: unknown;
}
/**
 * Process identity of the daemon build, published in `daemon_hello`.
 */
export interface DaemonRuntimeIdentity {
  buildId: string;
  executablePath: string;
  entrypointPath?: string | null;
  launcherPath?: string | null;
  [k: string]: unknown;
}
/**
 * The successor supervisor's hello resume contract (spec §10.3): whether
 * the restore pass has finished. `update_id` is `None` on a normal boot.
 * Rust-only extension (the TS close frame carries no resume contract).
 */
export interface DaemonUpdateResume {
  updateId?: UpdateId | null;
  complete: boolean;
  [k: string]: unknown;
}
export interface DaemonEventMeta {
  id: string;
  protocol: DaemonProtocolInfo;
  activeSessionId?: string | null;
  sequence?: number | null;
  cursor?: DaemonEventCursor | null;
  emittedAt: string;
  replayed?: boolean | null;
  [k: string]: unknown;
}
export interface DaemonEventCursor {
  generation: string;
  sequence: number;
  [k: string]: unknown;
}
export interface UserMessage {
  role: "user";
  [k: string]: unknown;
}
export interface AssistantMessage {
  role: "assistant";
  [k: string]: unknown;
}
export interface ToolResultMessage {
  role: "toolResult";
  [k: string]: unknown;
}
/**
 * `role: "bashExecution"`: a `!`-command execution, rendered as a user turn.
 */
export interface BashExecutionMessage {
  role: "bashExecution";
  [k: string]: unknown;
}
/**
 * `role: "custom"`: bookkeeping message with a `customType` tag.
 */
export interface CustomMessage {
  role: "custom";
  [k: string]: unknown;
}
/**
 * `role: "branchSummary"`: summary of a branch the conversation returned from.
 */
export interface BranchSummaryMessage {
  role: "branchSummary";
  [k: string]: unknown;
}
/**
 * `role: "compactionSummary"`: compaction result presented as a user turn.
 */
export interface CompactionSummaryMessage {
  role: "compactionSummary";
  [k: string]: unknown;
}
/**
 * Full session snapshot (attach, replacement, resync).
 */
export interface DaemonSessionSnapshot {
  activeSessionId: string;
  summary: unknown;
  state: unknown;
  messages: AgentMessage[];
  sessionContext?: unknown;
  sessionTree?: unknown;
  lastEventSequence: number;
  lastEventCursor?: DaemonEventCursor | null;
  parent?: unknown;
  children?: unknown;
  [k: string]: unknown;
}
export interface DaemonReplayInfo {
  status: DaemonReplayStatus;
  fromSequence?: number | null;
  toSequence: number;
  fromCursor?: DaemonEventCursor | null;
  toCursor?: DaemonEventCursor | null;
  reason?: string | null;
  [k: string]: unknown;
}

