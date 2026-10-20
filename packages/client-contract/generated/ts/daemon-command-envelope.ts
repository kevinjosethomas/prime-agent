/* eslint-disable */
/**
 * Generated from daemon-command-envelope.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with `npm run gen:ts` and commit.
 */
export type DaemonCommandFrameType = "command";
/**
 * Client commands, tagged by `type`. Every variant carries `id` (bare
 * commands) and a catch-all for unknown fields, so round-trips stay lossless.
 */
export type DaemonCommand =
  | {
      type: "get_global_harness_defaults";
      id?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "set_global_harness_defaults";
      id?: string | null;
      patch: GlobalHarnessDefaultsPatch;
      [k: string]: unknown;
    }
  | {
      type: "get_subagent_model_default";
      id?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "set_subagent_model_default";
      id?: string | null;
      model: string | null;
      [k: string]: unknown;
    }
  | {
      type: "list";
      id?: string | null;
      all?: boolean | null;
      cwd?: string | null;
      sessionDir?: string | null;
      includeClientOwned?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "list_saved_sessions";
      id?: string | null;
      activeSessionId?: string | null;
      cwd?: string | null;
      sessionDir?: string | null;
      scope?: {
        [k: string]: unknown;
      };
      [k: string]: unknown;
    }
  | {
      type: "list_agent_peers";
      id?: string | null;
      /**
       * The requester's worker token; optional on the wire (an absent
       * token is an auth failure, not a parse one).
       */
      workerToken?: string;
      [k: string]: unknown;
    }
  | {
      type: "get_direct_worker_transport";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_worker_peer_transport";
      id?: string | null;
      workerToken: string;
      targetActiveSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "roster_subscribe";
      id?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "worker_idle_passivation";
      id?: string | null;
      workerToken: string;
      idleMinutes?: number | null;
      [k: string]: unknown;
    }
  | {
      type: "worker_roster_delta";
      id?: string | null;
      workerToken: string;
      summary: unknown;
      removed?: string[] | null;
      /**
       * Monotonic per-worker sequence; the supervisor drops a delta older
       * than one already applied. Absent means unsequenced (always applied).
       */
      sequence?: number | null;
      /**
       * The sending worker process instance: a replacement reuses the
       * worker id but restarts its sequence, so the stale-delta gate keeps
       * one generation slot per worker and drops a predecessor's deltas.
       */
      workerInstanceId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "roster_unsubscribe";
      id?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "create";
      id?: string | null;
      sessionPath?: string | null;
      continueRecent?: boolean | null;
      noSession?: boolean | null;
      name?: string | null;
      config?: unknown;
      /**
       * Telemetry opt-out: only ever `Some(true)`; absent means enabled.
       */
      telemetryDisabled?: boolean | null;
      runtimeMetadata?: unknown;
      lifecycle?: DaemonSessionLifecycle | null;
      /**
       * Allowlisted client env vars (`env`), carried on create only.
       */
      env?: {
        [k: string]: string;
      } | null;
      launchEnv?: {
        [k: string]: string;
      } | null;
      [k: string]: unknown;
    }
  | {
      type: "attach";
      id?: string | null;
      activeSessionId: string;
      clientId?: string | null;
      capabilities?: string[] | null;
      resumeCursor?: DaemonResumeCursor | null;
      telemetryDisabled?: boolean | null;
      recoveryConfig?: unknown;
      env?: {
        [k: string]: string;
      } | null;
      launchEnv?: {
        [k: string]: string;
      } | null;
      [k: string]: unknown;
    }
  | {
      type: "reattach";
      id?: string | null;
      activeSessionId: string;
      targetActiveSessionId: string;
      clientId?: string | null;
      capabilities?: string[] | null;
      resumeCursor?: DaemonResumeCursor | null;
      telemetryDisabled?: boolean | null;
      recoveryConfig?: unknown;
      env?: {
        [k: string]: string;
      } | null;
      launchEnv?: {
        [k: string]: string;
      } | null;
      [k: string]: unknown;
    }
  | {
      type: "detach";
      id?: string | null;
      activeSessionId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "complete_owned_session";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "promote_owned_session";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "kill";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "rename";
      id?: string | null;
      activeSessionId: string;
      name: string;
      /**
       * Who directed the rename (`renamedBy` on the wire, TS
       * `AgentFamilyRelationship` — only `"parent"` is sent): set when a
       * parent session renames one of its direct children, so the
       * renamed session's transcript notice can name it.
       */
      renamedBy?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "prompt";
      id?: string | null;
      activeSessionId: string;
      message: string;
      clientInputId?: string | null;
      replyToMessageId?: string | null;
      replyContext?: FocusedThreadContextRow[] | null;
      content?: unknown;
      images?: unknown;
      streamingBehavior?: StreamingBehavior | null;
      queueIfBusy?: boolean | null;
      expandPromptTemplates?: boolean | null;
      source?: unknown;
      agentMessageId?: string | null;
      customMessage?: unknown;
      queueKey?: string | null;
      prefixMessages?: unknown;
      /**
       * Unique only when the caller needs cancellable pre-ownership admission.
       */
      admissionId?: string | null;
      /**
       * One-shot capability only the daemon's own RLM children delivery mints; never a client field:
       * a caller-supplied value can never name a live mint.
       */
      rlmNoticeNonce?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "cancel_prompt_admission";
      id?: string | null;
      activeSessionId: string;
      admissionId: string;
      /**
       * Cancel session-owned work too when it has not started delivery.
       */
      cancelOwned?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "prompt_and_wait";
      id?: string | null;
      activeSessionId: string;
      message: string;
      clientInputId?: string | null;
      replyToMessageId?: string | null;
      replyContext?: FocusedThreadContextRow[] | null;
      content?: unknown;
      images?: unknown;
      streamingBehavior?: StreamingBehavior | null;
      queueIfBusy?: boolean | null;
      expandPromptTemplates?: boolean | null;
      source?: unknown;
      agentMessageId?: string | null;
      customMessage?: unknown;
      queueKey?: string | null;
      prefixMessages?: unknown;
      /**
       * Unique only when the caller needs cancellable pre-ownership admission.
       */
      admissionId?: string | null;
      /**
       * One-shot capability only the daemon's own RLM children delivery mints; never a client field:
       * a caller-supplied value can never name a live mint.
       */
      rlmNoticeNonce?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "steer";
      id?: string | null;
      activeSessionId: string;
      message: string;
      clientInputId?: string | null;
      replyToMessageId?: string | null;
      replyContext?: FocusedThreadContextRow[] | null;
      content?: unknown;
      images?: unknown;
      streamingBehavior?: StreamingBehavior | null;
      queueIfBusy?: boolean | null;
      expandPromptTemplates?: boolean | null;
      source?: unknown;
      agentMessageId?: string | null;
      customMessage?: unknown;
      queueKey?: string | null;
      prefixMessages?: unknown;
      /**
       * Unique only when the caller needs cancellable pre-ownership admission.
       */
      admissionId?: string | null;
      /**
       * One-shot capability only the daemon's own RLM children delivery mints; never a client field:
       * a caller-supplied value can never name a live mint.
       */
      rlmNoticeNonce?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "follow_up";
      id?: string | null;
      activeSessionId: string;
      message: string;
      clientInputId?: string | null;
      replyToMessageId?: string | null;
      replyContext?: FocusedThreadContextRow[] | null;
      content?: unknown;
      images?: unknown;
      streamingBehavior?: StreamingBehavior | null;
      queueIfBusy?: boolean | null;
      expandPromptTemplates?: boolean | null;
      source?: unknown;
      agentMessageId?: string | null;
      customMessage?: unknown;
      queueKey?: string | null;
      prefixMessages?: unknown;
      /**
       * Unique only when the caller needs cancellable pre-ownership admission.
       */
      admissionId?: string | null;
      /**
       * One-shot capability only the daemon's own RLM children delivery mints; never a client field:
       * a caller-supplied value can never name a live mint.
       */
      rlmNoticeNonce?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "restore_next_turn";
      id?: string | null;
      activeSessionId: string;
      messages: AgentMessage[];
      [k: string]: unknown;
    }
  | {
      type: "restore_actions";
      id?: string | null;
      activeSessionId: string;
      snapshot: unknown;
      [k: string]: unknown;
    }
  | {
      type: "append_custom_message";
      id?: string | null;
      activeSessionId: string;
      message: unknown;
      [k: string]: unknown;
    }
  | {
      type: "resume_queue";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "send_message";
      id?: string | null;
      targetActiveSessionId: string;
      message: string;
      fromActiveSessionId?: string | null;
      agentOrigin?: boolean | null;
      deliveryMode?: unknown;
      [k: string]: unknown;
    }
  | {
      type: "agent_messages_status";
      id?: string | null;
      activeSessionId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "agent_messages_pause";
      id?: string | null;
      activeSessionId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "agent_messages_resume";
      id?: string | null;
      activeSessionId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "agent_messages_clear";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "abort";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "abort_and_send_queued";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "start_side_question";
      id?: string | null;
      activeSessionId: string;
      sideQuestionId?: string;
      question?: string;
      previousTurns?: unknown;
      [k: string]: unknown;
    }
  | {
      type: "abort_side_question";
      id?: string | null;
      activeSessionId: string;
      sideQuestionId?: string;
      [k: string]: unknown;
    }
  | {
      type: "execute_bash";
      id?: string | null;
      activeSessionId: string;
      command: string;
      excludeFromContext?: boolean | null;
      transient?: boolean | null;
      runId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "abort_bash";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "list_kernel_bash";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "tail_kernel_bash";
      id?: string | null;
      activeSessionId: string;
      activityId: string;
      lines?: number | null;
      [k: string]: unknown;
    }
  | {
      type: "kill_kernel_bash";
      id?: string | null;
      activeSessionId: string;
      activityId: string;
      [k: string]: unknown;
    }
  | {
      type: "factory_activity";
      id?: string | null;
      activeSessionId: string;
      action: string;
      runId?: string | null;
      specId?: string | null;
      timeoutMs?: number | null;
      [k: string]: unknown;
    }
  | {
      type: "cancel_rlm_child";
      id?: string | null;
      activeSessionId: string;
      childId: string;
      [k: string]: unknown;
    }
  | {
      type: "delete_rlm_subagent";
      id?: string | null;
      activeSessionId: string;
      childId: string;
      [k: string]: unknown;
    }
  | {
      type: "wait_for_idle";
      id?: string | null;
      activeSessionId: string;
      /**
       * Also hold until every RLM child run of the session settled
       * (the `wait_for_headless_completion` barrier of the same name).
       */
      waitForRlmQuiescence?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "wait_for_headless_completion";
      id?: string | null;
      activeSessionId: string;
      waitForRlmQuiescence?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "get_session_header";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_state";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_connection_state";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_messages";
      id?: string | null;
      activeSessionId: string;
      before?: number | null;
      capabilities?: string[] | null;
      [k: string]: unknown;
    }
  | {
      type: "get_rlm_children";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_session_stats";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_context_tree";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_commands";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_resource_snapshot";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_mcp_connections";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "set_mcp_static_token";
      id?: string | null;
      activeSessionId: string;
      server: string;
      token: string;
      [k: string]: unknown;
    }
  | {
      type: "remove_mcp_connection";
      id?: string | null;
      activeSessionId: string;
      server: string;
      [k: string]: unknown;
    }
  | {
      type: "set_mcp_connection_label";
      id?: string | null;
      activeSessionId: string;
      connectionId: string;
      label: string;
      [k: string]: unknown;
    }
  | {
      type: "replace_acp_mcp_servers";
      id?: string | null;
      activeSessionId: string;
      ownerId: string;
      servers: unknown;
      [k: string]: unknown;
    }
  | {
      type: "get_model_catalog";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "classify_workstream";
      id?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "get_available_models";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_queue";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "mutate_queued_message";
      id?: string | null;
      activeSessionId: string;
      lane: unknown;
      index: number;
      expectedText: string;
      mutation: unknown;
      [k: string]: unknown;
    }
  | {
      type: "list_queued_inputs";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "mutate_queued_input";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "clear_queue";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "abort_and_clear_queue";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "acquire_session_input_pause";
      id?: string | null;
      activeSessionId: string;
      leaseKey: string;
      [k: string]: unknown;
    }
  | {
      type: "release_session_input_pause";
      id?: string | null;
      activeSessionId: string;
      pauseId: string;
      [k: string]: unknown;
    }
  | {
      type: "cron_list";
      id?: string | null;
      activeSessionId?: string | null;
      includeInactive?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "heartbeats_list";
      id?: string | null;
      activeSessionId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "heartbeat_manage";
      id?: string | null;
      activeSessionId: string;
      jobId: string;
      action: unknown;
      [k: string]: unknown;
    }
  | {
      type: "cron_add";
      id?: string | null;
      activeSessionId: string;
      schedule: string;
      prompt: string;
      promoteOwnedSession?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "cron_cancel";
      id?: string | null;
      activeSessionId?: string | null;
      jobId: string;
      [k: string]: unknown;
    }
  | {
      type: "heartbeat_get";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "heartbeat_set";
      id?: string | null;
      activeSessionId: string;
      schedule: string;
      prompt: string;
      deliveryMode?: unknown;
      promoteOwnedSession?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "heartbeat_update";
      id?: string | null;
      activeSessionId: string;
      action: unknown;
      [k: string]: unknown;
    }
  | {
      type: "heartbeat_update_job";
      id?: string | null;
      activeSessionId: string;
      jobId: string;
      instruction?: string | null;
      interval?: string | null;
      deliveryMode?: string | null;
      label?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "set_model";
      id?: string | null;
      activeSessionId: string;
      provider: string;
      modelId: string;
      [k: string]: unknown;
    }
  | {
      type: "cycle_model";
      id?: string | null;
      activeSessionId: string;
      direction?: CycleDirection | null;
      [k: string]: unknown;
    }
  | {
      type: "set_scoped_models";
      id?: string | null;
      activeSessionId: string;
      scopedModels: unknown;
      [k: string]: unknown;
    }
  | {
      type: "set_thinking_level";
      id?: string | null;
      activeSessionId: string;
      level?: string;
      [k: string]: unknown;
    }
  | {
      type: "set_service_tier";
      id?: string | null;
      activeSessionId: string;
      serviceTier?: ServiceTier | null;
      [k: string]: unknown;
    }
  | {
      type: "cycle_thinking_level";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "set_transport";
      id?: string | null;
      activeSessionId: string;
      transport: Transport;
      [k: string]: unknown;
    }
  | {
      type: "set_steering_mode";
      id?: string | null;
      activeSessionId: string;
      mode: unknown;
      [k: string]: unknown;
    }
  | {
      type: "set_follow_up_mode";
      id?: string | null;
      activeSessionId: string;
      mode: unknown;
      [k: string]: unknown;
    }
  | {
      type: "set_auto_compaction";
      id?: string | null;
      activeSessionId: string;
      enabled: boolean;
      [k: string]: unknown;
    }
  | {
      type: "set_auto_retry";
      id?: string | null;
      activeSessionId: string;
      enabled: boolean;
      [k: string]: unknown;
    }
  | {
      type: "compact";
      id?: string | null;
      activeSessionId: string;
      customInstructions?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "abort_compaction";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "abort_branch_summary";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "abort_retry";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "execute_bash_and_wait";
      id?: string | null;
      activeSessionId: string;
      command: string;
      [k: string]: unknown;
    }
  | {
      type: "reload";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "new_session";
      id?: string | null;
      activeSessionId: string;
      parentSession?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "switch_session";
      id?: string | null;
      activeSessionId: string;
      sessionPath: string;
      cwdOverride?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "fork";
      id?: string | null;
      activeSessionId: string;
      entryId: string;
      position?: ForkPosition | null;
      [k: string]: unknown;
    }
  | {
      type: "navigate_tree";
      id?: string | null;
      activeSessionId: string;
      targetId: string;
      summarize?: boolean | null;
      customInstructions?: string | null;
      replaceInstructions?: boolean | null;
      label?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "import_jsonl";
      id?: string | null;
      activeSessionId: string;
      inputPath: string;
      cwdOverride?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "export_html";
      id?: string | null;
      activeSessionId: string;
      outputPath?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "export_jsonl";
      id?: string | null;
      activeSessionId: string;
      outputPath?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "set_session_name";
      id?: string | null;
      activeSessionId: string;
      name: string;
      workerToken?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "get_rlm_max_depth_status";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "set_rlm_max_depth";
      id?: string | null;
      activeSessionId: string;
      maxDepth: number;
      global?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "rename_saved_session";
      id?: string | null;
      activeSessionId?: string | null;
      sessionPath: string;
      name: string;
      [k: string]: unknown;
    }
  | {
      type: "delete_saved_session";
      id?: string | null;
      activeSessionId?: string | null;
      sessionPath: string;
      [k: string]: unknown;
    }
  | {
      type: "get_session_context";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_session_tree";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_user_messages_for_forking";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_last_assistant_text";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_system_prompt";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "get_tool_definition";
      id?: string | null;
      activeSessionId: string;
      name: string;
      [k: string]: unknown;
    }
  | {
      type: "set_session_entry_label";
      id?: string | null;
      activeSessionId: string;
      entryId: string;
      label?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "mark_anthropic_warning_shown";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "set_prompt_guidelines";
      id?: string | null;
      activeSessionId: string;
      appendSystemPrompt: string[];
      [k: string]: unknown;
    }
  | {
      type: "ack_result";
      id?: string | null;
      commandId: string;
      [k: string]: unknown;
    }
  | {
      type: "prepare_update_restart";
      id?: string | null;
      updateId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "commit_update_restart";
      id?: string | null;
      updateId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "retry_worker";
      id?: string | null;
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      type: "update_restore_status";
      id?: string | null;
      updateId?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "restart";
      id?: string | null;
      [k: string]: unknown;
    }
  | {
      type: "shutdown";
      id?: string | null;
      force?: boolean | null;
      [k: string]: unknown;
    }
  | {
      type: "worker_register";
      id?: string | null;
      activeSessionId: string;
      sessionId?: string | null;
      socketPath: string;
      workerInstanceId: string;
      token: string;
      pid: number;
      [k: string]: unknown;
    };
export type HarnessQueueMode = "all" | "one-at-a-time";
export type DaemonSessionLifecycle = "resident" | "client_owned";
export type StreamingBehavior = "steer" | "followUp";
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
export type CycleDirection = "forward" | "backward";
/**
 * TS `ServiceTier = "auto" | "default" | "flex" | "scale" | "priority" | null`.
 * The wire value can be absent or an explicit null, both mapping to `None`.
 */
export type ServiceTier = "auto" | "default" | "flex" | "scale" | "priority";
export type Transport = "sse" | "websocket" | "websocket-cached" | "auto";
export type ForkPosition = "before" | "at";

/**
 * `type: "command"` envelope wrapping a [`DaemonCommand`].
 */
export interface DaemonCommandEnvelope {
  type: DaemonCommandFrameType;
  id: string;
  protocol: DaemonProtocolInfo;
  clientId?: string | null;
  command: DaemonCommand;
  [k: string]: unknown;
}
export interface DaemonProtocolInfo {
  name: string;
  version: number;
  [k: string]: unknown;
}
/**
 * Omitted fields stay unchanged. Explicit null is not a reset operation.
 */
export interface GlobalHarnessDefaultsPatch {
  autoCompactionEnabled?: boolean;
  steeringMode?: HarnessQueueMode;
  followUpMode?: HarnessQueueMode;
  maxDepth?: number;
}
/**
 * Resume cursor accepted on attach. The TS wire shape is a union (`DaemonEventCursor`, or a bare
 * `eventSequence`, each with optional `activeSessionId`); this optional-field struct accepts both
 * forms and serializes each back to its original shape.
 */
export interface DaemonResumeCursor {
  activeSessionId?: string | null;
  generation?: string | null;
  sequence?: number | null;
  eventSequence?: number | null;
  [k: string]: unknown;
}
export interface FocusedThreadContextRow {
  id: string;
  role: string;
  text: string;
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

