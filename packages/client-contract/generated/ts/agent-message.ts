/* eslint-disable */
/**
 * Generated from agent-message.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with `npm run gen:ts` and commit.
 */
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

