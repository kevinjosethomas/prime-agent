/* eslint-disable */
/**
 * Generated from daemon-event-meta.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with `npm run gen:ts` and commit.
 */
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
export interface DaemonProtocolInfo {
  name: string;
  version: number;
  [k: string]: unknown;
}
export interface DaemonEventCursor {
  generation: string;
  sequence: number;
  [k: string]: unknown;
}

