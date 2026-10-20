/* eslint-disable */
/**
 * Generated from daemon-resume-cursor.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with `npm run gen:ts` and commit.
 */
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

