/* eslint-disable */
/**
 * Generated from client-inbound-frame.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with `npm run gen:ts` and commit.
 */
/**
 * Open receive envelope: every supervisor line is an object with a string `type`. Unknown kinds pass through raw; known kinds are additionally shape-validated against daemon-outbound.
 */
export interface ClientInboundFrame {
  type: string;
  [k: string]: unknown;
}

