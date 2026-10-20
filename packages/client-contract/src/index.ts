/**
 * The Prime Agent daemon client contract (generated surface).
 *
 * Types come from generated/ts (derived from the pa-types wire graph);
 * validators are Ajv compiled from the same canonical schemas; metadata
 * carries the derived command/error/capability catalogs.
 *
 * Non-goals (see README): transport, lifecycle, reconnect policy, platform
 * packaging. This package is unused until client adoption.
 */
export {
  loadContract,
  loadMetadata,
  checkInboundFrame,
  type Contract,
  type SchemaValidator,
  type FrameVerdict,
} from "./validators.js";
export type * from "../generated/ts/index.js";
